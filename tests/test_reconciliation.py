import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from src.domain import Actor, ConflictError, PermissionDenied
from src.matching import angular_separation_deg, parse_utc, reports_match
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "reconciliation.db"
        self.service = DomainService(SQLiteRepository(self.db_path), RuleEngine())
        self.analyst = Actor("analyst-1", "analyst")
        self.coordinator = Actor("coordinator-1", "coordinator")

    def tearDown(self):
        self.tmp.cleanup()

    def _source(self, name):
        return self.service.create(
            self.analyst,
            "source",
            {"name": name, "survey_name": name},
        )["id"]

    def _report(self, source_id, event_id, report_id=None, **overrides):
        payload = {
            "source_id": source_id,
            "event_id": event_id,
            "ra": 10.0,
            "dec": 0.0,
            "magnitude": 18.0,
            "transient_type": "unknown",
            "observed_at": "2026-09-27T00:00:00Z",
        }
        payload.update(overrides)
        return self.service.create(
            self.analyst,
            "candidate",
            payload,
            idempotency_key=report_id,
        )

    def test_matching_boundaries_require_cross_source_position_and_time(self):
        first = {"source_id": "a", "ra": 10.0, "dec": 0.0, "observed_at": "2026-09-27T00:00:00Z"}
        exact = dict(first, source_id="b", ra=10.0 + 2.0 / 60.0)
        exact_time = dict(first, source_id="b", observed_at="2026-09-28T00:00:00Z")
        too_far = dict(first, source_id="b", ra=10.0 + 2.01 / 60.0)
        too_late = dict(first, source_id="b", observed_at="2026-09-28T00:00:01Z")
        same_source = dict(first, ra=10.0 + 2.0 / 60.0)
        self.assertTrue(reports_match(first, exact))
        self.assertTrue(reports_match(first, exact_time))
        self.assertFalse(reports_match(first, too_far))
        self.assertFalse(reports_match(first, too_late))
        self.assertFalse(reports_match(first, same_source))
        self.assertAlmostEqual(angular_separation_deg(10, 0, 11, 0), 1.0)
        self.assertEqual(parse_utc("2026-09-27T00:00:00Z").utcoffset().total_seconds(), 0)

    def test_same_source_event_is_once_but_cross_sources_form_one_group(self):
        source_a = self._source("AA")
        source_b = self._source("BB")
        first = self._report(source_a, "SRC-E1", report_id="report-1")
        retry = self._report(source_a, "SRC-E1", report_id="report-1")
        self.assertEqual(first["id"], retry["id"])
        with self.assertRaises(ConflictError):
            self._report(source_a, "SRC-E1", report_id="report-duplicate")

        second = self._report(
            source_b,
            "FOREIGN-9",
            report_id="report-2",
            ra=10.0 + 1.0 / 60.0,
            magnitude=17.0,
        )
        self.assertEqual(
            first["data"]["event_group_id"],
            second["data"]["event_group_id"],
        )
        group = self.service.get(first["data"]["event_group_id"])
        self.assertEqual(group["status"], "provisional")
        self.assertEqual(group["data"]["member_count"], 2)
        self.assertEqual(group["data"]["primary_candidate_id"], second["id"])
        self.assertEqual(first["data"]["event_id"], "SRC-E1")
        self.assertEqual(second["data"]["event_id"], "FOREIGN-9")
        self.assertEqual(first["data"]["measurements"], first["data"]["measurements"])

    def test_source_data_changes_reselect_primary_and_last_report_voids_group(self):
        source_a = self._source("AA")
        source_b = self._source("BB")
        faint = self._report(source_a, "E1", report_id="r1", magnitude=19)
        bright = self._report(source_b, "E2", report_id="r2", magnitude=17)
        group_id = faint["data"]["event_group_id"]
        self.assertEqual(self.service.get(group_id)["data"]["primary_candidate_id"], bright["id"])

        faint = self.service.transition(
            self.analyst,
            faint["id"],
            "correct",
            {"reason": "calibration", "magnitude": 15.0},
        )
        self.assertEqual(self.service.get(group_id)["data"]["primary_candidate_id"], faint["id"])

        bright = self.service.transition(
            self.analyst, bright["id"], "withdraw", {"reason": "artifact"}
        )
        self.assertEqual(bright["status"], "withdrawn")
        self.assertEqual(self.service.get(group_id)["status"], "provisional")
        self.assertEqual(self.service.get(group_id)["data"]["candidate_ids"], [faint["id"]])

        faint = self.service.transition(
            self.analyst, faint["id"], "withdraw", {"reason": "artifact"}
        )
        group = self.service.get(group_id)
        self.assertEqual(group["status"], "void")
        self.assertEqual(group["data"]["candidate_ids"], [])
        self.assertIsNone(group["data"]["primary_candidate_id"])

    def test_concurrent_split_has_one_winner_and_loser_keeps_group(self):
        sources = [self._source(name) for name in ("AA", "BB", "CC")]
        candidates = [
            self._report(source, "E-%d" % index, report_id="r-%d" % index, ra=10.0 + index * 0.001)
            for index, source in enumerate(sources)
        ]
        group_id = candidates[0]["data"]["event_group_id"]
        barrier = threading.Barrier(2)
        results = []

        def split(candidate_id):
            try:
                barrier.wait(2)
                self.service.transition(
                    self.coordinator,
                    group_id,
                    "split",
                    {"candidate_ids": [candidate_id], "reason": "different host"},
                )
                results.append("ok")
            except ConflictError:
                results.append("conflict")

        threads = [
            threading.Thread(target=split, args=(candidates[1]["id"],)),
            threading.Thread(target=split, args=(candidates[2]["id"],)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(results), ["conflict", "ok"])
        groups = self.service.list("event_group")
        self.assertEqual(len(groups), 2)
        original = self.service.get(group_id)
        self.assertEqual(original["data"]["member_count"], 2)
        self.assertTrue(
            any(action == "split_conflict_denied" for action in (item["action"] for item in self.service.audit_log(group_id)))
        )

    def test_unauthorized_confirm_or_split_is_rejected_and_audited(self):
        source_a = self._source("AA")
        source_b = self._source("BB")
        first = self._report(source_a, "E1", report_id="r1")
        self._report(source_b, "E2", report_id="r2")
        group_id = first["data"]["event_group_id"]
        viewer = Actor("viewer-1", "viewer")
        with self.assertRaises(PermissionDenied):
            self.service.transition(viewer, group_id, "confirm", {})
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                viewer,
                group_id,
                "split",
                {"candidate_ids": [first["id"]], "reason": "bad"},
            )
        actions = {entry["action"] for entry in self.service.audit_log(group_id)}
        self.assertIn("confirm_denied", actions)
        self.assertIn("split_denied", actions)

    def test_failed_write_leaves_no_partial_group_and_report_id_can_retry(self):
        source_id = self._source("AA")
        with patch.object(SQLiteRepository, "_insert_group", side_effect=RuntimeError("disk full")):
            with self.assertRaises(RuntimeError):
                self._report(source_id, "E1", report_id="durable-report")
        self.assertEqual(self.service.list("candidate"), [])
        self.assertEqual(self.service.list("event_group"), [])
        candidate = self._report(source_id, "E1", report_id="durable-report")
        self.assertTrue(candidate["data"]["event_group_id"])
        self.assertEqual(len(self.service.list("event_group")), 1)

    def test_historical_groups_are_backfilled_as_singleton_groups_and_resume(self):
        legacy_path = Path(self.tmp.name) / "legacy.db"
        created_at = "2026-01-01T00:00:00+00:00"
        with sqlite3.connect(legacy_path) as connection:
            connection.executescript(
                """
                CREATE TABLE entities (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
                    version INTEGER NOT NULL, data TEXT NOT NULL, created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL, actor_role TEXT NOT NULL, action TEXT NOT NULL,
                    from_status TEXT, to_status TEXT NOT NULL, detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE idempotency (
                    actor_id TEXT NOT NULL, idem_key TEXT NOT NULL, entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL, PRIMARY KEY(actor_id, idem_key)
                );
                """
            )
            for index in range(3):
                connection.execute(
                    "INSERT INTO entities VALUES (?, 'candidate', 'detected', 1, ?, 'old', ?, ?)",
                    (
                        "legacy-%d" % index,
                        '{"source_id":"old-source","event_id":"old-%d","ra":1,"dec":2,'
                        '"magnitude":18,"observed_at":"2026-01-01T00:00:00Z"}' % index,
                        created_at,
                        created_at,
                    ),
                )
        with patch.object(SQLiteRepository, "backfill_event_groups", lambda self, batch_size=100: 0):
            repository = SQLiteRepository(legacy_path)
        self.assertEqual(repository.backfill_event_group_batch(limit=1), 1)
        self.assertEqual(repository.backfill_event_group_batch(limit=1), 1)
        self.assertEqual(repository.backfill_event_group_batch(limit=10), 1)
        self.assertEqual(repository.backfill_event_group_batch(limit=10), 0)
        groups = repository.list_entities(kind="event_group")
        self.assertEqual(len(groups), 3)
        self.assertTrue(all(group["data"].get("single_source") for group in groups))
        candidate = repository.get_entity("legacy-0")
        self.assertEqual(
            repository.get_entity(candidate["data"]["event_group_id"])["data"]["candidate_ids"],
            ["legacy-0"],
        )


if __name__ == "__main__":
    unittest.main()
