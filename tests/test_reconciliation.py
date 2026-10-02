import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine, angular_separation_deg, within_reconciliation
from src.service import DomainService


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "recon.db"),
            RuleEngine(),
        )
        self.analyst = Actor("analyst-1", "analyst")
        self.supervisor = Actor("supervisor-1", "supervisor")
        self.viewer = Actor("viewer-1", "viewer")
        self.admin = Actor("admin-1", "admin")
        self.src1 = self.service.create(
            self.analyst, "source", {"name": "Survey A", "survey_name": "A"}
        )
        self.src2 = self.service.create(
            self.analyst, "source", {"name": "Survey B", "survey_name": "B"}
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _candidate(self, source, event_id, ra, dec, magnitude, observed_at, **extra):
        data = {
            "source_id": source["id"],
            "event_id": event_id,
            "ra": ra,
            "dec": dec,
            "magnitude": magnitude,
            "transient_type": extra.get("transient_type", "unknown"),
            "observed_at": observed_at,
        }
        return self.service.create(self.analyst, "candidate", data, **extra)

    def _group(self, candidate):
        return self.service.get(candidate["data"]["group_id"])

    # ------------------------------------------------------------ matching

    def test_angular_separation_and_time_window(self):
        # 2 arcmin = 2/60 degree.
        self.assertLessEqual(angular_separation_deg(10.0, 20.0, 10.0, 20.0), 2 / 60.0)
        self.assertLessEqual(
            angular_separation_deg(10.0, 20.0, 10.0005, 20.0005), 2 / 60.0
        )
        # RA wrap-around: 359.9 and 0.1 are 0.2 deg apart -> still within 2 arcmin? No, 0.2 deg > 2 arcmin.
        self.assertGreater(angular_separation_deg(359.9, 0.0, 0.1, 0.0), 2 / 60.0)
        # 1 day boundary: exactly 1 day apart matches, 1 day + 1 second does not.
        self.assertTrue(
            within_reconciliation(
                10, 20, "2026-09-27T00:00:00Z",
                10, 20, "2026-09-28T00:00:00Z",
            )
        )
        self.assertFalse(
            within_reconciliation(
                10, 20, "2026-09-27T00:00:00Z",
                10, 20, "2026-09-28T00:00:01Z",
            )
        )

    def test_cross_source_grouping_preserves_original_reports(self):
        first = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        second = self._candidate(self.src2, "AT-2", 10.0005, 20.0005, 17.0, "2026-09-27T05:00:00Z")
        group = self._group(first)
        self.assertEqual(group["id"], self._group(second)["id"])
        # Original source numbers and measurements are preserved.
        self.assertIn(first["id"], group["data"]["member_ids"])
        self.assertIn(second["id"], group["data"]["member_ids"])
        self.assertEqual(first["data"]["event_id"], "AT-1")
        self.assertEqual(second["data"]["event_id"], "AT-2")
        self.assertEqual(len(first["data"]["measurements"]), 1)
        # Brightest report is the primary.
        self.assertEqual(group["data"]["primary_candidate_id"], second["id"])

    def test_far_apart_reports_form_separate_groups(self):
        first = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        far = self._candidate(self.src2, "AT-3", 50.0, 20.0, 19.0, "2026-09-27T06:00:00Z")
        self.assertNotEqual(self._group(first)["id"], self._group(far)["id"])

    def test_same_source_event_number_journaled_once(self):
        self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        with self.assertRaises(ConflictError):
            self._candidate(self.src1, "AT-1", 11.0, 21.0, 17.0, "2026-09-27T00:01:00Z")

    def test_retry_by_report_number_is_idempotent(self):
        first = self._candidate(
            self.src1, "AT-4", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z",
            idempotency_key="report-42",
        )
        retry = self._candidate(
            self.src1, "AT-4", 99.0, 99.0, 99.0, "2026-09-27T00:00:00Z",
            idempotency_key="report-42",
        )
        self.assertEqual(first["id"], retry["id"])
        self.assertEqual(first["data"]["group_id"], retry["data"]["group_id"])
        self.assertEqual(len(self.service.list("candidate")), 1)

    # ------------------------------------------------------------ lifecycle

    def test_primary_reidentified_when_data_changes(self):
        first = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        second = self._candidate(self.src2, "AT-2", 10.0005, 20.0005, 17.0, "2026-09-27T05:00:00Z")
        group = self._group(first)
        self.assertEqual(group["data"]["primary_candidate_id"], second["id"])
        # A brighter measurement on the first report makes it the new primary.
        self.service.transition(
            self.analyst, first["id"], "merge_measurement",
            {"measurement": {"observed_at": "2026-09-27T08:00:00Z", "ra": 10.0, "dec": 20.0, "magnitude": 14.0}},
        )
        group = self.service.get(group["id"])
        self.assertEqual(group["data"]["primary_candidate_id"], first["id"])

    def test_single_member_group_voided_on_withdraw(self):
        candidate = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        group = self._group(candidate)
        candidate = self.service.transition(
            self.analyst, candidate["id"], "withdraw", {"reason": "false positive"}
        )
        group = self.service.get(group["id"])
        self.assertEqual(group["status"], "void")
        self.assertEqual(candidate["status"], "void")

    def test_withdraw_keeps_group_when_other_valid_reports_exist(self):
        first = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        second = self._candidate(self.src2, "AT-2", 10.0005, 20.0005, 17.0, "2026-09-27T05:00:00Z")
        group = self._group(first)
        self.service.transition(
            self.analyst, first["id"], "withdraw", {"reason": "false positive"}
        )
        group = self.service.get(group["id"])
        self.assertNotEqual(group["status"], "void")
        # The withdrawn report is no longer the primary; the valid report is.
        self.assertEqual(group["data"]["primary_candidate_id"], second["id"])
        # The withdrawn candidate's status reflects the withdrawal.
        first = self.service.get(first["id"])
        self.assertEqual(first["status"], "withdrawn")

    # ------------------------------------------------------------ permissions

    def test_unauthorized_confirm_is_denied_and_audited(self):
        candidate = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        group = self._group(candidate)
        with self.assertRaises(PermissionDenied):
            self.service.transition(self.viewer, group["id"], "confirm", {})
        audits = self.service.audit_log(group["id"])
        self.assertTrue(any(a["action"] == "confirm_denied" for a in audits))

    def test_unauthorized_split_is_denied_and_audited(self):
        first = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        second = self._candidate(self.src2, "AT-2", 10.0005, 20.0005, 17.0, "2026-09-27T05:00:00Z")
        group = self._group(first)
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                self.viewer, group["id"], "split", {"candidate_id": second["id"]}
            )
        audits = self.service.audit_log(group["id"])
        self.assertTrue(any(a["action"] == "split_denied" for a in audits))

    def test_split_moves_member_to_its_own_group(self):
        first = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        second = self._candidate(self.src2, "AT-2", 10.0005, 20.0005, 17.0, "2026-09-27T05:00:00Z")
        group = self._group(first)
        self.service.transition(
            self.supervisor, group["id"], "split", {"candidate_id": second["id"]}
        )
        group = self.service.get(group["id"])
        second = self.service.get(second["id"])
        self.assertNotIn(second["id"], group["data"]["member_ids"])
        self.assertEqual(len(group["data"]["member_ids"]), 1)
        new_group = self.service.get(second["data"]["group_id"])
        self.assertEqual(new_group["data"]["primary_candidate_id"], second["id"])
        self.assertEqual(new_group["data"]["member_ids"], [second["id"]])

    def test_concurrent_split_one_wins_other_conflicts(self):
        first = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        second = self._candidate(self.src2, "AT-2", 10.0005, 20.0005, 17.0, "2026-09-27T05:00:00Z")
        group = self._group(first)
        version = group["version"]
        results = []
        barrier = threading.Barrier(2)

        def do_split(actor):
            barrier.wait()
            try:
                self.service.transition(
                    actor, group["id"], "split",
                    {"candidate_id": second["id"]}, expected_version=version,
                )
                results.append("ok")
            except ConflictError:
                results.append("conflict")

        t1 = threading.Thread(target=do_split, args=(self.supervisor,))
        t2 = threading.Thread(target=do_split, args=(Actor("supervisor-2", "supervisor"),))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertIn("ok", results)
        self.assertIn("conflict", results)
        # The loser's transaction rolled back: the group is intact and audited both ways.
        group = self.service.get(group["id"])
        self.assertIsNotNone(group)
        audits = self.service.audit_log(group["id"])
        self.assertTrue(any(a["action"] == "split" for a in audits))
        self.assertTrue(any(a["action"] == "split_denied" for a in audits))

    # ------------------------------------------------------------ resilience

    def test_no_half_group_after_failure_and_retry(self):
        # Force a failure after the candidate/group writes but before commit.
        first = self._candidate(self.src1, "AT-1", 10.0, 20.0, 18.0, "2026-09-27T00:00:00Z")
        group = self._group(first)
        original_save = self.service.repository.save_idempotency

        def failing_save(*args, **kwargs):
            raise RuntimeError("simulated write failure")

        self.service.repository.save_idempotency = failing_save
        with self.assertRaises(RuntimeError):
            self._candidate(
                self.src1, "AT-2", 10.0005, 20.0005, 17.0, "2026-09-27T05:00:00Z",
                idempotency_key="report-fail",
            )
        self.service.repository.save_idempotency = original_save
        # No half-group: the failed attempt left no new candidate or group.
        self.assertEqual(len(self.service.list("candidate")), 1)
        self.assertEqual(len(self.service.list("event_group")), 1)
        # Retrying by report number succeeds and joins the existing group.
        retry = self._candidate(
            self.src1, "AT-2", 10.0005, 20.0005, 17.0, "2026-09-27T05:00:00Z",
            idempotency_key="report-fail",
        )
        self.assertEqual(len(self.service.list("candidate")), 2)
        self.assertEqual(self._group(retry)["id"], group["id"])

    def test_backfill_is_resumable(self):
        # Seed historical candidates directly (no group_id).
        for i in range(3):
            self.service.repository.create_entity(
                f"hist-{i}", "candidate", "detected",
                {"source_id": "src-x", "event_id": f"H-{i}", "ra": 10.0, "dec": 20.0,
                 "magnitude": 18.0, "transient_type": "unknown",
                 "observed_at": "2026-09-27T00:00:00Z", "measurements": []},
                "old-user",
            )
        result = self.service.backfill_groups(self.admin)
        self.assertEqual(result["created"], 3)
        self.assertEqual(result["skipped"], 0)
        for i in range(3):
            candidate = self.service.get(f"hist-{i}")
            group = self.service.get(candidate["data"]["group_id"])
            self.assertEqual(group["data"]["member_ids"], [f"hist-{i}"])
        # Re-running skips everything.
        result = self.service.backfill_groups(self.admin)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["skipped"], 3)
        # Interruption + resume: add two more, then backfill.
        for i in range(3, 5):
            self.service.repository.create_entity(
                f"hist-{i}", "candidate", "detected",
                {"source_id": "src-x", "event_id": f"H-{i}", "ra": 10.0, "dec": 20.0,
                 "magnitude": 18.0, "transient_type": "unknown",
                 "observed_at": "2026-09-27T00:00:00Z", "measurements": []},
                "old-user",
            )
        result = self.service.backfill_groups(self.admin)
        self.assertEqual(result["created"], 2)
        self.assertEqual(result["skipped"], 3)
        self.assertEqual(len(self.service.list("event_group")), 5)

    def test_backfill_requires_admin(self):
        with self.assertRaises(PermissionDenied):
            self.service.backfill_groups(self.analyst)


if __name__ == "__main__":
    unittest.main()
