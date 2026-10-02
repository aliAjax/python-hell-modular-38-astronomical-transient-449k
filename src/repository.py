import json
import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

from .domain import ConflictError, NotFoundError
from .matching import reports_match


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_id():
    return str(uuid4())


class SQLiteRepository:
    SCHEMA_VERSION = 1

    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS event_reports (
                    report_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL UNIQUE,
                    source_id TEXT NOT NULL,
                    source_event_id TEXT NOT NULL,
                    ra REAL NOT NULL,
                    dec REAL NOT NULL,
                    observed_at TEXT NOT NULL,
                    magnitude REAL NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_active_source_event
                    ON event_reports(source_id, source_event_id)
                    WHERE active = 1;
                CREATE INDEX IF NOT EXISTS idx_event_reports_match
                    ON event_reports(active, observed_at);
                CREATE TABLE IF NOT EXISTS event_group_members (
                    group_id TEXT NOT NULL,
                    candidate_id TEXT NOT NULL PRIMARY KEY,
                    active INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_event_group_members_group
                    ON event_group_members(group_id, active);
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES('schema_version', ?)",
                (str(self.SCHEMA_VERSION),),
            )
        self.backfill_event_groups(batch_size=100)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _dump(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _report_payload(candidate_id, data, active=True, now=None):
        return {
            "report_id": data.get("report_id") or candidate_id,
            "candidate_id": candidate_id,
            "source_id": data["source_id"],
            "source_event_id": data["event_id"],
            "ra": float(data["ra"]),
            "dec": float(data["dec"]),
            "observed_at": data["observed_at"],
            "magnitude": float(data["magnitude"]),
            "active": 1 if active else 0,
            "now": now or utcnow(),
        }

    @staticmethod
    def _append_audit(
        connection,
        entity_id,
        actor_id,
        actor_role,
        action,
        from_status,
        to_status,
        detail,
        created_at=None,
    ):
        connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, "
            "to_status, detail, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id,
                actor_id,
                actor_role,
                action,
                from_status,
                to_status,
                json.dumps(detail or {}, ensure_ascii=False, sort_keys=True),
                created_at or utcnow(),
            ),
        )

    @staticmethod
    def _group_status_rank(status):
        return {"confirmed": 2, "provisional": 1, "void": 0}.get(status, 0)

    @staticmethod
    def _group_payload(member_rows, candidates_by_id):
        members = [
            candidate_id
            for candidate_id in (row["candidate_id"] for row in member_rows)
            if candidates_by_id.get(candidate_id, {}).get("status") != "withdrawn"
        ]
        members.sort()
        primary = None
        if members:
            def primary_key(candidate_id):
                candidate = candidates_by_id[candidate_id]
                return (
                    -float(candidate["data"].get("priority_score", 0.0)),
                    str(candidate["data"].get("observed_at", "")),
                    candidate_id,
                )

            primary = min(members, key=primary_key)
        has_members = bool(members)
        return {
            "member_count": len(members),
            "candidate_ids": members,
            "primary_candidate_id": primary,
            "active": has_members,
        }, has_members

    def _insert_group(self, connection, group_id, member_rows, candidates_by_id, status, actor_id, now):
        data, active = self._group_payload(member_rows, candidates_by_id)
        if status is None:
            status = "provisional" if active else "void"
        connection.execute(
            "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
            "VALUES (?, 'event_group', ?, 1, ?, ?, ?, ?)",
            (group_id, status, self._dump(data), actor_id, now, now),
        )
        return status, data

    def _refresh_group(self, connection, group_id, candidates_by_id, now, force_void=False):
        row = connection.execute(
            "SELECT id, status, version, data FROM entities WHERE id = ? AND kind = 'event_group'",
            (group_id,),
        ).fetchone()
        if not row:
            raise NotFoundError("event group not found: " + group_id)
        member_rows = connection.execute(
            "SELECT candidate_id FROM event_group_members WHERE group_id = ? AND active = 1 ORDER BY candidate_id",
            (group_id,),
        ).fetchall()
        data, active = self._group_payload(member_rows, candidates_by_id)
        old_status = row["status"]
        next_status = "void" if force_void or not active else old_status
        if next_status == "void":
            data["active"] = False
        connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? WHERE id = ?",
            (next_status, self._dump(data), now, group_id),
        )
        return old_status, next_status, data

    def _active_candidate_rows(self, connection):
        rows = connection.execute(
            """
            SELECT e.*, r.report_id, r.source_id AS report_source_id,
                   r.source_event_id, r.ra AS report_ra, r.dec AS report_dec,
                   r.observed_at AS report_observed_at, r.magnitude AS report_magnitude
            FROM event_reports r
            JOIN entities e ON e.id = r.candidate_id
            WHERE r.active = 1 AND e.status != 'withdrawn'
            """
        ).fetchall()
        return rows

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def ingest_candidate(self, entity_id, status, data, actor_id, idempotency_key=None, actor_role=""):
        """Create a report and reconcile its event group in one SQLite transaction."""
        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                row = connection.execute(
                    "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                    (actor_id, idempotency_key),
                ).fetchone()
                if row:
                    existing = connection.execute(
                        "SELECT * FROM entities WHERE id = ?", (row["entity_id"],)
                    ).fetchone()
                    if existing:
                        connection.commit()
                        return self._entity_from_row(existing)

            existing_report = connection.execute(
                "SELECT candidate_id FROM event_reports WHERE report_id = ?",
                (data.get("report_id"),),
            ).fetchone()
            if existing_report:
                row = connection.execute(
                    "SELECT * FROM entities WHERE id = ?", (existing_report["candidate_id"],)
                ).fetchone()
                connection.commit()
                return self._entity_from_row(row)

            duplicate = connection.execute(
                """
                SELECT r.candidate_id
                FROM event_reports r
                JOIN entities e ON e.id = r.candidate_id
                WHERE r.source_id = ? AND r.source_event_id = ? AND r.active = 1
                  AND e.status != 'withdrawn'
                """,
                (data["source_id"], data["event_id"]),
            ).fetchone()
            if duplicate:
                raise ConflictError(
                    "candidate already exists for event key %s:%s"
                    % (data["source_id"], data["event_id"])
                )
            if connection.execute("SELECT 1 FROM entities WHERE id = ?", (entity_id,)).fetchone():
                raise ConflictError("entity already exists: " + entity_id)

            payload = dict(data)
            payload["report_id"] = data.get("report_id") or entity_id
            payload["priority_score"] = data.get("priority_score")
            payload["event_key"] = data.get("event_key") or "%s:%s" % (
                data["source_id"],
                data["event_id"],
            )
            payload["measurements"] = [
                {
                    "observed_at": data["observed_at"],
                    "ra": float(data["ra"]),
                    "dec": float(data["dec"]),
                    "magnitude": float(data["magnitude"]),
                }
            ]

            active_rows = self._active_candidate_rows(connection)
            candidates = {row["id"]: self._entity_from_row(row) for row in active_rows}
            incoming = {
                "source_id": data["source_id"],
                "ra": float(data["ra"]),
                "dec": float(data["dec"]),
                "observed_at": data["observed_at"],
            }
            matched_groups = []
            matched_candidate_ids = []
            for row in active_rows:
                candidate = candidates[row["id"]]
                other = {
                    "source_id": row["report_source_id"],
                    "ra": row["report_ra"],
                    "dec": row["report_dec"],
                    "observed_at": row["report_observed_at"],
                }
                if reports_match(incoming, other):
                    membership = connection.execute(
                        "SELECT group_id FROM event_group_members WHERE candidate_id = ? AND active = 1",
                        (row["id"],),
                    ).fetchone()
                    matched_candidate_ids.append(row["id"])
                    if membership and membership["group_id"] not in matched_groups:
                        matched_groups.append(membership["group_id"])

            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, 'candidate', ?, 1, ?, ?, ?, ?)",
                (entity_id, status, self._dump(payload), actor_id, now, now),
            )
            report = self._report_payload(entity_id, payload, True, now)
            connection.execute(
                """
                INSERT INTO event_reports(report_id, candidate_id, source_id, source_event_id,
                                          ra, dec, observed_at, magnitude, active, created_at, updated_at)
                VALUES (:report_id, :candidate_id, :source_id, :source_event_id, :ra, :dec,
                        :observed_at, :magnitude, :active, :now, :now)
                """,
                report,
            )

            if not matched_groups:
                group_id = _new_id()
                payload["event_group_id"] = group_id
                connection.execute(
                    "UPDATE entities SET data = ? WHERE id = ?", (self._dump(payload), entity_id)
                )
                connection.execute(
                    "INSERT INTO event_group_members(group_id, candidate_id, active) VALUES (?, ?, 1)",
                    (group_id, entity_id),
                )
                group_status, group_data = self._insert_group(
                    connection,
                    group_id,
                    [{"candidate_id": entity_id}],
                    {entity_id: self._entity_from_row(
                        connection.execute("SELECT * FROM entities WHERE id = ?", (entity_id,)).fetchone()
                    )},
                    "provisional",
                    actor_id,
                    now,
                )
                self._append_audit(
                    connection, entity_id, actor_id, actor_role, "create", None, status,
                    {"kind": "candidate", "event_group_id": group_id}, now,
                )
                self._append_audit(
                    connection, group_id, actor_id, actor_role, "create", None, group_status,
                    {"reason": "auto_reconcile", "members": group_data["candidate_ids"]}, now,
                )
            else:
                target_id = self._select_target_group(connection, matched_groups)
                merged_from = [group_id for group_id in matched_groups if group_id != target_id]
                confirmed_merge = any(
                    connection.execute(
                        "SELECT status FROM entities WHERE id = ?", (group_id,)
                    ).fetchone()["status"]
                    == "confirmed"
                    for group_id in matched_groups
                )

                for old_group in matched_groups:
                    members = connection.execute(
                        "SELECT candidate_id FROM event_group_members WHERE group_id = ? AND active = 1",
                        (old_group,),
                    ).fetchall()
                    if old_group == target_id:
                        continue
                    for member in members:
                        candidate_id = member["candidate_id"]
                        connection.execute(
                            "UPDATE event_group_members SET group_id = ? WHERE candidate_id = ?",
                            (target_id, candidate_id),
                        )
                        candidate = self._entity_from_row(
                            connection.execute(
                                "SELECT * FROM entities WHERE id = ?", (candidate_id,)
                            ).fetchone()
                        )
                        candidate["data"]["event_group_id"] = target_id
                        connection.execute(
                            "UPDATE entities SET data = ?, version = version + 1, updated_at = ? WHERE id = ?",
                            (self._dump(candidate["data"]), now, candidate_id),
                        )
                        candidates[candidate_id] = candidate
                    connection.execute(
                        "UPDATE entities SET status = 'void', version = version + 1, data = ?, updated_at = ? WHERE id = ?",
                        (
                            self._dump(
                                {
                                    **self._entity_from_row(
                                        connection.execute(
                                            "SELECT * FROM entities WHERE id = ?", (old_group,)
                                        ).fetchone()
                                    )["data"],
                                    "active": False,
                                    "merged_into": target_id,
                                }
                            ),
                            now,
                            old_group,
                        ),
                    )
                    self._append_audit(
                        connection, old_group, actor_id, actor_role, "merge", None, "void",
                        {"merged_into": target_id, "reason": "matching_report"}, now,
                    )

                payload["event_group_id"] = target_id
                connection.execute(
                    "UPDATE entities SET data = ? WHERE id = ?", (self._dump(payload), entity_id)
                )
                connection.execute(
                    "INSERT INTO event_group_members(group_id, candidate_id, active) VALUES (?, ?, 1)",
                    (target_id, entity_id),
                )
                candidate_row = connection.execute(
                    "SELECT * FROM entities WHERE id = ?", (entity_id,)
                ).fetchone()
                candidates[entity_id] = self._entity_from_row(candidate_row)
                old_status, next_status, group_data = self._refresh_group(
                    connection, target_id, candidates, now
                )
                if confirmed_merge:
                    next_status = "confirmed"
                    connection.execute(
                        "UPDATE entities SET status = 'confirmed' WHERE id = ?", (target_id,)
                    )
                self._append_audit(
                    connection, entity_id, actor_id, actor_role, "create", None, status,
                    {"kind": "candidate", "event_group_id": target_id}, now,
                )
                self._append_audit(
                    connection, target_id, actor_id, actor_role, "reconcile", old_status, next_status,
                    {
                        "added_candidate_id": entity_id,
                        "matched_candidate_ids": matched_candidate_ids,
                        "merged_groups": merged_from,
                        "members": group_data["candidate_ids"],
                        "primary_candidate_id": group_data["primary_candidate_id"],
                    },
                    now,
                )

            if idempotency_key:
                connection.execute(
                    "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (actor_id, idempotency_key, entity_id, now),
                )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            connection.close()
            raise ConflictError(str(exc))
        except Exception:
            connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
        return self.get_entity(entity_id)

    @staticmethod
    def _select_target_group(connection, group_ids):
        groups = []
        for group_id in group_ids:
            row = connection.execute(
                "SELECT id, status, created_at FROM entities WHERE id = ? AND kind = 'event_group'",
                (group_id,),
            ).fetchone()
            groups.append(row)
    # Prefer an already-confirmed record, then the oldest group.
        return sorted(
            groups,
            key=lambda row: (-SQLiteRepository._group_status_rank(row["status"]), row["created_at"], row["id"]),
        )[0]["id"]

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def find_active_candidate_by_event_key(self, source_id, event_id):
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT e.* FROM event_reports r
                JOIN entities e ON e.id = r.candidate_id
                WHERE r.source_id = ? AND r.source_event_id = ? AND r.active = 1
                  AND e.status != 'withdrawn'
                """,
                (source_id, event_id),
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            connection.close()
            raise ConflictError(str(exc))
        except Exception:
            connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
        return self.get_entity(entity_id)

    def update_candidate_and_group(
        self,
        entity_id,
        expected_version,
        next_status,
        data,
        actor,
        action,
        from_status,
        deactivate_report=False,
        sync_report=False,
    ):
        """Update candidate, membership and group atomically."""
        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ? AND kind = 'candidate'",
                (entity_id,),
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )

            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? WHERE id = ?",
                (next_status, self._dump(data), now, entity_id),
            )
            if deactivate_report:
                connection.execute(
                    "UPDATE event_reports SET active = 0, updated_at = ? WHERE candidate_id = ?",
                    (now, entity_id),
                )
                connection.execute(
                    "UPDATE event_group_members SET active = 0 WHERE candidate_id = ?",
                    (entity_id,),
                )
            elif sync_report:
                connection.execute(
                    """
                    UPDATE event_reports
                    SET ra = ?, dec = ?, observed_at = ?, magnitude = ?, updated_at = ?
                    WHERE candidate_id = ?
                    """,
                    (
                        float(data["ra"]),
                        float(data["dec"]),
                        data["observed_at"],
                        float(data.get("latest_magnitude", data["magnitude"])),
                        now,
                        entity_id,
                    ),
                )

            membership = connection.execute(
                "SELECT group_id FROM event_group_members WHERE candidate_id = ?",
                (entity_id,),
            ).fetchone()
            group_info = None
            if membership:
                group_id = membership["group_id"]
                member_rows = connection.execute(
                    "SELECT candidate_id FROM event_group_members WHERE group_id = ? AND active = 1",
                    (group_id,),
                ).fetchall()
                candidate_rows = connection.execute(
                    "SELECT * FROM entities WHERE id IN (%s)"
                    % ",".join("?" for _ in member_rows),
                    [member["candidate_id"] for member in member_rows],
                ).fetchall() if member_rows else []
                candidates = {candidate["id"]: self._entity_from_row(candidate) for candidate in candidate_rows}
                old_group_status, group_status, group_data = self._refresh_group(
                    connection, group_id, candidates, now
                )
                group_info = {
                    "group_id": group_id,
                    "from_status": old_group_status,
                    "status": group_status,
                    "data": group_data,
                }
                self._append_audit(
                    connection, group_id, actor.user_id, actor.role,
                    "refresh_" + action, old_group_status, group_status,
                    {"candidate_id": entity_id, **group_data}, now,
                )

            self._append_audit(
                connection, entity_id, actor.user_id, actor.role,
                action, from_status, next_status, {"group": group_info}, now,
            )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            connection.close()
            raise ConflictError(str(exc))
        except Exception:
            connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
        return self.get_entity(entity_id), group_info

    def split_event_group(self, group_id, actor, moved_candidate_ids, reason, expected_version):
        now = utcnow()
        moved = sorted(set(moved_candidate_ids or []))
        connection = self._connect()
        new_group_id = _new_id()
        try:
            connection.execute("BEGIN IMMEDIATE")
            group_row = connection.execute(
                "SELECT * FROM entities WHERE id = ? AND kind = 'event_group'",
                (group_id,),
            ).fetchone()
            if not group_row:
                raise NotFoundError("event group not found: " + group_id)
            group = self._entity_from_row(group_row)
            if expected_version is not None and group["version"] != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, group["version"])
                )

            placeholders = ",".join("?" for _ in moved)
            member_rows = connection.execute(
                "SELECT candidate_id FROM event_group_members WHERE group_id = ? AND active = 1 "
                "AND candidate_id IN (%s)" % placeholders,
                [group_id, *moved],
            ).fetchall()
            found = {row["candidate_id"] for row in member_rows}
            missing = [candidate_id for candidate_id in moved if candidate_id not in found]
            if missing:
                raise ConflictError("candidates are not active members of this group: " + ",".join(missing))

            all_rows = connection.execute(
                "SELECT candidate_id FROM event_group_members WHERE group_id = ? AND active = 1",
                (group_id,),
            ).fetchall()
            remaining_ids = [row["candidate_id"] for row in all_rows if row["candidate_id"] not in found]
            if not remaining_ids:
                raise ConflictError("cannot split every member out of an event group")

            candidate_rows = connection.execute(
                "SELECT * FROM entities WHERE id IN (%s)"
                % ",".join("?" for _ in {*moved, *remaining_ids}),
                [*moved, *remaining_ids],
            ).fetchall()
            candidates = {row["id"]: self._entity_from_row(row) for row in candidate_rows}

            connection.execute(
                "UPDATE event_group_members SET group_id = ? WHERE candidate_id IN (%s)" % placeholders,
                [new_group_id, *moved],
            )
            for candidate_id in moved:
                candidate = candidates[candidate_id]
                candidate["data"]["event_group_id"] = new_group_id
                connection.execute(
                    "UPDATE entities SET data = ?, version = version + 1, updated_at = ? WHERE id = ?",
                    (self._dump(candidate["data"]), now, candidate_id),
                )

            new_status, new_data = self._insert_group(
                connection,
                new_group_id,
                [{"candidate_id": candidate_id} for candidate_id in moved],
                candidates,
                "provisional",
                actor.user_id,
                now,
            )
            old_status, next_status, remaining_data = self._refresh_group(
                connection, group_id, candidates, now
            )
            # The original confirmation remains; the detached group must be reviewed.
            connection.execute(
                "UPDATE entities SET status = ? WHERE id = ?", (group["status"], group_id)
            )
            next_status = group["status"]
            detail = {
                "reason": reason,
                "new_group_id": new_group_id,
                "moved_candidate_ids": moved,
                "remaining_candidate_ids": remaining_data["candidate_ids"],
            }
            self._append_audit(
                connection, group_id, actor.user_id, actor.role,
                "split", old_status, next_status, detail, now,
            )
            self._append_audit(
                connection, new_group_id, actor.user_id, actor.role,
                "split_create", None, new_status,
                {"reason": reason, "from_group_id": group_id, "members": new_data["candidate_ids"]},
                now,
            )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            connection.close()
            raise ConflictError(str(exc))
        except Exception:
            connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
        return self.get_entity(group_id), self.get_entity(new_group_id)

    def backfill_event_group_batch(self, actor_id="system-migration", limit=100):
        """Backfill one resume-safe batch of historical singleton groups."""
        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT e.*
                FROM entities e
                LEFT JOIN event_group_members m ON m.candidate_id = e.id
                WHERE e.kind = 'candidate' AND m.candidate_id IS NULL
                ORDER BY e.created_at, e.id
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            candidates = [self._entity_from_row(row) for row in rows]
            for candidate in candidates:
                group_id = _new_id()
                data = dict(candidate["data"])
                data["report_id"] = data.get("report_id") or candidate["id"]
                data["event_key"] = data.get("event_key") or "%s:%s" % (
                    data.get("source_id"),
                    data.get("event_id"),
                )
                data["event_group_id"] = group_id
                active = candidate["status"] != "withdrawn"
                status = "provisional" if active else "void"
                connection.execute(
                    "UPDATE entities SET data = ?, updated_at = ? WHERE id = ?",
                    (self._dump(data), now, candidate["id"]),
                )
                report = self._report_payload(candidate["id"], data, active, now)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO event_reports(report_id, candidate_id, source_id, source_event_id,
                                                        ra, dec, observed_at, magnitude, active,
                                                        created_at, updated_at)
                    VALUES (:report_id, :candidate_id, :source_id, :source_event_id, :ra, :dec,
                            :observed_at, :magnitude, :active, :now, :now)
                    """,
                    report,
                )
                connection.execute(
                    "INSERT INTO event_group_members(group_id, candidate_id, active) VALUES (?, ?, ?)",
                    (group_id, candidate["id"], 1 if active else 0),
                )
                group_data = {
                    "member_count": 1 if active else 0,
                    "candidate_ids": [candidate["id"]] if active else [],
                    "primary_candidate_id": candidate["id"] if active else None,
                    "active": active,
                    "single_source": True,
                    "backfilled": True,
                }
                connection.execute(
                    "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                    "VALUES (?, 'event_group', ?, 1, ?, ?, ?, ?)",
                    (group_id, status, self._dump(group_data), actor_id, now, now),
                )
                self._append_audit(
                    connection, candidate["id"], actor_id, "system", "backfill_event_group",
                    candidate["status"], candidate["status"], {"event_group_id": group_id}, now,
                )
                self._append_audit(
                    connection, group_id, actor_id, "system", "backfill_create",
                    None, status, {"candidate_id": candidate["id"], "single_source": True}, now,
                )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            connection.close()
            raise ConflictError(str(exc))
        except Exception:
            connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
        return len(candidates)

    def backfill_event_groups(self, batch_size=100):
        total = 0
        while True:
            count = self.backfill_event_group_batch(limit=batch_size)
            total += count
            if count < batch_size:
                return total

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            self._append_audit(
                connection, entity_id, actor_id, actor_role,
                action, from_status, to_status, detail,
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
