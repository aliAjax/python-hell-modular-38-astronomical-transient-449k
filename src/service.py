from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, PermissionDenied
from .rules import RuleEngine, latest_measurement, select_primary, within_reconciliation


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def _lookup_with(self, connection):
        def lookup(kind, field, value):
            return self.repository.find_entities(
                self.rules.normalize_kind(kind), field, value, connection=connection
            )
        return lookup

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    # ------------------------------------------------------------------ ingest

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        if kind == "candidate":
            return self._ingest_candidate(actor, data, idempotency_key)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def _ingest_candidate(self, actor, data, idempotency_key):
        """Journal a source report atomically.

        The candidate and its event group are written in one transaction so a
        failure never leaves a half-group. Retrying with the same report number
        (idempotency key) returns the already-journaled candidate.
        """
        with self.repository.transaction() as conn:
            if idempotency_key:
                existing_id = self.repository.get_idempotency(
                    actor.user_id, idempotency_key, connection=conn
                )
                if existing_id:
                    existing = self.repository.get_entity(existing_id, connection=conn)
                    if existing and existing["kind"] == "candidate":
                        return existing
            payload = dict(data or {})
            validated = self.rules.validate_create(actor, "candidate", payload, self._lookup_with(conn))
            if validated:
                payload.update(validated)
            entity_id = str(payload.pop("id", "") or uuid4())
            if self.repository.get_entity(entity_id, connection=conn):
                raise ConflictError("entity already exists: " + entity_id)
            status = self.rules.initial_status("candidate")
            measurement = (payload.get("measurements") or [{}])[-1]
            group = self._find_matching_group(
                measurement.get("ra"),
                measurement.get("dec"),
                measurement.get("observed_at"),
                conn,
            )
            if group is None:
                group_id = str(uuid4())
                self.repository.create_entity(
                    group_id,
                    "event_group",
                    "active",
                    {"member_ids": [entity_id], "primary_candidate_id": entity_id},
                    actor.user_id,
                    connection=conn,
                )
            else:
                group_id = group["id"]
            payload["group_id"] = group_id
            candidate = self.repository.create_entity(
                entity_id, "candidate", status, payload, actor.user_id, connection=conn
            )
            if group is not None:
                self._add_candidate_to_group(group, candidate, actor, conn)
            self.audit.record(
                entity_id,
                actor,
                "create",
                None,
                status,
                {"kind": "candidate", "group_id": group_id},
                connection=conn,
            )
            if idempotency_key:
                self.repository.save_idempotency(
                    actor.user_id, idempotency_key, entity_id, connection=conn
                )
            return candidate

    def _find_matching_group(self, ra, dec, observed_at, connection):
        if ra is None or dec is None or not observed_at:
            return None
        groups = self.repository.list_entities(
            kind="event_group", status="active", connection=connection
        )
        for group in groups:
            for member_id in group["data"].get("member_ids") or []:
                member = self.repository.get_entity(member_id, connection=connection)
                if not member or member["status"] in ("withdrawn", "void"):
                    continue
                latest = latest_measurement(member)
                if not latest:
                    continue
                if within_reconciliation(
                    ra, dec, observed_at,
                    latest.get("ra"), latest.get("dec"), latest.get("observed_at"),
                ):
                    return group
        return None

    def _add_candidate_to_group(self, group, candidate, actor, connection):
        member_ids = list(group["data"].get("member_ids") or [])
        if candidate["id"] not in member_ids:
            member_ids.append(candidate["id"])
        members = [self.repository.get_entity(mid, connection=connection) for mid in member_ids]
        members = [m for m in members if m and m["status"] not in ("withdrawn", "void")]
        new_primary = select_primary(members)
        new_data = {
            **group["data"],
            "member_ids": member_ids,
            "primary_candidate_id": new_primary,
        }
        self.repository.update_entity(
            group["id"], group["version"], group["status"], new_data, connection=connection
        )
        self.audit.record(
            group["id"],
            actor,
            "add_member",
            group["status"],
            group["status"],
            {"candidate_id": candidate["id"]},
            connection=connection,
        )

    # -------------------------------------------------------------- transitions

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        try:
            with self.repository.transaction() as conn:
                expected = int(expected_version) if expected_version is not None else entity["version"]
                next_status, patch = self.rules.validate_transition(
                    actor, entity, action, dict(data or {}), self._lookup_with(conn)
                )
                merged = dict(entity["data"])
                merged.update(patch)
                self.repository.update_entity(
                    entity_id, expected, next_status, merged, connection=conn
                )
                self._after_transition(actor, entity, action, data, conn)
                updated = self.repository.get_entity(entity_id, connection=conn)
                self.audit.record(
                    entity_id,
                    actor,
                    action,
                    entity["status"],
                    updated["status"],
                    {"patch": patch},
                    connection=conn,
                )
            return updated
        except PermissionDenied as exc:
            self._record_denial(actor, entity, action, exc)
            raise
        except ConflictError as exc:
            self._record_denial(actor, entity, action, exc)
            raise

    def _record_denial(self, actor, entity, action, exc):
        """Persist an auditable record of a rejected attempt.

        Denials are written on their own connection so they survive the rollback
        of the failed transaction.
        """
        try:
            self.audit.record(
                entity["id"],
                actor,
                action + "_denied",
                entity["status"],
                entity["status"],
                {"error": str(exc), "type": type(exc).__name__},
            )
        except Exception:
            pass

    def _after_transition(self, actor, entity, action, data, connection):
        kind = entity["kind"]
        if kind == "candidate":
            self._after_candidate_transition(actor, entity, action, connection)
        elif kind == "event_group":
            if action == "split":
                self._after_group_split(actor, entity, action, data, connection)

    def _after_candidate_transition(self, actor, entity, action, connection):
        updated = self.repository.get_entity(entity["id"], connection=connection)
        group_id = updated["data"].get("group_id")
        if not group_id:
            return
        group = self.repository.get_entity(group_id, connection=connection)
        if not group:
            return
        member_ids = list(group["data"].get("member_ids") or [])
        members = [self.repository.get_entity(mid, connection=connection) for mid in member_ids]
        members = [m for m in members if m]
        valid = [m for m in members if m["status"] not in ("withdrawn", "void")]
        if not valid:
            if group["status"] != "void":
                self.repository.update_entity(
                    group_id, group["version"], "void", group["data"], connection=connection
                )
                self.audit.record(
                    group_id, actor, "void", group["status"], "void",
                    {"reason": "no valid reports"}, connection=connection,
                )
            if updated["status"] != "void":
                self.repository.update_entity(
                    updated["id"], updated["version"], "void", updated["data"], connection=connection
                )
                self.audit.record(
                    updated["id"], actor, "void", updated["status"], "void",
                    {"reason": "no valid reports in group"}, connection=connection,
                )
            return
        new_primary = select_primary(valid)
        if new_primary and new_primary != group["data"].get("primary_candidate_id"):
            new_data = {**group["data"], "primary_candidate_id": new_primary}
            self.repository.update_entity(
                group_id, group["version"], group["status"], new_data, connection=connection
            )
            self.audit.record(
                group_id, actor, "reidentify_primary", group["status"], group["status"],
                {"primary": new_primary}, connection=connection,
            )

    def _after_group_split(self, actor, entity, action, data, connection):
        candidate_id = (data or {}).get("candidate_id")
        if not candidate_id:
            return
        new_group_id = str(uuid4())
        self.repository.create_entity(
            new_group_id,
            "event_group",
            "active",
            {"member_ids": [candidate_id], "primary_candidate_id": candidate_id},
            actor.user_id,
            connection=connection,
        )
        candidate = self.repository.get_entity(candidate_id, connection=connection)
        if candidate:
            self.repository.update_entity(
                candidate_id,
                candidate["version"],
                candidate["status"],
                {**candidate["data"], "group_id": new_group_id},
                connection=connection,
            )
        self.audit.record(
            new_group_id,
            actor,
            "split",
            None,
            "active",
            {"from_group": entity["id"], "candidate_id": candidate_id},
            connection=connection,
        )

    # -------------------------------------------------------------- backfill

    def backfill_groups(self, actor):
        """Assign every historical candidate its own single-source group.

        Each candidate is journaled in its own transaction, so an interruption
        leaves a consistent prefix and re-running resumes where it stopped.
        """
        if actor.role != "admin":
            raise PermissionDenied("only admins can backfill event groups")
        candidates = self.repository.list_entities(kind="candidate")
        created = 0
        skipped = 0
        for candidate in candidates:
            if candidate["data"].get("group_id"):
                skipped += 1
                continue
            with self.repository.transaction() as conn:
                current = self.repository.get_entity(candidate["id"], connection=conn)
                if current["data"].get("group_id"):
                    skipped += 1
                    continue
                group_id = str(uuid4())
                self.repository.create_entity(
                    group_id,
                    "event_group",
                    "active",
                    {"member_ids": [current["id"]], "primary_candidate_id": current["id"]},
                    actor.user_id,
                    connection=conn,
                )
                self.repository.update_entity(
                    current["id"],
                    current["version"],
                    current["status"],
                    {**current["data"], "group_id": group_id},
                    connection=conn,
                )
                self.audit.record(
                    group_id, actor, "backfill", None, "active",
                    {"candidate_id": current["id"]}, connection=conn,
                )
                created += 1
        return {"created": created, "skipped": skipped, "total": len(candidates)}

    # ------------------------------------------------------------------ reads

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
