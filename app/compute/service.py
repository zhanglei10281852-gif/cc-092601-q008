from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.database import get_connection, transaction

REVIEW_PERMISSION = "compute.review"


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本晋级和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        review_thresholds = payload.get("review_thresholds") or {}
        self._validate_thresholds(review_thresholds)
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                review_thresholds=review_thresholds,
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["latest_result_version"] = row["current_result_version"]
        result["results"] = self.repository.result_versions(task_id)
        result["reviews"] = self.repository.result_reviews(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,stage,created_by,created_at) VALUES(?,?,?,?,?,'candidate',?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled", "succeeded"}:
                raise ConflictError("只有失败、已取消或已成功（重算）的任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted}

    def list_results(self, task_id: int) -> dict[str, Any]:
        task = self.repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        return {
            "task_id": task_id,
            "latest_result_version": task["current_result_version"],
            "published_result_version": task["published_result_version"],
            "items": self.repository.result_versions(task_id),
        }

    def compare_versions(self, task_id: int, candidate_version: int, base_version: int | None = None) -> dict[str, Any]:
        task = self.repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        candidate = self.repository.result_version(task_id, candidate_version)
        if candidate is None:
            raise NotFoundError("候选结果版本不存在")
        if base_version is None:
            base_version = task["published_result_version"]
        base = None
        if base_version is not None:
            base = self.repository.result_version(task_id, base_version)
            if base is None:
                raise NotFoundError("基准结果版本不存在")
        return self._comparison(task, base, candidate)

    def validate_result(self, task_id: int, version: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        rejected_checks: list[dict[str, Any]] | None = None
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task, result = self._task_and_result(repository, task_id, version)
            self._require_reviewer(connection, actor, task, result)
            if result["stage"] not in {"candidate", "validated"}:
                raise ConflictError("只有候选或已验证状态的结果版本可以验证")
            comparison = self._comparison(task, self._published_result(repository, task), result)
            report = comparison["thresholds"]
            base_version = task["published_result_version"]
            if not report["passed"]:
                repository.add_review(task_id=task_id, result_version=version, action="validate", outcome="rejected", actor=actor, reason=reason, base_published_version=base_version, restored_version=None, diff=self._diff_summary(comparison), thresholds=report, now=now)
                rejected_checks = report["checks"]
            else:
                cursor = connection.execute("UPDATE compute_results SET stage='validated' WHERE id=? AND stage IN ('candidate','validated')", (result["id"],))
                if cursor.rowcount != 1:
                    raise ConflictError("结果版本状态已变化，请重新验证")
                repository.add_review(task_id=task_id, result_version=version, action="validate", outcome="approved", actor=actor, reason=reason, base_published_version=base_version, restored_version=None, diff=self._diff_summary(comparison), thresholds=report, now=now)
                return {"task_id": task_id, "version": version, "stage": "validated", "base_published_version": base_version, "comparison": comparison}
        raise ConflictError("结果版本未满足模板定义的发布阈值", context={"checks": rejected_checks})

    def publish_result(self, task_id: int, version: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        stale = False
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task, result = self._task_and_result(repository, task_id, version)
            self._require_reviewer(connection, actor, task, result)
            if result["stage"] != "validated":
                raise ConflictError("只有已验证的结果版本可以发布")
            approval = repository.latest_review(task_id, version, action="validate", outcome="approved")
            if approval is None:
                raise ConflictError("结果版本缺少已通过的验证审批")
            base_version = approval["base_published_version"]
            cursor = connection.execute(
                "UPDATE compute_tasks SET published_result_version=?,updated_at=?,version=version+1 WHERE id=? AND published_result_version IS ?",
                (version, now, task_id, base_version),
            )
            if cursor.rowcount != 1:
                repository.add_review(task_id=task_id, result_version=version, action="publish", outcome="rejected", actor=actor, reason=reason, base_published_version=base_version, restored_version=None, diff={}, thresholds={}, now=now)
                stale = True
            else:
                connection.execute("UPDATE compute_results SET stage='published' WHERE id=?", (result["id"],))
                base = repository.result_version(task_id, base_version) if base_version is not None else None
                comparison = self._comparison(task, base, result)
                repository.add_review(task_id=task_id, result_version=version, action="publish", outcome="approved", actor=actor, reason=reason, base_published_version=base_version, restored_version=None, diff=self._diff_summary(comparison), thresholds=comparison["thresholds"], now=now)
                return {"task_id": task_id, "version": version, "stage": "published", "published_result_version": version, "previous_published_version": base_version, "comparison": comparison}
        if stale:
            raise ConflictError("验证审批的基线版本已过期，需要重新验证后再发布")
        raise ConflictError("结果版本发布失败")

    def retract_result(self, task_id: int, version: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task, result = self._task_and_result(repository, task_id, version)
            self._require_reviewer(connection, actor, task, result)
            if result["stage"] != "published" or task["published_result_version"] != version:
                raise ConflictError("只有当前对外发布的结果版本可以撤回")
            restored_version = repository.previous_published_version(task_id, version)
            cursor = connection.execute(
                "UPDATE compute_tasks SET published_result_version=?,updated_at=?,version=version+1 WHERE id=? AND published_result_version=?",
                (restored_version, now, task_id, version),
            )
            if cursor.rowcount != 1:
                raise ConflictError("对外发布版本已变化，请刷新后重试")
            connection.execute("UPDATE compute_results SET stage='retracted' WHERE id=?", (result["id"],))
            restored = repository.result_version(task_id, restored_version) if restored_version is not None else None
            diff = self._diff_summary(self._comparison(task, result, restored)) if restored is not None else {}
            repository.add_review(task_id=task_id, result_version=version, action="retract", outcome="approved", actor=actor, reason=reason, base_published_version=version, restored_version=restored_version, diff=diff, thresholds={}, now=now)
            return {"task_id": task_id, "version": version, "stage": "retracted", "restored_version": restored_version, "published_result_version": restored_version}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    @staticmethod
    def _task_and_result(repository: ComputeRepository, task_id: int, version: int) -> tuple[sqlite3.Row, sqlite3.Row]:
        task = repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        result = repository.result_version(task_id, version)
        if result is None:
            raise NotFoundError("结果版本不存在")
        return task, result

    @staticmethod
    def _published_result(repository: ComputeRepository, task: sqlite3.Row) -> sqlite3.Row | None:
        published = task["published_result_version"]
        if published is None:
            return None
        return repository.result_version(task["id"], published)

    @staticmethod
    def _require_reviewer(connection: sqlite3.Connection, actor: str, task: sqlite3.Row, result: sqlite3.Row) -> None:
        user = connection.execute("SELECT id,status FROM users WHERE username=?", (actor.strip().casefold(),)).fetchone()
        if user is None or user["status"] != "active":
            raise PermissionDeniedError("复核人必须是活跃的系统用户")
        rows = connection.execute(
            "SELECT DISTINCT p.code FROM permissions p JOIN role_permissions rp ON rp.permission_id=p.id JOIN user_roles ur ON ur.role_id=rp.role_id WHERE ur.user_id=?",
            (user["id"],),
        ).fetchall()
        permissions = {str(row[0]) for row in rows}
        if "*" not in permissions and REVIEW_PERMISSION not in permissions:
            raise PermissionDeniedError(f"缺少权限：{REVIEW_PERMISSION}")
        if actor in {task["requested_by"], result["created_by"]}:
            raise PermissionDeniedError("提交人不能复核自己提交的结果")

    def _comparison(self, task: sqlite3.Row, base: sqlite3.Row | None, candidate: sqlite3.Row) -> dict[str, Any]:
        base_result = json.loads(base["result_json"]) if base is not None else {}
        base_metrics = json.loads(base["metrics_json"]) if base is not None else {}
        candidate_result = json.loads(candidate["result_json"])
        candidate_metrics = json.loads(candidate["metrics_json"])
        thresholds = json.loads(task["review_thresholds_json"] or "{}")
        return {
            "task_id": task["id"],
            "base_version": base["version"] if base is not None else None,
            "candidate_version": candidate["version"],
            "metric_changes": self._metric_changes(base_metrics, candidate_metrics),
            "result_diff": self._diff_json(base_result, candidate_result),
            "metrics_diff": self._diff_json(base_metrics, candidate_metrics),
            "thresholds": self._evaluate_thresholds(thresholds, candidate_metrics, base_metrics if base is not None else None),
        }

    @staticmethod
    def _diff_summary(comparison: dict[str, Any]) -> dict[str, Any]:
        return {key: comparison[key] for key in ("metric_changes", "result_diff", "metrics_diff")}

    @classmethod
    def _flatten(cls, value: Any, prefix: str = "") -> dict[str, Any]:
        items: dict[str, Any] = {}
        if isinstance(value, dict):
            for key, child in value.items():
                path = f"{prefix}.{key}" if prefix else str(key)
                items.update(cls._flatten(child, path))
        else:
            items[prefix] = value
        return items

    @classmethod
    def _metric_changes(cls, before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
        before_flat = cls._flatten(before)
        after_flat = cls._flatten(after)
        changes: list[dict[str, Any]] = []
        for name in sorted(set(before_flat) | set(after_flat)):
            old, new = before_flat.get(name), after_flat.get(name)
            if old == new:
                continue
            numeric = all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in (old, new))
            changes.append({"metric": name, "before": old, "after": new, "delta": new - old if numeric else None})
        return changes

    @classmethod
    def _diff_json(cls, before: Any, after: Any, *, limit: int = 200) -> dict[str, Any]:
        added: list[str] = []
        removed: list[str] = []
        changed: list[dict[str, Any]] = []
        truncated = False

        def walk(old: Any, new: Any, path: str) -> None:
            nonlocal truncated
            if truncated:
                return
            if len(added) + len(removed) + len(changed) >= limit:
                truncated = True
                return
            if isinstance(old, dict) and isinstance(new, dict):
                for key in sorted(new.keys() - old.keys()):
                    added.append(f"{path}.{key}" if path else str(key))
                for key in sorted(old.keys() - new.keys()):
                    removed.append(f"{path}.{key}" if path else str(key))
                for key in sorted(old.keys() & new.keys()):
                    walk(old[key], new[key], f"{path}.{key}" if path else str(key))
            elif old != new:
                changed.append({"path": path or "$", "before": old, "after": new})

        walk(before, after, "")
        return {"added": added, "removed": removed, "changed": changed, "truncated": truncated}

    @classmethod
    def _evaluate_thresholds(cls, thresholds: dict[str, Any], candidate_metrics: dict[str, Any], published_metrics: dict[str, Any] | None) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        rules = (thresholds or {}).get("metrics", {})
        candidate_flat = cls._flatten(candidate_metrics)
        published_flat = cls._flatten(published_metrics or {})
        for name in sorted(rules):
            rule = rules[name]
            direction = rule.get("direction", "higher")
            actual = candidate_flat.get(name)
            if not isinstance(actual, (int, float)) or isinstance(actual, bool):
                checks.append({"metric": name, "rule": "required", "expected": "数值指标", "actual": actual, "passed": False})
                continue
            if rule.get("min") is not None:
                checks.append({"metric": name, "rule": "min", "expected": rule["min"], "actual": actual, "passed": actual >= rule["min"]})
            if rule.get("max") is not None:
                checks.append({"metric": name, "rule": "max", "expected": rule["max"], "actual": actual, "passed": actual <= rule["max"]})
            baseline = published_flat.get(name)
            if rule.get("max_regression") is not None and isinstance(baseline, (int, float)) and not isinstance(baseline, bool):
                regression = (baseline - actual) if direction == "higher" else (actual - baseline)
                checks.append({"metric": name, "rule": "max_regression", "expected": rule["max_regression"], "actual": regression, "passed": regression <= rule["max_regression"]})
        return {"passed": all(check["passed"] for check in checks), "checks": checks}

    @staticmethod
    def _validate_thresholds(thresholds: dict[str, Any]) -> None:
        if not isinstance(thresholds, dict):
            raise ValidationError("发布阈值必须是对象")
        metrics = thresholds.get("metrics", {})
        if not isinstance(metrics, dict):
            raise ValidationError("发布阈值的 metrics 必须是对象")
        for name, rule in metrics.items():
            if not name or not isinstance(rule, dict):
                raise ValidationError(f"指标阈值 {name or '<empty>'} 的规则不合法")
            unknown = set(rule) - {"direction", "min", "max", "max_regression"}
            if unknown:
                raise ValidationError(f"指标 {name} 包含未支持的阈值项", context={"rules": sorted(unknown)})
            if rule.get("direction", "higher") not in {"higher", "lower"}:
                raise ValidationError(f"指标 {name} 的 direction 必须是 higher 或 lower")
            for key in ("min", "max", "max_regression"):
                value = rule.get(key)
                if value is not None and (not isinstance(value, (int, float)) or isinstance(value, bool)):
                    raise ValidationError(f"指标 {name} 的 {key} 必须是数值")
            if rule.get("max_regression") is not None and rule["max_regression"] < 0:
                raise ValidationError(f"指标 {name} 的 max_regression 不能为负数")

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
