from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.diff import compare_results, evaluate_thresholds, normalize_policy
from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        policy = normalize_policy(payload.get("promotion_policy") or {})
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                promotion_policy=policy,
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
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        result["promotions"] = self.repository.promotions(task_id)
        result["promotion_events"] = self.repository.promotion_events(task_id)
        result["release_history"] = self.repository.release_history(task_id)
        result["latest_result_version"] = result["current_result_version"]
        result["published_result"] = self._published_view(task_id, result["published_result_version"])
        return result

    def version_overview(self, task_id: int) -> dict[str, Any]:
        """对外查询：同时给出计算最新版本与当前发布版本。"""
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        latest_version = row["current_result_version"]
        published_version = row["published_result_version"]
        latest = self.repository.result_by_version(task_id, latest_version) if latest_version else None
        return {
            "task_id": task_id,
            "latest_result_version": latest_version,
            "published_result_version": published_version,
            "latest": None if latest is None else self._result_payload(latest),
            "published": self._published_view(task_id, published_version),
            "promotions": self.repository.promotions(task_id),
            "release_history": self.repository.release_history(task_id),
        }

    def promotion_diff(self, task_id: int, result_version: int) -> dict[str, Any]:
        repository = self.repository
        task = repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        candidate = repository.result_by_version(task_id, result_version)
        if candidate is None:
            raise NotFoundError("结果版本不存在")
        return self._build_diff(repository, task, candidate)

    def submit_promotion(self, task_id: int, result_version: int, submitter: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            candidate = repository.result_by_version(task_id, result_version)
            if candidate is None:
                raise NotFoundError("结果版本不存在")
            promotion = repository.promotion_by_version(task_id, result_version)
            if promotion is None:
                raise NotFoundError("结果版本缺少晋级记录")
            if promotion["lifecycle_stage"] in {"published", "withdrawn"}:
                raise ConflictError(f"版本已经处于 {promotion['lifecycle_stage']} 状态，不能重复提交")
            template = repository.template_by_id(task["template_id"])
            if template is None:
                raise NotFoundError("参数模板不存在")
            policy = normalize_policy(json.loads(template["promotion_policy_json"] or "{}"))
            diff_summary = self._build_diff(repository, task, candidate)
            threshold = evaluate_thresholds(policy, diff_summary)
            ttl = int(policy["approval_ttl_seconds"])
            expires = to_storage(now_value + timedelta(seconds=ttl))
            connection.execute(
                "UPDATE compute_result_promotions SET lifecycle_stage='candidate',diff_summary_json=?,threshold_checks_json=?,policy_snapshot_json=?,submitted_by=?,submitted_at=?,reviewed_by='',reviewed_at=NULL,review_comment='',approval_expires_at=?,updated_at=? WHERE id=?",
                (json.dumps(diff_summary, ensure_ascii=False, sort_keys=True), json.dumps(threshold, ensure_ascii=False, sort_keys=True), json.dumps(policy, ensure_ascii=False, sort_keys=True), submitter, now, expires, now, promotion["id"]),
            )
            repository.add_promotion_event(
                task_id=task_id, promotion_id=promotion["id"], result_version=result_version,
                action="submit", actor=submitter, from_stage=promotion["lifecycle_stage"], to_stage="candidate",
                summary={"threshold_passed": threshold["passed"], "approval_expires_at": expires}, now=now,
            )
            refreshed = dict(repository.promotion_by_id(promotion["id"]))
        if not threshold["passed"]:
            raise ConflictError("候选版本未满足模板定义的关键指标阈值", context={"threshold_checks": threshold["checks"], "diff_summary": diff_summary})
        return refreshed

    def review_promotion(self, task_id: int, result_version: int, reviewer: str, approve: bool, comment: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task, promotion = self._load_open_promotion(connection, task_id, result_version)
            snapshot = promotion["policy_snapshot_json"] or "{}"
            if json.loads(snapshot):
                policy = normalize_policy(json.loads(snapshot))
            else:
                template = repository.template_by_id(task["template_id"])
                policy = normalize_policy(json.loads((template["promotion_policy_json"] if template else "{}") or "{}"))
            self._assert_reviewer_allowed(policy, promotion, reviewer)
            self._assert_not_expired(promotion, now)
            threshold = json.loads(promotion["threshold_checks_json"] or "{}")
            if approve:
                if not threshold.get("passed"):
                    raise ConflictError("候选版本未满足模板定义的关键指标阈值", context={"threshold_checks": threshold.get("checks", {})})
                connection.execute(
                    "UPDATE compute_result_promotions SET lifecycle_stage='validated',reviewed_by=?,reviewed_at=?,review_comment=?,updated_at=? WHERE id=?",
                    (reviewer, now, comment[:1000], now, promotion["id"]),
                )
                to_stage = "validated"
                action = "approve"
            else:
                # 驳回后退回候选并清空提交/审批有效期，必须重新提交才能再次复核。
                connection.execute(
                    "UPDATE compute_result_promotions SET lifecycle_stage='candidate',submitted_by='',submitted_at=NULL,approval_expires_at='',reviewed_by=?,reviewed_at=?,review_comment=?,updated_at=? WHERE id=?",
                    (reviewer, now, comment[:1000], now, promotion["id"]),
                )
                to_stage = "candidate"
                action = "reject"
            repository.add_promotion_event(
                task_id=task_id, promotion_id=promotion["id"], result_version=result_version,
                action=action, actor=reviewer, from_stage=promotion["lifecycle_stage"], to_stage=to_stage,
                summary={"comment": comment[:1000], "threshold_passed": threshold.get("passed")}, now=now,
            )
            return dict(repository.promotion_by_id(promotion["id"]))

    def publish_promotion(self, task_id: int, result_version: int, publisher: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            promotion = repository.promotion_by_version(task_id, result_version)
            if promotion is None:
                raise NotFoundError("晋级记录不存在")
            if promotion["lifecycle_stage"] != "validated":
                raise ConflictError("只有已验证（validated）的版本才能发布")
            policy = normalize_policy(json.loads(promotion["policy_snapshot_json"] or "{}"))
            # 发布执行人与提交人也必须不同，确保职责分离；复核人名单约束在复核环节已经执行。
            if promotion["submitted_by"] and publisher == promotion["submitted_by"]:
                raise PermissionDeniedError("提交人不能发布自己提交的结果版本")
            self._assert_not_expired(promotion, now)
            current_published = task["published_result_version"]
            if current_published is not None and int(result_version) <= int(current_published):
                raise ConflictError("审批结果已过期：已有更新的版本完成晋级，不能覆盖后来发布的结果")
            # 原子切换当前发布版本，并在同事务内写入发布历史。
            connection.execute(
                "UPDATE compute_tasks SET published_result_version=?,updated_at=?,version=version+1 WHERE id=?",
                (result_version, now, task_id),
            )
            connection.execute(
                "UPDATE compute_result_promotions SET lifecycle_stage='published',published_at=?,updated_at=? WHERE id=?",
                (now, now, promotion["id"]),
            )
            sequence = int(connection.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM compute_release_history WHERE task_id=?", (task_id,)).fetchone()[0])
            repository.add_release(task_id=task_id, sequence=sequence, action="publish", from_version=current_published, to_version=result_version, actor=publisher, reason="复核通过后发布", now=now)
            repository.add_promotion_event(
                task_id=task_id, promotion_id=promotion["id"], result_version=result_version,
                action="publish", actor=publisher, from_stage="validated", to_stage="published",
                summary={"from_result_version": current_published, "to_result_version": result_version}, now=now,
            )
            return dict(repository.task_by_id(task_id))

    def withdraw_promotion(self, task_id: int, result_version: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            promotion = repository.promotion_by_version(task_id, result_version)
            if promotion is None:
                raise NotFoundError("晋级记录不存在")
            if promotion["lifecycle_stage"] != "published":
                raise ConflictError("只有已发布的版本可以撤回")
            if task["published_result_version"] is None or int(task["published_result_version"]) != int(result_version):
                raise ConflictError("只能撤回当前对外发布的版本")
            if not reason.strip():
                raise ValidationError("撤回必须填写原因")
            # 恢复到上一可用发布版本：沿发布历史向前寻找最近一个仍可用的发布版本。
            restore_version = self._previous_available_release(connection, task_id, result_version)
            connection.execute(
                "UPDATE compute_tasks SET published_result_version=?,updated_at=?,version=version+1 WHERE id=?",
                (restore_version, now, task_id),
            )
            connection.execute(
                "UPDATE compute_result_promotions SET lifecycle_stage='withdrawn',withdrawn_by=?,withdrawn_at=?,withdraw_reason=?,restored_result_version=?,updated_at=? WHERE id=?",
                (actor, now, reason[:1000], restore_version, now, promotion["id"]),
            )
            sequence = int(connection.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM compute_release_history WHERE task_id=?", (task_id,)).fetchone()[0])
            repository.add_release(task_id=task_id, sequence=sequence, action="withdraw", from_version=result_version, to_version=restore_version, actor=actor, reason=reason[:1000], now=now)
            repository.add_promotion_event(
                task_id=task_id, promotion_id=promotion["id"], result_version=result_version,
                action="withdraw", actor=actor, from_stage="published", to_stage="withdrawn",
                summary={"restored_result_version": restore_version, "reason": reason[:1000]}, now=now,
            )
            return dict(repository.task_by_id(task_id))

    @staticmethod
    def _previous_available_release(connection: sqlite3.Connection, task_id: int, withdrawing_version: int) -> int | None:
        rows = connection.execute(
            "SELECT rh.to_result_version AS version FROM compute_release_history rh WHERE rh.task_id=? AND rh.action='publish' AND rh.to_result_version<? ORDER BY rh.sequence DESC",
            (task_id, withdrawing_version),
        ).fetchall()
        for row in rows:
            version = row["version"]
            if version is None:
                continue
            state = connection.execute(
                "SELECT lifecycle_stage FROM compute_result_promotions WHERE task_id=? AND result_version=?",
                (task_id, version),
            ).fetchone()
            if state is not None and state["lifecycle_stage"] == "published":
                return int(version)
        return None

    def _load_open_promotion(self, connection: sqlite3.Connection, task_id: int, result_version: int) -> tuple[sqlite3.Row, sqlite3.Row]:
        repository = ComputeRepository(connection)
        task = repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        promotion = repository.promotion_by_version(task_id, result_version)
        if promotion is None:
            raise NotFoundError("晋级记录不存在")
        if promotion["lifecycle_stage"] not in {"candidate", "validated"}:
            raise ConflictError(f"版本当前处于 {promotion['lifecycle_stage']} 状态，不能复核")
        return task, promotion

    @staticmethod
    def _assert_reviewer_allowed(policy: dict[str, Any], promotion: sqlite3.Row, reviewer: str) -> None:
        submitter = promotion["submitted_by"]
        if submitter and reviewer == submitter:
            raise PermissionDeniedError("同一用户不能既提交又复核同一结果版本")
        reviewers = policy.get("reviewers") or []
        if reviewers and reviewer not in reviewers:
            raise PermissionDeniedError("该用户不在模板授权的复核人名单中")

    @staticmethod
    def _assert_not_expired(promotion: sqlite3.Row, now: str) -> None:
        expires = promotion["approval_expires_at"]
        if not expires or not promotion["submitted_at"]:
            raise ConflictError("候选版本尚未提交晋级")
        if now > expires:
            raise ConflictError("审批已过期，需要重新提交晋级", context={"approval_expires_at": expires})

    def _build_diff(self, repository: ComputeRepository, task: sqlite3.Row, candidate: sqlite3.Row) -> dict[str, Any]:
        published_version = task["published_result_version"]
        if published_version is None:
            base_result: dict[str, Any] = {}
            base_metrics: dict[str, Any] = {}
            base_version = None
        else:
            base = repository.result_by_version(task["id"], int(published_version))
            if base is None:
                raise NotFoundError("当前发布版本缺少结果数据")
            base_result = json.loads(base["result_json"])
            base_metrics = json.loads(base["metrics_json"])
            base_version = int(published_version)
        diff = compare_results(base_result, base_metrics, json.loads(candidate["result_json"]), json.loads(candidate["metrics_json"]))
        diff["base_result_version"] = base_version
        diff["candidate_result_version"] = int(candidate["version"])
        return diff

    @staticmethod
    def _result_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "version": row["version"],
            "result": json.loads(row["result_json"]),
            "metrics": json.loads(row["metrics_json"]),
            "result_digest": row["result_digest"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    def _published_view(self, task_id: int, published_version: int | None) -> dict[str, Any] | None:
        if published_version is None:
            return None
        row = self.repository.result_by_version(task_id, int(published_version))
        if row is None:
            return None
        return self._result_payload(row)

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
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            promotion = repository.create_promotion(
                task_id=task_id, result_version=version,
                diff_summary={}, threshold_checks={}, policy_snapshot={},
                submitted_by="", submitted_at="", approval_expires_at="", now=now,
            )
            repository.add_promotion_event(
                task_id=task_id, promotion_id=promotion["id"], result_version=version,
                action="produce", actor=worker_id, from_stage="", to_stage="candidate",
                summary={"note": "新算法结果已生成，等待提交晋级"}, now=now,
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

    def recompute(self, task_id: int, actor: str, reason: str) -> dict[str, Any]:
        """用新算法对同一批输入重新计算：复用任务参数，重新排队并允许产生新的结果版本。"""
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] in {"queued", "running", "cancel_requested"}:
                raise ConflictError("任务尚未结束，不能发起重算")
            before = dict(task)
            connection.execute(
                "UPDATE compute_tasks SET status='queued',attempt_count=0,available_at=?,lease_owner='',lease_expires_at='',last_error_code='',last_error_message='',started_at=NULL,finished_at=NULL,updated_at=?,version=version+1 WHERE id=?",
                (now, now, task_id),
            )
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action="recompute", reason=reason, before=before, after=after, batch_key="", now=now)
            repository.add_promotion_event(
                task_id=task_id, promotion_id=None, result_version=None,
                action="recompute", actor=actor, from_stage="", to_stage="",
                summary={"reason": reason[:1000]}, now=now,
            )
            return after

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
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
