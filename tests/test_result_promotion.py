from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError, PermissionDeniedError
from app.database import get_connection


POLICY_TEMPLATE = {
    "code": "solver-p",
    "name": "晋级流程模板",
    "algorithm": "solver-p",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 3,
    "promotion_policy": {
        "reviewers": ["reviewer-1", "reviewer-2"],
        "approval_ttl_seconds": 60,
        "metric_thresholds": {"rmse": {"max": 0.1, "direction": "minimize"}},
        "max_added_paths": 0,
    },
}


def _payload(key: str) -> dict:
    return {
        "template_code": "solver-p",
        "project_code": "project-p",
        "requested_by": "model-team",
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": 50,
        "idempotency_key": key,
    }


@pytest.fixture()
def service(client) -> ComputeOperationsService:
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    instance = ComputeOperationsService(get_connection(), clock)
    instance.create_template(POLICY_TEMPLATE, "administrator")
    return instance


def _produce_first(service: ComputeOperationsService, key: str, result: dict, metrics: dict, *, worker: str = "worker-1") -> int:
    task = service.submit(_payload(key))
    claimed = service.claim(worker, ["solver-p"], 60)
    assert claimed and claimed["id"] == task["id"]
    service.complete(task["id"], worker, result, metrics)
    return task["id"]


def _produce(service: ComputeOperationsService, task_id: int, result: dict, metrics: dict, *, worker: str = "worker-2", actor: str = "model-team") -> int:
    service.recompute(task_id, actor, "新算法重算同一批输入")
    claimed = service.claim(worker, ["solver-p"], 60)
    assert claimed and claimed["id"] == task_id
    service.complete(task_id, worker, result, metrics)
    return task_id


def _promote(service: ComputeOperationsService, task_id: int, version: int, *, submitter="model-team", reviewer="reviewer-1", publisher="release-bot") -> None:
    service.submit_promotion(task_id, version, submitter)
    service.review_promotion(task_id, version, reviewer, True, "指标达标")
    service.publish_promotion(task_id, version, publisher)


def test_new_result_is_candidate_and_overview_shows_latest_and_published(service):
    task_id = _produce_first(service, "promo-0001", {"value": 1.0}, {"rmse": 0.05})
    overview = service.version_overview(task_id)
    assert overview["latest_result_version"] == 1
    assert overview["published_result_version"] is None
    assert overview["latest"]["version"] == 1
    assert overview["published"] is None
    promotions = service.get_task(task_id)["promotions"]
    assert len(promotions) == 1 and promotions[0]["lifecycle_stage"] == "candidate"


def test_full_lifecycle_publish_atomically_switches_published_version(service):
    task_id = _produce_first(service, "promo-0002", {"value": 1.0}, {"rmse": 0.05})
    _promote(service, task_id, 1)
    overview = service.version_overview(task_id)
    assert overview["latest_result_version"] == 1 and overview["published_result_version"] == 1

    _produce(service, task_id, {"value": 0.99}, {"rmse": 0.04})
    service.submit_promotion(task_id, 2, "model-team")
    with pytest.raises(PermissionDeniedError):
        service.review_promotion(task_id, 2, "model-team", True, "自审")
    with pytest.raises(PermissionDeniedError):
        service.review_promotion(task_id, 2, "outsider", True, "无权复核")
    service.review_promotion(task_id, 2, "reviewer-2", True, "指标改善")
    with pytest.raises(PermissionDeniedError):
        service.publish_promotion(task_id, 2, "model-team")  # 提交人不能发布自己提交的版本
    service.publish_promotion(task_id, 2, "release-bot")

    overview = service.version_overview(task_id)
    assert overview["latest_result_version"] == 2
    assert overview["published_result_version"] == 2
    assert overview["published"]["metrics"] == {"rmse": 0.04}
    stages = {item["result_version"]: item["lifecycle_stage"] for item in service.get_task(task_id)["promotions"]}
    assert stages == {1: "published", 2: "published"}


def test_threshold_failure_blocks_submission_but_keeps_candidate_and_evidence(service):
    task_id = _produce_first(service, "promo-0004", {"value": 1.0}, {"rmse": 0.05})
    _promote(service, task_id, 1)
    _produce(service, task_id, {"value": 1.5}, {"rmse": 0.5})
    with pytest.raises(ConflictError) as exc:
        service.submit_promotion(task_id, 2, "model-team")
    assert "阈值" in exc.value.message
    promotion = service.repository.promotion_by_version(task_id, 2)
    assert promotion["lifecycle_stage"] == "candidate"
    assert promotion["submitted_by"] == "model-team"
    checks = json.loads(promotion["threshold_checks_json"])
    assert checks["passed"] is False
    assert checks["checks"]["rmse"]["passed"] is False
    with pytest.raises(ConflictError):
        service.review_promotion(task_id, 2, "reviewer-1", True, "阈值未过")


def test_diff_reports_metric_and_structural_changes_against_published(service):
    task_id = _produce_first(service, "promo-0006", {"value": 1.0, "extra": 9}, {"rmse": 0.05})
    _promote(service, task_id, 1)
    _produce(service, task_id, {"value": 0.99}, {"rmse": 0.04})
    diff = service.promotion_diff(task_id, 2)
    assert diff["base_result_version"] == 1 and diff["candidate_result_version"] == 2
    assert diff["structural"]["removed_paths"] == ["$.extra"]
    rmse = diff["metrics"]["rmse"]
    assert rmse["base"] == 0.05 and rmse["candidate"] == 0.04
    assert rmse["delta"] == pytest.approx(-0.01)


def test_withdraw_restores_previous_available_version_without_deleting_history(service):
    task_id = _produce_first(service, "promo-0008", {"value": 1.0}, {"rmse": 0.05})
    _promote(service, task_id, 1)
    _produce(service, task_id, {"value": 0.99}, {"rmse": 0.04})
    _promote(service, task_id, 2, reviewer="reviewer-2")
    assert service.version_overview(task_id)["published_result_version"] == 2

    withdrawn = service.withdraw_promotion(task_id, 2, "release-bot", "新算法结论存在疑点")
    assert withdrawn["published_result_version"] == 1
    promotion = service.repository.promotion_by_version(task_id, 2)
    assert promotion["lifecycle_stage"] == "withdrawn"
    assert promotion["restored_result_version"] == 1

    details = service.get_task(task_id)
    assert [item["action"] for item in details["release_history"]] == ["publish", "publish", "withdraw"]
    assert {item["version"] for item in details["results"]} == {1, 2}
    assert [event["action"] for event in details["promotion_events"] if event["result_version"] == 2] == ["produce", "submit", "approve", "publish", "withdraw"]

    # 历史版本仍可追溯；重复撤回已撤回版本应失败；而恢复后的 v1 是当前发布，可以继续撤回并清空发布指针
    with pytest.raises(ConflictError):
        service.withdraw_promotion(task_id, 2, "release-bot", "再次撤回")
    service.withdraw_promotion(task_id, 1, "release-bot", "基线也作废")
    assert service.version_overview(task_id)["published_result_version"] is None


def test_withdraw_without_prior_release_clears_published_pointer(service):
    task_id = _produce_first(service, "promo-0010", {"value": 1.0}, {"rmse": 0.05})
    _promote(service, task_id, 1)
    withdrawn = service.withdraw_promotion(task_id, 1, "release-bot", "首个发布作废")
    assert withdrawn["published_result_version"] is None
    assert service.version_overview(task_id)["published"] is None


def test_expired_approval_cannot_be_reviewed_or_published(service):
    task_id = _produce_first(service, "promo-0011", {"value": 1.0}, {"rmse": 0.05})
    _promote(service, task_id, 1)
    _produce(service, task_id, {"value": 0.99}, {"rmse": 0.04})
    service.submit_promotion(task_id, 2, "model-team")
    service.clock.advance(seconds=61)
    with pytest.raises(ConflictError) as exc:
        service.review_promotion(task_id, 2, "reviewer-1", True, "晚了")
    assert "过期" in exc.value.message
    with pytest.raises(ConflictError):
        service.publish_promotion(task_id, 2, "release-bot")
    # 重新提交后审批窗口刷新，可以继续晋级
    service.submit_promotion(task_id, 2, "model-team")
    service.review_promotion(task_id, 2, "reviewer-1", True, "重新复核")
    service.publish_promotion(task_id, 2, "release-bot")
    assert service.version_overview(task_id)["published_result_version"] == 2


def test_stale_approval_cannot_override_later_published_version(service):
    task_id = _produce_first(service, "promo-0013", {"value": 1.0}, {"rmse": 0.05})
    _promote(service, task_id, 1)
    _produce(service, task_id, {"value": 0.98}, {"rmse": 0.04})
    service.submit_promotion(task_id, 2, "model-team")
    service.review_promotion(task_id, 2, "reviewer-1", True, "先验证但暂不发布")
    # 版本 3 走完流程并先发布
    _produce(service, task_id, {"value": 0.97}, {"rmse": 0.03}, worker="worker-3")
    _promote(service, task_id, 3, reviewer="reviewer-2")
    assert service.version_overview(task_id)["published_result_version"] == 3
    # 版本 2 的旧审批不能覆盖后来晋级发布的版本 3
    with pytest.raises(ConflictError) as exc:
        service.publish_promotion(task_id, 2, "release-bot")
    assert "过期" in exc.value.message


def test_rejection_returns_to_candidate_and_requires_resubmission(service):
    task_id = _produce_first(service, "promo-0016", {"value": 1.0}, {"rmse": 0.05})
    _promote(service, task_id, 1)
    _produce(service, task_id, {"value": 0.99}, {"rmse": 0.04})
    service.submit_promotion(task_id, 2, "model-team")
    service.review_promotion(task_id, 2, "reviewer-1", False, "材料不全")
    with pytest.raises(ConflictError):
        service.review_promotion(task_id, 2, "reviewer-2", True, "未重新提交")
    service.submit_promotion(task_id, 2, "model-team")
    service.review_promotion(task_id, 2, "reviewer-2", True, "补充后通过")


def test_http_endpoints_expose_diff_and_both_versions(client):
    response = client.post("/api/compute/templates?actor=administrator", json=POLICY_TEMPLATE)
    assert response.status_code == 201, response.text
    task = client.post("/api/compute/tasks", json=_payload("promo-http-1")).json()
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-p"], "lease_seconds": 60})
    client.post(f"/api/compute/tasks/{task['id']}/complete", json={"worker_id": "w1", "result": {"value": 1.0}, "metrics": {"rmse": 0.05}})

    diff = client.get(f"/api/compute/tasks/{task['id']}/results/1/diff")
    assert diff.status_code == 200
    assert diff.json()["base_result_version"] is None

    submit = client.post(f"/api/compute/tasks/{task['id']}/results/1/promotions/submit", json={"submitter": "model-team"})
    assert submit.status_code == 200, submit.text
    self_review = client.post(f"/api/compute/tasks/{task['id']}/results/1/promotions/review", json={"reviewer": "model-team", "approve": True, "comment": "x"})
    assert self_review.status_code == 403
    review = client.post(f"/api/compute/tasks/{task['id']}/results/1/promotions/review", json={"reviewer": "reviewer-1", "approve": True, "comment": "ok"})
    assert review.status_code == 200, review.text
    publish = client.post(f"/api/compute/tasks/{task['id']}/results/1/promotions/publish", json={"publisher": "release-bot"})
    assert publish.status_code == 200, publish.text

    overview = client.get(f"/api/compute/tasks/{task['id']}/versions").json()
    assert overview["latest_result_version"] == overview["published_result_version"] == 1
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["promotions"][0]["lifecycle_stage"] == "published"
    assert details["release_history"][0]["action"] == "publish"
    assert len(details["promotion_events"]) >= 4

    # 撤回通过 HTTP 恢复到无发布版本
    withdraw = client.post(f"/api/compute/tasks/{task['id']}/results/1/promotions/withdraw", json={"actor": "release-bot", "reason": "结论作废"})
    assert withdraw.status_code == 200, withdraw.text
    assert client.get(f"/api/compute/tasks/{task['id']}/versions").json()["published_result_version"] is None
