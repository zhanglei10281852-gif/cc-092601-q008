from __future__ import annotations

import pytest

TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "review_thresholds": {
        "metrics": {
            "accuracy": {"direction": "higher", "min": 0.8, "max_regression": 0.05},
            "loss": {"direction": "lower", "max": 0.5},
        }
    },
    "max_runtime_seconds": 300,
    "max_attempts": 3,
}


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def run_task(client, key: str, *, user: str = "researcher-1", worker: str = "worker-1", result: dict, metrics: dict) -> int:
    submitted = client.post(
        "/api/compute/tasks",
        json={
            "template_code": "solver-a",
            "project_code": "project-a",
            "requested_by": user,
            "parameters": {"iterations": 100, "mode": "accurate"},
            "priority": 50,
            "idempotency_key": key,
        },
    )
    assert submitted.status_code == 202, submitted.text
    task_id = submitted.json()["id"]
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.status_code == 200 and claimed.json()["task"], claimed.text
    completed = client.post(f"/api/compute/tasks/{task_id}/complete", json={"worker_id": worker, "result": result, "metrics": metrics})
    assert completed.status_code == 200, completed.text
    return task_id


def recompute(client, task_id: int, *, worker: str, result: dict, metrics: dict) -> None:
    retried = client.post(f"/api/compute/tasks/{task_id}/retry", json={"actor": "administrator", "reason": "新算法重算同批输入"})
    assert retried.status_code == 200 and retried.json()["status"] == "queued", retried.text
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.status_code == 200 and claimed.json()["task"]["id"] == task_id, claimed.text
    completed = client.post(f"/api/compute/tasks/{task_id}/complete", json={"worker_id": worker, "result": result, "metrics": metrics})
    assert completed.status_code == 200, completed.text


def review(actor: str, reason: str = "复核确认") -> dict:
    return {"actor": actor, "reason": reason}


def create_clerk(client, admin) -> None:
    response = client.post(
        "/api/users",
        headers=admin["headers"],
        json={"username": "clerk-reviewer", "password": "Clerk!23456", "display_name": "经办员", "role_codes": ["clerk"]},
    )
    assert response.status_code == 201, response.text


def test_new_result_starts_as_candidate_until_reviewed_and_published(client, admin):
    create_template(client)
    task_id = run_task(client, "promote-000001", result={"estimate": 1.5}, metrics={"accuracy": 0.9, "loss": 0.3})
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["latest_result_version"] == 1
    assert details["published_result_version"] is None
    assert details["results"][0]["stage"] == "candidate"

    validated = client.post(f"/api/compute/tasks/{task_id}/results/1/validate", json=review("admin"))
    assert validated.status_code == 200, validated.text
    assert validated.json()["stage"] == "validated"
    assert validated.json()["base_published_version"] is None
    assert validated.json()["comparison"]["thresholds"]["passed"] is True

    published = client.post(f"/api/compute/tasks/{task_id}/results/1/publish", json=review("admin"))
    assert published.status_code == 200, published.text
    assert published.json()["previous_published_version"] is None

    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["latest_result_version"] == 1
    assert details["published_result_version"] == 1
    assert details["results"][0]["stage"] == "published"
    assert [(item["action"], item["outcome"], item["actor"]) for item in details["reviews"]] == [
        ("validate", "approved", "admin"),
        ("publish", "approved", "admin"),
    ]

    listing = client.get(f"/api/compute/tasks/{task_id}/results").json()
    assert listing["latest_result_version"] == 1
    assert listing["published_result_version"] == 1


def test_recompute_compare_and_promote_new_version(client, admin):
    create_template(client)
    task_id = run_task(
        client,
        "promote-000002",
        result={"estimate": 1.5, "meta": {"model": "v1"}},
        metrics={"accuracy": 0.9, "loss": 0.3},
    )
    client.post(f"/api/compute/tasks/{task_id}/results/1/validate", json=review("admin"))
    client.post(f"/api/compute/tasks/{task_id}/results/1/publish", json=review("admin"))

    recompute(
        client,
        task_id,
        worker="worker-2",
        result={"estimate": 1.7, "meta": {"model": "v2"}, "extra": True},
        metrics={"accuracy": 0.93, "loss": 0.28},
    )
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["latest_result_version"] == 2
    assert details["published_result_version"] == 1
    assert details["results"][1]["stage"] == "candidate"

    comparison = client.get(f"/api/compute/tasks/{task_id}/results/compare?candidate_version=2").json()
    assert comparison["base_version"] == 1
    assert comparison["candidate_version"] == 2
    accuracy = next(item for item in comparison["metric_changes"] if item["metric"] == "accuracy")
    assert accuracy["before"] == 0.9 and accuracy["after"] == 0.93
    assert accuracy["delta"] == pytest.approx(0.03)
    assert comparison["result_diff"]["added"] == ["extra"]
    changed_paths = {item["path"] for item in comparison["result_diff"]["changed"]}
    assert changed_paths == {"estimate", "meta.model"}
    assert comparison["thresholds"]["passed"] is True

    validated = client.post(f"/api/compute/tasks/{task_id}/results/2/validate", json=review("admin"))
    assert validated.status_code == 200 and validated.json()["base_published_version"] == 1
    published = client.post(f"/api/compute/tasks/{task_id}/results/2/publish", json=review("admin"))
    assert published.status_code == 200 and published.json()["previous_published_version"] == 1

    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["published_result_version"] == 2
    assert [item["stage"] for item in details["results"]] == ["published", "published"]


def test_threshold_violation_rejected_and_decision_traced(client, admin):
    create_template(client)
    task_id = run_task(client, "promote-000003", result={"estimate": 1.5}, metrics={"accuracy": 0.9, "loss": 0.3})
    client.post(f"/api/compute/tasks/{task_id}/results/1/validate", json=review("admin"))
    client.post(f"/api/compute/tasks/{task_id}/results/1/publish", json=review("admin"))

    recompute(client, task_id, worker="worker-2", result={"estimate": 1.1}, metrics={"accuracy": 0.8, "loss": 0.3})
    rejected = client.post(f"/api/compute/tasks/{task_id}/results/2/validate", json=review("admin"))
    assert rejected.status_code == 409
    checks = rejected.json()["error"]["context"]["checks"]
    regression = next(item for item in checks if item["rule"] == "max_regression")
    assert regression["metric"] == "accuracy" and regression["passed"] is False

    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["results"][1]["stage"] == "candidate"
    assert details["published_result_version"] == 1
    rejected_reviews = [item for item in details["reviews"] if item["outcome"] == "rejected"]
    assert len(rejected_reviews) == 1
    assert rejected_reviews[0]["action"] == "validate"
    assert rejected_reviews[0]["result_version"] == 2

    publish = client.post(f"/api/compute/tasks/{task_id}/results/2/publish", json=review("admin"))
    assert publish.status_code == 409


def test_reviewer_must_have_permission_and_cannot_be_submitter(client, admin):
    create_template(client)
    create_clerk(client, admin)
    task_id = run_task(client, "promote-000004", result={"estimate": 1.5}, metrics={"accuracy": 0.9, "loss": 0.3})

    stranger = client.post(f"/api/compute/tasks/{task_id}/results/1/validate", json=review("stranger"))
    assert stranger.status_code == 403
    clerk = client.post(f"/api/compute/tasks/{task_id}/results/1/validate", json=review("clerk-reviewer"))
    assert clerk.status_code == 403

    own_task = run_task(client, "promote-000005", user="admin", result={"estimate": 1.5}, metrics={"accuracy": 0.9, "loss": 0.3})
    self_review = client.post(f"/api/compute/tasks/{own_task}/results/1/validate", json=review("admin"))
    assert self_review.status_code == 403
    assert "提交人" in self_review.json()["error"]["message"]

    worker_task = run_task(client, "promote-000006", worker="admin", result={"estimate": 1.5}, metrics={"accuracy": 0.9, "loss": 0.3})
    worker_review = client.post(f"/api/compute/tasks/{worker_task}/results/1/validate", json=review("admin"))
    assert worker_review.status_code == 403


def test_stale_approval_cannot_override_newer_promotion(client, admin):
    create_template(client)
    task_id = run_task(client, "promote-000007", result={"estimate": 1.0}, metrics={"accuracy": 0.9, "loss": 0.3})
    client.post(f"/api/compute/tasks/{task_id}/results/1/validate", json=review("admin"))
    client.post(f"/api/compute/tasks/{task_id}/results/1/publish", json=review("admin"))

    recompute(client, task_id, worker="worker-2", result={"estimate": 1.2}, metrics={"accuracy": 0.91, "loss": 0.3})
    validated_v2 = client.post(f"/api/compute/tasks/{task_id}/results/2/validate", json=review("admin"))
    assert validated_v2.status_code == 200 and validated_v2.json()["base_published_version"] == 1

    recompute(client, task_id, worker="worker-3", result={"estimate": 1.3}, metrics={"accuracy": 0.92, "loss": 0.3})
    client.post(f"/api/compute/tasks/{task_id}/results/3/validate", json=review("admin"))
    published_v3 = client.post(f"/api/compute/tasks/{task_id}/results/3/publish", json=review("admin"))
    assert published_v3.status_code == 200

    stale = client.post(f"/api/compute/tasks/{task_id}/results/2/publish", json=review("admin"))
    assert stale.status_code == 409
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["published_result_version"] == 3
    rejected = [item for item in details["reviews"] if item["action"] == "publish" and item["outcome"] == "rejected"]
    assert len(rejected) == 1 and rejected[0]["result_version"] == 2

    revalidated = client.post(f"/api/compute/tasks/{task_id}/results/2/validate", json=review("admin", "基于最新发布版本重新验证"))
    assert revalidated.status_code == 200 and revalidated.json()["base_published_version"] == 3
    republished = client.post(f"/api/compute/tasks/{task_id}/results/2/publish", json=review("admin"))
    assert republished.status_code == 200
    assert client.get(f"/api/compute/task-details/{task_id}").json()["published_result_version"] == 2


def test_retract_restores_previous_available_version_without_deleting_history(client, admin):
    create_template(client)
    task_id = run_task(client, "promote-000008", result={"estimate": 1.0}, metrics={"accuracy": 0.9, "loss": 0.3})
    client.post(f"/api/compute/tasks/{task_id}/results/1/validate", json=review("admin"))
    client.post(f"/api/compute/tasks/{task_id}/results/1/publish", json=review("admin"))
    recompute(client, task_id, worker="worker-2", result={"estimate": 1.4}, metrics={"accuracy": 0.92, "loss": 0.3})
    client.post(f"/api/compute/tasks/{task_id}/results/2/validate", json=review("admin"))
    client.post(f"/api/compute/tasks/{task_id}/results/2/publish", json=review("admin"))
    assert client.get(f"/api/compute/task-details/{task_id}").json()["published_result_version"] == 2

    retracted = client.post(f"/api/compute/tasks/{task_id}/results/2/retract", json=review("admin", "发现数据污染"))
    assert retracted.status_code == 200, retracted.text
    assert retracted.json()["restored_version"] == 1

    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["published_result_version"] == 1
    assert [item["stage"] for item in details["results"]] == ["published", "retracted"]
    retract_reviews = [item for item in details["reviews"] if item["action"] == "retract"]
    assert len(retract_reviews) == 1
    assert retract_reviews[0]["restored_version"] == 1
    assert retract_reviews[0]["base_published_version"] == 2

    stale_retract = client.post(f"/api/compute/tasks/{task_id}/results/2/retract", json=review("admin"))
    assert stale_retract.status_code == 409

    retracted_root = client.post(f"/api/compute/tasks/{task_id}/results/1/retract", json=review("admin", "全部下线"))
    assert retracted_root.status_code == 200 and retracted_root.json()["restored_version"] is None
    assert client.get(f"/api/compute/task-details/{task_id}").json()["published_result_version"] is None


def test_template_threshold_definition_is_validated(client, admin):
    create_template(client)
    invalid = dict(TEMPLATE, code="solver-b", review_thresholds={"metrics": {"accuracy": {"direction": "sideways"}}})
    response = client.post("/api/compute/templates?actor=administrator", json=invalid)
    assert response.status_code == 422
    unknown = dict(TEMPLATE, code="solver-c", review_thresholds={"metrics": {"accuracy": {"window": 3}}})
    assert client.post("/api/compute/templates?actor=administrator", json=unknown).status_code == 422

    created = client.get("/api/compute/templates").json()["items"][0]
    assert "accuracy" in created["review_thresholds_json"]


def test_migration_backfills_existing_results(tmp_path, monkeypatch):
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(tmp_path / "legacy.db"))
    from app.database import close_connection, get_connection, init_db, transaction

    close_connection()
    legacy_schema = """
    CREATE TABLE compute_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        algorithm TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        parameter_schema_json TEXT NOT NULL,
        default_parameters_json TEXT NOT NULL DEFAULT '{}',
        max_runtime_seconds INTEGER NOT NULL,
        max_attempts INTEGER NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_by TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE TABLE compute_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        template_id INTEGER NOT NULL REFERENCES compute_templates(id),
        project_code TEXT NOT NULL,
        requested_by TEXT NOT NULL,
        parameters_json TEXT NOT NULL,
        parameter_digest TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 50,
        idempotency_key TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL,
        available_at TEXT NOT NULL,
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_expires_at TEXT NOT NULL DEFAULT '',
        current_result_version INTEGER,
        last_error_code TEXT NOT NULL DEFAULT '',
        last_error_message TEXT NOT NULL DEFAULT '',
        version INTEGER NOT NULL DEFAULT 1,
        started_at TEXT,
        finished_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE TABLE compute_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL REFERENCES compute_tasks(id),
        version INTEGER NOT NULL,
        result_json TEXT NOT NULL,
        metrics_json TEXT NOT NULL DEFAULT '{}',
        result_digest TEXT NOT NULL,
        created_by TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    """
    with transaction(immediate=True) as connection:
        connection.executescript(legacy_schema)
        connection.execute(
            "INSERT INTO compute_templates(code,name,algorithm,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,created_by,created_at,updated_at) VALUES('legacy','旧模板','legacy','{}','{}',60,3,'tester','2026-09-01T00:00:00+00:00','2026-09-01T00:00:00+00:00')"
        )
        connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,current_result_version,created_at,updated_at) VALUES(1,'p','u','{}','d',50,'k','succeeded',1,3,'2026-09-01T00:00:00+00:00',1,'2026-09-01T00:00:00+00:00','2026-09-01T00:00:00+00:00')"
        )
        connection.execute(
            "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(1,1,'{}','{}','d','w','2026-09-01T00:00:00+00:00')"
        )
    close_connection()

    init_db()
    connection = get_connection()
    assert connection.execute("SELECT published_result_version FROM compute_tasks WHERE id=1").fetchone()[0] == 1
    assert connection.execute("SELECT stage FROM compute_results WHERE id=1").fetchone()[0] == "published"
    assert connection.execute("SELECT review_thresholds_json FROM compute_templates WHERE id=1").fetchone()[0] == "{}"
    assert connection.execute("SELECT code FROM permissions WHERE code='compute.review'").fetchone() is not None
    close_connection()
