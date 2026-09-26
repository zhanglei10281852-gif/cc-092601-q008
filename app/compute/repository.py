from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable


class ComputeRepository:
    """封装计算任务运营领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def template_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE code=?", (code,)).fetchone()

    def template_by_id(self, template_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE id=?", (template_id,)).fetchone()

    def active_templates(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM compute_templates WHERE active=1 ORDER BY code,version").fetchall()
        return [dict(row) for row in rows]

    def create_template(self, *, code: str, name: str, algorithm: str, parameter_schema: dict[str, Any], defaults: dict[str, Any], promotion_policy: dict[str, Any], max_runtime_seconds: int, max_attempts: int, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,promotion_policy_json,max_runtime_seconds,max_attempts,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,?,?,?,1,?,?,?)",
            (code, name, algorithm, json.dumps(parameter_schema, ensure_ascii=False, sort_keys=True), json.dumps(defaults, ensure_ascii=False, sort_keys=True), json.dumps(promotion_policy, ensure_ascii=False, sort_keys=True), max_runtime_seconds, max_attempts, created_by, now, now),
        )
        return dict(self.template_by_id(cursor.lastrowid))

    def quota(self, subject_type: str, subject_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_quotas WHERE subject_type=? AND subject_key=?", (subject_type, subject_key)).fetchone()

    def upsert_quota(self, *, subject_type: str, subject_key: str, max_queued: int, max_running: int, daily_submissions: int, actor: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_quotas(subject_type,subject_key,max_queued,max_running,daily_submissions,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(subject_type,subject_key) DO UPDATE SET max_queued=excluded.max_queued,max_running=excluded.max_running,daily_submissions=excluded.daily_submissions,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
            (subject_type, subject_key, max_queued, max_running, daily_submissions, actor, now, now),
        )
        return dict(self.quota(subject_type, subject_key))

    def count_user_states(self, requested_by: str) -> dict[str, int]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks WHERE requested_by=? GROUP BY status", (requested_by,)).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def count_user_submissions_since(self, requested_by: str, since: str) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM compute_tasks WHERE requested_by=? AND created_at>=?", (requested_by, since)).fetchone()[0])

    def task_by_id(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.id=?", (task_id,)).fetchone()

    def task_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_tasks WHERE requested_by=? AND idempotency_key=?", (requested_by, key)).fetchone()

    def create_task(self, *, template_id: int, project_code: str, requested_by: str, parameters: dict[str, Any], parameter_digest: str, priority: int, idempotency_key: str, max_attempts: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',0,?,?,?,?)",
            (template_id, project_code, requested_by, json.dumps(parameters, ensure_ascii=False, sort_keys=True), parameter_digest, priority, idempotency_key, max_attempts, now, now, now),
        )
        return dict(self.task_by_id(cursor.lastrowid))

    def queued_candidate(self, capabilities: Iterable[str], now: str) -> sqlite3.Row | None:
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            condition = f" AND tpl.algorithm IN ({placeholders})"
            params.extend(capability_list)
        return self.connection.execute(
            "SELECT t.*,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status='queued' AND t.available_at<=?" + condition + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT 1",
            params,
        ).fetchone()

    def result_versions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_results WHERE task_id=? ORDER BY version", (task_id,)).fetchall()]

    def interventions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_interventions WHERE task_id=? ORDER BY id", (task_id,)).fetchall()]

    def add_intervention(self, *, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], batch_key: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (task_id, actor, action, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), batch_key, now),
        )

    def list_tasks(self, *, status: str | None, project_code: str | None, requested_by: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("t.status=?")
            values.append(status)
        if project_code:
            clauses.append("t.project_code=?")
            values.append(project_code)
        if requested_by:
            clauses.append("t.requested_by=?")
            values.append(requested_by)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id" + where + " ORDER BY t.priority DESC,t.created_at DESC,t.id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]

    def result_by_version(self, task_id: int, result_version: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM compute_results WHERE task_id=? AND version=?",
            (task_id, result_version),
        ).fetchone()

    def promotion_by_version(self, task_id: int, result_version: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM compute_result_promotions WHERE task_id=? AND result_version=?",
            (task_id, result_version),
        ).fetchone()

    def promotion_by_id(self, promotion_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM compute_result_promotions WHERE id=?",
            (promotion_id,),
        ).fetchone()

    def promotions(self, task_id: int) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM compute_result_promotions WHERE task_id=? ORDER BY result_version",
                (task_id,),
            ).fetchall()
        ]

    def create_promotion(self, *, task_id: int, result_version: int, diff_summary: dict[str, Any], threshold_checks: dict[str, Any], policy_snapshot: dict[str, Any], submitted_by: str, submitted_at: str, approval_expires_at: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_result_promotions(task_id,result_version,lifecycle_stage,diff_summary_json,threshold_checks_json,policy_snapshot_json,submitted_by,submitted_at,approval_expires_at,created_at,updated_at) VALUES(?,?,'candidate',?,?,?,?,?,?,?,?)",
            (task_id, result_version, json.dumps(diff_summary, ensure_ascii=False, sort_keys=True), json.dumps(threshold_checks, ensure_ascii=False, sort_keys=True), json.dumps(policy_snapshot, ensure_ascii=False, sort_keys=True), submitted_by, submitted_at, approval_expires_at, now, now),
        )
        return dict(self.promotion_by_id(cursor.lastrowid))

    def add_promotion_event(self, *, task_id: int, promotion_id: int | None, result_version: int | None, action: str, actor: str, from_stage: str, to_stage: str, summary: dict[str, Any], now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_promotion_events(task_id,promotion_id,result_version,action,actor,from_stage,to_stage,summary_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (task_id, promotion_id, result_version, action, actor, from_stage, to_stage, json.dumps(summary, ensure_ascii=False, sort_keys=True), now),
        )

    def promotion_events(self, task_id: int) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM compute_promotion_events WHERE task_id=? ORDER BY id",
                (task_id,),
            ).fetchall()
        ]

    def last_release(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM compute_release_history WHERE task_id=? ORDER BY sequence DESC,id DESC LIMIT 1",
            (task_id,),
        ).fetchone()

    def add_release(self, *, task_id: int, sequence: int, action: str, from_version: int | None, to_version: int | None, actor: str, reason: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_release_history(task_id,sequence,action,from_result_version,to_result_version,actor,reason,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (task_id, sequence, action, from_version, to_version, actor, reason, now),
        )

    def release_history(self, task_id: int) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM compute_release_history WHERE task_id=? ORDER BY sequence,id",
                (task_id,),
            ).fetchall()
        ]
