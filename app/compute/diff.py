from __future__ import annotations

from typing import Any

from app.core.errors import ValidationError

_NUMBER = (int, float)


def compare_results(
    base_result: dict[str, Any],
    base_metrics: dict[str, Any],
    candidate_result: dict[str, Any],
    candidate_metrics: dict[str, Any],
) -> dict[str, Any]:
    """比较两个结果版本，产出结构化差异与关键指标对比。

    - structural：结果对象的键集合变化（added/removed）以及叶子路径的值变化；
    - metrics：模板可在晋级策略中声明的关键指标逐项对比；
    - 所有差异都按稳定路径排序，保证摘要可追溯、可复现。
    """
    structural = _diff_values(base_result, candidate_result, prefix="$")
    metric_keys = sorted(set(base_metrics) | set(candidate_metrics))
    metrics: dict[str, Any] = {}
    for key in metric_keys:
        in_base = key in base_metrics
        in_candidate = key in candidate_metrics
        old_value = base_metrics.get(key)
        new_value = candidate_metrics.get(key)
        entry: dict[str, Any] = {
            "base": old_value,
            "candidate": new_value,
            "changed": old_value != new_value,
        }
        if in_base and in_candidate and isinstance(old_value, _NUMBER) and isinstance(new_value, _NUMBER) and not isinstance(old_value, bool) and not isinstance(new_value, bool):
            delta = float(new_value) - float(old_value)
            entry["delta"] = delta
            entry["relative_delta"] = None if float(old_value) == 0 else delta / abs(float(old_value))
        metrics[key] = entry
    return {
        "structural": {
            "added_paths": sorted(item["path"] for item in structural if item["type"] == "added"),
            "removed_paths": sorted(item["path"] for item in structural if item["type"] == "removed"),
            "changed_paths": sorted(item["path"] for item in structural if item["type"] == "changed"),
            "details": structural,
        },
        "metrics": metrics,
    }


def _diff_values(base: Any, candidate: Any, *, prefix: str) -> list[dict[str, Any]]:
    if isinstance(base, dict) and isinstance(candidate, dict):
        changes: list[dict[str, Any]] = []
        for key in sorted(set(base) | set(candidate)):
            path = f"{prefix}.{key}"
            if key not in base:
                changes.append({"path": path, "type": "added", "base": None, "candidate": candidate[key]})
            elif key not in candidate:
                changes.append({"path": path, "type": "removed", "base": base[key], "candidate": None})
            else:
                changes.extend(_diff_values(base[key], candidate[key], prefix=path))
        return changes
    if base != candidate:
        return [{"path": prefix, "type": "changed", "base": base, "candidate": candidate}]
    return []


def evaluate_thresholds(policy: dict[str, Any], diff_summary: dict[str, Any]) -> dict[str, Any]:
    """依据模板晋级策略评估候选版本是否达到发布阈值。

    策略形如::

        {
          "review_required": true,
          "approval_ttl_seconds": 86400,
          "metric_thresholds": {
            "rmse": {"max": 0.05, "direction": "minimize"},
            "score": {"min": 0.9}
          },
          "max_relative_metric_delta": {"rmse": 0.1},
          "max_added_paths": 0
        }

    结构路径阈值只在存在已发布基线版本时生效；首个发布版本不做结构约束。
    """
    metrics_diff = diff_summary.get("metrics", {})
    checks: dict[str, Any] = {}
    passed = True
    has_baseline = diff_summary.get("base_result_version") is not None

    for name, rule in (policy.get("metric_thresholds") or {}).items():
        entry = metrics_diff.get(name)
        check = _evaluate_metric_rule(name, rule, entry)
        checks[name] = check
        passed = passed and check["passed"]

    if has_baseline:
        for name, bound in (policy.get("max_relative_metric_delta") or {}).items():
            entry = metrics_diff.get(name)
            relative_delta = None if entry is None else entry.get("relative_delta")
            ok = relative_delta is not None and abs(relative_delta) <= float(bound)
            checks[f"{name}.relative_delta"] = {
                "rule": {"max_abs_relative_delta": float(bound)},
                "actual": relative_delta,
                "passed": bool(ok),
            }
            passed = passed and bool(ok)

        if policy.get("max_added_paths") is not None:
            amount = len(diff_summary.get("structural", {}).get("added_paths", []))
            bound = int(policy["max_added_paths"])
            checks["structural.added_paths"] = {"rule": {"max": bound}, "actual": amount, "passed": amount <= bound}
            passed = passed and amount <= bound
        if policy.get("max_removed_paths") is not None:
            amount = len(diff_summary.get("structural", {}).get("removed_paths", []))
            bound = int(policy["max_removed_paths"])
            checks["structural.removed_paths"] = {"rule": {"max": bound}, "actual": amount, "passed": amount <= bound}
            passed = passed and amount <= bound

    return {"passed": passed, "checks": checks, "has_baseline": has_baseline}


def _evaluate_metric_rule(name: str, rule: dict[str, Any], entry: dict[str, Any] | None) -> dict[str, Any]:
    value = None if entry is None else entry.get("candidate")
    if not isinstance(value, _NUMBER) or isinstance(value, bool):
        return {"rule": rule, "actual": value, "passed": False, "reason": "关键指标缺失或不是数值"}
    numeric = float(value)
    ok = True
    if rule.get("min") is not None:
        ok = ok and numeric >= float(rule["min"])
    if rule.get("max") is not None:
        ok = ok and numeric <= float(rule["max"])
    direction = rule.get("direction")
    if direction == "minimize" and entry is not None and entry.get("base") is not None:
        ok = ok and numeric <= float(entry["base"])
    elif direction == "maximize" and entry is not None and entry.get("base") is not None:
        ok = ok and numeric >= float(entry["base"])
    if direction not in {None, "minimize", "maximize"}:
        raise ValidationError(f"关键指标 {name} 的 direction 只能是 minimize 或 maximize")
    return {"rule": rule, "actual": numeric, "passed": ok}


def normalize_policy(raw: dict[str, Any]) -> dict[str, Any]:
    """校验并规范化模板晋级策略。"""
    policy = dict(raw or {})
    policy.setdefault("review_required", True)
    policy.setdefault("approval_ttl_seconds", 86400)
    ttl = int(policy["approval_ttl_seconds"])
    if ttl <= 0:
        raise ValidationError("审批有效期必须大于 0 秒")
    policy["approval_ttl_seconds"] = ttl
    thresholds = policy.get("metric_thresholds") or {}
    if not isinstance(thresholds, dict):
        raise ValidationError("metric_thresholds 必须是对象")
    for name, rule in thresholds.items():
        has_bound = isinstance(rule, dict) and (rule.get("min") is not None or rule.get("max") is not None or rule.get("direction"))
        if not has_bound:
            raise ValidationError(f"关键指标 {name} 至少需要声明 min、max 或 direction")
    for key in ("max_relative_metric_delta",):
        value = policy.get(key)
        if value is not None and not isinstance(value, dict):
            raise ValidationError(f"{key} 必须是指标名到阈值的映射")
    return policy
