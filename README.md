# 科学计算任务运营服务

这是一个面向科研平台、实验室和计算中心的 Python 后端，使用 FastAPI 与 SQLite 管理参数模板、计算任务提交、优先级排队、工作者领取、取消、失败重试、租约恢复、用户配额、结果版本和管理员人工干预记录。服务同时保留用户、角色、会话和审计等基础能力，所有运行数据都在单个本地数据库文件中，不需要另行部署数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 参数模板：保存参数类型、必填项、数值范围、默认值、最大运行时间和最大尝试次数。
- 任务提交：根据模板校验参数，使用用户与幂等键避免重复创建，并保存项目、提交人和输入摘要。
- 排队领取：按优先级和进入队列的顺序分配任务，工作者可声明算法能力并获得有期限的租约。
- 执行回执：工作者可以续租、提交结果或报告失败；可重试错误使用确定的退避时间重新排队。
- 失败恢复：租约过期后可由恢复入口将任务重新排队，达到最大尝试次数的任务转为失败。
- 配额控制：可保存用户、角色或项目的排队数、运行数和每日提交上限；当前提交路径执行用户配额。
- 结果版本：每次成功回执保存不可变结果、指标摘要和内容摘要，任务指向当前结果版本。
- 同输入重算：已结束任务可通过重算入口复用同一批输入重新排队，产生新的结果版本而不是新任务。
- 结果晋级：每个结果版本依次经历候选（candidate）、已验证（validated）、已发布（published）、已撤回（withdrawn）。提交时按模板晋级策略比较候选版本与当前发布版本的关键指标和结构化差异，只有全部阈值通过且由授权复核人（不得是提交人本人）在审批有效期内确认，才能在同一事务内原子切换 `published_result_version`。
- 撤回与回溯：撤回只作用于当前发布版本，自动恢复到发布历史中最近一个仍可用的版本（没有则清空发布指针），结果、晋级记录、发布历史和事件全部保留；查询接口同时返回计算最新版本与对外发布版本。
- 人工干预：取消、人工重试、优先级调整和批量操作均保留操作者、原因、前后状态和批次标识。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`。可以复制 `.env.example` 并通过 `TOWNSHIP_DATABASE_PATH` 指定其他本地路径。

## 数据库初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

计算任务摘要位于 `/api/compute/summary`，模板、配额、提交、领取、回执和人工操作接口统一使用 `/api/compute` 前缀。

### 结果晋级流程

模板可通过 `promotion_policy` 声明发布门槛（均可选，未声明的约束不生效）：

```json
{
  "reviewers": ["reviewer-1"],
  "approval_ttl_seconds": 86400,
  "metric_thresholds": {"rmse": {"max": 0.1, "direction": "minimize"}},
  "max_relative_metric_delta": {"rmse": 0.2},
  "max_added_paths": 0,
  "max_removed_paths": 0
}
```

结果版本的晋级与查询接口（`{v}` 为结果版本号）：

- `POST /api/compute/tasks/{id}/recompute`：对同一批输入用新算法重算，产生新版本。
- `GET  /api/compute/tasks/{id}/results/{v}/diff`：候选版本相对当前发布版本的结构化差异与关键指标对比。
- `POST /api/compute/tasks/{id}/results/{v}/promotions/submit`：提交晋级，立即按模板阈值校验，未达标保留候选并返回未通过项。
- `POST /api/compute/tasks/{id}/results/{v}/promotions/review`：授权复核人批准（→ validated）或驳回（→ candidate，需重新提交）；提交人不能复核自己的版本。
- `POST /api/compute/tasks/{id}/results/{v}/promotions/publish`：在审批有效期内原子切换发布版本；旧审批不能覆盖后来已发布的更新版本。
- `POST /api/compute/tasks/{id}/results/{v}/promotions/withdraw`：撤回当前发布版本，恢复到上一可用版本且不删除历史。
- `GET  /api/compute/tasks/{id}/versions`：同时返回计算最新版本（`latest_result_version`）与对外发布版本（`published_result_version`）。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、取消、人工重试、批量操作和租约恢复，并保留身份与既有科学计算模块的回归用例。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路。

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本和人工干预
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
