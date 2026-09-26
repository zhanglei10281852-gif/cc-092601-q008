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
- 结果晋级：新结果先进入候选状态，比较其与当前发布版本的关键指标与结构化差异，满足模板定义的发布阈值并由具备 `compute.review` 权限的复核人确认后，才能原子切换对外发布版本；提交人不能复核自己的结果，过期审批不能覆盖后来晋级的结果，撤回会恢复到上一可用版本且保留全部历史。
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

## 结果版本晋级

模型团队用新算法重算同一批输入时（对已成功的任务调用 `retry` 重新排队），新结果版本以 `candidate` 状态落地，不会自动替代当前科研结论。版本状态机为：候选 `candidate` → 已验证 `validated` → 已发布 `published`，以及已撤回 `retracted`。

- `GET /api/compute/tasks/{id}/results`：同时返回计算最新版本 `latest_result_version` 与对外发布版本 `published_result_version`，以及每个版本的状态。
- `GET /api/compute/tasks/{id}/results/compare?candidate_version=N&base_version=M`：比较两个版本的关键指标变化（含差值）与结构化差异（新增、删除、修改的字段路径），并按模板阈值给出评估结果；`base_version` 缺省时对应当前发布版本。
- `POST /api/compute/tasks/{id}/results/{version}/validate`：复核人确认。模板可在 `review_thresholds.metrics` 中为数值指标声明 `direction`（higher/lower）、`min`、`max` 和 `max_regression`（相对当前发布版本允许的最大退步）；阈值不满足时拒绝并留痕。
- `POST /api/compute/tasks/{id}/results/{version}/publish`：原子切换对外发布版本。发布时校验验证审批的基线版本仍然有效，审批之后有更新的版本晋级时本次发布会被拒绝，需要重新验证。
- `POST /api/compute/tasks/{id}/results/{version}/retract`：撤回当前发布版本，发布指针在同一事务内恢复到上一可用版本，历史版本与审批记录全部保留。

验证、发布、撤回（含被阈值拒绝的尝试）都会写入 `compute_result_reviews`，记录操作者、理由、基线版本、差异摘要和阈值评估报告，可通过任务详情接口追溯。复核人必须是拥有 `compute.review` 权限的活跃系统用户，且不能是任务提交人或结果提交人。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、版本晋级（阈值、权限、过期审批、撤回恢复）、取消、人工重试、批量操作和租约恢复，并保留身份与既有科学计算模块的回归用例。

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
