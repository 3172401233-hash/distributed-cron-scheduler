# distributed-cron-scheduler

Python 分布式 cron 调度核心。时间推进（`Clock`）和持久化（`Storage`）全部通过
接口注入，因此同一份代码既可以在测试里用 `ManualClock` + `InMemoryStorage`
做确定性验证，也可以在生产里替换成数据库后端与系统时钟。

## 架构

```
            ┌─────────── 每个节点一个 Scheduler ───────────┐
 plan_tick  │  选主(LeaderLease, TTL) → cron 计划 → 实例落库 │
 ─────────▶ Storage (Job / JobState / JobInstance / 锁)      │
 worker_tick│  原子领取租约 → 心跳续租 → 执行 → fencing 回写  │
            └────────────────────────────────────────────────┘
```

- `distributed_cron/cron.py`：零依赖五段式 cron 解析器（含 `*/n`、列表、范围、
  名称、`@hourly` 等宏，DOM/DOW 遵循 Vixie cron 的 OR 语义）。
- `distributed_cron/planner.py`：每轮根据 cron 表达式计算计划时间，应用
  misfire 上限与 coalesce 策略，并通过水位线（`JobState.last_planned`）去重。
- `distributed_cron/storage.py`：存储协议 + 线程安全的内存实现。租约与
  fencing token 保证“同一计划实例最多一个有效 owner”。
- `distributed_cron/runner.py`：`ThreadPoolTaskRunner`（真实线程 + 心跳线程 +
  超时）与 `FakeRunner`（确定性测试）。
- `distributed_cron/scheduler.py`：调度器，`plan_tick` 仅 leader 执行，
  `worker_tick` 所有节点都可执行。

## 快速开始

```python
from datetime import timezone
from distributed_cron import (
    CoalescePolicy, InMemoryStorage, Job, Scheduler, SystemClock,
    ThreadPoolTaskRunner,
)

def send_report(context):
    print("running", context.instance.id, "attempt", context.attempt)

storage = InMemoryStorage()
scheduler = Scheduler(
    storage=storage,
    clock=SystemClock(),
    runner=ThreadPoolTaskRunner(timeout_seconds=120),
    worker_id="node-1",
)
scheduler.add_job(Job(
    id="daily-report",
    cron="0 9 * * *",
    func=send_report,
    timezone=timezone.utc,
    max_misfires=3,
    coalesce=CoalescePolicy.ON_COUNT,
    max_attempts=3,
    retry_backoff_seconds=30,
    lease_seconds=30,
))
scheduler.start()   # 后台线程循环 plan_tick + worker_tick
```

也可以不用后台线程，在外部循环里显式调用 `scheduler.plan_tick()` 和
`scheduler.worker_tick()`（测试与嵌入式部署推荐这种方式）。

## 时区语义

- `Job.timezone` 决定 cron 表达式按哪个时区解释；`JobInstance.fire_time`
  始终以带时区的 UTC 瞬间存储，实例 ID 为 `job_id:<UTC ISO 时间>`，因此
  DST 回拨等本地时间歧义不会产生重复实例。
- 所有 `Clock.now()` 必须返回 timezone-aware 的 UTC 时间；`ManualClock`
  会拒绝 naive datetime。
- 计划计算在任务本地时区逐分钟推进，再转换回 UTC，DST 切换由时区库处理。

## Misfire（错过触发）语义

每轮 tick 扫描 `(last_planned, now]` 区间内的全部计划时间，然后两道闸门保证
**永远不会无限追赶**：

1. `max_misfires`：单个 job 每轮最多补跑的实例数硬上限；扫描迭代次数也有
   安全上限（`due_fires(max_iterations=...)`）。
2. `coalesce` 策略决定如何折叠积压：
   - `ALWAYS`（默认）：任何积压都合并为 1 个实例，取最近一次计划时间。
   - `ON_COUNT`：积压数不超过 `max_misfires` 时逐个补跑；超过则合并为 1 个。
   - `NEVER`：逐个补跑，但仍受 `max_misfires` 截断，超出的旧触发直接丢弃。

无论是否补跑，水位线都会推进到扫描到的最新计划时间，因此被 coalesce 或丢弃
的触发在下一轮不会复活。节点时钟慢几分钟后一次性追上真实时间时，也只会产生
有界的补跑量而不是连续补跑风暴。

### 时钟回拨

`now < last_planned` 时该 job 本轮直接跳过并记录告警，不重新生成任何实例，
已落库实例的幂等 ID 也会拦截重复创建。

## 租约、fencing 与重试语义

- **领取**：`claim_instance` 是原子操作。`PENDING` 实例可直接领取；
  `RUNNING` 实例只有在 `lease_expires_at <= now`（租约过期）时才能被其他
  worker 接管。
- **心跳**：执行期间 runner 按 `min(heartbeat_interval, lease/3)` 的频率调用
  `renew_lease` 续租；长任务只要心跳正常就不会被误判下线。
- **Fencing token**：每次领取（含过期接管）都会让该实例的 token 单调递增。
  续租、完成、失败回写都必须携带 owner 与当前 token，旧 owner 的一切写操作
  都会被存储层拒绝。这保证了即使旧节点发生 GC 停顿或网络分区后“复活”，
  **同一计划实例在任意时刻最多只有一个有效 owner**。
- **重试**：`fail_instance` 在 `attempts < max_attempts` 时把实例重新置为
  `PENDING`，并设置 `not_before = now + retry_backoff_seconds * attempts`
  （线性退避）；达到上限后置为终态 `FAILED`，不再被领取。
- **超时**：Python 无法强杀线程，超时后 runner 停止心跳，租约随即过期、可被
  其他节点接管；孤儿线程即使稍后返回，其迟到的完成/失败回写也会被 fencing
  校验拒绝，不会造成双写。
- **主节点失联**：计划能力由带 TTL 的 leader 锁保护。leader 宕机后，锁到期，
  其他节点凭持久化的水位线无缝接管，不会重复计划已生成的实例。

## 实现自定义存储后端

实现 `distributed_cron.storage.Storage` 协议即可（Postgres 可用行锁/
`SELECT ... FOR UPDATE`，KV 存储可用 CAS 事务）。后端必须保证：

- `claim_instance` 对同一实例的并发调用只有一个成功；
- fencing token 对同一实例严格单调递增；
- `compare_and_set_planned` 提供基于期望值的 CAS；
- 所有时间字段以 UTC 持久化并保留时区信息。

## 测试

```bash
python -m unittest
```

覆盖场景（共 40 个用例）：cron 解析边界、misfire/coalesce 三种策略、
时钟回拨、长停摆补跑上限、主节点失联接管、租约过期接管与旧 owner fencing、
同一时刻多 worker 竞争（真实线程 barrier）、心跳续租、任务执行超时、
重试退避与终态失败，以及两个真实调度节点的端到端单 owner 验证。
