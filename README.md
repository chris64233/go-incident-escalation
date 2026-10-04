# go-incident-escalation

值班事件升级服务：事件开始后按冻结的升级策略，分步、定时地通知不同目标；
值班人员确认后停止升级，事件解决后不再确认或通知。
所有状态（事件、定时器、通知 outbox、投递回执表、幂等请求表、历史）一次性原子落盘，
**进程重启后会从持久化的定时器继续执行**。

开发环境：Go 1.23.0，仅依赖标准库。

运行测试：

    go test -race ./...

## 核心模型与保证

- **策略（Policy）**：由按顺序执行的若干 **步骤（Step）** 组成，每步有
  `WaitBefore`（相对上一动作触发的等待时长，第 0 步相对事件开始时间）和
  一个或多个通知 `Targets`。
- **创建即冻结**：创建事件时把当时策略的步骤快照复制进事件（`FrozenSteps`），
  之后更新策略不影响任何存量事件。
- **持久化定时器**：每个进行中的事件至多有一个定时器，指向下一步；
  事件创建/推进与定时器登记在**同一次原子写入**中完成。重启后重新打开存储即可恢复。
- **通知意图去重**：通知意图先写入事务性 outbox，键
  `(事件 ID, 步骤下标, 目标)` 全局去重——同一事件、同一步骤、同一目标只有一次意图，
  重复扫描或崩溃恢复都不会产生重复 outbox。
- **一次扫描只推进一步**：即使定时器已落后真实时间很多（例如重启后很久才恢复），
  一次 `ProcessDue` 对每个事件最多触发一步，并按“实际触发时间 + 该步等待”
  重新登记下一步定时器，绝不会一次跨过多个步骤。
- **确认 / 解决 / 计时触发的竞态**：所有变更在同一把互斥锁内完成并原子落盘，
  状态机以事件状态为准：
  - 确认（acknowledged）→ 删除定时器，升级终止，不再产生通知；
  - 解决（resolved）→ 删除定时器；解决之后确认返回 `ErrIncidentResolved`，
    计时触发也不会再产生任何 outbox；
  - 并发交错最终只会收敛为一个合法状态，通知步骤始终是从 0 开始的连续前缀。
- **按外部请求号幂等**：事件创建、确认、解决都必须带 `RequestID`。
  同号同内容重放返回原结果；同号异内容（或跨操作类型复用请求号）返回
  `ErrConflict`。幂等表同样持久化，重启后仍然生效。
- **原子持久化**：采用“临时文件 + fsync + 原子 rename（+ 目录 fsync）”写整份快照，
  事件状态、定时器、outbox 要么全部生效、要么全部不生效。

## API 概览

| 能力 | 入口 |
| --- | --- |
| 策略配置 | `Store.CreatePolicy` / `UpdatePolicy` / `GetPolicy` / `ListPolicies` |
| 创建事件（冻结策略、登记定时器） | `Store.CreateIncident` |
| 确认（停止升级） | `Store.Acknowledge` |
| 解决（禁止确认与通知） | `Store.Resolve` |
| 到期推进（后台循环或手工调用） | `Store.ProcessDue(now)` |
| 查看定时器 | `Store.NextDue` / `Store.Timer` |
| 通知 outbox | `Store.PendingOutbox` / `ListOutbox` / `DueOutbox(now)` |
| 投递状态推进 | `Store.MarkDispatched` / `MarkAttemptFailed` / `MarkDelivered` |
| 渠道回执 | `Store.ReportReceipt(ReceiptRequest, now)` |
| 重试策略 | `Store.SetRetryPolicy` / `RetryPolicyOf`（持久化） |
| 事件级投递视图 | `Store.IncidentDelivery(incidentID)` |
| 历史查询 | `Store.History(incidentID)`（空 ID 查全部） |
| 持久化 / 内存存储 | `OpenStore(path)` / `NewMemoryStore()` |
| 后台自动推进与投递 | `NewService(store, dispatcher, cfg)` + `Start` / `Stop` |

错误均为哨兵错误，用 `errors.Is` 判断：

| 错误 | 含义 |
| --- | --- |
| `ErrInvalidArgument` | 入参缺失或非法（空请求号、空步骤、空目标、负等待等） |
| `ErrNotFound` | 策略/事件/outbox 项不存在 |
| `ErrAlreadyExists` | 同名策略或事件已存在 |
| `ErrConflict` | 外部请求号被不同内容（或不同操作）复用 |
| `ErrAlreadyAcknowledged` | 新的确认请求到达时事件已确认 |
| `ErrAlreadyResolved` | 新的解决请求到达时事件已解决 |
| `ErrIncidentResolved` | 事件已解决后再确认（解决后也不再通知） |
| `ErrStaleReceipt` | 回执携带的意图版本落后于当前版本（迟到回执），状态不被改回 |
| `ErrDeliveryClosed` | 意图已进入终态（acknowledged/failed/stopped），不能再投递或重试 |

## 使用示例

```go
store, _ := incidentescalation.OpenStore("/var/lib/escalation/state.json")

store.CreatePolicy(incidentescalation.PolicyInput{
    ID:   "oncall-standard",
    Name: "标准值班升级",
    Steps: []incidentescalation.Step{
        {WaitBefore: 0,                Targets: []string{"oncall-l1"}},
        {WaitBefore: 5 * time.Minute,  Targets: []string{"oncall-l2"}},
        {WaitBefore: 10 * time.Minute, Targets: []string{"manager"}},
    },
}, time.Now())

inc, _ := store.CreateIncident(incidentescalation.CreateIncidentRequest{
    RequestID: "ext-req-001", // 外部请求号：幂等键
    PolicyID:  "oncall-standard",
    Severity:  "critical",
    Detail:    "数据库主库连接耗尽",
}, time.Now())

// 后台循环：周期扫描到期定时器并投递 outbox。
// Service 不持有易失状态——进程重启后重新 NewService(...).Start() 即接着执行。
svc := incidentescalation.NewService(store, incidentescalation.DispatcherFunc(func(item incidentescalation.OutboxItem) error {
    return notify(item.Target, item.Message) // 邮件/IM/电话
}), incidentescalation.Config{TickInterval: time.Second})
svc.Start()
defer svc.Stop()

// 值班人员确认（重复提交同一 RequestID 安全）。
store.Acknowledge(incidentescalation.AckRequest{
    RequestID: "ack-001", IncidentID: inc.ID, AcknowledgedBy: "oncall-l1",
}, time.Now())
```

更多端到端用法见 `example_test.go`。

## 投递状态与回执

每条通知意图（`OutboxItem`）有自己的投递状态机，与事件状态、历史在同一次
原子写入中落盘；重试只更新本条意图，**绝不会生成第二条意图**：

| 状态 | 含义 |
| --- | --- |
| `pending` | 意图已生成，等待（首次或重试）交给渠道 |
| `dispatched` | 已交给渠道，等待渠道回执 |
| `acknowledged` | 渠道明确接收（终态） |
| `failed` | 最终投递失败：尝试次数达到 `RetryPolicy.MaxAttempts`（终态） |
| `stopped` | 事件已确认/解决，未完成的投递被停止（终态，`StopReason` 记录原因） |

意图同时记录 `Attempts`（尝试次数）、`LastError`（最近错误）、
`LastAttemptAt`（最后尝试时间）、`NextAttemptAt`（下次可重试时间，持久化）、
`AckedAt`（渠道确认时间）与 `Version`（意图版本，每次交给渠道递增）。

**渠道回执**（`ReportReceipt`）按 `(事件, 步骤, 目标, 意图版本)` 匹配意图：

- 回执可重复、乱序、或在进程重启后到达：`ReceiptID` 幂等，
  相同回执重放返回第一次的处理结果，同号异内容返回 `ErrConflict`；
- 版本落后于当前意图的迟到回执返回 `ErrStaleReceipt`，状态不被改回；
- 已 `acknowledged` 之后到达的失败回执被忽略（记入历史），不会降回失败；
- 事件确认/解决时，所有 `pending`/`dispatched` 意图原子地变为 `stopped`
  并留下停止原因（`incident_acknowledged` / `incident_resolved`），
  之后到达的回执不再生效。

**重试策略**（`RetryPolicy`，持久化）：`MaxAttempts`（最大尝试次数，默认 3）、
`Backoff`（失败退避，默认 0）、`ReceiptTimeout`（等待回执超时，默认 1 分钟，
超时视为失败尝试并重新入队）。待重试意图完全由持久化的 `NextAttemptAt` 驱动，
进程重启后 `DueOutbox` / 后台 `Service` 会接着处理。

**事件级投递视图**：`Store.IncidentDelivery(id)` 返回事件本体、
`EscalationLevel`（已触发的升级步骤数）以及每个通知目标的最终投递结果
（状态、尝试次数、最近错误、确认时间、停止原因）。
每次投递状态变化都会向事件历史追加一条带原因的记录
（`delivery_dispatched` / `delivery_retry_scheduled` / `delivery_acknowledged` /
`delivery_failed` / `delivery_stopped` / `receipt_ignored`）。

## Outbox 投递语义

`Dispatcher` 为 **at-least-once**：若进程在“已通知下游、尚未标记 delivered”之间崩溃，
重启后该意图会再次投递。下游通知渠道应按 `OutboxItem.ID`
（等价于 `(事件, 步骤, 目标)`）做幂等。投递失败的意图按重试策略退避后
自动重试（达到上限进入 `failed` 终态），不阻塞其它意图。

## 持久化格式

单个 JSON 快照文件，包含策略、事件（含冻结步骤与进度）、定时器表、outbox
（含投递状态与重试时间）、通知意图去重集合、外部请求幂等表、回执幂等表、
重试策略与历史。写入采用同目录临时文件 rename，
崩溃时只会保留上一份完整文件，不会出现半截状态。需要更强扩展性时，
可将 `Store` 内部替换为带事务的数据库实现，领域语义（同锁内推进 + 意图去重）不变。

## 测试覆盖

- 策略校验与增改查、版本递增；
- 创建事件冻结策略，事后改策略不影响存量事件；
- 创建/确认/解决按请求号幂等，同号异内容报冲突，请求号不能跨操作复用；
- 到期推进每次恰好一步、落后多步不跨步、重复扫描不产生重复 outbox、
  下一步定时器按实际触发时间重登记；
- 确认后删定时器且不再通知；解决后确认被拒、不再通知；
- 16 路 goroutine 并发“确认 + 解决 + 计时推进”竞态收敛且无重复意图、无跳步；
- 同请求号并发创建只产生一个事件；
- 落盘后重新 `OpenStore`：定时器、outbox、事件进度、幂等表全部恢复并继续推进；
- 后台 `Service` 循环按序投递、投递失败重试、确认后后续步骤不再触发。
- 回执乱序与迟到：低版本回执被拒（`ErrStaleReceipt`），成功后的失败回执不降状态；
- 重试上限：达到 `MaxAttempts` 进入 `failed` 终态，终态后拒绝再投递；
- 确认与回执/重试竞态：确认后待重试投递停止并留下原因，迟到回执不再生效；
- 重启恢复：退避中的待重试意图、回执幂等表、终态全部从快照恢复；
- 重复回执：重放返回首次结果且不重复写历史，同号异内容报 `ErrConflict`；
- 回执超时重新入队、事件级投递视图（升级级别 + 每个目标的最终投递结果）。
