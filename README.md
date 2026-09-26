# go-incident-escalation

值班事件升级服务：事件开始后按冻结的升级策略，分步、定时地通知不同目标；
值班人员确认后停止升级，事件解决后不再确认或通知。
所有状态（事件、定时器、通知 outbox、幂等请求表、历史）一次性原子落盘，
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
| 通知 outbox | `Store.PendingOutbox` / `ListOutbox` / `MarkDelivered` |
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

## Outbox 投递语义

`Dispatcher` 为 **at-least-once**：若进程在“已通知下游、尚未标记 delivered”之间崩溃，
重启后该意图会再次投递。下游通知渠道应按 `OutboxItem.ID`
（等价于 `(事件, 步骤, 目标)`）做幂等。投递失败的意图保持 `pending`，
后续周期自动重试，不阻塞其它意图。

## 持久化格式

单个 JSON 快照文件，包含策略、事件（含冻结步骤与进度）、定时器表、outbox、
通知意图去重集合、外部请求幂等表与历史。写入采用同目录临时文件 rename，
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
