# go-incident-escalation

值班事件升级服务：事件开始后按冻结的升级策略，分步、定时地通知不同目标；
值班人员确认后停止升级，事件解决后不再确认或通知。
**班次切换时可执行值班交接（handoff）**：未解决事件及其尚未派发的升级通知
整体移交给新值班班组继续处理，交接期间不丢失处理责任。
所有状态（事件、定时器、通知 outbox、交接记录、幂等请求表、历史）一次性原子落盘，
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
- **值班交接（Handoff）**：`Store.Handoff(from, to)` 在同一次原子写入中完成
  整个班次切换，保证未解决事件在新旧班组之间不掉责任：
  - **冻结待移交清单**：交接生效瞬间只有仍处于 `firing` 且责任班组为
    `FromTeam` 的事件进入清单；已确认或已解决的事件不再移交（负责人不变）。
  - **已发通知保留原记录，未派发通知只能由一方继续**：outbox 中已 delivered
    的通知保持原记录与原投递责任方不改写；pending 通知的 `OwnerTeam` 原子改写
    为新班组——交接提交后一条意图只有一个持久化投递责任方，旧班组立即停止
    开始新的派发，新班组无缝接手（交接边界上已取走的在途意图仍按
    at-least-once 处理，见下文「Outbox 投递语义」，由下游幂等兜底）。
  - **交接通知**：每个移交事件产生一条发给新班组的 handoff 通知（与升级通知
    使用不同的去重域，按 `(交接通知, 事件, 新班组)` 去重），保证新班组接得住手。
  - **升级不中断**：定时器与升级进度原样保留，交接后继续按原计划到期推进，
    新触发的升级通知归属新班组。
  - **确认/解决/交接并发只形成一个结果**：三者共用同一把锁，冻结清单以落盘
    瞬间的事件状态为准——交接先赢则事件进清单并换负责人（之后的确认/解决仍由
    新班组负责），确认/解决先赢则事件留在原班组、不进清单，不存在“既移交又未移交”。
  - **重复交接幂等**：同一 `RequestID` 重放返回原交接记录；事件已不属于
    `FromTeam` 时新交接冻结到空清单，不会再次更换负责人或重复产生通知。
- **按班组投递**：`Config.Team` 指定 Service 实例代表的值班班组，派发循环只取
  `OwnerTeam` 为本班组的 pending 意图（`PendingOutboxForTeam`）；不使用班组
  概念（事件未设 `AssignedTeam`、Service 未设 `Team`）时行为与旧版本完全一致。
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
| 值班交接（冻结清单、换负责人、转交 pending、发交接通知） | `Store.Handoff` / `GetHandoff` / `ListHandoffs` |
| 到期推进（后台循环或手工调用） | `Store.ProcessDue(now)` |
| 查看定时器 | `Store.NextDue` / `Store.Timer` |
| 通知 outbox | `Store.PendingOutbox` / `PendingOutboxForTeam` / `ListOutbox` / `MarkDelivered` |
| 历史查询 | `Store.History(incidentID)`（空 ID 查全部） |
| 持久化 / 内存存储 | `OpenStore(path)` / `NewMemoryStore()` |
| 后台自动推进与投递 | `NewService(store, dispatcher, cfg)` + `Start` / `Stop` |

错误均为哨兵错误，用 `errors.Is` 判断：

| 错误 | 含义 |
| --- | --- |
| `ErrInvalidArgument` | 入参缺失或非法（空请求号、空步骤、空目标、负等待等） |
| `ErrNotFound` | 策略/事件/outbox 项/交接记录不存在 |
| `ErrAlreadyExists` | 同名策略、事件或交接 ID 已存在 |
| `ErrConflict` | 外部请求号被不同内容（或不同操作，含交接）复用 |
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

班次切换时执行交接（同样按 `RequestID` 幂等，重复提交安全）：

```go
h, err := store.Handoff(incidentescalation.HandoffRequest{
    RequestID: "handoff-2026-03-30-day-to-night",
    FromTeam:  "team-day",
    ToTeam:    "team-night",
}, time.Now())
// h.IncidentIDs 即交接生效瞬间冻结下来的未解决事件清单。

// 两个班组各跑一个带 Team 身份的 Service；交接落盘后，旧班组的派发循环
// 立即不再取走 pending 意图，新班组的派发循环接着派发（含交接通知）。
nightSvc := incidentescalation.NewService(store, nightDispatcher, incidentescalation.Config{
    Team: "team-night", TickInterval: time.Second,
})
nightSvc.Start()
```

更多端到端用法见 `example_test.go`（`Example` 与 `ExampleHandoff`）。

## Outbox 投递语义

`Dispatcher` 为 **at-least-once**：若进程在“已通知下游、尚未标记 delivered”之间崩溃，
重启后该意图会再次投递。下游通知渠道应按 `OutboxItem.ID`
（等价于 `(事件, 类型, 步骤, 目标)`）做幂等。投递失败的意图保持 `pending`，
后续周期由**当前归属班组**自动重试，不阻塞其它意图。

交接与投递责任的衔接完全靠持久化字段，不依赖进程内状态：每条意图带有
`OwnerTeam`，交接原子改写全部 pending 意图的归属。交接提交之后，只有新班组
的扫描（`PendingOutboxForTeam`）能取到这些意图，旧班组不会再开始新的投递，
因此“继续处理”的责任方始终只有一方。

需要注意一个与崩溃重投同源的边界：旧班组恰好在交接提交**之前**取到意图副本、
在提交**之后**才投递，而新班组在旧班组 `MarkDelivered` 之前也扫描到了它，
两个班组可能各投递一次。这与“已投递、未标记即崩溃”一样属于 at-least-once，
交接不会也无法在此提供 exactly-once；下游通知渠道按 `OutboxItem.ID`
幂等即可去重。要进一步收窄窗口，可在投递前增加持久化的 in-flight 租约，
领域语义（单一归属 + 下游幂等）不变。

## 持久化格式

单个 JSON 快照文件，包含策略、事件（含冻结步骤、进度、责任班组与交接轨迹）、
交接记录表、定时器表、outbox（含意图类型与投递归属班组）、通知意图去重集合、
外部请求幂等表与历史。写入采用同目录临时文件 rename，崩溃时只会保留上一份
完整文件，不会出现半截状态。旧版本快照可直接加载：缺少的交接字段按零值处理、
旧 outbox 项补认为升级通知。需要更强扩展性时，可将 `Store` 内部替换为带
事务的数据库实现，领域语义（同锁内推进/交接 + 意图去重）不变。

## 测试覆盖

- 策略校验与增改查、版本递增；
- 创建事件冻结策略，事后改策略不影响存量事件；
- 创建/确认/解决/交接按请求号幂等，同号异内容报冲突，请求号不能跨操作复用；
- 到期推进每次恰好一步、落后多步不跨步、重复扫描不产生重复 outbox、
  下一步定时器按实际触发时间重登记；
- 确认后删定时器且不再通知；解决后确认被拒、不再通知；
- 16 路 goroutine 并发“确认 + 解决 + 计时推进”竞态收敛且无重复意图、无跳步；
- 同请求号并发创建只产生一个事件；
- 交接生效瞬间冻结清单：已确认/已解决/非下班组事件不移交，firing 事件换负责人；
- 交接保留已投递通知原记录，pending 通知与交接通知转交新班组，定时器继续推进；
- 重复交接（同号重放与同向再次交接）不再次换人、不重复通知；多跳交接链式跟随；
- 16 路 goroutine 并发“确认 + 解决 + 计时推进 + 交接”只形成一个交接结果，
  冻结清单、归属、轨迹三者自洽，无重复意图；
- 落盘后重新 `OpenStore`：定时器、outbox、交接记录、归属班组、幂等表全部恢复，
  且新班组接着投递、升级继续；
- 后台 `Service` 循环按班组取件：旧班组投递失败的 pending 意图在交接后由
  新班组投递成功，旧班组交接后不再派发；投递失败重试、确认后后续步骤不再触发。
