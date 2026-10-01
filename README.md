# go-incident-escalation

值班事件升级服务：事件开始后按冻结的升级策略，分步、定时地通知不同目标；
值班人员确认后停止升级，事件解决后不再确认或通知。
支持班次切换时的值班交接，以及维护窗口内对升级通知的临时抑制——
抑制不吞掉真正需要升级的事件，窗口结束后按事件最新级别把未处理的级别补回来。
所有状态（事件、定时器、通知 outbox、幂等请求表、交接单、抑制规则、历史）一次性原子落盘，
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
- **值班交接（Handoff）**：交接生效瞬间冻结“FromGroup 名下仍 firing 的事件”
  清单，在同一次原子写入中把事件负责人、尚未派发的意图（含抑制中的 held
  意图）改派给新班组，并为每个移交事件产生一条交接通知；已投递通知保留原
  记录，已确认/已解决事件不移交。同一事件多次交接时，每一次的接手班组与
  时间都保留在事件上（`Incident.Handoffs`）。
- **维护窗口抑制（Suppression）**：一条规则 = 事件范围（事件 ID / 策略 /
  当前级别，条件为“与”，空范围覆盖全部）+ 有效时间 `[ValidFrom, ValidUntil)`
  + 解除条件（`manual` / `window_end` / `on_resolve`）+ 抑制原因。
  规则不溯及既往：只影响规则生效之后触发的升级，已经 pending 的照常投递。
- **抑制不吞事件、按最新级别补发**：窗口内升级步骤照常触发，但通知意图记为
  `held`（带规则 ID 与原因），任何班组都拿不到、不派发；同时事件级别
  （severity）变化始终记录、持续可见。级别一旦变化到全部抑制范围之外
  （真正需要升级），held 立即恢复投递；窗口/规则解除时对剩余 held 按事件
  **最新级别与状态**裁决：仍 firing 才补发为 `suppression_resume`，已确认/
  解决则撤销（`canceled`）。每一级从头到尾最多形成一次有效通知。
- **重复规则显式拒绝**：规则内容身份为“事件范围 + 抑制原因”。内容相同的
  生效规则即使解除时间或解除条件不同，也返回 `ErrAlreadyExists`，错误信息
  中带已有规则 ID 与解除时间，绝不静默延长或覆盖原窗口；原规则解除后同内容
  规则才能再次创建。
- **综合查询视图**：`GetIncidentView(id, now)` 一次给出事件本体（含完整级别
  轨迹与每次交接的接手人/时间）、每一级升级的状态（pending/held/delivered/
  canceled/补发）与抑制原因、以及此刻仍覆盖该事件的生效规则。
- **原子持久化**：采用“临时文件 + fsync + 原子 rename（+ 目录 fsync）”写整份快照，
  事件状态、定时器、outbox 要么全部生效、要么全部不生效。

## API 概览

| 能力 | 入口 |
| --- | --- |
| 策略配置 | `Store.CreatePolicy` / `UpdatePolicy` / `GetPolicy` / `ListPolicies` |
| 创建事件（冻结策略、登记定时器） | `Store.CreateIncident` |
| 确认（停止升级） | `Store.Acknowledge` |
| 解决（禁止确认与通知） | `Store.Resolve` |
| 更新事件级别（轨迹记录、逃出抑制立即恢复） | `Store.UpdateSeverity` |
| 值班交接（冻结清单、改派未派发意图） | `Store.Handoff` / `GetHandoff` / `ListHandoffs` |
| 创建 / 手工解除抑制规则 | `Store.CreateSuppression` / `ReleaseSuppression` |
| 查询抑制规则 | `Store.GetSuppression` / `ListSuppressions` |
| 事件综合视图（事件+级别+交接+升级+抑制原因） | `Store.GetIncidentView(id, now)` |
| 到期推进（后台循环或手工调用） | `Store.ProcessDue(now)` |
| 查看定时器 | `Store.NextDue` / `Store.Timer` |
| 通知 outbox | `Store.PendingOutbox` / `PendingOutboxFor(group)` / `ListOutbox` / `MarkDelivered` |
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

其余操作沿用同一套哨兵错误：交接/抑制参数非法返回 `ErrInvalidArgument`；
交接单或抑制规则不存在返回 `ErrNotFound`；手工解除一条已解除的规则返回
`ErrConflict`；同内容抑制规则已在生效或指定了已存在的 ID 返回
`ErrAlreadyExists`（错误信息中带已有规则 ID 与解除时间）。

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

意图 `Status` 除 `pending` / `delivered` 外还有：

- `held`：升级已触发，但被维护窗口抑制，任何班组的 `PendingOutbox*` 都看不到；
  意图上的 `SuppressionID` / `SuppressionReason` 说明被哪条规则、因为什么抑制。
- `canceled`：解除抑制时事件已确认/解决，该级升级被永久撤销（记录保留可审计）。
- 解除抑制后补发的意图 `Kind = suppression_resume`，仍是一条普通的 `pending`
  意图，由事件当前负责班组派发。

## 值班交接

`Handoff(HandoffRequest{FromGroup, ToGroup, RequestID})` 在生效瞬间冻结
FromGroup 名下所有仍 `firing` 的事件清单（已确认/解决的不移交），并原子完成：

1. 事件 `OwnerGroup` 改为 ToGroup，事件上追加一条 `HandoffEntry`
   （同一事件多次交接时每次的接手班组与时间都保留）；
2. 这些事件所有尚未派发的意图——`pending` 与抑制中的 `held`——改派 ToGroup，
   已 `delivered`/`canceled` 的历史记录保持原样，旧班组从此无法再派发它们；
3. 为每个移交事件产生一条发给 ToGroup 的交接通知（`Kind=handoff`）。

后台 `Service` 可配置 `Config.Group`，只投递本班组名下的 pending 意图，
因此新旧两个班组的服务可以同时运行而不会重复派发。同 `RequestID` 重放返回
原交接单；对同一 FromGroup 再次交接只会冻结到（可能为空的）新清单。

## 维护窗口抑制

```go
rule, err := store.CreateSuppression(incidentescalation.CreateSuppressionRequest{
    RequestID: "mw-1",
    RuleID:    "db-reindex",
    // 范围：只抑制这两个 critical 事件；三个条件可任意组合，全空=全部事件。
    Scope: incidentescalation.SuppressionScope{
        IncidentIDs: []string{"inc-7", "inc-9"},
        Severities:  []string{"critical"},
        // PolicyID: "oncall-standard",
    },
    Reason:     "凌晨索引重建维护窗口",
    ValidFrom:  time.Now(),
    ValidUntil: time.Now().Add(2 * time.Hour),
    ReleaseCondition: incidentescalation.ReleaseWindowEnd, // manual / on_resolve
}, time.Now())
```

- 窗口内升级步骤照常触发、定时器照常推进，只是通知意图变为 `held`，
  查询视图/历史能看到“第 N 级在某时触发、被哪条规则因什么原因暂缓”。
- 抑制期间调用 `UpdateSeverity`：级别轨迹持续可见；新级别逃出规则范围时
  held 立即恢复，真正需要升级的通知不会被吞掉。
- 窗口到期由 `ProcessDue` 自动解除；`manual` 允许随时通过
  `ReleaseSuppression` 提前解除（到期扫描同样会以 `ValidUntil` 兜底结束，
  保证窗口结束后 held 意图一定被裁决）；`on_resolve` 在范围内事件解决时解除，
  未解决则在 `ValidUntil` 兜底解除，规则绝不会悬挂。
- 解除时 held 按事件**最新级别与状态**裁决，而不是照搬触发时的记录：
  仍 `firing` 且不再被任何生效规则覆盖 → 补发；已确认/解决 → 撤销。
  已解决事件不会因为解除抑制而重新发通知。
- 内容相同（范围+原因）的生效规则重复创建时返回 `ErrAlreadyExists`，
  错误信息形如 `active suppression "db-reindex" already covers the same
  scope with reason "..." (valid_until=...)`；改解除时间或解除条件也一样被拒。

## 持久化格式

单个 JSON 快照文件，包含策略、事件（含冻结步骤与进度）、定时器表、outbox、
通知意图去重集合、外部请求幂等表、交接单、抑制规则与历史。写入采用同目录临时文件 rename，
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
- 值班交接：仅移交 firing 事件；已发通知保留原记录、未派发意图（含 held）
  改派新班组；同请求号幂等不重复通知；同一事件多次交接保留每次接手人与时间；
  交接/确认/解决/计时推进 24 路并发收敛，意图归属始终与事件负责人一致；
  交接状态重启后恢复；分组 `Service` 各只投递本班组意图。
- 维护窗口抑制：窗口内升级触发但 held（带规则与原因）、不派发；窗口到期自动
  解除并按最新级别补发未处理级别，每级最多一次有效通知；手工解除、重复解除
  报冲突、幂等重放；已确认/已解决事件解除时撤销而不是补发，已解决事件绝不
  重新通知；on_resolve 解除与 ValidUntil 兜底；抑制期间级别变化持续可见、
  逃出范围立即恢复、解除按最新级别裁决；内容相同仅解除时间不同的规则被
  `ErrAlreadyExists` 明确拒绝；范围按级别/策略/事件过滤；抑制 held 随交接
  改派；升级/交接/级别变更/解除/确认并发收敛无重复；规则与 held 意图重启
  后恢复并在窗口结束后补发；综合视图展示事件、级别轨迹、交接轨迹与抑制原因；
  后台 `Service` 抑制期间零投递、解除后自动补发（见 `Example_suppressionWindow`）。
