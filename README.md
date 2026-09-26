# go-incident-escalation

值班事件升级服务：进程重启后仍能继续执行的、带持久化定时器与 outbox 的事件升级流程。

开发环境：Go 1.23.0，仅依赖标准库。

## 核心概念

- **策略（Policy）**：由按顺序执行的响应步骤组成，每步包含「等待时长」和「通知目标列表」。事件创建时把策略快照**冻结**进事件，之后修改策略不影响已创建的事件。
- **事件（Incident）**：生命周期为 `open → acknowledged → resolved`（也可 `open → resolved`）。
- **定时器（Timer）**：每个事件至多一个活跃定时器，随状态一起原子落盘；到期而事件未确认时推进到下一步。
- **Outbox**：通知意图表，主键为 `事件|步骤|目标`——同一事件、同一步骤、同一目标只会形成一次通知意图，重复扫描不会产生重复条目。
- **历史（History）**：创建、通知、升级、确认、解决、耗尽等动作按序记录，可随时查询。

## 关键保证

1. **原子持久化**：事件状态、定时器、outbox、历史、幂等记录在同一个持锁临界区内写入，并通过「临时文件 + 原子重命名」落盘，进程在任意时刻崩溃都不会留下半截状态。
2. **重启续跑**：定时器全部持久化。进程重启后重新打开存储并启动调度器（或调用 `CatchupDue`），升级流程从断点继续。
3. **竞态收敛**：确认、解决、定时器触发都经由存储的同一把互斥锁串行化。确认后定时器被删除、停止升级；解决后不能再确认、不再产生任何通知；并发竞态最终只形成一个合法状态。
4. **幂等**：创建 / 确认 / 解决分别按外部请求号幂等。同号同内容重放返回首次结果；同号异内容返回 `ErrConflict`。
5. **单步推进**：一次 `TickDue` 扫描中，同一事件至多推进一步（即使停机很久、后续步骤全部过期），需要重复扫描逐步追赶；`CatchupDue` 是封装好的追赶入口。

## API 概览

```go
store, _ := incidentescalation.OpenStore("./data")
svc := incidentescalation.NewService(store)

// 策略配置
svc.CreatePolicy(incidentescalation.CreatePolicyInput{ID: "oncall", Steps: ...})
svc.GetPolicy("oncall")

// 事件生命周期（RequestID 为外部请求号，幂等）
svc.CreateIncident(incidentescalation.CreateIncidentInput{RequestID: "req-1", PolicyID: "oncall", Title: "..."})
svc.Acknowledge("ack-1", incidentID, "oncall-a")
svc.Resolve("res-1", incidentID, "oncall-a")

// 到期推进（由调度器周期触发）
go svc.RunScheduler(ctx, 10*time.Second) // 或手动 svc.TickDue() / svc.CatchupDue()

// 查询
svc.GetIncident(id)
svc.ListIncidents()
svc.ListOutbox() / svc.ListOutboxForIncident(id)
svc.MarkOutboxSent(key) // 通知投递成功后移出 outbox
svc.GetHistory(id)
```

错误通过哨兵错误返回，可用 `errors.Is` 判定：`ErrNotFound`、`ErrConflict`（同号异内容）、`ErrInvalidState`（非法状态迁移，如解决后再确认）、`ErrValidation`（参数校验失败）。

## 运行测试

    go test ./... -race
