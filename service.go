package incidentescalation

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Service 在 Store 之上提供策略配置、事件创建、确认、解决、到期推进与历史查询。
// 所有写操作都在 Store 的同一把互斥锁内完成「状态 + 定时器 + outbox」的原子持久化，
// 因此人员确认、事件解决与计时器触发即使并发到达，最终也只会形成一个合法状态。
type Service struct {
	store *Store
	clock Clock
}

// NewService 创建服务。
func NewService(store *Store) *Service {
	return &Service{store: store, clock: realClock{}}
}

// WithClock 返回使用指定时钟的服务副本，主要用于测试。
func (s *Service) WithClock(c Clock) *Service {
	return &Service{store: s.store, clock: c}
}

// ---------------- 策略配置 ----------------

// CreatePolicyInput 创建策略入参。
type CreatePolicyInput struct {
	ID    string
	Name  string
	Steps []Step
}

// CreatePolicy 创建（或整体替换）一个升级策略。
func (s *Service) CreatePolicy(in CreatePolicyInput) (Policy, error) {
	if strings.TrimSpace(in.ID) == "" {
		return Policy{}, validationErr("policy id is required")
	}
	steps, err := normalizeSteps(in.Steps)
	if err != nil {
		return Policy{}, err
	}
	now := s.clock.Now()
	p := Policy{ID: in.ID, Name: in.Name, Steps: steps, CreatedAt: now, UpdatedAt: now}
	err = s.store.update(func(d *storeData) error {
		if old, ok := d.Policies[in.ID]; ok {
			p.CreatedAt = old.CreatedAt // 保留创建时间
		}
		d.Policies[in.ID] = p
		return nil
	})
	if err != nil {
		return Policy{}, err
	}
	return clonePolicy(p), nil
}

// GetPolicy 读取策略。
func (s *Service) GetPolicy(id string) (Policy, error) {
	var out Policy
	err := notFoundErr("policy %q", id)
	s.store.read(func(d *storeData) {
		if p, ok := d.Policies[id]; ok {
			out = clonePolicy(p)
			err = nil
		}
	})
	return out, err
}

func normalizeSteps(in []Step) ([]Step, error) {
	if len(in) == 0 {
		return nil, validationErr("policy must have at least one step")
	}
	out := make([]Step, len(in))
	for i, st := range in {
		if st.WaitDuration <= 0 {
			return nil, validationErr("step %d wait duration must be positive", i)
		}
		if len(st.Targets) == 0 {
			return nil, validationErr("step %d must notify at least one target", i)
		}
		seen := map[string]struct{}{}
		targets := make([]string, 0, len(st.Targets))
		for _, t := range st.Targets {
			t = strings.TrimSpace(t)
			if t == "" {
				return nil, validationErr("step %d contains empty target", i)
			}
			if _, dup := seen[t]; dup {
				continue // 同一步骤内重复目标只保留一次
			}
			seen[t] = struct{}{}
			targets = append(targets, t)
		}
		out[i] = Step{WaitDuration: st.WaitDuration, Targets: targets}
	}
	return out, nil
}

// ---------------- 事件创建（幂等 + 策略冻结） ----------------

// CreateIncidentInput 创建事件入参。RequestID 为外部请求号，同号重放幂等、异内容冲突。
type CreateIncidentInput struct {
	RequestID   string
	PolicyID    string
	Title       string
	Description string
}

// CreateIncident 按冻结的策略快照创建事件：立即登记第 0 步通知意图，
// 并持久化第 0 步的等待定时器，状态、outbox、定时器、历史在同一原子写入中落盘。
func (s *Service) CreateIncident(in CreateIncidentInput) (*Incident, error) {
	if strings.TrimSpace(in.RequestID) == "" {
		return nil, validationErr("request id is required")
	}
	hash := hashContent("create", in.PolicyID, in.Title, in.Description)
	var result *Incident

	err := s.store.update(func(d *storeData) error {
		if rec, ok := d.Idempotency["create:"+in.RequestID]; ok {
			if rec.RequestHash != hash {
				return fmt.Errorf("%w: create request %s reused with different content", ErrConflict, in.RequestID)
			}
			// 幂等重放：返回首次创建的同一事件。
			result = cloneIncident(d.Incidents[rec.Result])
			return nil
		}

		policy, ok := d.Policies[in.PolicyID]
		if !ok {
			return notFoundErr("policy %q", in.PolicyID)
		}
		if strings.TrimSpace(in.Title) == "" {
			return validationErr("incident title is required")
		}

		id, err := newID("inc")
		if err != nil {
			return err
		}
		now := s.clock.Now()
		inc := &Incident{
			ID:          id,
			PolicyID:    policy.ID,
			Policy:      clonePolicy(policy), // 冻结快照：后续策略变更不影响本事件
			Title:       in.Title,
			Description: in.Description,
			Status:      StatusOpen,
			CurrentStep: 0,
			CreatedAt:   now,
		}

		// 第 0 步立即通知，并登记其等待定时器；三者与事件记录原子落盘。
		notifyLocked(d, id, 0, policy.Steps[0].Targets, now)
		d.Timers[id] = Timer{IncidentID: id, Step: 0, DueAt: now.Add(policy.Steps[0].WaitDuration)}
		d.History = append(d.History, HistoryEvent{IncidentID: id, Type: HistoryCreated, At: now})

		d.Incidents[id] = inc
		d.Idempotency["create:"+in.RequestID] = idempotencyRecord{RequestHash: hash, Result: id}
		result = cloneIncident(inc)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetIncident 读取事件。
func (s *Service) GetIncident(id string) (*Incident, error) {
	var out *Incident
	err := notFoundErr("incident %q", id)
	s.store.read(func(d *storeData) {
		if in, ok := d.Incidents[id]; ok {
			out = cloneIncident(in)
			err = nil
		}
	})
	return out, err
}

// ListIncidents 按创建时间顺序返回全部事件。
func (s *Service) ListIncidents() []*Incident {
	var out []*Incident
	s.store.read(func(d *storeData) { out = sortedIncidents(d) })
	return out
}

// ---------------- 确认 / 解决（幂等 + 竞态收敛） ----------------

// Acknowledge 以外部请求号 requestID 幂等确认事件，确认后删除定时器、停止升级。
// 已解决的事件不能再确认（ErrInvalidState）。
func (s *Service) Acknowledge(requestID, incidentID, by string) (*Incident, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, validationErr("request id is required")
	}
	if strings.TrimSpace(by) == "" {
		return nil, validationErr("acknowledger is required")
	}
	hash := hashContent("ack", incidentID, by)
	var result *Incident

	err := s.store.update(func(d *storeData) error {
		if rec, ok := d.Idempotency["ack:"+requestID]; ok {
			if rec.RequestHash != hash {
				return fmt.Errorf("%w: acknowledge request %s reused with different content", ErrConflict, requestID)
			}
			result = cloneIncident(d.Incidents[rec.Result])
			return nil
		}

		inc, ok := d.Incidents[incidentID]
		if !ok {
			return notFoundErr("incident %q", incidentID)
		}
		if inc.Status == StatusResolved {
			return invalidStateErr("incident %q is already resolved and cannot be acknowledged", incidentID)
		}
		now := s.clock.Now()
		if inc.Status != StatusAcknowledged {
			inc.Status = StatusAcknowledged
			inc.AckedBy = by
			inc.AckedAt = &now
			delete(d.Timers, incidentID) // 确认即停止升级
			d.History = append(d.History, HistoryEvent{
				IncidentID: incidentID, Type: HistoryAcknowledged, Step: inc.CurrentStep, Detail: by, At: now,
			})
		}
		d.Idempotency["ack:"+requestID] = idempotencyRecord{RequestHash: hash, Result: incidentID}
		result = cloneIncident(inc)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Resolve 以外部请求号 requestID 幂等解决事件，解决后删除定时器，
// 不能再被确认，也不会再产生任何通知。
func (s *Service) Resolve(requestID, incidentID, by string) (*Incident, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, validationErr("request id is required")
	}
	if strings.TrimSpace(by) == "" {
		return nil, validationErr("resolver is required")
	}
	hash := hashContent("resolve", incidentID, by)
	var result *Incident

	err := s.store.update(func(d *storeData) error {
		if rec, ok := d.Idempotency["resolve:"+requestID]; ok {
			if rec.RequestHash != hash {
				return fmt.Errorf("%w: resolve request %s reused with different content", ErrConflict, requestID)
			}
			result = cloneIncident(d.Incidents[rec.Result])
			return nil
		}

		inc, ok := d.Incidents[incidentID]
		if !ok {
			return notFoundErr("incident %q", incidentID)
		}
		now := s.clock.Now()
		if inc.Status != StatusResolved {
			inc.Status = StatusResolved
			inc.ResolvedBy = by
			inc.ResolvedAt = &now
			delete(d.Timers, incidentID) // 解决即终止一切后续动作
			d.History = append(d.History, HistoryEvent{
				IncidentID: incidentID, Type: HistoryResolved, Step: inc.CurrentStep, Detail: by, At: now,
			})
		}
		d.Idempotency["resolve:"+requestID] = idempotencyRecord{RequestHash: hash, Result: incidentID}
		result = cloneIncident(inc)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ---------------- 到期推进（一次至多一步） ----------------

// TickResult 描述一次扫描中某事件的推进结果。
type TickResult struct {
	IncidentID string
	FromStep   int
	ToStep     int           // -1 表示所有步骤已耗尽
	Notified   []OutboxEntry // 本次新登记的通知意图
	Exhausted  bool
}

// TickDue 扫描所有到期定时器并推进。保证：
//   - 只推进在扫描开始时已到期、且仍与事件当前步骤一致的定时器；
//   - 同一个事件在一次调用中至多推进一步，即使其后续定时器也已到期
//     （例如进程长时间停机后重启），需要重复扫描逐步追赶；
//   - 通知意图按 事件|步骤|目标 去重，重复扫描不会制造重复 outbox；
//   - 已确认 / 已解决的事件不推进。
func (s *Service) TickDue() []TickResult {
	now := s.clock.Now()

	// 先在锁内快照「本次应当处理」的到期定时器。
	var due []Timer
	s.store.read(func(d *storeData) {
		for _, t := range d.Timers {
			if !t.DueAt.After(now) {
				due = append(due, t)
			}
		}
	})
	if len(due) == 0 {
		return nil
	}

	var results []TickResult
	// 每个到期定时器独立成一个原子事务；同一事件若被重复扫描到，
	// 第二次会因步骤号不再匹配而空转，绝不会连跨多步。
	for _, t := range due {
		var res *TickResult
		_ = s.store.update(func(d *storeData) error {
			cur, ok := d.Timers[t.IncidentID]
			if !ok || cur.Step != t.Step {
				return nil // 定时器已被确认/解决删除、或已被上一次扫描推进
			}
			inc, ok := d.Incidents[t.IncidentID]
			if !ok || inc.Status != StatusOpen || inc.CurrentStep != t.Step {
				// 状态已终结或步骤错位：清理悬挂定时器，不产生任何通知。
				delete(d.Timers, t.IncidentID)
				return nil
			}

			next := t.Step + 1
			at := s.clock.Now()
			r := TickResult{IncidentID: t.IncidentID, FromStep: t.Step}

			if next >= len(inc.Policy.Steps) {
				// 最后一步的等待也已结束：升级链耗尽。
				inc.Exhausted = true
				inc.CurrentStep = t.Step
				delete(d.Timers, t.IncidentID)
				d.History = append(d.History, HistoryEvent{
					IncidentID: t.IncidentID, Type: HistoryExhausted, Step: t.Step, At: at,
				})
				r.ToStep = -1
				r.Exhausted = true
			} else {
				inc.CurrentStep = next
				entries := notifyLocked(d, inc.ID, next, inc.Policy.Steps[next].Targets, at)
				// 到期时间沿计划时间链推算：重复扫描每次只补一步，逐次追赶。
				d.Timers[inc.ID] = Timer{
					IncidentID: inc.ID,
					Step:       next,
					DueAt:      cur.DueAt.Add(inc.Policy.Steps[next].WaitDuration),
				}
				d.History = append(d.History, HistoryEvent{
					IncidentID: inc.ID, Type: HistoryEscalated, Step: next, At: at,
				})
				r.ToStep = next
				r.Notified = entries
			}
			res = &r
			return nil
		})
		if res != nil {
			results = append(results, *res)
		}
	}
	return results
}

// CatchupDue 重复扫描直到没有任何到期定时器（停机重启后的便捷追赶入口）。
// 每次扫描仍严格遵守「每事件至多一步」，返回全部推进结果。
func (s *Service) CatchupDue() []TickResult {
	var all []TickResult
	for {
		rs := s.TickDue()
		if len(rs) == 0 {
			return all
		}
		all = append(all, rs...)
	}
}

// RunScheduler 按 interval 周期性触发到期推进，直到 ctx 取消。
// 定时器全部持久化在存储中，进程重启后重新启动本调度器即可接着执行。
func (s *Service) RunScheduler(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.TickDue()
		}
	}
}

// notifyLocked 必须在持锁状态下调用：登记一步中所有目标的通知意图。
// 以 事件|步骤|目标 为主键，重复推进 / 重复目标都不会产生第二条意图。
// 返回本次实际新登记的条目。
func notifyLocked(d *storeData, incidentID string, step int, targets []string, at time.Time) []OutboxEntry {
	var created []OutboxEntry
	for _, target := range targets {
		key := outboxKey(incidentID, step, target)
		if _, exists := d.Outbox[key]; exists {
			continue
		}
		e := OutboxEntry{Key: key, IncidentID: incidentID, Step: step, Target: target, CreatedAt: at}
		d.Outbox[key] = e
		created = append(created, e)
		d.History = append(d.History, HistoryEvent{
			IncidentID: incidentID, Type: HistoryNotified, Step: step, Detail: target, At: at,
		})
	}
	return created
}

func outboxKey(incidentID string, step int, target string) string {
	return fmt.Sprintf("%s|%d|%s", incidentID, step, target)
}

// ---------------- Outbox 与历史查询 ----------------

// ListOutbox 返回全部待投递通知意图，按创建时间排序。
func (s *Service) ListOutbox() []OutboxEntry {
	var out []OutboxEntry
	s.store.read(func(d *storeData) {
		for _, e := range d.Outbox {
			out = append(out, e)
		}
	})
	sortOutbox(out)
	return out
}

// ListOutboxForIncident 返回某事件的全部通知意图。
func (s *Service) ListOutboxForIncident(incidentID string) []OutboxEntry {
	var out []OutboxEntry
	s.store.read(func(d *storeData) {
		for _, e := range d.Outbox {
			if e.IncidentID == incidentID {
				out = append(out, e)
			}
		}
	})
	sortOutbox(out)
	return out
}

// MarkOutboxSent 表示通知意图已被外部投递通道成功处理，将其移出 outbox。
func (s *Service) MarkOutboxSent(key string) error {
	return s.store.update(func(d *storeData) error {
		if _, ok := d.Outbox[key]; !ok {
			return notFoundErr("outbox entry %q", key)
		}
		delete(d.Outbox, key)
		return nil
	})
}

func sortOutbox(out []OutboxEntry) {
	// 简单插入排序即可，条目规模通常不大；保持稳定顺序便于阅读与测试。
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && outboxLess(out[j], out[j-1]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
}

func outboxLess(a, b OutboxEntry) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	if a.Step != b.Step {
		return a.Step < b.Step
	}
	return a.Target < b.Target
}

// GetHistory 返回某事件按发生顺序排列的历史记录。
func (s *Service) GetHistory(incidentID string) ([]HistoryEvent, error) {
	var out []HistoryEvent
	var err error
	s.store.read(func(d *storeData) {
		if _, ok := d.Incidents[incidentID]; !ok {
			err = notFoundErr("incident %q", incidentID)
			return
		}
		for _, h := range d.History {
			if h.IncidentID == incidentID {
				out = append(out, h)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------- 工具函数 ----------------

func newID(prefix string) (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return prefix + "-" + hex.EncodeToString(b[:]), nil
}

// hashContent 计算请求内容的稳定哈希，用于「同号异内容报冲突」。
func hashContent(op string, parts ...string) string {
	canonical, _ := json.Marshal(struct {
		Op    string   `json:"op"`
		Parts []string `json:"parts"`
	}{Op: op, Parts: parts})
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}
