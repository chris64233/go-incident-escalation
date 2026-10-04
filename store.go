package incidentescalation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Timer 是一个持久化的升级定时器：到达 FireAt 时，若事件仍未确认/解决，
// 则触发 IncidentID 的 StepIndex 步骤。
// 每个事件至多存在一个定时器（指向“下一步”），随状态在同一原子写入中落盘，
// 因此重启后可以原样恢复并继续执行。
type Timer struct {
	IncidentID string    `json:"incident_id"`
	StepIndex  int       `json:"step_index"`
	FireAt     time.Time `json:"fire_at"`
}

// PolicyInput 是创建/更新策略时由调用方提供的内容。
type PolicyInput struct {
	ID    string
	Name  string
	Steps []Step
}

// CreateIncidentRequest 创建事件。RequestID 是外部请求号，用于幂等。
type CreateIncidentRequest struct {
	RequestID  string
	IncidentID string // 可选，为空则自动生成
	PolicyID   string
	Severity   string
	Detail     string
}

// AckRequest 确认事件。RequestID 是外部请求号，用于幂等。
type AckRequest struct {
	RequestID      string
	IncidentID     string
	AcknowledgedBy string
}

// ResolveRequest 解决事件。RequestID 是外部请求号，用于幂等。
type ResolveRequest struct {
	RequestID  string
	IncidentID string
	ResolvedBy string
}

// requestRecord 记录一个外部请求号对应的请求内容指纹与结果，用于幂等与冲突检测。
type requestRecord struct {
	Kind        string `json:"kind"`        // create_incident | acknowledge | resolve
	Fingerprint string `json:"fingerprint"` // 请求内容指纹
	IncidentID  string `json:"incident_id"` // 幂等重放时定位结果
}

// receiptRecord 记录一条已处理渠道回执的指纹与处理结果，用于幂等重放与冲突检测。
type receiptRecord struct {
	Fingerprint string         `json:"fingerprint"`
	Applied     bool           `json:"applied"`
	Status      DeliveryStatus `json:"status"`
	Reason      string         `json:"reason"`
	// ErrKind 记录首次处理返回的哨兵错误类别（"" 或 "stale"），重放时原样返回。
	ErrKind string `json:"err_kind,omitempty"`
}

// timerRecord 是 Timer 的持久化形态（当前同构，独立命名以便演进）。
type timerRecord = Timer

// snapshot 是一次性原子落盘的完整状态：事件状态、定时器、outbox 同生共死。
type snapshot struct {
	Policies     map[string]*Policy       `json:"policies"`
	Incidents    map[string]*Incident     `json:"incidents"`
	Outbox       []*OutboxItem            `json:"outbox"`
	NextOutboxID int64                    `json:"next_outbox_id"`
	Timers       map[string]*timerRecord  `json:"timers"`       // incidentID -> 唯一定时器
	SentIntents  map[string]struct{}      `json:"sent_intents"` // "incidentID\x00step\x00target" 去重
	Requests     map[string]requestRecord `json:"requests"`
	Receipts     map[string]receiptRecord `json:"receipts"` // receiptID -> 处理结果
	RetryPolicy  RetryPolicy              `json:"retry_policy"`
	History      []HistoryEntry           `json:"history"`
	NextSeq      int64                    `json:"next_seq"`
}

func newSnapshot() *snapshot {
	return &snapshot{
		Policies:    map[string]*Policy{},
		Incidents:   map[string]*Incident{},
		Timers:      map[string]*timerRecord{},
		SentIntents: map[string]struct{}{},
		Requests:    map[string]requestRecord{},
		Receipts:    map[string]receiptRecord{},
		RetryPolicy: RetryPolicy{}.normalized(),
	}
}

// Store 是事件升级服务的持久化核心。
// 所有变更都在互斥锁内完成并以“临时文件 + fsync + 原子 rename”整体落盘，
// 保证事件状态、定时器与 outbox 要么全部生效、要么全部不生效。
type Store struct {
	mu   sync.Mutex
	path string // 为空表示纯内存存储
	snap *snapshot
}

// OpenStore 打开（不存在则创建）路径上的持久化存储，并在重启后恢复全部状态。
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, snap: newSnapshot()}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(data) > 0 {
			if err := json.Unmarshal(data, s.snap); err != nil {
				return nil, fmt.Errorf("incident-escalation: corrupt state file %q: %w", path, err)
			}
			s.snap.afterLoad()
		}
	case errors.Is(err, os.ErrNotExist):
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("incident-escalation: open store %q: %w", path, err)
	}
	return s, nil
}

// NewMemoryStore 返回不落盘的存储（主要用于测试）。
func NewMemoryStore() *Store {
	return &Store{snap: newSnapshot()}
}

// afterLoad 修复旧版本快照中可能为 nil 的 map。
func (snap *snapshot) afterLoad() {
	if snap.Policies == nil {
		snap.Policies = map[string]*Policy{}
	}
	if snap.Incidents == nil {
		snap.Incidents = map[string]*Incident{}
	}
	if snap.Timers == nil {
		snap.Timers = map[string]*timerRecord{}
	}
	if snap.SentIntents == nil {
		snap.SentIntents = map[string]struct{}{}
	}
	if snap.Requests == nil {
		snap.Requests = map[string]requestRecord{}
	}
	if snap.Receipts == nil {
		snap.Receipts = map[string]receiptRecord{}
	}
	snap.RetryPolicy = snap.RetryPolicy.normalized()
}

// persistLocked 将当前快照原子写入磁盘；调用方必须持有 mu。
func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	// 先在锁内完成序列化，避免落盘期间状态被改写导致写一半。
	data, err := json.MarshalIndent(s.snap, "", "  ")
	if err != nil {
		return fmt.Errorf("incident-escalation: marshal state: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("incident-escalation: create temp state: %w", err)
	}
	tmpName := tmp.Name()
	rollback := func(cause error) error {
		tmp.Close()
		os.Remove(tmpName)
		return cause
	}
	if _, err := tmp.Write(data); err != nil {
		return rollback(fmt.Errorf("incident-escalation: write state: %w", err))
	}
	if err := tmp.Sync(); err != nil {
		return rollback(fmt.Errorf("incident-escalation: fsync state: %w", err))
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("incident-escalation: close state: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("incident-escalation: rename state: %w", err)
	}
	// fsync 目录，保证 rename 也持久化。
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// commitLocked 统一处理“改完即落盘”。
func (s *Store) commitLocked() error { return s.persistLocked() }

// ---------- 工具 ----------

func fingerprint(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// checkRequest 实现“按外部请求号幂等；同号异内容冲突”。
// 命中同内容请求时返回已记录的 incidentID（重放）；同号异内容返回 ErrConflict。
func (s *Store) checkRequestLocked(kind, requestID string, content any) (string, bool, error) {
	if strings.TrimSpace(requestID) == "" {
		return "", false, fmt.Errorf("%w: request id is required", ErrInvalidArgument)
	}
	fp := fingerprint(content)
	rec, ok := s.snap.Requests[requestID]
	if !ok {
		return "", false, nil
	}
	if rec.Kind != kind || rec.Fingerprint != fp {
		return "", false, fmt.Errorf("%w: request id %q reused with different payload", ErrConflict, requestID)
	}
	return rec.IncidentID, true, nil
}

func (s *Store) rememberRequestLocked(kind, requestID, incidentID string, content any) {
	s.snap.Requests[requestID] = requestRecord{
		Kind:        kind,
		Fingerprint: fingerprint(content),
		IncidentID:  incidentID,
	}
}

func (s *Store) appendHistoryLocked(incidentID, kind, detail string, now time.Time) {
	s.snap.History = append(s.snap.History, HistoryEntry{
		Time:       now,
		IncidentID: incidentID,
		Kind:       kind,
		Detail:     detail,
	})
}

func cloneSteps(in []Step) []Step {
	out := make([]Step, len(in))
	for i, st := range in {
		out[i] = Step{WaitBefore: st.WaitBefore, Targets: append([]string(nil), st.Targets...)}
	}
	return out
}

func intentKey(incidentID string, stepIndex int, target string) string {
	return fmt.Sprintf("%s\x00%d\x00%s", incidentID, stepIndex, target)
}

// ---------- 策略配置 ----------

func validatePolicy(in PolicyInput) error {
	if strings.TrimSpace(in.ID) == "" {
		return fmt.Errorf("%w: policy id is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.Name) == "" {
		return fmt.Errorf("%w: policy name is required", ErrInvalidArgument)
	}
	if len(in.Steps) == 0 {
		return fmt.Errorf("%w: policy %q must contain at least one step", ErrInvalidArgument, in.ID)
	}
	seen := map[string]struct{}{}
	for i, step := range in.Steps {
		if step.WaitBefore < 0 {
			return fmt.Errorf("%w: step %d wait_before must be >= 0", ErrInvalidArgument, i)
		}
		if len(step.Targets) == 0 {
			return fmt.Errorf("%w: step %d must notify at least one target", ErrInvalidArgument, i)
		}
		for _, t := range step.Targets {
			if strings.TrimSpace(t) == "" {
				return fmt.Errorf("%w: step %d contains an empty target", ErrInvalidArgument, i)
			}
			if _, dup := seen[t]; dup {
				return fmt.Errorf("%w: target %q appears more than once in step %d", ErrInvalidArgument, t, i)
			}
			seen[t] = struct{}{}
		}
	}
	return nil
}

// CreatePolicy 创建一份升级策略。
func (s *Store) CreatePolicy(in PolicyInput, now time.Time) (*Policy, error) {
	if err := validatePolicy(in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.snap.Policies[in.ID]; exists {
		return nil, fmt.Errorf("%w: policy %q", ErrAlreadyExists, in.ID)
	}
	p := &Policy{
		ID:        in.ID,
		Name:      in.Name,
		Steps:     cloneSteps(in.Steps),
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
	}
	s.snap.Policies[in.ID] = p
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return clonePolicy(p), nil
}

// UpdatePolicy 更新策略。已创建的事件持有冻结快照，不会受到影响。
func (s *Store) UpdatePolicy(in PolicyInput, now time.Time) (*Policy, error) {
	if err := validatePolicy(in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.snap.Policies[in.ID]
	if !ok {
		return nil, fmt.Errorf("%w: policy %q", ErrNotFound, in.ID)
	}
	p.Name = in.Name
	p.Steps = cloneSteps(in.Steps)
	p.UpdatedAt = now
	p.Version++
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return clonePolicy(p), nil
}

// GetPolicy 读取策略。
func (s *Store) GetPolicy(id string) (*Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.snap.Policies[id]
	if !ok {
		return nil, fmt.Errorf("%w: policy %q", ErrNotFound, id)
	}
	return clonePolicy(p), nil
}

// ListPolicies 列出全部策略，按 ID 排序。
func (s *Store) ListPolicies() []*Policy {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.snap.Policies))
	for id := range s.snap.Policies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Policy, 0, len(ids))
	for _, id := range ids {
		out = append(out, clonePolicy(s.snap.Policies[id]))
	}
	return out
}

func clonePolicy(p *Policy) *Policy {
	cp := *p
	cp.Steps = cloneSteps(p.Steps)
	return &cp
}

// ---------- 事件生命周期 ----------

// CreateIncident 创建事件：冻结当时策略、登记第 0 步的持久化定时器。
// 同 RequestID 的重复请求：内容相同则幂等重放原事件；内容不同返回 ErrConflict。
func (s *Store) CreateIncident(req CreateIncidentRequest, now time.Time) (*Incident, error) {
	if strings.TrimSpace(req.PolicyID) == "" {
		return nil, fmt.Errorf("%w: policy_id is required", ErrInvalidArgument)
	}
	content := struct {
		IncidentID string `json:"incident_id"`
		PolicyID   string `json:"policy_id"`
		Severity   string `json:"severity"`
		Detail     string `json:"detail"`
	}{req.IncidentID, req.PolicyID, req.Severity, req.Detail}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("create_incident", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getIncidentLocked(existingID)
	}

	p, ok := s.snap.Policies[req.PolicyID]
	if !ok {
		return nil, fmt.Errorf("%w: policy %q", ErrNotFound, req.PolicyID)
	}

	id := req.IncidentID
	if id == "" {
		s.snap.NextSeq++
		id = fmt.Sprintf("inc-%d", s.snap.NextSeq)
	}
	if _, exists := s.snap.Incidents[id]; exists {
		return nil, fmt.Errorf("%w: incident %q", ErrAlreadyExists, id)
	}

	inc := &Incident{
		ID:          id,
		PolicyID:    req.PolicyID,
		Severity:    req.Severity,
		Detail:      req.Detail,
		Status:      StatusFiring,
		FrozenSteps: cloneSteps(p.Steps), // 冻结：之后策略如何改都与本事件无关
		StartedAt:   now,
		FiredAt:     make([]time.Time, len(p.Steps)),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	s.snap.Incidents[id] = inc
	// 第 0 步定时器与事件在同一次原子写入中持久化。
	s.snap.Timers[id] = &timerRecord{
		IncidentID: id,
		StepIndex:  0,
		FireAt:     now.Add(p.Steps[0].WaitBefore),
	}
	s.appendHistoryLocked(id, "created", fmt.Sprintf("policy=%s severity=%s", req.PolicyID, req.Severity), now)
	s.rememberRequestLocked("create_incident", req.RequestID, id, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return s.getIncidentLocked(id)
}

// Acknowledge 确认事件并停止升级：删除未触发的定时器，后续到期扫描不再产生通知。
func (s *Store) Acknowledge(req AckRequest, now time.Time) (*Incident, error) {
	content := struct {
		IncidentID     string `json:"incident_id"`
		AcknowledgedBy string `json:"acknowledged_by"`
	}{req.IncidentID, req.AcknowledgedBy}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("acknowledge", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getIncidentLocked(existingID)
	}

	inc, ok := s.snap.Incidents[req.IncidentID]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, req.IncidentID)
	}
	switch inc.Status {
	case StatusResolved:
		return nil, fmt.Errorf("%w: incident %q", ErrIncidentResolved, inc.ID)
	case StatusAcknowledged:
		return nil, fmt.Errorf("%w: incident %q", ErrAlreadyAcknowledged, inc.ID)
	}
	inc.Status = StatusAcknowledged
	inc.AcknowledgedAt = now
	inc.AcknowledgedBy = req.AcknowledgedBy
	inc.UpdatedAt = now
	delete(s.snap.Timers, inc.ID) // 状态与定时器删除原子落盘
	s.stopDeliveriesLocked(inc.ID, "incident_acknowledged", now)
	s.appendHistoryLocked(inc.ID, "acknowledged", "by="+req.AcknowledgedBy, now)
	s.rememberRequestLocked("acknowledge", req.RequestID, inc.ID, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return s.getIncidentLocked(inc.ID)
}

// Resolve 解决事件。解决后不能再确认（返回 ErrIncidentResolved），
// 定时器被删除，到期扫描也不会再产生任何 outbox 通知。
func (s *Store) Resolve(req ResolveRequest, now time.Time) (*Incident, error) {
	content := struct {
		IncidentID string `json:"incident_id"`
		ResolvedBy string `json:"resolved_by"`
	}{req.IncidentID, req.ResolvedBy}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingID, replay, err := s.checkRequestLocked("resolve", req.RequestID, content); err != nil {
		return nil, err
	} else if replay {
		return s.getIncidentLocked(existingID)
	}

	inc, ok := s.snap.Incidents[req.IncidentID]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, req.IncidentID)
	}
	if inc.Status == StatusResolved {
		return nil, fmt.Errorf("%w: incident %q", ErrAlreadyResolved, inc.ID)
	}
	inc.Status = StatusResolved
	inc.ResolvedAt = now
	inc.ResolvedBy = req.ResolvedBy
	inc.UpdatedAt = now
	delete(s.snap.Timers, inc.ID)
	s.stopDeliveriesLocked(inc.ID, "incident_resolved", now)
	s.appendHistoryLocked(inc.ID, "resolved", "by="+req.ResolvedBy, now)
	s.rememberRequestLocked("resolve", req.RequestID, inc.ID, content)
	if err := s.commitLocked(); err != nil {
		return nil, err
	}
	return s.getIncidentLocked(inc.ID)
}

// GetIncident 读取事件。
func (s *Store) GetIncident(id string) (*Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getIncidentLocked(id)
}

func (s *Store) getIncidentLocked(id string) (*Incident, error) {
	inc, ok := s.snap.Incidents[id]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, id)
	}
	return cloneIncident(inc), nil
}

// ListIncidents 列出全部事件，按创建时间（即 ID 写入顺序）排序。
func (s *Store) ListIncidents() []*Incident {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Incident, 0, len(s.snap.Incidents))
	for _, inc := range s.snap.Incidents {
		out = append(out, cloneIncident(inc))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func cloneIncident(in *Incident) *Incident {
	cp := *in
	cp.FrozenSteps = cloneSteps(in.FrozenSteps)
	cp.FiredAt = append([]time.Time(nil), in.FiredAt...)
	return &cp
}

// ---------- 定时器与到期推进 ----------

// NextDue 返回最近一个待触发定时器的时间；没有定时器时 ok 为 false。
// 重启后该信息完全来自持久化状态。
func (s *Store) NextDue() (fireAt time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.snap.Timers {
		if !ok || t.FireAt.Before(fireAt) {
			fireAt, ok = t.FireAt, true
		}
	}
	return fireAt, ok
}

// Timer 返回某事件当前持久化的定时器（调试/测试用）。
func (s *Store) Timer(incidentID string) (Timer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.snap.Timers[incidentID]
	if !ok {
		return Timer{}, false
	}
	return *t, true
}

// ProcessDue 扫描所有到期定时器并推进升级。
// 关键并发/重放保证：
//   - 每个事件每次调用至多推进一步（到期多步也不会一次跨过多个步骤）；
//   - 仅对仍处于 firing 的事件推进，确认/解决与触发竞态时最终只有一个合法状态；
//   - 通知意图按 (事件, 步骤, 目标) 去重，重复扫描不会产生重复 outbox；
//   - 事件推进、outbox 追加、下一步定时器登记在同一次原子写入中完成。
//
// 返回本次实际推进的步骤数。
func (s *Store) ProcessDue(now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 固定处理顺序，使行为与历史可预测。
	ids := make([]string, 0, len(s.snap.Timers))
	for id := range s.snap.Timers {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	advanced := 0
	for _, id := range ids {
		t := s.snap.Timers[id]
		if t == nil || t.FireAt.After(now) {
			continue // 尚未到期
		}
		inc := s.snap.Incidents[id]
		if inc == nil {
			delete(s.snap.Timers, id)
			continue
		}
		// 与确认/解决竞态：以事件状态为准，非 firing 绝不通知。
		if inc.Status != StatusFiring {
			delete(s.snap.Timers, id)
			continue
		}
		idx := inc.NextStepIndex
		if idx != t.StepIndex || idx >= len(inc.FrozenSteps) {
			// 定时器与进度不一致（理论上不应出现）：丢弃过期定时器，
			// 不做任何推进，交由状态自身的下一步定时器驱动。
			delete(s.snap.Timers, id)
			continue
		}

		step := inc.FrozenSteps[idx]
		var targets []string
		for _, target := range step.Targets {
			key := intentKey(id, idx, target)
			if _, dup := s.snap.SentIntents[key]; dup {
				continue // 同一事件、同一步骤、同一目标只有一次通知意图
			}
			s.snap.SentIntents[key] = struct{}{}
			s.snap.NextOutboxID++
			s.snap.Outbox = append(s.snap.Outbox, &OutboxItem{
				ID:         s.snap.NextOutboxID,
				IncidentID: id,
				StepIndex:  idx,
				Target:     target,
				Channel:    "default",
				Message: fmt.Sprintf("[incident %s] escalation step %d: %s",
					id, idx, inc.Detail),
				Status:    OutboxPending,
				CreatedAt: now,
			})
			targets = append(targets, target)
		}

		inc.FiredAt[idx] = now
		inc.NextStepIndex = idx + 1
		inc.UpdatedAt = now
		s.appendHistoryLocked(id, "step_fired",
			fmt.Sprintf("step=%d targets=%s", idx, strings.Join(targets, ",")), now)

		// 推进一步后立即重新登记下一步定时器（基于实际触发时间等待），
		// 或在最后一步后清除定时器。
		if next := idx + 1; next < len(inc.FrozenSteps) {
			s.snap.Timers[id] = &timerRecord{
				IncidentID: id,
				StepIndex:  next,
				FireAt:     now.Add(inc.FrozenSteps[next].WaitBefore),
			}
		} else {
			delete(s.snap.Timers, id)
		}
		advanced++
	}
	if advanced > 0 {
		if err := s.commitLocked(); err != nil {
			return 0, err
		}
	}
	return advanced, nil
}

// ---------- Outbox ----------

// PendingOutbox 返回所有尚未投递的通知意图，按 ID 排序。
func (s *Store) PendingOutbox() []*OutboxItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*OutboxItem
	for _, item := range s.snap.Outbox {
		if item.Status == OutboxPending {
			cp := *item
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ListOutbox 返回某事件（空字符串表示全部）的通知意图。
func (s *Store) ListOutbox(incidentID string) []*OutboxItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*OutboxItem
	for _, item := range s.snap.Outbox {
		if incidentID == "" || item.IncidentID == incidentID {
			cp := *item
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// MarkDelivered 将一条 outbox 意图标记为渠道已确认接收（等同收到 accepted 回执）。
// 投递与状态落盘是分离的：崩溃会导致重投（at-least-once），
// 通知渠道应以 item.ID / (事件,步骤,目标) 做幂等。
func (s *Store) MarkDelivered(id int64, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.findOutboxLocked(id)
	if item == nil {
		return fmt.Errorf("%w: outbox item %d", ErrNotFound, id)
	}
	switch item.Status {
	case DeliveryAcknowledged:
		return nil
	case DeliveryFailed, DeliveryStopped:
		return fmt.Errorf("%w: outbox item %d is %s", ErrDeliveryClosed, id, item.Status)
	}
	item.Status = DeliveryAcknowledged
	item.AckedAt = now
	item.NextAttemptAt = time.Time{}
	s.appendHistoryLocked(item.IncidentID, "delivery_acknowledged",
		fmt.Sprintf("step=%d target=%s source=mark_delivered", item.StepIndex, item.Target), now)
	return s.commitLocked()
}

func (s *Store) findOutboxLocked(id int64) *OutboxItem {
	for _, item := range s.snap.Outbox {
		if item.ID == id {
			return item
		}
	}
	return nil
}

func (s *Store) findIntentLocked(incidentID string, stepIndex int, target string) *OutboxItem {
	for _, item := range s.snap.Outbox {
		if item.IncidentID == incidentID && item.StepIndex == stepIndex && item.Target == target {
			return item
		}
	}
	return nil
}

// stopDeliveriesLocked 把事件所有未完成的投递（pending/dispatched）置为 stopped，
// 并记录停止原因；与事件状态变更在同一次原子写入中落盘。调用方必须持有 mu。
func (s *Store) stopDeliveriesLocked(incidentID, reason string, now time.Time) {
	for _, item := range s.snap.Outbox {
		if item.IncidentID != incidentID {
			continue
		}
		if item.Status != DeliveryPending && item.Status != DeliveryDispatched {
			continue
		}
		item.Status = DeliveryStopped
		item.StopReason = reason
		item.NextAttemptAt = time.Time{}
		s.appendHistoryLocked(incidentID, "delivery_stopped",
			fmt.Sprintf("step=%d target=%s reason=%s", item.StepIndex, item.Target, reason), now)
	}
}

// ---------- 投递状态机 ----------

// SetRetryPolicy 设置投递重试策略（持久化，重启后仍然生效）。零值字段取默认值。
func (s *Store) SetRetryPolicy(rp RetryPolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap.RetryPolicy = rp.normalized()
	return s.commitLocked()
}

// RetryPolicyOf 返回当前生效的重试策略。
func (s *Store) RetryPolicyOf() RetryPolicy {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap.RetryPolicy
}

// DueOutbox 返回当前可（重）试的意图：状态为 pending 且已到 NextAttemptAt。
// 重启后待重试意图由此恢复处理。
func (s *Store) DueOutbox(now time.Time) []*OutboxItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*OutboxItem
	for _, item := range s.snap.Outbox {
		if item.Status != DeliveryPending {
			continue
		}
		if !item.NextAttemptAt.IsZero() && item.NextAttemptAt.After(now) {
			continue // 退避中，还未到重试时间
		}
		cp := *item
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// MarkDispatched 记录一次成功交给渠道的尝试：
// 尝试次数与意图版本递增，状态变为 dispatched（等待渠道回执）。
func (s *Store) MarkDispatched(id int64, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.findOutboxLocked(id)
	if item == nil {
		return fmt.Errorf("%w: outbox item %d", ErrNotFound, id)
	}
	if item.Status != DeliveryPending {
		return fmt.Errorf("%w: outbox item %d is %s", ErrDeliveryClosed, id, item.Status)
	}
	item.Attempts++
	item.Version++
	item.LastAttemptAt = now
	item.NextAttemptAt = time.Time{}
	item.Status = DeliveryDispatched
	s.appendHistoryLocked(item.IncidentID, "delivery_dispatched",
		fmt.Sprintf("step=%d target=%s attempt=%d version=%d",
			item.StepIndex, item.Target, item.Attempts, item.Version), now)
	return s.commitLocked()
}

// MarkAttemptFailed 记录一次失败的投递尝试（渠道调用本身失败）。
// 达到重试上限则进入 failed 终态，否则按退避策略安排下次重试。
func (s *Store) MarkAttemptFailed(id int64, cause string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.findOutboxLocked(id)
	if item == nil {
		return fmt.Errorf("%w: outbox item %d", ErrNotFound, id)
	}
	if item.Status != DeliveryPending && item.Status != DeliveryDispatched {
		return fmt.Errorf("%w: outbox item %d is %s", ErrDeliveryClosed, id, item.Status)
	}
	item.Attempts++
	item.LastError = cause
	item.LastAttemptAt = now
	s.scheduleRetryOrFailLocked(item, "dispatch_error", now)
	return s.commitLocked()
}

// scheduleRetryOrFailLocked 在一次失败（尝试失败/回执失败/回执超时）后
// 决定是安排重试还是进入 failed 终态；追加带原因的历史。调用方必须持有 mu。
func (s *Store) scheduleRetryOrFailLocked(item *OutboxItem, reason string, now time.Time) {
	rp := s.snap.RetryPolicy
	if item.Attempts >= rp.MaxAttempts {
		item.Status = DeliveryFailed
		item.NextAttemptAt = time.Time{}
		s.appendHistoryLocked(item.IncidentID, "delivery_failed",
			fmt.Sprintf("step=%d target=%s attempts=%d reason=%s error=%s",
				item.StepIndex, item.Target, item.Attempts, reason, item.LastError), now)
		return
	}
	item.Status = DeliveryPending
	item.NextAttemptAt = now.Add(rp.Backoff)
	s.appendHistoryLocked(item.IncidentID, "delivery_retry_scheduled",
		fmt.Sprintf("step=%d target=%s attempt=%d reason=%s next_at=%s error=%s",
			item.StepIndex, item.Target, item.Attempts, reason,
			item.NextAttemptAt.Format(time.RFC3339), item.LastError), now)
}

// RequeueTimedOutDispatches 把等待回执超时的 dispatched 意图重新入队（或置为失败）。
// 返回重新入队/置失败的条数。重启后同样适用：超时判断完全基于持久化的时间戳。
func (s *Store) RequeueTimedOutDispatches(now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rp := s.snap.RetryPolicy
	n := 0
	for _, item := range s.snap.Outbox {
		if item.Status != DeliveryDispatched || item.LastAttemptAt.IsZero() {
			continue
		}
		if now.Before(item.LastAttemptAt.Add(rp.ReceiptTimeout)) {
			continue
		}
		item.LastError = "receipt timeout"
		s.scheduleRetryOrFailLocked(item, "receipt_timeout", now)
		n++
	}
	if n > 0 {
		if err := s.commitLocked(); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// ReportReceipt 处理一条渠道回执。
//
// 保证：
//   - 回执按 (事件, 步骤, 目标, 意图版本) 匹配意图；版本落后于当前意图时
//     返回 ErrStaleReceipt，绝不把更高版本的状态改回去；
//   - 相同 ReceiptID 重放返回第一次的处理结果；同号异内容返回 ErrConflict；
//   - 已 acknowledged 的意图不会被迟到的失败回执降回失败；
//   - 回执处理、状态推进与历史追加在同一次原子写入中落盘。
func (s *Store) ReportReceipt(req ReceiptRequest, now time.Time) (ReceiptResult, error) {
	if strings.TrimSpace(req.ReceiptID) == "" {
		return ReceiptResult{}, fmt.Errorf("%w: receipt id is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(req.IncidentID) == "" || strings.TrimSpace(req.Target) == "" {
		return ReceiptResult{}, fmt.Errorf("%w: incident id and target are required", ErrInvalidArgument)
	}
	if req.Outcome != ReceiptAccepted && req.Outcome != ReceiptFailed {
		return ReceiptResult{}, fmt.Errorf("%w: unknown receipt outcome %q", ErrInvalidArgument, req.Outcome)
	}
	content := struct {
		IncidentID    string         `json:"incident_id"`
		StepIndex     int            `json:"step_index"`
		Target        string         `json:"target"`
		IntentVersion int            `json:"intent_version"`
		Outcome       ReceiptOutcome `json:"outcome"`
		Error         string         `json:"error"`
	}{req.IncidentID, req.StepIndex, req.Target, req.IntentVersion, req.Outcome, req.Error}
	fp := fingerprint(content)

	s.mu.Lock()
	defer s.mu.Unlock()

	// 幂等：相同回执重放返回第一次处理结果；同号异内容冲突。
	if rec, ok := s.snap.Receipts[req.ReceiptID]; ok {
		if rec.Fingerprint != fp {
			return ReceiptResult{}, fmt.Errorf("%w: receipt id %q reused with different payload", ErrConflict, req.ReceiptID)
		}
		res := ReceiptResult{Applied: rec.Applied, Status: rec.Status, Reason: rec.Reason}
		if rec.ErrKind == "stale" {
			return res, fmt.Errorf("%w: receipt %q", ErrStaleReceipt, req.ReceiptID)
		}
		return res, nil
	}

	item := s.findIntentLocked(req.IncidentID, req.StepIndex, req.Target)
	if item == nil {
		return ReceiptResult{}, fmt.Errorf("%w: intent (%s, step %d, %s)",
			ErrNotFound, req.IncidentID, req.StepIndex, req.Target)
	}

	var res ReceiptResult
	var errKind string
	switch {
	case req.IntentVersion != item.Version:
		// 迟到回执：意图已进入更高版本，拒绝改回。
		res = ReceiptResult{Applied: false, Status: item.Status, Reason: "stale_intent_version"}
		errKind = "stale"
		s.appendHistoryLocked(item.IncidentID, "receipt_ignored",
			fmt.Sprintf("step=%d target=%s receipt=%s reason=stale_intent_version receipt_version=%d current_version=%d",
				item.StepIndex, item.Target, req.ReceiptID, req.IntentVersion, item.Version), now)
	case item.Status == DeliveryAcknowledged:
		// 成功之后到达的回执（含失败）都不能把状态降回去。
		res = ReceiptResult{Applied: false, Status: item.Status, Reason: "already_acknowledged"}
		if req.Outcome == ReceiptFailed {
			s.appendHistoryLocked(item.IncidentID, "receipt_ignored",
				fmt.Sprintf("step=%d target=%s receipt=%s reason=failure_after_acknowledged",
					item.StepIndex, item.Target, req.ReceiptID), now)
		}
	case item.Status == DeliveryFailed || item.Status == DeliveryStopped:
		res = ReceiptResult{Applied: false, Status: item.Status, Reason: "already_terminal"}
		s.appendHistoryLocked(item.IncidentID, "receipt_ignored",
			fmt.Sprintf("step=%d target=%s receipt=%s reason=already_terminal status=%s",
				item.StepIndex, item.Target, req.ReceiptID, item.Status), now)
	case req.Outcome == ReceiptAccepted:
		item.Status = DeliveryAcknowledged
		item.AckedAt = now
		item.NextAttemptAt = time.Time{}
		s.appendHistoryLocked(item.IncidentID, "delivery_acknowledged",
			fmt.Sprintf("step=%d target=%s receipt=%s version=%d",
				item.StepIndex, item.Target, req.ReceiptID, req.IntentVersion), now)
		res = ReceiptResult{Applied: true, Status: item.Status, Reason: "applied"}
	default: // ReceiptFailed
		item.LastError = req.Error
		s.scheduleRetryOrFailLocked(item, "receipt_failed", now)
		res = ReceiptResult{Applied: true, Status: item.Status, Reason: "applied"}
	}

	s.snap.Receipts[req.ReceiptID] = receiptRecord{
		Fingerprint: fp,
		Applied:     res.Applied,
		Status:      res.Status,
		Reason:      res.Reason,
		ErrKind:     errKind,
	}
	if err := s.commitLocked(); err != nil {
		return ReceiptResult{}, err
	}
	if errKind == "stale" {
		return res, fmt.Errorf("%w: receipt %q", ErrStaleReceipt, req.ReceiptID)
	}
	return res, nil
}

// IncidentDelivery 返回事件级投递视图：事件本体、升级进度（已触发步骤数）
// 以及每个通知目标的最终投递结果。
func (s *Store) IncidentDelivery(incidentID string) (*IncidentDeliveryView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.snap.Incidents[incidentID]
	if !ok {
		return nil, fmt.Errorf("%w: incident %q", ErrNotFound, incidentID)
	}
	view := &IncidentDeliveryView{
		Incident:        cloneIncident(inc),
		EscalationLevel: inc.NextStepIndex,
	}
	for _, item := range s.snap.Outbox {
		if item.IncidentID != incidentID {
			continue
		}
		view.Deliveries = append(view.Deliveries, TargetDelivery{
			StepIndex:     item.StepIndex,
			Target:        item.Target,
			Channel:       item.Channel,
			Status:        item.Status,
			Version:       item.Version,
			Attempts:      item.Attempts,
			LastError:     item.LastError,
			LastAttemptAt: item.LastAttemptAt,
			AckedAt:       item.AckedAt,
			StopReason:    item.StopReason,
		})
	}
	sort.Slice(view.Deliveries, func(i, j int) bool {
		if view.Deliveries[i].StepIndex != view.Deliveries[j].StepIndex {
			return view.Deliveries[i].StepIndex < view.Deliveries[j].StepIndex
		}
		return view.Deliveries[i].Target < view.Deliveries[j].Target
	})
	return view, nil
}

// ---------- 历史 ----------

// History 返回指定事件的历史；incidentID 为空时返回全部历史。
func (s *Store) History(incidentID string) ([]HistoryEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if incidentID != "" {
		if _, ok := s.snap.Incidents[incidentID]; !ok {
			return nil, fmt.Errorf("%w: incident %q", ErrNotFound, incidentID)
		}
	}
	var out []HistoryEntry
	for _, h := range s.snap.History {
		if incidentID == "" || h.IncidentID == incidentID {
			out = append(out, h)
		}
	}
	return out, nil
}
