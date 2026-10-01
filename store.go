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

// timerRecord 是 Timer 的持久化形态（当前同构，独立命名以便演进）。
type timerRecord = Timer

// snapshot 是一次性原子落盘的完整状态：事件状态、定时器、outbox 同生共死。
type snapshot struct {
	Policies     map[string]*Policy          `json:"policies"`
	Incidents    map[string]*Incident        `json:"incidents"`
	Outbox       []*OutboxItem               `json:"outbox"`
	NextOutboxID int64                       `json:"next_outbox_id"`
	Timers       map[string]*timerRecord     `json:"timers"`       // incidentID -> 唯一定时器
	SentIntents  map[string]struct{}         `json:"sent_intents"` // "incidentID\x00step\x00target" 去重
	Requests     map[string]requestRecord    `json:"requests"`
	Suppressions map[string]*SuppressionRule `json:"suppressions"` // ruleID -> 抑制规则
	History      []HistoryEntry              `json:"history"`
	NextSeq      int64                       `json:"next_seq"`
}

func newSnapshot() *snapshot {
	return &snapshot{
		Policies:     map[string]*Policy{},
		Incidents:    map[string]*Incident{},
		Timers:       map[string]*timerRecord{},
		SentIntents:  map[string]struct{}{},
		Requests:     map[string]requestRecord{},
		Suppressions: map[string]*SuppressionRule{},
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
	if snap.Suppressions == nil {
		snap.Suppressions = map[string]*SuppressionRule{}
	}
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
	cp.Handovers = append([]HandoverRecord(nil), in.Handovers...)
	cp.SeverityHistory = append([]SeverityChange(nil), in.SeverityHistory...)
	cp.SuppressedSteps = append([]SuppressedStep(nil), in.SuppressedSteps...)
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

	// 先处理窗口到期自动解除（可能在解除时补发通知）：
	// 这样解除抑制与步骤触发同刻到达时，同一次扫描内按“解除后的最新规则”决策。
	changed := s.autoReleaseDueLocked(now)

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

		inc.FiredAt[idx] = now
		inc.NextStepIndex = idx + 1
		inc.UpdatedAt = now

		// 抑制窗口内：升级照常推进（级别变化持续可见），但不产生通知，
		// 记录抑制原因，等解除抑制时按事件最新状态补发。
		var targets []string
		if rule := s.activeSuppressionLocked(inc, now); rule != nil {
			inc.SuppressedSteps = append(inc.SuppressedSteps, SuppressedStep{
				StepIndex: idx,
				FiredAt:   now,
				RuleID:    rule.ID,
				Reason:    rule.Reason,
			})
			s.appendHistoryLocked(id, "step_suppressed",
				fmt.Sprintf("step=%d rule=%s reason=%s", idx, rule.ID, rule.Reason), now)
		} else {
			for _, target := range inc.FrozenSteps[idx].Targets {
				if s.emitIntentLocked(inc, idx, target, now, false) {
					targets = append(targets, target)
				}
			}
			s.appendHistoryLocked(id, "step_fired",
				fmt.Sprintf("step=%d targets=%s", idx, strings.Join(targets, ",")), now)
		}

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
	if advanced > 0 || changed {
		if err := s.commitLocked(); err != nil {
			return 0, err
		}
	}
	return advanced, nil
}

// emitIntentLocked 形成一条通知意图并写入事务性 outbox。
// 正常升级与解除抑制后的补发共用同一套 (事件,步骤,目标) 去重，
// 因此每个级别的每个目标最多只有一次有效通知。
// 返回是否真正写入了新意图。调用方必须持有 mu。
func (s *Store) emitIntentLocked(inc *Incident, stepIndex int, target string, now time.Time, catchUp bool) bool {
	key := intentKey(inc.ID, stepIndex, target)
	if _, dup := s.snap.SentIntents[key]; dup {
		return false
	}
	s.snap.SentIntents[key] = struct{}{}
	s.snap.NextOutboxID++
	message := fmt.Sprintf("[incident %s] escalation step %d: %s", inc.ID, stepIndex, inc.Detail)
	if catchUp {
		message = fmt.Sprintf("[incident %s] catch-up escalation step %d (after suppression): %s",
			inc.ID, stepIndex, inc.Detail)
	}
	s.snap.Outbox = append(s.snap.Outbox, &OutboxItem{
		ID:         s.snap.NextOutboxID,
		IncidentID: inc.ID,
		StepIndex:  stepIndex,
		Target:     target,
		Channel:    "default",
		Message:    message,
		Status:     OutboxPending,
		Handler:    inc.CurrentHandler(),
		CatchUp:    catchUp,
		CreatedAt:  now,
	})
	return true
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

// MarkDelivered 将一条 outbox 意图标记为已投递。
// 投递与状态落盘是分离的：崩溃会导致重投（at-least-once），
// 通知渠道应以 item.ID / (事件,步骤,目标) 做幂等。
func (s *Store) MarkDelivered(id int64, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range s.snap.Outbox {
		if item.ID == id {
			if item.Status == OutboxPending {
				item.Status = OutboxDelivered
				item.DeliveredAt = now
				return s.commitLocked()
			}
			return nil
		}
	}
	return fmt.Errorf("%w: outbox item %d", ErrNotFound, id)
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
