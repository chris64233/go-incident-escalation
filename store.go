package incidentescalation

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// storeData 是落盘的完整状态：策略、事件、定时器、outbox、历史与幂等记录。
// 所有这些内容在同一次原子写入中持久化，保证状态、定时器与 outbox 一致。
type storeData struct {
	Policies    map[string]Policy            `json:"policies"`
	Incidents   map[string]*Incident         `json:"incidents"`
	Timers      map[string]Timer             `json:"timers"` // key: incidentID，每事件至多一个
	Outbox      map[string]OutboxEntry       `json:"outbox"` // key: incidentID|step|target
	History     []HistoryEvent               `json:"history"`
	Idempotency map[string]idempotencyRecord `json:"idempotency"`
}

func newStoreData() *storeData {
	return &storeData{
		Policies:    map[string]Policy{},
		Incidents:   map[string]*Incident{},
		Timers:      map[string]Timer{},
		Outbox:      map[string]OutboxEntry{},
		History:     []HistoryEvent{},
		Idempotency: map[string]idempotencyRecord{},
	}
}

// Store 是基于单个 JSON 文件的持久化存储。
// 所有读写都经由同一把互斥锁串行化，因此确认、解决、定时器推进即使同时发生，
// 也会按获取锁的顺序依次生效，竞态最终只会形成一个合法状态。
type Store struct {
	mu   sync.Mutex
	path string
	data *storeData
}

// OpenStore 打开（或创建）目录 dir 下的持久化存储。
func OpenStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, validationErr("storage dir is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create storage dir: %w", err)
	}
	s := &Store{
		path: filepath.Join(dir, "incident-escalation.json"),
		data: newStoreData(),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read storage: %w", err)
	}
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, s.data); err != nil {
		return fmt.Errorf("corrupt storage file %s: %w", s.path, err)
	}
	if s.data.Policies == nil {
		s.data = newStoreData()
	}
	return nil
}

// persistLocked 必须在持锁状态下调用：先写临时文件再原子重命名，
// 进程在任意时刻崩溃都不会留下半截状态文件。
func (s *Store) persistLocked() error {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode storage: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("write storage temp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace storage file: %w", err)
	}
	return nil
}

// update 在一个持锁临界区中执行 fn，并在其成功后与状态变更一起原子落盘。
func (s *Store) update(fn func(d *storeData) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := fn(s.data); err != nil {
		return err
	}
	return s.persistLocked()
}

// read 在持锁状态下执行只读 fn。
func (s *Store) read(fn func(d *storeData)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.data)
}

func cloneIncident(in *Incident) *Incident {
	if in == nil {
		return nil
	}
	cp := *in
	cp.Policy = clonePolicy(in.Policy)
	if in.AckedAt != nil {
		t := *in.AckedAt
		cp.AckedAt = &t
	}
	if in.ResolvedAt != nil {
		t := *in.ResolvedAt
		cp.ResolvedAt = &t
	}
	return &cp
}

func clonePolicy(p Policy) Policy {
	cp := p
	cp.Steps = make([]Step, len(p.Steps))
	for i, st := range p.Steps {
		cp.Steps[i] = Step{WaitDuration: st.WaitDuration, Targets: append([]string(nil), st.Targets...)}
	}
	return cp
}

func sortedIncidents(d *storeData) []*Incident {
	out := make([]*Incident, 0, len(d.Incidents))
	for _, in := range d.Incidents {
		out = append(out, cloneIncident(in))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}
