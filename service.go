package incidentescalation

import (
	"sync"
	"time"
)

// Dispatcher 负责把 outbox 中的通知意图真正发出去（邮件/IM/电话等）。
// 语义为 at-least-once：服务崩溃重启后未确认投递的意图会被再次投递，
// 实现方应按 OutboxItem.ID 或 (事件,步骤,目标) 做下游幂等。
type Dispatcher interface {
	Dispatch(item OutboxItem) error
}

// DispatcherFunc 让普通函数满足 Dispatcher。
type DispatcherFunc func(item OutboxItem) error

// Dispatch 实现 Dispatcher。
func (f DispatcherFunc) Dispatch(item OutboxItem) error { return f(item) }

// Config 是 Service 的可调参数。
type Config struct {
	// TickInterval 是后台扫描到期定时器的间隔。默认 100ms。
	TickInterval time.Duration
	// DispatchInterval 是后台捞取 pending outbox 的间隔。默认 100ms。
	DispatchInterval time.Duration
	// Now 可注入时钟（测试用）；为 nil 时用 time.Now。
	Now func() time.Time
}

// Service 在 Store 之上提供后台“到期推进 + outbox 投递”循环。
// 它不持有任何易失状态：所有定时器都已持久化，因此进程重启后 Start 即继续。
type Service struct {
	store      *Store
	dispatcher Dispatcher
	cfg        Config

	mu              sync.Mutex
	stop            chan struct{}
	stopped         chan struct{}
	dispatchStop    chan struct{}
	dispatchStopped chan struct{}
	running         bool
}

// NewService 创建服务。dispatcher 为 nil 时 outbox 只持久化不自动投递，
// 调用方仍可手动通过 PendingOutbox/MarkDelivered 处理。
func NewService(store *Store, dispatcher Dispatcher, cfg Config) *Service {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.DispatchInterval <= 0 {
		cfg.DispatchInterval = 100 * time.Millisecond
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Service{store: store, dispatcher: dispatcher, cfg: cfg}
}

// Start 启动后台循环。重复 Start 返回已在运行的同一实例，不另起 goroutine。
func (svc *Service) Start() {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.running {
		return
	}
	svc.running = true
	svc.stop = make(chan struct{})
	svc.stopped = make(chan struct{})
	svc.dispatchStop = make(chan struct{})
	svc.dispatchStopped = make(chan struct{})

	go svc.runTicks(svc.stop, svc.stopped)
	if svc.dispatcher != nil {
		go svc.runDispatch(svc.dispatchStop, svc.dispatchStopped)
	} else {
		close(svc.dispatchStopped)
	}
}

// Stop 停止后台循环并等待退出。
func (svc *Service) Stop() {
	svc.mu.Lock()
	if !svc.running {
		svc.mu.Unlock()
		return
	}
	stop, stopped := svc.stop, svc.stopped
	dStop, dStopped := svc.dispatchStop, svc.dispatchStopped
	svc.running = false
	svc.mu.Unlock()

	close(stop)
	close(dStop)
	<-stopped
	<-dStopped
}

func (svc *Service) runTicks(stop, stopped chan struct{}) {
	defer close(stopped)
	ticker := time.NewTicker(svc.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// 推进失败（如磁盘错误）不致命：下个 tick 基于持久化状态重试，
			// 不会漏推（定时器还在）也不会重推（意图去重 + 每次一步）。
			_, _ = svc.store.ProcessDue(svc.cfg.Now())
		}
	}
}

func (svc *Service) runDispatch(stop, stopped chan struct{}) {
	defer close(stopped)
	ticker := time.NewTicker(svc.cfg.DispatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			svc.drainOutbox()
		}
	}
}

func (svc *Service) drainOutbox() {
	for _, item := range svc.store.PendingOutbox() {
		if err := svc.dispatcher.Dispatch(*item); err != nil {
			// 投递失败：保留 pending，下轮重试；跳过本条，不阻塞其它意图。
			continue
		}
		_ = svc.store.MarkDelivered(item.ID, svc.cfg.Now())
	}
}

// Store 暴露底层存储以便直接调用各领域操作。
func (svc *Service) Store() *Store { return svc.store }
