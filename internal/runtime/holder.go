package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// 重载流程的可区分错误。
var (
	// ErrClosed Holder 已关闭。
	ErrClosed = errors.New("runtime holder closed")
	// ErrBuildFailed 新状态构建失败；旧状态保持不变并继续可用。
	ErrBuildFailed = errors.New("reload build failed")
)

// generation 是一次装入的状态以及它当前在途读者的计数。
// 新读者只会被引到最新一代；旧一代的读者数归零后才允许回收其资源，
// 因此读者要么使用完整旧状态，要么使用完整新状态，不存在中间态。
type generation struct {
	state *State
	// readers 计数"装载之后才开始"的在途读者。装载之前开始的旧读者持有旧代租约，
	// 不会计入本代，因此代际切换不会让 Wait 提前/滞后返回。
	readers sync.WaitGroup
}

// lease 是一次读取的租约。
type lease struct {
	gen  *generation
	once sync.Once
}

func (l *lease) State() *State { return l.gen.state }

func (l *lease) Release() {
	l.once.Do(func() { l.gen.readers.Done() })
}

// Lease 是一次读取的租约；完成读取必须调用 Release 归还。
type Lease interface {
	State() *State
	Release()
}

// Builder 根据当前需要构建一个全新的完整状态。
// 返回 (nil, nil) 被视为构建失败，避免一次失误把运行时清空。
type Builder func(ctx context.Context) (*State, error)

// Holder 以原子方式持有当前 State，支持并发读取与整体热重载。
// 保证：
//   - Load 的读者要么看到完整旧状态，要么看到完整新状态，永不看到半状态；
//   - Reload 构建或校验失败时旧状态原样保留且继续可用；
//   - Reload 在新状态原子替换完成后立即返回，不被旧状态的在途慢读者阻塞；
//   - 旧状态只有在所有在途读者释放租约后才由后台 reaper 调用其 Close，且恰好一次。
type Holder struct {
	mu     sync.Mutex
	cur    *generation
	closed bool
	reapWG sync.WaitGroup // 等待后台旧态回收协程全部结束（Close 时使用）
}

// NewHolder 创建持有空状态的 Holder。
func NewHolder() *Holder { return &Holder{} }

// Load 获取当前状态的读取租约；Holder 关闭后返回包装 ErrClosed 的错误。
// 调用方必须在读取结束后调用 Lease.Release（建议 defer）。
// 尚无状态时返回 (nil, nil)，与"已关闭"明确区分。
func (h *Holder) Load() (Lease, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	if h.cur == nil {
		return nil, nil
	}
	h.cur.readers.Add(1)
	return &lease{gen: h.cur}, nil
}

// Current 以只读回调方式访问当前状态，回调期间状态保证不被回收。
// 尚未装入任何状态时以 nil 调用 fn；Holder 关闭返回 ErrClosed。
func (h *Holder) Current(fn func(*State)) error {
	l, err := h.Load()
	if err != nil {
		return err
	}
	if l == nil {
		fn(nil)
		return nil
	}
	defer l.Release()
	fn(l.State())
	return nil
}

// Reload 构建新状态并原子替换。
//
// 流程：
//  1. 在不持锁的情况下执行 build（可能较慢，如读文件/建连接），
//     这期间所有读者继续无阻塞地使用旧状态；
//  2. build 返回错误或 nil 状态：放弃本次重载，返回包装 ErrBuildFailed
//     的错误（可用 errors.Is 区分并取到根因），旧状态原样保留；
//  3. build 成功：加锁整体替换当前代后立即返回——新读者从此刻起只看到新状态，
//     旧状态交由后台 reaper 等其全部在途读者释放租约后回收，重载不被慢读者阻塞。
func (h *Holder) Reload(ctx context.Context, build Builder) error {
	if build == nil {
		return fmt.Errorf("%w: nil builder", ErrBuildFailed)
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrClosed
	}
	h.mu.Unlock()

	// 构建期间不持锁：读者不受影响，也不会看到半成品。
	state, err := build(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBuildFailed, err)
	}
	if state == nil {
		return fmt.Errorf("%w: builder returned nil state", ErrBuildFailed)
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		// 与 Close 并发竞争落败：销毁刚构建好的状态，避免资源泄漏。
		closeState(state)
		return ErrClosed
	}
	old := h.cur
	h.cur = &generation{state: state}
	// 仍在持锁区登记 reaper 计数：Close 的 reapWG.Wait() 只会在置 closed 之后发生，
	// 因此这里的 Add 不可能与 Wait 并发（WaitGroup 要求 Add 先于 Wait）。
	if old != nil {
		h.reapWG.Add(1)
	}
	h.mu.Unlock()

	// 替换此刻已对所有读者原子可见。旧态回收放到后台，避免慢读者阻塞重载。
	if old != nil {
		go h.reap(old)
	}
	return nil
}

// reap 在后台等待 gen 的全部在途读者释放租约，然后恰好回收一次其状态。
// 多个 reaper 各自等待不同的旧代；由于旧读者持有各自代的租约，回收顺序天然安全
// （更早的代若被慢读者持有，只是更晚回收，不会影响较新代）。
func (h *Holder) reap(gen *generation) {
	defer h.reapWG.Done()
	gen.readers.Wait()
	closeState(gen.state)
}

// Close 阻止后续 Load/Reload，等待在途读者与后台回收结束，并回收当前状态。
// 重复关闭返回包装 ErrClosed 的错误。
func (h *Holder) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrClosed
	}
	h.closed = true
	cur := h.cur
	h.cur = nil
	h.mu.Unlock()

	// 关闭后不会再有新读者；等待当前代在途读者排空后回收。
	if cur != nil {
		cur.readers.Wait()
		closeState(cur.state)
	}
	// 等待所有后台旧态回收协程结束，保证 Close 返回后不再触碰任何旧资源。
	h.reapWG.Wait()
	return nil
}

// HasState 报告是否已成功装入至少一个状态。
func (h *Holder) HasState() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cur != nil
}

func closeState(s *State) {
	if s != nil && s.Close != nil {
		s.Close()
	}
}
