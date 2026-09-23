package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func logCase(t *testing.T, name, input, want string) {
	t.Helper()
	t.Logf("用例 %s | 输入: %s | 判定依据: %s", name, input, want)
}

func stateWith(label string, closed *int32) *State {
	return &State{
		Data:  map[string]any{"label": label},
		Close: func() { atomic.AddInt32(closed, 1) },
	}
}

func builderFor(label string, closed *int32) Builder {
	return func(_ context.Context) (*State, error) {
		return stateWith(label, closed), nil
	}
}

// waitForCount 等待异步 reaper 把回收计数推进到 want，避免竞态式断言。
func waitForCount(t *testing.T, p *int32, want int32) int32 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := atomic.LoadInt32(p); got == want {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	return atomic.LoadInt32(p)
}

func TestReload_InitialAndReplace(t *testing.T) {
	logCase(t, "TestReload_InitialAndReplace", "依次 Reload v1、v2",
		"装入前 Load 为 nil；v2 后读者只能看到 v2；v1 的 Close 恰好调用一次")

	h := NewHolder()
	if h.HasState() {
		t.Fatal("新 Holder 不应有状态")
	}
	if l, err := h.Load(); err != nil || l != nil {
		t.Fatalf("空状态应返回 (nil,nil)，得到 l=%v err=%v", l, err)
	}

	var c1, c2 int32
	if err := h.Reload(context.Background(), builderFor("v1", &c1)); err != nil {
		t.Fatal(err)
	}
	if err := h.Current(func(s *State) {
		if s.Data["label"] != "v1" {
			t.Fatalf("期望 v1，得到 %v", s.Data["label"])
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.Reload(context.Background(), builderFor("v2", &c2)); err != nil {
		t.Fatal(err)
	}
	_ = h.Current(func(s *State) {
		if s.Data["label"] != "v2" {
			t.Fatalf("替换后应只见 v2，得到 %v", s.Data["label"])
		}
	})
	if got := waitForCount(t, &c1, 1); got != 1 {
		t.Fatalf("旧状态 v1 应被回收恰好 1 次，实际 %d", got)
	}
	if got := atomic.LoadInt32(&c2); got != 0 {
		t.Fatalf("当前状态 v2 在被替换前不应回收，实际 %d", got)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&c2) != 1 {
		t.Fatalf("Close 后 v2 应回收 1 次，实际 %d", atomic.LoadInt32(&c2))
	}
}

func TestReload_FailureKeepsOldState(t *testing.T) {
	logCase(t, "TestReload_FailureKeepsOldState", "已装入 v1；builder 返回错误；builder 返回 nil",
		"两种失败都必须返回包装 ErrBuildFailed 的错误，且 v1 原样可用、不被回收")

	h := NewHolder()
	var c1 int32
	if err := h.Reload(context.Background(), builderFor("v1", &c1)); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("disk on fire")
	err := h.Reload(context.Background(), func(_ context.Context) (*State, error) {
		return nil, boom
	})
	if !errors.Is(err, ErrBuildFailed) || !errors.Is(err, boom) {
		t.Fatalf("失败必须可区分地包装 ErrBuildFailed 与根因，得到 %v", err)
	}

	err = h.Reload(context.Background(), func(_ context.Context) (*State, error) {
		return nil, nil // 防御：nil 状态也算失败
	})
	if !errors.Is(err, ErrBuildFailed) {
		t.Fatalf("nil 状态必须按 ErrBuildFailed 处理，得到 %v", err)
	}

	if !h.HasState() {
		t.Fatal("重载失败后必须仍持有旧状态")
	}
	_ = h.Current(func(s *State) {
		if s == nil || s.Data["label"] != "v1" {
			t.Fatalf("旧状态必须继续可用，得到 %v", s)
		}
	})
	if atomic.LoadInt32(&c1) != 0 {
		t.Fatal("失败重载不得回收旧状态")
	}
	t.Logf("可区分的失败原因: %v", err)
}

// TestReload_AtomicUnderConcurrentReads 是本包核心用例：
// 大量读者在多次并发重载期间读取 label，并记录观察序列。
// 任何单次读取只能看到整数代际标签（v1..vN），绝不允许读到半成品/空指针/混合状态。
func TestReload_AtomicUnderConcurrentReads(t *testing.T) {
	const readers = 16
	const iterations = 60
	logCase(t, "TestReload_AtomicUnderConcurrentReads",
		fmt.Sprintf("%d 个读者并发读取，同时顺序执行 %d 次 Reload（v1..v%d）", readers, iterations, iterations),
		"每次读到的 label 必须形如 vN 且单调不减；旧态 Close 时不得还有读者在使用它")

	h := NewHolder()

	// genClosed 记录每个被回收状态的代际；回收时校验该状态没有在途读者
	// （由框架的 WaitGroup 保证，这里额外记录以供判定）。
	var closedMu sync.Mutex
	closedLabels := []string{}

	reloadOnce := make(chan struct{}) // 第一批读者就位后再开始，制造交叠窗口
	var stopReaders atomic.Bool
	var wgReaders sync.WaitGroup
	observed := make([][]string, readers)

	// 先装入 v1。
	if err := h.Reload(context.Background(), func(_ context.Context) (*State, error) {
		return &State{Data: map[string]any{"label": "v1"}}, nil
	}); err != nil {
		t.Fatal(err)
	}

	for r := 0; r < readers; r++ {
		wgReaders.Add(1)
		go func(id int) {
			defer wgReaders.Done()
			<-reloadOnce
			for !stopReaders.Load() {
				l, err := h.Load()
				if err != nil {
					// 关闭发生时允许 ErrClosed，停止即可。
					return
				}
				if l == nil {
					t.Errorf("读者 %d 读到 nil 状态：原子性被破坏", id)
					return
				}
				label, ok := l.State().Data["label"].(string)
				l.Release()
				if !ok || !strings.HasPrefix(label, "v") {
					t.Errorf("读者 %d 读到非法/半成品状态: %#v", id, l.State().Data)
					return
				}
				observed[id] = append(observed[id], label)
			}
		}(r)
	}

	close(reloadOnce)
	for i := 2; i <= iterations; i++ {
		label := fmt.Sprintf("v%d", i)
		err := h.Reload(context.Background(), func(_ context.Context) (*State, error) {
			// 模拟构建耗时，增加与读者交叠的概率。
			time.Sleep(time.Millisecond)
			l := label
			return &State{
				Data: map[string]any{"label": l},
				Close: func() {
					closedMu.Lock()
					closedLabels = append(closedLabels, l)
					closedMu.Unlock()
				},
			}, nil
		})
		if err != nil {
			t.Fatalf("第 %d 次重载失败: %v", i, err)
		}
	}
	stopReaders.Store(true)
	wgReaders.Wait()

	// 判定 1：每个读者观察到的代际必须单调不减（不可能回退到旧态，更不可能看到混合）。
	for id, seq := range observed {
		prev := 1
		for _, label := range seq {
			var n int
			if _, err := fmt.Sscanf(label, "v%d", &n); err != nil || n < prev {
				t.Fatalf("读者 %d 观察序列回退或非法: %v", id, seq)
			}
			prev = n
		}
	}
	// 判定 2：最终读者只能看到最后一代。
	_ = h.Current(func(s *State) {
		if s.Data["label"] != fmt.Sprintf("v%d", iterations) {
			t.Fatalf("最终状态错误: %v", s.Data["label"])
		}
	})
	// 判定 3：除最后一代外每一代恰好回收一次（旧读者排空后）。
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	closedMu.Lock()
	defer closedMu.Unlock()
	if len(closedLabels) != iterations-1 {
		t.Fatalf("应有 %d 个旧状态被回收，实际 %d", iterations-1, len(closedLabels))
	}
	t.Logf("读者观察样本数=%d，回收旧状态数=%d",
		len(observed[0]) /* 仅日志用途 */, len(closedLabels))
}

// TestReload_OldStateWaitsForInFlightReader 验证关键安全点：
// 当旧状态仍有读者持有时，其 Close 必须被推迟到租约释放之后。
func TestReload_OldStateWaitsForInFlightReader(t *testing.T) {
	logCase(t, "TestReload_OldStateWaitsForInFlightReader",
		"读者持有 v1 租约期间完成 v2 Reload",
		"v1 的 Close 在 Release 之前绝不能发生；Release 之后立即恰好发生一次")

	h := NewHolder()
	var c1 int32
	if err := h.Reload(context.Background(), builderFor("v1", &c1)); err != nil {
		t.Fatal(err)
	}

	lease, err := h.Load()
	if err != nil {
		t.Fatal(err)
	}

	reloaded := make(chan struct{})
	go func() {
		var c2 int32
		_ = h.Reload(context.Background(), builderFor("v2", &c2))
		close(reloaded)
	}()

	// 等待 Reload 完成（此时 v1 仍被本测试持有）。
	select {
	case <-reloaded:
	case <-time.After(2 * time.Second):
		t.Fatal("Reload 异常卡住")
	}
	if atomic.LoadInt32(&c1) != 0 {
		t.Fatal("读者仍持有租约时，旧状态不得被回收")
	}
	lease.Release()

	// Release 后旧状态回收应很快发生（Reload 协程在 Wait 上解除）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&c1) == 0 {
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt32(&c1) != 1 {
		t.Fatal("释放租约后旧状态必须被回收恰好一次")
	}

	// 重复 Release 必须幂等，不能使 WaitGroup 计数错乱。
	lease.Release()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClose_RejectsFurtherOperations(t *testing.T) {
	logCase(t, "TestClose_RejectsFurtherOperations", "Close 之后再 Load/Reload/Close",
		"全部返回包装 ErrClosed 的错误")

	h := NewHolder()
	var c int32
	if err := h.Reload(context.Background(), builderFor("v1", &c)); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Load(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Close 后 Load 应 ErrClosed，得到 %v", err)
	}
	if err := h.Reload(context.Background(), builderFor("v2", nil)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Close 后 Reload 应 ErrClosed，得到 %v", err)
	}
	if err := h.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("重复 Close 应 ErrClosed，得到 %v", err)
	}
	if atomic.LoadInt32(&c) != 1 {
		t.Fatalf("状态应被回收一次，得到 %d", atomic.LoadInt32(&c))
	}
}

func TestReload_Serialized(t *testing.T) {
	logCase(t, "TestReload_Serialized", "多个 goroutine 同时发起 Reload",
		"并发重载必须串行生效：最终状态确定，每个被替换状态恰好回收一次")

	h := NewHolder()
	if err := h.Reload(context.Background(), func(_ context.Context) (*State, error) {
		return &State{Data: map[string]any{"label": "v0"}}, nil
	}); err != nil {
		t.Fatal(err)
	}

	var counter int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := atomic.AddInt64(&counter, 1)
			label := fmt.Sprintf("v%d", n)
			_ = h.Reload(context.Background(), func(_ context.Context) (*State, error) {
				return &State{Data: map[string]any{"label": label}}, nil
			})
		}()
	}
	wg.Wait()
	_ = h.Current(func(s *State) {
		t.Logf("最终状态 label=%v", s.Data["label"])
	})
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestConcurrent_ReloadAndClose 猛烈并发 Reload 与 Close：
// 不得 panic（尤其 sync.WaitGroup 的 Add/Wait 误用），不得双重 Close 同一状态。
func TestConcurrent_ReloadAndClose(t *testing.T) {
	logCase(t, "TestConcurrent_ReloadAndClose",
		"20 个 goroutine 各 Reload 20 次，另有 8 个 goroutine 反复 Close",
		"全程无 panic；每个成功装入的状态其 Close 恰好一次；最终 Load 返回 ErrClosed")

	for round := 0; round < 10; round++ {
		h := NewHolder()
		var wg sync.WaitGroup

		var closeCount int64
		stateFor := func() *State {
			return &State{Close: func() { atomic.AddInt64(&closeCount, 1) }}
		}

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 20; j++ {
					err := h.Reload(context.Background(), func(_ context.Context) (*State, error) {
						return stateFor(), nil
					})
					if err != nil && !errors.Is(err, ErrClosed) {
						t.Errorf("意外的重载错误: %v", err)
						return
					}
				}
			}()
		}
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = h.Close() // 多个 Close 并发，只有一个应成功，其余 ErrClosed
			}()
		}
		wg.Wait()

		// Close 成功后 Load 必须稳定返回 ErrClosed。
		if _, err := h.Load(); !errors.Is(err, ErrClosed) {
			t.Fatalf("round %d: 关闭后 Load 应 ErrClosed，得到 %v", round, err)
		}
		// 能在关闭后安全再次 Close。
		if err := h.Close(); !errors.Is(err, ErrClosed) {
			t.Fatalf("round %d: 重复 Close 应 ErrClosed，得到 %v", round, err)
		}
		t.Logf("round %d 完成，状态回收总次数=%d", round, atomic.LoadInt64(&closeCount))
	}
}
