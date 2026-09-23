package plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func logCase(t *testing.T, name, input, want string) {
	t.Helper()
	t.Logf("用例 %s | 输入: %s | 判定依据: %s", name, input, want)
}

// fakePlugin 是可编排的测试插件，用事件切片记录 init/close 的实际顺序。
type fakePlugin struct {
	name      string
	deps      []string
	initErr   error
	closeMu   *sync.Mutex // 可共享，用于在 Close 中阻塞
	block     chan struct{}
	events    *[]string
	afterInit func()
	mu        sync.Mutex
	inited    bool
	closed    bool
}

func (p *fakePlugin) Name() string        { return p.name }
func (p *fakePlugin) DependsOn() []string { return p.deps }

func (p *fakePlugin) Init(_ context.Context) error {
	if p.initErr != nil {
		return p.initErr
	}
	p.mu.Lock()
	p.inited = true
	p.mu.Unlock()
	*p.events = append(*p.events, "init:"+p.name)
	if p.afterInit != nil {
		p.afterInit()
	}
	return nil
}

func (p *fakePlugin) Close(_ context.Context) error {
	if p.block != nil {
		<-p.block // 测试需要时让 Close 等待外部放行
	}
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	*p.events = append(*p.events, "close:"+p.name)
	return nil
}

func regOf(plugins ...Plugin) *Registry {
	r := NewRegistry()
	for _, p := range plugins {
		if err := r.Register(p); err != nil {
			panic(err)
		}
	}
	return r
}

func TestRegister_DuplicateRejected(t *testing.T) {
	logCase(t, "TestRegister_DuplicateRejected", "连续注册两个名为 db 的插件",
		"第二次注册必须失败并包装 ErrDuplicatePlugin，且不覆盖原插件")

	r := NewRegistry()
	a := &fakePlugin{name: "db"}
	b := &fakePlugin{name: "db"}
	if err := r.Register(a); err != nil {
		t.Fatal(err)
	}
	err := r.Register(b)
	if !errors.Is(err, ErrDuplicatePlugin) {
		t.Fatalf("期望 ErrDuplicatePlugin，得到 %v", err)
	}
	got, _ := r.Get("db")
	if got != a {
		t.Fatal("重复注册不得覆盖已存在的插件")
	}
	t.Logf("得到预期拒绝: %v", err)
}

func TestTopoOrder_DependencyMissing(t *testing.T) {
	logCase(t, "TestTopoOrder_DependencyMissing", "a 依赖 ghost，但 ghost 从未注册",
		"必须返回包装 ErrDependencyMissing 的错误并点名 a -> ghost")

	r := regOf(&fakePlugin{name: "a", deps: []string{"ghost"}})
	_, err := TopoOrder(r)
	if !errors.Is(err, ErrDependencyMissing) {
		t.Fatalf("期望 ErrDependencyMissing，得到 %v", err)
	}
	if !strings.Contains(err.Error(), "a -> ghost") {
		t.Fatalf("错误应指出缺失边，得到 %v", err)
	}
}

func TestTopoOrder_CircularDependency(t *testing.T) {
	logCase(t, "TestTopoOrder_CircularDependency", "a->b->c->a 成环，另加独立节点 d",
		"必须返回 ErrCircularDependency，并只把环上节点列为 involved")

	r := regOf(
		&fakePlugin{name: "a", deps: []string{"b"}},
		&fakePlugin{name: "b", deps: []string{"c"}},
		&fakePlugin{name: "c", deps: []string{"a"}},
		&fakePlugin{name: "d"},
	)
	_, err := TopoOrder(r)
	if !errors.Is(err, ErrCircularDependency) {
		t.Fatalf("期望 ErrCircularDependency，得到 %v", err)
	}
	for _, involved := range []string{"a", "b", "c"} {
		if !strings.Contains(err.Error(), involved) {
			t.Fatalf("环上节点 %s 应出现在错误中: %v", involved, err)
		}
	}
	if strings.Contains(err.Error(), " d]") || strings.Contains(err.Error(), "[d ") {
		t.Fatalf("独立节点 d 不应被卷入环: %v", err)
	}
	t.Logf("得到预期错误: %v", err)
}

func TestTopoOrder_Deterministic(t *testing.T) {
	logCase(t, "TestTopoOrder_Deterministic", "a,c 无依赖；b 依赖 a；d 依赖 a,c",
		"同层候选按字典序选出，多次排序结果完全一致")

	mk := func() *Registry {
		return regOf(
			&fakePlugin{name: "a"},
			&fakePlugin{name: "c"},
			&fakePlugin{name: "b", deps: []string{"a"}},
			&fakePlugin{name: "d", deps: []string{"a", "c"}},
		)
	}
	// 取走 a 后 b（仅依赖 a）与 c 同时就绪，按字典序 b 先于 c；
	// b、c 都完成后 d 才就绪。
	want := []string{"a", "b", "c", "d"}
	for i := 0; i < 10; i++ {
		o, err := TopoOrder(mk())
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(o.Names()) != fmt.Sprint(want) {
			t.Fatalf("第 %d 次排序不确定: got %v want %v", i, o.Names(), want)
		}
	}
}

func TestStart_OrderAndCloseReverse(t *testing.T) {
	logCase(t, "TestStart_OrderAndCloseReverse", "logger <- store <- api, api 同时依赖 logger",
		"Init 必须满足拓扑（依赖在前）；CloseAll 必须严格逆序")

	var events []string
	logger := &fakePlugin{name: "logger", events: &events}
	store := &fakePlugin{name: "store", deps: []string{"logger"}, events: &events}
	api := &fakePlugin{name: "api", deps: []string{"store", "logger"}, events: &events}

	m := NewManager(regOf(logger, store, api))
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantStart := []string{"init:logger", "init:store", "init:api"}
	if fmt.Sprint(events) != fmt.Sprint(wantStart) {
		t.Fatalf("启动顺序错误: got %v want %v", events, wantStart)
	}
	if !m.IsRunning() {
		t.Fatal("启动后应处于运行态")
	}

	if err := m.CloseAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantFull := append(append([]string{}, wantStart...), "close:api", "close:store", "close:logger")
	if fmt.Sprint(events) != fmt.Sprint(wantFull) {
		t.Fatalf("关闭顺序错误: got %v want %v", events, wantFull)
	}
}

func TestStart_RollbackOnFailure(t *testing.T) {
	logCase(t, "TestStart_RollbackOnFailure", "a,b 启动成功；c 的 Init 失败",
		"已启动插件必须被逆序 Close（b 然后 a），c 未初始化不关闭，管理器不残留运行态")

	var events []string
	a := &fakePlugin{name: "a", events: &events}
	b := &fakePlugin{name: "b", deps: []string{"a"}, events: &events}
	c := &fakePlugin{name: "c", deps: []string{"b"}, events: &events, initErr: errors.New("boom")}

	m := NewManager(regOf(a, b, c))
	err := m.Start(context.Background())
	if !errors.Is(err, ErrInitFailed) {
		t.Fatalf("期望 ErrInitFailed，得到 %v", err)
	}
	if !strings.Contains(err.Error(), "c") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("错误应点名失败插件与原因: %v", err)
	}
	want := []string{"init:a", "init:b", "close:b", "close:a"}
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Fatalf("回滚顺序错误: got %v want %v", events, want)
	}
	if m.IsRunning() {
		t.Fatal("回滚后管理器不得处于运行态")
	}
	if len(m.Started()) != 0 {
		t.Fatalf("回滚后不得残留已启动插件: %v", m.Started())
	}
	if c.closed {
		t.Fatal("Init 失败的插件 c 不应被 Close")
	}
	// 回滚后 CloseAll 应当幂等无副作用（无运行插件）。
	if err := m.CloseAll(context.Background()); err != nil {
		t.Fatalf("回滚后 CloseAll 应安全: %v", err)
	}
	t.Logf("回滚事件序列: %v", events)
}

func TestStart_CancelledContextRollsBack(t *testing.T) {
	logCase(t, "TestStart_CancelledContextRollsBack", "a Init 成功后立即取消 ctx，b 尚未 Init",
		"取消必须视同启动失败：a 被回滚 Close，b 从未 Init，管理器不残留运行态")

	var events []string
	ctx, cancel := context.WithCancel(context.Background())
	a := &fakePlugin{name: "a", events: &events, afterInit: cancel}
	b := &fakePlugin{name: "b", deps: []string{"a"}, events: &events}

	m := NewManager(regOf(a, b))
	err := m.Start(ctx)
	if !errors.Is(err, ErrInitFailed) {
		t.Fatalf("ctx 取消应导致 ErrInitFailed，得到 %v", err)
	}
	want := []string{"init:a", "close:a"}
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Fatalf("应只回滚 a: got %v want %v", events, want)
	}
	if b.inited {
		t.Fatal("b 不应被初始化")
	}
	if m.IsRunning() || len(m.Started()) != 0 {
		t.Fatal("回滚后不得残留运行态")
	}
}

func TestCloseAll_AggregatesErrorsAndClosesAll(t *testing.T) {
	logCase(t, "TestCloseAll_AggregatesErrorsAndClosesAll", "三个插件，中间和最后 Close 返回错误",
		"即使个别 Close 失败也必须继续关闭其余插件，错误用 errors.Join 聚合")

	var events []string
	ec1 := errors.New("close fail 1")
	ec2 := errors.New("close fail 2")
	a := &fakePlugin{name: "a", events: &events}
	b := &fakePlugin{name: "b", deps: []string{"a"}, events: &events}
	c := &fakePlugin{name: "c", deps: []string{"b"}, events: &events}

	// 给 b、c 注入 close 错误：用 closeErrPlugin 包装。
	rb := &closeErrPlugin{fakePlugin: b, err: ec1}
	rc := &closeErrPlugin{fakePlugin: c, err: ec2}

	m := NewManager(regOf(a, rb, rc))
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := m.CloseAll(context.Background())
	if err == nil {
		t.Fatal("期望聚合的关闭错误")
	}
	if !errors.Is(err, ec1) || !errors.Is(err, ec2) {
		t.Fatalf("两个关闭错误都应可被 errors.Is 命中: %v", err)
	}
	if !a.closed || !b.closed || !c.closed {
		t.Fatal("即使出错，所有插件都必须被调用 Close")
	}
	// 再次关闭应返回 ErrPluginClosed。
	if err := m.CloseAll(context.Background()); !errors.Is(err, ErrPluginClosed) {
		t.Fatalf("重复 CloseAll 期望 ErrPluginClosed，得到 %v", err)
	}
}

// closeErrPlugin 在 fakePlugin 之上让 Close 返回指定错误，同时保留事件记录。
type closeErrPlugin struct {
	*fakePlugin
	err error
}

func (p *closeErrPlugin) Close(ctx context.Context) error {
	if err := p.fakePlugin.Close(ctx); err != nil {
		return err
	}
	return p.err
}
