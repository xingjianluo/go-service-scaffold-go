# Go Service Scaffold

一套可复用的 Go 服务脚手架，把**配置、插件、运行时状态**组织成一致且可预测的
启动 / 运行 / 热重载流程。仅依赖 Go 标准库，Go 1.25+。

## 包结构

| 包 | 职责 |
| --- | --- |
| `config` | 分层配置合并、类型校验、必填校验、取值校验、密钥脱敏、来源可解释 |
| `plugin` | 插件注册、依赖拓扑排序、按序初始化、失败回滚、逆序关闭 |
| `runtime` | 原子快照（`atomic.Pointer`）、热重载、失败保留旧状态；**代际（Generation）+ 租约（Lease）**的在途保护、有界回收、可观测状态 |
| `app` | 门面：把三者串成 `Boot → Reload* → Shutdown` 的确定流程；可选开启代际回收 |
| `cmd/example` | 可运行的最小示例服务 |

## 一分钟上手

```go
loader := config.NewLoader(schema, "configs/example.json", "APP")
// 默认即与历史行为一致；开启 WithGenerationRetirement 后获得在途保护：
application := app.New(loader, buildSnapshot,
    app.WithGenerationRetirement(4, 5*time.Second))

if err := application.Boot(ctx); err != nil { /* 配置非法或首启失败，进程退出 */ }

// 推荐的读取生命周期：先拿租约，整次操作只用这一代，用完释放
lease, err := application.Acquire()   // 钉住“当前”这一代
if err != nil { /* 未启动或已 Shutdown */ }
defer lease.Release()                 // 用完释放，旧代才能被回收
snap := lease.Snapshot()              // 这次操作从头到尾是同一份 config + 同一组 plugins

_ = application.Reload(ctx)           // 重新读配置并原子换入新一代；失败/超容则旧状态继续服务
_ = application.Shutdown()            // 摘除快照，排空在途租约后逆序关闭插件
```

完整可运行示例见 `cmd/example`：

```bash
go run ./cmd/example -config configs/example.json
```

## 配置优先级

三层数据按以下**从低到高**的固定优先级合并（高优先级覆盖低优先级，逐字段覆盖）：

1. **默认值** `Schema.Default`
2. **配置文件**（JSON，扁平点号键，例如 `"server.port": 8080`；文件不存在时静默跳过）
3. **环境变量**（最高优先级）

环境变量名由点号键推导：`前缀_` + 键大写并把 `.`、`-` 替换为 `_`。
例如前缀 `APP` 时，`db.password` → `APP_DB_PASSWORD`，`server.port` → `APP_SERVER_PORT`。

- `int` / `bool` 环境变量按 `strconv.Atoi` / `strconv.ParseBool` 解析。
- 每个字段的**最终获胜来源**（`default` / `file` / `env`）由 `Loader.Explain` 返回，
  判定依据完全可解释。
- 运行时可用 `Config.Describe(schema)` 打印确定性的、按行排序的配置概览。

### 拒绝规则与可区分的失败原因

加载失败统一返回 `*config.LoadError`，每个字段携带可机读的 `FailureKind`：

| Kind | 触发条件 |
| --- | --- |
| `FailureMissingRequired` | 必填字段在三层中均缺失，或值为空字符串 |
| `FailureTypeMismatch` | 文件/环境提供的值无法转换为声明类型（string/int/bool） |
| `FailureInvalidValue` | 通过 `Validator` 校验失败，或文件中出现 Schema 未声明的字段 |

JSON 语法错误、文件读取错误不作为字段错误聚合，而是直接返回包装后的底层错误
（可用 `errors.As(err, &jsonSyntaxErr)` 之类方式区分）。

### 密钥安全

- `FieldSpec.Secret: true` 的字段在 `Describe` 中固定渲染为 `key=*** (source)`。
- 所有 `FieldError` / `LoadError` 消息**只包含字段名、来源与原因，从不包含值**。
- 密钥字段的 `Validator` 即使误把值拼进 error 文本，也会被框架统一替换为
  `"value rejected by validator (value hidden as field is secret)"`。

## 插件契约

```go
type Plugin interface {
    Name() string              // 唯一名称，空名/重名一律拒绝
    Deps() []string            // 依赖的其他插件名
    Init(ctx context.Context) error
    Close() error
}
```

- **注册**：`Registry.Register` 拒绝 nil、空名、重名，绝不静默覆盖已有注册。
- **排序**：`Manager.InitOrder` 以 Kahn 拓扑排序解析依赖；同层按名字母序打破平局，
  保证顺序确定。
- **失败前置拒绝**：依赖缺失（`PluginMissingDep`）或循环依赖（`PluginCycle`）
  在调用任何 `Init` 之前即被拒绝，不会产生半初始化状态。
- **启动**：`Manager.Start` 按拓扑序逐个 `Init`；任一插件失败时，
  已启动的插件**严格按启动逆序** `Close` 回滚，返回的 `*plugin.Error`
  （Kind=`PluginInitFailed`）带：
  - `Rollback`：被回滚关闭的插件名单（逆序）；
  - `WrapErr`：初始化失败的原始错误；
  - `CloseErr`：回滚过程中出现的关闭错误（如有，聚合而非中断）。
  回滚后 `Manager.Active()` 为空。
- **关闭**：`Manager.Close` 按启动逆序关闭，收集所有关闭错误为一个
  `PluginCloseFailed`，不会在第一个错误处中断；重复调用安全。

## 热重载规则

`runtime.Holder` 内部是一个 `atomic.Pointer[Snapshot]`，`Snapshot` 是
"`*config.Config` + `[]plugin.Plugin`" 的不可变一代：

1. **先建后换**：`Reloader.Reload` 先调用 `BuildFunc` 构建并初始化新一代；
   构建期间旧快照继续对外服务。
2. **失败保留旧状态**：构建返回 error（或 context 取消）时不触碰旧快照，
   返回 `*runtime.ReloadError`，其 `OldKept=true` 且 Kind 可区分：
   `ReloadBuildFailed`（构建失败）/ `ReloadTimeout`（context 取消/超时）。
3. **原子发布**：构建成功后执行**唯一一次**指针 `Swap`。并发读取方
   要么看到完整旧快照、要么看到完整新快照，不可能看到半合并状态。
4. **换后清理**：上一代插件在 Swap 成功之后才按逆序 `Close`，
   不会影响已持有旧快照的在途请求的读取一致性（注意：默认的 `Reloader`
   路径在 Swap 返回前即关闭旧插件；要让"整次请求都能用旧插件"，请使用下面的
   **代际 + 租约**能力 `runtime.Guard` / `app.WithGenerationRetirement`）。

## 代际与租约：在途不被打断、旧状态有界回收

在只有原子快照时，"读取瞬间一致"成立，但一次跨越重载的**长操作**可能在
调用旧插件时发现它已被关闭。`runtime.Guard` 在快照之上再加一层
**代际（Generation）+ 租约（Lease）**，解决"重载进行中，旧使用者仍在干活"。

### 新的读取生命周期

1. 使用者在开始一次操作前调用 `Acquire()` 拿到一个 `Lease`；
2. 这次操作从头到尾只用 `lease.Snapshot()`：它钉住**唯一一代**，
   配置与插件始终完整一致，绝不会中途变成半新半旧；
3. 操作结束必须 `lease.Release()`（可 `defer`；重复调用安全，是 no-op）。

保证：

- **重载不打断在途操作**：重载前拿到的旧租约，在其释放前一直能正常使用
  旧配置和旧插件，底层资源不会被提前清理。
- **新请求立刻见新状态**：重载一旦生效，之后的 `Acquire()` 全部拿到新一代；
  旧状态只对更早开始、尚未结束的那批租约有效。
- **发布仍是单次原子决策**：构建发生在旧状态继续服务期间；只有构建成功才换入。

### 回收规则与可观测性

- 被换下来的代进入 `retiring`（在途）状态：**等它的租约全部 `Release` 后**
  才按逆序 `ClosePlugins`，由"最后一个释放者"事件驱动回收，**不需要每个
  旧代起一个常驻 goroutine**，也不轮询。没有任何在途租约的旧代会在 `Reload`
  返回前同步回收（与历史行为一致）。
- `Guard.Status()` / `App.RuntimeStatus()` 返回可观测快照：
  - `ActiveID`：当前生效的是哪一代；
  - `Reloading`：此刻是否有重载在进行（重载彼此串行化）；
  - `CapacityUsed` / `Retiring`：还有哪些旧版本没回收完、各自在途租约数；
  - `RecentReclaims`：每个旧版本**最终为什么被回收**——
    `ReclaimDrained`（租约正常清空后回收）还是
    `ReclaimForcedShutdown`（停机时超过排空等待上限被强制回收），
    两者明确区分，不混成一句"重载完成"。

### 容量上限与拒绝策略

`GuardConfig.MaxRetiredGenerations`（默认 1）限制"已换下但仍有人用"的旧代数量：

- 上限以内：重载照常进行，旧状态等确实没人用了再清理；
- **一旦达到上限，新的重载被明确拒绝**，返回 `*runtime.ReloadError`，
  `Kind=ReloadCapacityExceeded`、`OldKept=true`，错误信息带可机读原因和
  占用中的代/租约数。系统**既不会偷偷拆掉仍在被使用的旧状态，也不会无限堆积**；
- 被拒绝时连新一代都不会构建（容量检查在构建之前），当前活动状态和所有
  已发放租约完全不受影响；待在途租约释放、容量回落后即可再次重载。

`ShutdownDrainTimeout` 约束停机时的排空等待：

- `Reload` 主流程**永不等待**慢使用者——还有租约的旧代直接留在 `retiring`；
- 只有显式 `Shutdown(ctx)` 会等待在途租约，且最多等 `ShutdownDrainTimeout`
  （0 = 不等，立即关闭，等同历史停机行为）。超时后仍被占用的代被**强制回收**，
  记为 `ReclaimForcedShutdown`，所以一个迟迟不结束的使用者不可能拖住停机。
  迟到的 `Release()` 仍然安全（不会二次关闭、不会 panic）。

`app.App` 额外保证：

- `Boot`：配置加载失败时不会构建任何快照；首快照构建失败则进程保持未启动。
- `Reload`：配置重新加载失败（`*config.LoadError`）、快照构建失败或
  容量拒绝（`*runtime.ReloadError`，Kind 可为 `ReloadBuildFailed` /
  `ReloadTimeout` / `ReloadCapacityExceeded`，用 `errors.As` 区分）时，
  **当前服务状态原封不动**。
- `Boot/Reload/Shutdown` 之间用互斥锁串行化；代际模式下 `Acquire()`
  无锁计数、永不为半状态；重载彼此也串行（后到者排队，可用 ctx 限时）。
- `Shutdown` / `ShutdownCtx(ctx)` 原子摘除当前代、排空在途租约（受超时约束）
  后逆序关闭所有代的插件，重复调用安全。

### 对既有用法的兼容范围

- **默认完全向后兼容**：不调用 `WithGenerationRetirement` 时，`app.New`、
  `Boot/Reload/Snapshot/Shutdown` 的签名与语义和上一轮一致（重载成功后立即
  逆序关闭旧插件）。既有调用方无需任何改动。
- 新能力全部以**可选 Option + 新增方法**形式提供：`Acquire()/Lease.Release()`、
  `RuntimeStatus()`、`ShutdownCtx(ctx)`。`Snapshot()` 在代际模式下仍可用于
  "瞬时观察"，但它不钉住资源；需要"整次操作一致且插件不被提前关闭"时必须改用
  `Acquire()/Release()`（这是把宽限期从业务侧下沉到框架的关键用法变化）。
- 底层 `runtime.Holder` / `runtime.Reloader` 保持不变，直接使用它们的代码
  行为不变；`Guard` 是叠加的新类型，可与旧路径并存。
- 新增的失败 Kind（`ReloadCapacityExceeded` 等）使用独立数值段，不改变既有
  Kind 的编号与字符串。

## 验证方法

```bash
# 全部单元测试（含 -race 竞态检测）
go test ./... -race

# 仅跑某一层并查看测试日志（日志含“输入 + 判定依据”）
go test ./config/  -race -v
go test ./plugin/  -race -v
go test ./runtime/ -race -v
go test ./app/     -race -v

# 单独看代际/租约新增行为
go test ./runtime/ -race -v -run 'Lease|Interleave|Capacity|Resources|Shutdown|Stress'
go test ./app/     -race -v -run 'InFlight|Capacity|Drain|Retirement'

# 跑示例（可观察“租约钉住 v1 + RuntimeStatus”）
go run ./cmd/example -config configs/example.json -max-retired 4 -shutdown-drain 5s

go vet ./...
```

### 测试覆盖矩阵

| 关注点 | 测试 |
| --- | --- |
| 默认 < 文件 < 环境的优先级与来源解释 | `config.TestLayeredPrecedence`、`TestDefaultLayer` |
| 必填缺失 / 类型不匹配 / 取值非法 三类可区分失败 | `config.TestMissingRequired`、`TestTypeMismatch`、`TestInvalidValue` |
| 未知字段拒绝、JSON 语法错误可区分 | `config.TestUnknownFieldRejected`、`TestMalformedFile` |
| 密钥不出现在 Describe / 错误信息中 | `config.TestSecretNeverLeaked` |
| 重复注册 / 空名 / nil 拒绝 | `plugin.TestDuplicateRegistration` |
| 缺依赖 / 循环依赖在初始化前拒绝 | `plugin.TestMissingDependency`、`TestDependencyCycle` |
| 启动顺序确定、关闭严格逆序 | `plugin.TestStartOrderAndReverseClose`、`TestDeterministicTieBreak` |
| 启动失败回滚、回滚关闭错误聚合 | `plugin.TestRollbackOnInitFailure`、`TestRollbackCloseErrorIsReported` |
| 并发读取只看到完整一代（无撕裂状态） | `runtime.TestConcurrentReadersConsistency` |
| 重载失败保留旧状态与旧插件、错误可区分 | `runtime.TestReloadFailureKeepsOld`、`TestReloadCanceled`、`TestReloadNilConfig` |
| 重载成功原子换入并关闭旧一代 | `runtime.TestReloadSuccessSwapsAndClosesOld` |
| 高并发重载串行收敛、读取永不为 nil | `runtime.TestConcurrentReloads` |
| 旧租约跨重载全程用旧代、新用户立刻见新代、旧资源释放一次 | `runtime.TestLeasePinsOldGenerationAcrossReload` |
| 多用户 × 多次重载交错，各自结果自洽、按释放顺序回收 | `runtime.TestMultipleUsersAndReloadsInterleave`、`TestRandomInterleavedStress` |
| 容量触顶拒绝重载且原因可区分、新旧可用状态均不受影响 | `runtime.TestReloadRefusedAtCapacityKeepsUsableState` |
| 旧代真正回收后资源确实释放（drained，关闭恰好一次） | `runtime.TestResourcesReleasedAfterDrain` |
| Shutdown 正常排空 / 超时强制回收（outcome 可区分、迟到 Release 安全） | `runtime.TestShutdownDrainsInFlightLeases`、`TestShutdownForcesAfterDrainTimeout` |
| 重载进行中状态可见、并发重载串行化 | `runtime.TestReloadingFlagObservableAndSerializes` |
| 端到端：启动/非法配置拒绝/重载失败保旧/成功换代/并发读 | `app.TestBootAndShutdown` 等全部 `app/*_test.go` |
| 端到端：在途租约跨重载、容量拒绝、停机排空/强制、默认兼容 | `app.TestAppInFlightLeaseSurvivesReload`、`TestAppCapacityRejectionEndToEnd`、`TestAppShutdownDrainAndForce`、`TestAppRetirementDisabledByDefault` |

所有测试均使用 `log.Printf("INPUT ... / JUDGEMENT ...")` 打印输入数据与判定依据，
`go test -v` 输出可直接审计。

## 目录

```
config/            分层配置与校验
plugin/            插件注册与生命周期编排
runtime/           原子快照与热重载；generation.go 提供代际+租约、有界回收与状态可观测
app/               启动/重载/停止门面（可选开启代际回收）
cmd/example/       示例服务（演示 Acquire/Release 与 RuntimeStatus）
configs/           示例配置
```
