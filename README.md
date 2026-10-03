# Go Service Scaffold

一套可复用的 Go 服务脚手架，把**配置、插件、运行时状态**组织成一致且可预测的
启动 / 运行 / 热重载流程。仅依赖 Go 标准库，Go 1.25+。

## 包结构

| 包 | 职责 |
| --- | --- |
| `config` | 分层配置合并、类型校验、必填校验、取值校验、密钥脱敏、来源可解释 |
| `plugin` | 插件注册、依赖拓扑排序、按序初始化、失败回滚、逆序关闭 |
| `runtime` | 原子快照（`atomic.Pointer`）、热重载、失败保留旧状态、**代际租约与按租约回收、容量拒绝、可观测状态** |
| `app` | 门面：把三者串成 `Boot → Reload* → Shutdown` 的确定流程，提供 `Acquire/Release` 固定一整代 |
| `cmd/example` | 可运行的最小示例服务 |

## 一分钟上手

```go
loader := config.NewLoader(schema, "configs/example.json", "APP")
application := app.New(loader, buildSnapshot) // buildSnapshot 负责构建并初始化插件

if err := application.Boot(ctx); err != nil { /* 配置非法或首启失败，进程退出 */ }

snap := application.Snapshot()        // 无锁读取，永远拿到完整的一代状态
_ = application.Reload(ctx)           // 重新读配置并原子换入新一代；失败则旧状态继续服务
_ = application.Shutdown()            // 摘除快照并逆序关闭插件
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

## 热重载与读取生命周期

脚手架在原有"原子快照"之上提供**代际（generation）+ 租约（lease）**层。
`runtime.Manager` 每次成功重载发布一个带版本号的新代；业务每个处理单元
（一个请求、一次任务）通过 `app.Acquire()` **固定**当前代，用完 `Release()`。

```go
lease := application.Acquire()
if lease == nil { /* 尚未启动或已 Shutdown */ }
defer lease.Release()

snap := lease.Snapshot() // 整个处理期间，snap 的配置和插件都不会被动到
port := snap.Config.GetInt("server.port")
p, _ := snap.Plugin("b") // 即使中途发生重载，这里调到的也是同一代、已打开的插件
```

### 生命周期保证

1. **先建后换**：重载先构建并初始化新一代，构建期间旧代继续服务；构建失败
   （或 context 取消）时不触碰任何在服状态，返回 `*runtime.ReloadError`，
   `OldKept=true`，Kind 为 `reload_build_failed` / `reload_timeout`。
2. **原子发布**：构建成功后只执行**一次**指针交换。新来的处理单元**立刻**拿到
   新代；交换发生前已经 `Acquire` 的处理单元从头到尾都使用**完整一致的旧代**——
   配置和插件始终配套，绝不会出现半新半旧。
3. **用完才回收**：旧代只有在其上的**全部租约都 Release** 之后才会按逆序
   `Close` 插件。处理单元还在用时，底层资源绝不会被提前清理。
4. **主流程不被拖住**：旧代的插件关闭发生在锁外（无人占用时在重载路径同步关闭
   以保持换代语义；仍被占用时在最后一个 Release 后异步关闭）。慢使用者只会让旧代
   多活一会儿，不会阻塞重载本身，也不会阻塞调用 Release 的人。

`App` 侧对应接口：

| API | 语义 |
| --- | --- |
| `Acquire() *runtime.Lease` + `Release()` | **推荐**。固定一整代，跨重载安全，插件调用必须走它 |
| `Snapshot() *runtime.Snapshot` | 无锁只读快照，不固定代际；仅用于读取瞬间值。不要拿它跨重载调用插件 |
| `RuntimeStatus() runtime.ManagerStatus` | 可观测性：当前版本、是否正在重载、未回收旧代、回收历史与结局 |

### 回收状态与可观测性

`RuntimeStatus()` / `runtime.Manager.Status()` 返回：

- `ActiveVersion`：当前对外服务的版本号（首启前/Shutdown 后为 0）。
- `Reloading`：此刻是否有重载正在构建/发布。
- `Retained`：**已退役但尚未回收**的旧代（按时间从旧到新），每代带租约数和状态：
  - `draining`：正常等待在途使用者结束（尚在排空宽限时间内）；
  - `stuck`：超过 `DrainTimeout` 仍有人占用。stuck 代**不会被强拆**，仍可被持有者
    正常使用，继续占用容量；等持有者释放后按正常流程回收。
- `History`：最近回收的代（有界，最多 16 个，最新在前），每代带可区分的**结局**：
  - `drained`：所有租约正常释放后回收；
  - `drain_timeout`：曾超过排空时限（使用者太慢），但最终仍正常释放并回收；
  - `close_failed`：插件 `Close` 报错（错误文本在 `CloseErr` 字段），代仍从容量中移除；
  - `shutdown`：在 Shutdown 流程中排空并回收。

### 容量上限与拒绝策略

旧代不能无限堆积。`Options.MaxRetainedGenerations`（默认 2）限定同时可保留多少个
"已退役、仍被占用"的旧代：

- 容量以内：重载照常进行，旧代等到真正没人用了再回收；
- **到达上限**：新的重载在**构建之前**就被明确拒绝，返回
  `*runtime.ReloadError{Kind: runtime.ReloadDrainFull, OldKept: true}`，
  原因文本中区分 `draining` / `stuck` 的数量。脚手架**绝不**拆毁还在被使用的旧代，
  也不会默默堆积；
- 任意旧代随后排空（包括 `close_failed`）都会释放容量，之后重载恢复可用；
- 构建失败**不消耗**容量。

相关参数（`app.Options`，零值走默认值）：

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `MaxRetainedGenerations` | 2 | 可保留的在途旧代上限；达到后新重载被 `reload_drain_full` 拒绝 |
| `DrainTimeout` | 30s | 旧代等待在途使用者的时限；超时后置为 `stuck`（不强拆） |
| `ShutdownWait` | 10s | `Shutdown` 等待在途旧代排空的最长时间；超时后未结束者留给最后的 Release 回收 |

`New(loader, build)` 签名保持不变（使用默认参数）；需要自定义时用
`app.NewWithOptions(loader, build, app.Options{...})`。

### Shutdown

`Shutdown` 原子摘处在服代并立即回收所有无人占用的代；仍被租约固定的代**不会**在
活使用者脚下关闭，而是在其最后一次 `Release` 时回收（结局标记为 `shutdown`，
已经超时过的标 `drain_timeout`）。`Shutdown` 最多等待 `ShutdownWait`，重复调用安全，
并聚合同步回收阶段产生的插件关闭错误。

### 对既有用法的兼容范围

- `config` / `plugin` 包 API 完全不变，其全部既有测试无改动通过。
- `runtime.Holder` / `runtime.Reloader` / `runtime.ClosePlugins` 保留原语义：
  `Reloader` 仍是"换代后立即关闭旧插件"，适用于不存在跨重载长生命周期插件调用的场景。
  原有 runtime 用例全部保留并通过。
- `app.New(loader, build)`、`Boot` / `Reload` / `Snapshot()` / `Shutdown()` 签名不变：
  原有 app 用例（含"换代后旧插件即关闭"的断言与并发读断言）全部无改动通过。
  需要跨重载保护在途工作时，把处理逻辑从 `Snapshot()` 迁移到 `Acquire()/Release()`。
- 旧代码若只做瞬时读取（读一个配置值就结束），`Snapshot()` 行为与之前一致。

## 验证方法

```bash
# 全部单元测试（含 -race 竞态检测），建议多跑几轮确认并发用例稳定
go test ./... -race
go test ./... -race -count=3

# 仅跑某一层并查看测试日志（日志含“INPUT + JUDGEMENT”，可直接审计场景与判定依据）
go test ./runtime/ -race -v
go test ./app/     -race -v

go vet ./...
gofmt -l .
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
| **在途使用者跨重载完整使用旧代，新来者立刻见新代** | `runtime.TestLeasePinsOldGenerationAcrossReload` |
| **多使用者 × 多次重载交错，配置与插件始终自洽（-race）** | `runtime.TestInterleavedUsersAndReloadsStress` |
| **到达容量上限重载被明确拒绝（reload_drain_full），旧代不被拆** | `runtime.TestReloadRefusedAtCapacity` |
| **超时旧代标记 stuck、仍占用容量、释放后按 drain_timeout 回收** | `runtime.TestDrainTimeoutMarksStuck` |
| **Close 失败结局可区分（close_failed）且容量仍释放** | `runtime.TestCloseFailureOutcome` |
| **多租约引用计数：最后一个 Release 才回收，Release 幂等** | `runtime.TestMultipleLeasesRefcount` |
| **Shutdown 不强关在途旧代，释放后恰好关闭一次** | `runtime.TestShutdownWaitsAndKeepsLeasedOpen` |
| **构建失败不消耗容量；首启前/Shutdown 后 Acquire 为 nil** | `runtime.TestBuildFailureConsumesNoCapacity`、`TestAcquireBeforeBootAndAfterShutdown` |
| **端到端：请求跨重载用完旧代 / 门面容量拒绝** | `app.TestAppLeaseSurvivesReload`、`TestAppReloadRejectedAtCapacity` |
| 端到端：启动/非法配置拒绝/重载失败保旧/成功换代/并发读 | `app.TestBootAndShutdown` 等全部 `app/*_test.go` |

所有测试均使用 `log.Printf("INPUT ... / JUDGEMENT ...")` 打印输入数据与判定依据，
`go test -v` 输出可直接审计。

## 目录

```
config/            分层配置与校验
plugin/            插件注册与生命周期编排
runtime/           原子快照与热重载
app/               启动/重载/停止门面
cmd/example/       示例服务
configs/           示例配置
```
