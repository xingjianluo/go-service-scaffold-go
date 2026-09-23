# Go Service Scaffold

一套可复用的 Go 服务脚手架，把**配置、插件、运行时状态**组织成一致且可预测的
启动 / 运行 / 热重载流程。仅依赖 Go 标准库，Go 1.25+。

## 包结构

| 包 | 职责 |
| --- | --- |
| `config` | 分层配置合并、类型校验、必填校验、取值校验、密钥脱敏、来源可解释 |
| `plugin` | 插件注册、依赖拓扑排序、按序初始化、失败回滚、逆序关闭 |
| `runtime` | 原子快照（`atomic.Pointer`）、热重载、失败保留旧状态 |
| `app` | 门面：把三者串成 `Boot → Reload* → Shutdown` 的确定流程 |
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
   不会影响已持有旧快照的在途请求的读取一致性（注意：长请求若仍在调用旧插件，
   业务侧应自行通过引用计数/连接排空保证宽限期——脚手架保证的是状态可见性原子性）。

`app.App` 额外保证：

- `Boot`：配置加载失败时不会构建任何快照；首快照构建失败则进程保持未启动。
- `Reload`：配置重新加载失败（`*config.LoadError`）或快照构建失败
  （`*runtime.ReloadError`，可用 `errors.As` 区分）时，**当前服务状态原封不动**。
- `Boot/Reload/Shutdown` 之间用互斥锁串行化；`Snapshot()` 读取无锁、永不为半状态。
- `Shutdown` 原子摘除当前快照后逆序关闭插件，重复调用安全。

## 验证方法

```bash
# 全部单元测试（含 -race 竞态检测）
go test ./... -race

# 仅跑某一层并查看测试日志（日志含“输入 + 判定依据”）
go test ./config/  -race -v
go test ./plugin/  -race -v
go test ./runtime/ -race -v
go test ./app/     -race -v

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
