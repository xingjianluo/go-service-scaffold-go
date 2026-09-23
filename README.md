# go-service-scaffold

一套可复用的 Go 服务脚手架，把**配置（config）**、**插件（plugin）**与**运行时状态（runtime）**
组织成一致、可预测的 **启动 → 运行 → 热重载 → 关闭** 流程。在异常与并发场景下给出
可解释、一致的结论。

无第三方依赖，仅用标准库；Go ≥ 1.25。

## 目录结构

```
internal/
  config/    分层配置：规约、合并、类型/合法性校验、密钥脱敏、来源解释
  plugin/    插件注册、依赖拓扑排序、有序启动、失败回滚、逆序关闭
  runtime/   原子状态持有：并发读取、整体热重载、旧态延迟回收
cmd/
  scaffold-example/  端到端示例（配置 + 三插件 + SIGHUP 重载 + 信号退出）
```

## 快速开始

```bash
# 构建
go build ./...

# 跑全部测试（含 -race 并发检测）
go test -race -count=1 ./...

# 运行示例（注意环境变量命名规则，见下文）
APP_APP_NAME=demo APP_DB_PASSWORD=s3cret go run ./cmd/scaffold-example

# 用配置文件启动并热重载
APP_CONFIG_FILE=/tmp/app.json go run ./cmd/scaffold-example &
kill -HUP <pid>     # 改完文件后触发原子热重载
kill -TERM <pid>    # 优雅关闭（插件逆序关闭）
```

---

## 一、配置：分层合并与校验

### 1. 优先级（从低到高，后者覆盖前者）

```
FieldSpec.Default      默认值（最低）
  └─ AddLayer 的第 1 层（如编程式 flags / 静态层 Static）
       └─ AddLayer 的第 2 层（如可选 JSON 文件 File）
            └─ AddLayer 的最后一层（如环境变量 Env，最高）
```

层的顺序就是优先级——**先 `AddLayer` 的优先级低，后 `AddLayer` 的优先级高**，
因此合并结果完全确定、可解释。`Config.Origin(key)` 与 `Config.String()` 会标出
每个字段最终取值来自哪一层（`default` / `file:<path>` / `env` / 自定义层名）。

合并是**逐字段 winner-takes-all**，不是整层覆盖：高优先级层只覆盖它显式提供的字段，
其余字段继续沿用低层值。

### 2. 字段规约（Spec）

```go
spec := config.NewSpec().
    Add(config.FieldSpec{Key: "app.name", Kind: config.KindString, Required: true}).
    Add(config.FieldSpec{Key: "server.port", Kind: config.KindInt, Default: 8080,
        Validate: func(v config.Value) error { /* 范围/枚举等检查 */ return nil }}).
    Add(config.FieldSpec{Key: "db.password", Kind: config.KindString,
        Required: true, Secret: true})

cfg, err := config.NewLoader(spec).
    AddLayer(&config.File{Path: path, Optional: true}).
    AddLayer(config.NewEnv("APP_", spec, nil)). // lookup 为 nil 时用 os.LookupEnv
    Load()
```

- **类型**：`KindString` / `KindInt` / `KindBool`。所有层先统一成字符串 token，加载时按规约解析。
- **默认值**：类型必须与 `Kind` 匹配，否则在构建 Spec 时直接 panic（编程期错误）。
- **JSON 文件**：仅接受标量（string / 整数 number / bool），拒绝对象、数组、null、非整数；
  `Optional: true` 时文件不存在等价于空层。
- **环境变量名**：`前缀 + key 中非字母数字替换为下划线并整体大写`。
  前缀 `APP_`、key `app.name` → **`APP_APP_NAME`**；`server.port` → `APP_SERVER_PORT`。
  Env 层只接受 Spec 中声明过的字段，未声明的环境变量不可能注入配置。

### 3. 可区分的失败原因

一次加载会**收集全部问题**（不短路），返回 `*config.LoadError`，每个字段错误带 `ErrorCode`：

| 错误码                | 含义                                           |
|-----------------------|------------------------------------------------|
| `missing_required`    | 必填字段在所有层缺失且无默认值                 |
| `type_mismatch`       | 取值无法解析为声明类型（如 int 字段给了 "abc"）|
| `invalid_value`       | 类型正确但违反 `Validate`（如端口超范围）       |
| `unknown_field`       | 出现 Spec 未声明的字段（拒绝，绝不静默忽略）    |
| `source_failure`      | 源读取失败（必需文件不可读 / JSON 非法等）      |

```go
var le *config.LoadError
if errors.As(err, &le) {
    codes := le.Codes()                 // 可区分地判断失败类型
    fe, ok := le.ByCode(config.ErrTypeMismatch)
}
```

**错误信息只包含字段名、来源层与原因，绝不包含字段取值。**

### 4. 密钥安全（硬性保证）

`Secret: true` 的字段：

- `Value.String()`、`Config.String()`、`Config.Redacted()` 一律输出固定占位符 `***`；
- 不会出现在 `LoadError` 中（错误不含取值；即使 `Validate` 不慎把密钥拼进错误原因，
  Loader 也会在返回前把该子串替换为 `***` 做兜底）；
- 需要原文时只能**显式**调用 `Value.ExposeString()`（例如把口令交给 DB 客户端），
  调用方有责任不打印它。

---

## 二、插件：注册与生命周期编排

### 1. 插件契约

```go
type Plugin interface {
    Name() string                       // 全局唯一
    DependsOn() []string                // 所依赖插件名（依赖先初始化）
    Init(ctx context.Context) error     // 初始化
    Close(ctx context.Context) error    // 释放资源（按初始化的逆序被调用）
}
```

### 2. 注册规则（`plugin.Registry`）

- 并发安全；**同名重复注册返回包装 `ErrDuplicatePlugin` 的错误，绝不静默覆盖**
  （`MustRegister` 仅用于无法处理错误的包级初始化场景，失败即 panic）。

### 3. 启动顺序（`plugin.TopoOrder` / `Manager.Start`）

- 使用 Kahn 拓扑排序，**同一层就绪节点按名字字典序挑选**——
  在相同依赖关系下顺序确定、可复现、可解释。
- `DependsOn` 引用了未注册插件 → 返回包装 **`ErrDependencyMissing`** 的错误，
  一次列出全部缺失边（`a -> ghost`）。
- 存在环 → 返回包装 **`ErrCircularDependency`** 的错误，并列出环上涉及的插件。

### 4. 失败回滚（不残留半初始化状态）

`Start` 依次 `Init`，任一插件 `Init` 失败（或 `ctx` 取消）：

1. 立即停止后续启动；
2. 对**已经 Init 成功**的插件严格**逆序**调用 `Close`（Init 失败的那个不 Close）；
3. 返回包装 **`ErrInitFailed`** 的错误（含失败插件名、根因，以及回滚中的错误）；
4. Manager 回到未运行态，`Started()` 为空——没有任何半初始化插件残留。

### 5. 关闭（`Manager.CloseAll`）

- 严格按初始化顺序的**逆序**关闭全部已启动插件；
- 即使某个 `Close` 出错也继续关闭其余插件（尽力释放），最后用 `errors.Join` 聚合，
  每个错误都可用 `errors.Is` 命中；
- 关闭后再调用返回 `ErrPluginClosed`。

---

## 三、运行时：并发读取与原子热重载

`runtime.Holder` 持有一个不可变的 `runtime.State` 快照（`Data map[string]any` + 可选 `Close`），
热重载时**整体替换**。

### 1. 读取

```go
lease, err := holder.Load()   // 或 holder.Current(func(s *State){...})
if err != nil { /* ErrClosed */ }
defer lease.Release()
s := lease.State()            // 完整的某一代状态
```

- **代际 + WaitGroup 读者计数**：新读者只会被引导到最新一代。
- 读者要么看到**完整旧状态**，要么看到**完整新状态**，绝不会看到半成品或混合状态。
- 读取期间状态保证不被回收；读完必须 `Release`（建议 defer）。

### 2. 重载（`Holder.Reload(ctx, builder)`）

1. **先在锁外执行 `builder`**（可能慢：读文件、建连接），期间读者继续无阻塞使用旧态；
2. builder 返回错误或 `nil` 状态 → 返回包装 **`ErrBuildFailed`** 的错误
   （`errors.Is` 可区分、可取根因），**旧状态原样保留、继续可用**；
3. builder 成功 → 加锁整体替换当前代后**立即返回**（不被旧态慢读者阻塞），
   新读者从此刻起只看到新状态；
4. 旧状态交由后台 reaper，**等其全部在途读者释放租约后**才调用 `Close`，且恰好一次。

```go
err := holder.Reload(ctx, func(ctx context.Context) (*runtime.State, error) {
    cfg, err := loadConfig()        // 重新读配置
    if err != nil { return nil, err }
    // 建新插件集合并 Start（Start 内部已保证失败回滚）
    return &runtime.State{Data: ..., Close: func(){ mgr.CloseAll(...) }}, nil
})
if errors.Is(err, runtime.ErrBuildFailed) { /* 旧服务仍在跑，记录原因即可 */ }
```

### 3. 关闭（`Holder.Close`）

阻止后续 `Load/Reload`（返回 `ErrClosed`），等待在途读者与后台回收结束后回收当前状态；
重复关闭返回 `ErrClosed`。

> 推荐把"配置 + 已启动插件管理器"整体放进一个 `State`：热重载即构建一个全新的、
> 自洽的状态对象；旧状态的 `Close` 负责关闭旧插件。这样新旧两套运行时永不交错。

---

## 四、端到端流程（示例实现）

`cmd/scaffold-example`：

1. `buildState`：`Loader.Load()` 读配置并校验 → 注册 `logger / store / server`
   （依赖链 `logger <- store <- server`）→ `Manager.Start` 按序启动；
2. 初始 `holder.Reload(buildState)` 失败则进程退出，绝不以半状态服务；
3. `SIGHUP` 再次 `Reload`：成功则原子切换并回收旧插件，失败则旧状态继续服务；
4. `SIGINT/SIGTERM`：`holder.Close()`，插件逆序关闭后退出。

---

## 五、验证方法

```bash
# 全部单元测试（24 个用例），带竞态检测、禁用缓存
go test -race -count=1 ./...

# 查看每个用例打印的"输入 + 判定依据"
go test -v ./...

# 压测并发读取/重载的原子性
go test -race -count=20 -run TestReload_AtomicUnderConcurrentReads ./internal/runtime/
```

测试覆盖重点：

- **config**：三层优先级合并与来源、必填缺失、类型不匹配、取值非法、未知字段拒绝、
  必需/可选文件、多错误一次收集、**密钥不出现在任何输出与错误中**、环境变量名映射；
- **plugin**：重复注册拒绝、依赖缺失、循环依赖、拓扑顺序确定性、
  按依赖启动 + 逆序关闭、**Init 失败回滚**、ctx 取消回滚、Close 错误聚合仍全部关闭；
- **runtime**：首次/替换装入、**重载失败保留旧态**、16 读者 × 60 次重载的
  **并发原子性**（观测代际单调不回退、无半成品、旧态恰好回收一次）、
  在途读者持有租约时旧态 `Close` 被推迟、并发重载串行、Close 后拒绝操作。

所有用例日志都通过统一的 `logCase(...)` 打印**输入**与**判定依据**，
关键路径额外打印实际观测值/事件序列，便于复盘。

### 手动验证（示例程序）

```bash
go build -o /tmp/demo ./cmd/scaffold-example

# 正常：日志中 db.password 显示为 ***
APP_APP_NAME=demo APP_DB_PASSWORD=s3cret /tmp/demo

# 非法配置：退出码非 0，错误码分别为 missing_required / type_mismatch / invalid_value
APP_APP_NAME=demo                                          /tmp/demo   # 缺必填
APP_APP_NAME=demo APP_DB_PASSWORD=x APP_SERVER_PORT=abc    /tmp/demo   # 类型错误
APP_APP_NAME=demo APP_DB_PASSWORD=x APP_SERVER_PORT=70000  /tmp/demo   # 取值非法
```
