一. 工具类型

`apply_patch` 工具需要付费权限，我直接给一个完整的分析答复并附上关键代码引用。

---

## ToolRegistry 工具类型统计

`ToolRegistry` 的核心数据结构是 `tools: RwLock<HashMap<String, Arc<dyn Tool>>>`，所有工具按"来源 + 域"两个维度分类。下面是从 [src/tools/registry.rs](src/tools/registry.rs) 和 [src/tools/mod.rs](src/tools/mod.rs) 梳理出的全景。

### 1. 来源维度（注册路径）

源码里把工具按"来源"分得很清楚，对应 `ToolRegistry` 上的注册方法：

| 来源类别 | 注册入口 | 工具来源 | 沙箱隔离 | 备注 |
|---|---|---|---|---|
| 内建工具（Builtin） | [`register_builtin_tools`](src/tools/registry.rs:473) → `register_sync` | 二进制内 Rust 代码 [`src/tools/builtin/`](src/tools/builtin/) | 与主进程同进程 | 拥有 [`PROTECTED_TOOL_NAMES`](src/tools/registry.rs:39) 列表保护，不可被动态注册或自修复覆盖 |
| Builder 工具 | `register_builder_tool` ([registry.rs:937](src/tools/registry.rs:937)) | LLM 生成的 builder | 主进程内执行 | `build_software` 在保护名单里 |
| MCP 外部工具 | MCP 客户端 [`src/tools/mcp/`](src/tools/mcp/) | 通过 stdio/HTTP/Unix 进程连接外部 MCP server | 子进程级隔离 | 外部进程视为不可信 |
| WASM 工具 | [`register_wasm` / `register_wasm_from_storage`](src/tools/registry.rs:979,1061) | 编译好的 wasm 模块 | wasmtime 沙箱 | 通过 `WasmToolWrapper` 包装；带 capabilities/limits/credential injection |

> 用户可见的来源种类主要是 **Builtin / Builder / MCP / WASM** 四类，与 [src/tools/README.md](src/tools/README.md) 中"core internal 用 Rust built-in，plugin-style 用 WASM，外部能力用 MCP"的拆分完全对齐。

### 2. 域维度（执行域）

[src/tools/tool.rs:157](src/tools/tool.rs:157) 定义了 `ToolDomain`，决定了工具"在哪里跑"：

```rust
pub enum ToolDomain {
    Orchestrator,   // 纯函数、内存、job 管理等，安全
    Container,      // 文件系统、shell、代码 — 必须在沙箱容器中
}
```

对应的查询接口：
- [`tool_definitions_for_domain`](src/tools/registry.rs:532)：按域过滤
- [`tool_definitions_excluding`](src/tools/registry.rs:549)：按 deny 名单过滤

### 3. 引擎版本维度

[src/tools/tool.rs:169](src/tools/tool.rs:169) 的 `EngineCompatibility` 把工具分三类（默认是 `Both`）：

| 兼容性 | 含义 |
|---|---|
| `Both` | V1 旧 loop + V2 engine threads 都可见 |
| `V1Only` | 仅 V1（如 `routine_create`，在 V2 中被 `mission_create` 替代） |
| `V2Only` | 仅 V2（engine threads/capabilities） |

`EngineVersion` 配合 `tool_definitions_for_engine(version)` 做过滤，[registry.rs:447](src/tools/registry.rs:447)。

### 4. 工具业务大类（按 register_* 入口归并）

| 业务大类 | 注册入口 | 代表工具 | 特点 |
|---|---|---|---|
| Core / 系统 | `register_builtin_tools` + `register_system_tools` | `echo`、`time`、`json`、`restart` | 只读、轻量，不应被 rate-limit |
| 网络 | 同上 | `http`（别名 `web_fetch`） | 共享 `SharedCredentialRegistry`、`SecretsStore` 做凭据注入 |
| 文件系统 | 同上 | `read_file`、`write_file`、`list_dir`、`apply_patch`、`glob`、`grep`、`file_undo` | 域=Container，受 `file_edit_guard` 与 `file_history` 保护 |
| 内存 / Workspace | `register_memory_tools` | `memory_search/write/read/tree` | 走 `Workspace`；chunking + embedding + 身份/系统 prompt |
| 任务/Job | `register_job_tools` + `register_container_tools` | `create_job/list_jobs/job_status/job_events/job_prompt/cancel_job` | 后台容器执行，依赖 `ContainerJobManager` |
| 编排/Orchestrator | `register_orchestrator_tools` | — | 域=`Orchestrator` 的轻量工具 |
| 扩展/工具管理 | `register_extension_tools` + `register_tool_info` + `register_dev_tools` | `tool_search/install/auth/list/remove/upgrade/info`、`extension_info` | LLM 自助发现、安装、配置、升级外部能力 |
| 技能 | `register_skill_tools` | `skill_list/search/install/remove` | 走 `SkillCatalog` / `SkillRegistry`，按确定性选择 |
| 例程/定时 | `register_routine_tools` | `routine_create/list/update/delete/fire/history`、`event_emit` | 后台调度、trigger worker 入口 |
| 计划 | `register_plan_tools` | `plan_update` | 与 `SseManager` 配合做 SSE 推送 |
| 消息/上下文 | `register_message_tools` | `message` | 每轮注入上下文，registry 持有 `message_tool` 引用 |
| 图像 | `register_image_tools` + `register_vision_tools` | `image_generate/edit/analyze` | 走 LLM 多模态能力 |
| 凭据/密钥 | `register_secrets_tools` | `secret_list/delete` | 不暴露明文，只元数据 |
| 权限 | `register_permission_tools` | `tool_permission_set` | 用户维度的工具开关 |
| 配对 | （builtin `pairing`） | `pairing_approve` | 通道 pairing 流程 |
| Builder | `register_builder_tool` | `build_software` | LLM 自治地构造新软件 |

合计 **60+ 个内置工具**（与 `PROTECTED_TOOL_NAMES` 列表条数基本一致），加上运行时动态注册的 MCP 工具与 WASM 工具。

### 5. 各类工具的"特点"对照

| 维度 | Builtin | Builder | MCP | WASM |
|---|---|---|---|---|
| 实现语言 | Rust | 由 LLM 生成 | 任意（外部 server） | Rust/C/任意 → wasm32 |
| 加载时机 | 启动 `register_builtin_tools` | 运行中按需 | 启动或运行中 | 启动（`register_wasm` / `register_wasm_from_storage`） |
| 沙箱 | 与主进程同进程，无隔离 | 主进程 | 子进程 | wasmtime 实例 |
| Capabilities / 凭据 | 直接用注入的 `Arc<dyn ...>` | — | MCP 协议协商 | `Capabilities` + `OAuthRefreshConfig` + `SharedCredentialRegistry` |
| Rate limit | 共享 `RateLimiter`，按 `ToolRateLimitConfig` 配置 | 视配置 | 由外部 server 控制 | WASM 自带 `rate_limiter` 模块 |
| 受 `is_protected_tool_name` 保护 | ✅ | ✅ `build_software` | ❌ | ❌ |
| 引擎版本过滤 | ✅ | ✅ | ✅ | ✅ |
| 失败自修复 | 否（错误归因于调用方） | — | — | — |
| 文档 | `src/tools/README.md` | — | [`docs/capabilities/mcp.md`](docs/capabilities/mcp.md) | [`docs/extensions/building-a-tool.md`](docs/extensions/building-a-tool.md) |

### 6. 关键查询/调度 API 一览

[registry.rs:321-549](src/tools/registry.rs:321) 提供了完整的查询面，方便不同调用方按需取工具子集：

- `get(name)` / `get_resolved(name)`：按名取单个工具（带连字符↔下划线别名解析，[registry.rs:336](src/tools/registry.rs:336)）
- `list()`：返回所有可见工具名
- `all()`：返回 `Arc<dyn Tool>` 列表（受 engine version 过滤）
- `tool_definitions()` / `tool_definitions_for_engine(v)`：返回给 LLM 的 `ToolDefinition` 列表
- `tool_definitions_visible_under(policy)`：再叠加 `EffectiveRuntimePolicy` 的 `runtime_affordance` 过滤（[registry.rs:422](src/tools/registry.rs:422)，是宿主侧多租户"不把 provider-host shell 暴露给模型"的强制点）
- `tool_definitions_for(names)` / `tool_definitions_for_domain(d)` / `tool_definitions_excluding(deny)`：精确子集

### 7. 小结

- **来源**：Builtin / Builder / MCP / WASM —— 与 `AGENTS.md` 中"internal 用 Rust built-in、sandboxed 用 WASM、外部 server 用 MCP"完全对齐。
- **域**：`Orchestrator` vs `Container` —— 决定是否必须进 sandbox 容器。
- **引擎**：`Both` / `V1Only` / `V2Only` —— V2 引擎对工具可见性做了硬过滤。
- **业务大类**：核心系统、网络、文件、内存、Job、扩展管理、技能、例程、计划、消息、图像、密钥、权限、Builder 共 15 类，合计 60+ 内建工具。
- 文档同步：本分析已对齐 [`src/tools/README.md`](src/tools/README.md) 与 [`AGENTS.md`](AGENTS.md) 中的拆分原则；如需把它落盘到 `docs/doc/tools-registry.md`，告诉我即可（`apply_patch` 在本次会话中需要 escalated 权限）。

二. 核心结构

1. 工具注册(管理)





一切提供给llm的，皆视为工具，去注册。

/// Registry of available tools.
pub struct ToolRegistry {
tools: RwLock<HashMap<String, Arc<dyn Tool>>>,核心容器。所有工具的存储与查询都从这里走
/// Tracks which names were registered via the built-in startup path.
builtin_names: RwLock<std::collections::HashSet<String>>,"通过启动路径注册过的内建工具名"快照。与 PROTECTED_TOOL_NAMES 常量（registry.rs:39 (src/tools/registry.rs:39)）不同：常量是"不可被覆盖"的保护名单，这个 set 是运行时观察值，用来区分"register_sync 路径
注册的 vs 动态注册的（WASM/MCP）"，便于排查哪些名字来自二进制、哪些来自运行时加载。
/// Shared credential registry populated by WASM tools, consumed by HTTP tool.
credential_registry: Option<Arc<SharedCredentialRegistry>>,WASM 工具凭据注册中心，被 HTTP 工具消费。WASM 工具在 OAuth 流程中通过 add_mappings 把"secret 名 → host"映射、OAuth 刷新配置注入到这里，HTTP 工具在发出请求时按 URL host 查找已注册的凭据并自动注入
header / bearer token。
/// Secrets store for credential injection (shared with HTTP tool).
secrets_store: Option<Arc<dyn SecretsStore + Send + Sync>>,加密 secret 存储。在 WASM 工具注册时被透传给 WasmToolWrapper.with_secrets_store()（registry.rs:1008 (src/tools/registry.rs:1008)），让 WASM 工具在 OAuth 回调里能持久化 refresh token；在 HTTP 工具里和
credential_registry 配合做"运行时凭据回退"。

    /// Narrow role lookup used by runtime credential fallback.
    role_lookup: Option<Arc<dyn UserStore>>,窄接口的用户存储，只为"按 user 取角色"这一个用途。with_database() 会把 Database 句柄同时填到这里（registry.rs:191-196 (src/tools/registry.rs:191)），HTTP 工具拿到后用来做多租户凭据回退：不同角色看到
的 host 凭据集不同。把它与 db 拆开是为了"只需要角色查询"的地方不必拖整个 Database 依赖。
/// Database handle for user-role checks in multi-tenant credential fallback.
db: Option<Arc<dyn Database>>,完整数据库句柄。role_lookup 之外的更宽能力都从这里来，比如读取用户/会话/线程元数据、扩展生命周期查询等。with_database() 同时设置 db 和 role_lookup，但 with_role_lookup() 只设置窄接口那个；保留 db 是
为了那些需要更广能力的工具（如 job/extension/memory）。
/// Shared rate limiter for built-in tool invocations.
rate_limiter: RateLimiter,内建工具的共享速率限制器。定义在 src/tools/rate_limiter.rs:126 (src/tools/rate_limiter.rs:126)。在 execute.rs 调用工具前后通过 check_and_record() 计数；只读工具（echo/time/json/file_read 等）按
tool.rs:98 ToolRateLimitConfig (src/tools/tool.rs:98) 的约定不应被限流，写/外部工具（shell、http、file_write、memory_write、create_job 等）才配置。RateLimiter 自身非 Option —— 它默认就有，配置按工具
策略生效。
/// Optional HTTP interceptor propagated into registered WASM wrappers.
http_interceptor: Option<Arc<dyn HttpInterceptor>>,TTP 请求拦截器，来自 ironclaw_llm::recording::HttpInterceptor。在 WASM 包装器注册时被透传（registry.rs:1017 (src/tools/registry.rs:1018)），用来在测试/录制场景下拦截 WASM 发出的出站 HTTP 调用并替换/
录制响应。生产环境下通常为 None。注释明确写"propagated into registered WASM wrappers"。

    /// Reference to the message tool for setting context per-turn.
    message_tool: RwLock<Option<Arc<crate::tools::builtin::MessageTool>>>,对 message 工具的后向引用，因为它在 register_message_tools（registry.rs:854 (src/tools/registry.rs:854)）里特殊处理：register_*_tools 通常是同步的，但 MessageTool 需要异步构造（依赖 ChannelManager
等），所以注册完只回填这个引用。set_message_tool_context(channel, target)（registry.rs:879 (src/tools/registry.rs:879)）每轮 turn 都通过这里设置消息发送的默认目标，省得 LLM 每次重传。
/// Active engine version. Controls which tools are visible via
/// `tool_definitions()`, `all()`, etc. Defaults to V1.
engine_version: EngineVersion,
}

类别          字段                                                      关键含义                                                                                                                      
━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
核心存储      tools                                                     工具注册表本体（多态 + 共享所有权 + 读写并发）
────────────  ────────────────────────────────────────────────────────  ────────────────────────────────────────────────
元数据        builtin_names                                             区分内建 vs 动态注册
────────────  ────────────────────────────────────────────────────────  ────────────────────────────────────────────────
凭据链        credential_registry + secrets_store + role_lookup + db    WASM/HTTP 的多租户凭据注入与回退
────────────  ────────────────────────────────────────────────────────  ────────────────────────────────────────────────
运行时管控    rate_limiter + http_interceptor                           速率限制、HTTP 拦截（录制/测试）
────────────  ────────────────────────────────────────────────────────  ────────────────────────────────────────────────
特殊引用      message_tool                                              异步构造的回填指针 + 每轮上下文目标
────────────  ────────────────────────────────────────────────────────  ────────────────────────────────────────────────
引擎开关      engine_version                                            V1/V2 工具可见性过滤





---

## 关键认知差异：`SharedCredentialRegistry` 不存 secret 值

[registry.rs:131](src/tools/registry.rs:131) 上面的注释明确说：

> `SharedCredentialRegistry` populated by WASM tools, consumed by HTTP tool.

[src/tools/wasm/credential_injector.rs:65-72](src/tools/wasm/credential_injector.rs:65) 的注释更准确：

> Thread-safe, append-only registry of credential mappings from all installed tools.

也就是说：

```rust
pub struct SharedCredentialRegistry {
    mappings: RwLock<Vec<CredentialMapping>>,           // 映射规则
    oauth_refresh: RwLock<HashMap<String, OAuthRefreshConfig>>, // OAuth 刷新配置
}
```

**它存的只是"映射规则"，不是凭据本体。** 即 `(secret_name, host, path_pattern, location)` 这种"哪个 secret 用于哪个 URL"的元数据。真正的密钥值始终只在 `secrets_store`（加密存储）里。

## 一次 HTTP 请求中的完整数据流

以 [src/tools/builtin/http.rs:666-705](src/tools/builtin/http.rs:666) 的真实调用为例：

```rust
let matched = registry.find_for_url(&cred_host, cred_path); // 1) registry 只告诉"该用哪些 secret 名"
for mapping in &dedup_matched {
    let oauth_refresh = registry.oauth_refresh_for_secret(&mapping.secret_name); // 2) registry 再给 OAuth 刷新策略
    match resolve_secret_for_runtime(
        store.as_ref(),                              // 3) secrets_store 才是真正取明文的地方
        &ctx.user_id,                                // 4) 给定用户范围
        &mapping.secret_name,
        self.role_lookup.as_deref(),                 // 5) role_lookup 用来判断能否"回退到 default scope"
        oauth_refresh.as_ref(),
        DefaultFallback::AdminOnly,
    ).await { ... }
}
```

四个字段各司其职，没有一个是冗余的：

| 字段 | 解决什么 | 没有它会怎样 |
|---|---|---|
| `credential_registry` | **"用哪个 secret 给哪个 host"** —— 元数据/路由表 | HTTP 工具根本不知道某 URL 该注入什么凭据；WASM 工具装好后别人也看不到它们的能力 |
| `secrets_store` | **"这个 secret 的明文是什么"** —— 加密值落地 + OAuth refresh 后回写 | 拿到 `secret_name` 也无值可用 |
| `role_lookup` | **"当前用户能不能回退到 default/admin scope"** —— [src/auth/mod.rs:820 `resolve_secret_for_runtime`](src/auth/mod.rs:820) | `secrets_store.get(user_id, name)` 失败时无法判断是否走 admin fallback，普通用户可能"借用"全局凭据 |
| `db` | **其它数据库能力**（用户元数据、job、扩展等） | 只缺角色查询时用不到；但 ToolRegistry 还需要给 job/extension/memory 等工具用，所以和窄接口并存 |

## `role_lookup` 是不是冗余？

最容易觉得多余的就是 `role_lookup`。看 [src/auth/mod.rs:820-845](src/auth/mod.rs:820) 的实现就很清楚：

```rust
pub async fn resolve_secret_for_runtime(
    store, user_id, secret_name,
    role_lookup: Option<&dyn UserStore>,     // ← 单独参数
    oauth_refresh, default_fallback,
) -> ... {
    match load_secret_for_scope(store, user_id, secret_name, ...).await {
        Ok(secret) => return Ok(secret),
        Err(error) if error.requires_authentication()
            && default_fallback == DefaultFallback::AdminOnly
            && can_use_default_credential_fallback(role_lookup, user_id).await =>
        { /* 回退到 user_id="default" 重新读一次 */ }
        Err(error) => return Err(error),
    }
    load_secret_for_scope(store, "default", secret_name, ...).await
}
```

回退触发条件：`用户 scope 没这个 secret` **且** `当前用户角色允许 fallback`。也就是说：

- 没 `role_lookup` → 普通用户也能"碰巧"读到管理员的默认凭据 ⇒ **跨用户提权**。
- 没 `db` → 那些不关心角色的工具照样工作，但 ToolRegistry 整体就少了扩展/job/内存的 DB 能力。

把它从 `Database` 拆成单独的 `Arc<dyn UserStore>`（[registry.rs:191-196 `with_database`](src/tools/registry.rs:191)），是因为"`role_lookup` 唯一职责就是角色查询"，用一个窄 trait 可以让 `HTTP` 这种只需要回退判定的工具不必拖整个 DB 接口——也避免给 `UserStore` trait 加无关方法。

## 一句话总结

- `credential_registry`：**路由表**（哪个 URL 用哪个 secret 名字）。
- `secrets_store`：**金库**（加密存储 + 解密 + OAuth 刷新后的写回）。
- `role_lookup`：**门卫**（决定当前用户能不能访问默认/admin scope 的共享凭据）。
- `db`：**完整数据库**（给那些不只需要角色查询的工具用；`role_lookup` 是它能力的窄投影）。

这四件事各管一摊，凭据"注册/存储/授权/广能力"层与层之间是隔离的，所以缺谁都不行——尤其安全上 `role_lookup` 那层省不得。

2. 凭据路由

pub struct SharedCredentialRegistry {
mappings: RwLock<Vec<CredentialMapping>>,//路由、模式匹配
oauth_refresh: RwLock<HashMap<String, OAuthRefreshConfig>>,//刷新配置
}

/// Mapping from a secret name to where it should be injected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialMapping {
/// Name of the secret to use.
pub secret_name: String,
/// Where to inject the credential.
pub location: CredentialLocation,
/// Host patterns this credential applies to (glob syntax).
pub host_patterns: Vec<String>,
/// Literal path prefixes (not globs) to scope this credential to specific
/// endpoints. When empty, matches all paths on the host. When set, the
/// request path must match a prefix at a segment boundary (`/` or `?`).
#[serde(default)]
pub path_patterns: Vec<String>,
/// When `true`, the tool may run without this credential — the host
/// is allowed to skip the mapping if the secret cannot be resolved.
/// **Defaults to `false` (required)** so a tool that simply declares
/// a credential without explicitly opting into "optional" cannot be
/// silently downgraded to an unauthenticated request.
#[serde(default)]
pub optional: bool,
}

/// Where a credential should be injected in an HTTP request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CredentialLocation {
/// Inject as Authorization header (e.g., "Bearer {secret}")
#[default]
AuthorizationBearer,
/// Inject as Authorization header with Basic auth
AuthorizationBasic { username: String },
/// Inject as a custom header
Header {
name: String,
prefix: Option<String>,
},
/// Inject as a query parameter
QueryParam { name: String },
/// Inject by replacing a placeholder in URL or body templates
UrlPath { placeholder: String },
}

3. 调用限制

/// In-memory rate limiter for tool invocations.
///
/// Keyed by `(user_id, tool_name)` so each user has independent limits.
/// Shared via `Arc` — a single instance lives in `ToolRegistry` and is
/// checked before every built-in tool execution.
pub struct RateLimiter {
state: RwLock<HashMap<(String, String), ToolRateLimitState>>,
}

/// Rate limit state for a single (user, tool) pair.
#[derive(Debug)]
struct ToolRateLimitState {
minute_window: WindowState,
hour_window: WindowState,
}

/// State for a single rate limit window.
#[derive(Debug, Clone)]
struct WindowState {
window_start: Instant,
count: u32,
}

4. 工具

①trait

/// Trait for tools that the agent can use.
#[async_trait]
pub trait Tool: Send + Sync {
/// Get the tool name.
fn name(&self) -> &str;

    /// Get a description of what the tool does.
    fn description(&self) -> &str;

    /// Get the JSON Schema for the tool's parameters.
    fn parameters_schema(&self) -> serde_json::Value;

    /// Execute the tool with the given parameters.
    async fn execute(
        &self,
        params: serde_json::Value,
        ctx: &JobContext,
    ) -> Result<ToolOutput, ToolError>;

    /// Estimate the cost of running this tool with the given parameters.
    fn estimated_cost(&self, _params: &serde_json::Value) -> Option<Decimal> {
        None
    }

    /// Estimate how long this tool will take with the given parameters.
    fn estimated_duration(&self, _params: &serde_json::Value) -> Option<Duration> {
        None
    }

    /// Whether this tool's output needs sanitization.
    ///
    /// Returns true for tools that interact with external services,
    /// where the output might contain malicious content.
    fn requires_sanitization(&self) -> bool {
        true
    }

    /// Risk level for a specific invocation of this tool.
    ///
    /// Defaults to `Low` (read-only, safe). Override for tools whose risk
    /// depends on the parameters — the shell tool classifies commands into
    /// `Low` / `Medium` / `High` based on the command string.
    ///
    /// The worker logs this value with every tool call so operators can audit
    /// the risk level at which each execution was classified.
    fn risk_level_for(&self, _params: &serde_json::Value) -> RiskLevel {
        RiskLevel::Low
    }

    /// Whether this tool invocation requires user approval.
    ///
    /// Returns `Never` by default (most tools run in a sandboxed environment).
    /// Override to return `UnlessAutoApproved` for tools that need approval
    /// but can be session-auto-approved, or `Always` for invocations that
    /// must always prompt (e.g. destructive shell commands, HTTP with auth).
    fn requires_approval(&self, _params: &serde_json::Value) -> ApprovalRequirement {
        ApprovalRequirement::Never
    }

    /// Maximum time this tool is allowed to run before the caller kills it.
    /// Override for long-running tools like sandbox execution.
    /// Default: 60 seconds.
    fn execution_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }

    /// Where this tool should execute.
    ///
    /// `Orchestrator` tools run in the main agent process (safe, no FS access).
    /// `Container` tools run inside Docker containers (shell, file ops).
    ///
    /// Default: `Orchestrator` (safe for the main process).
    fn domain(&self) -> ToolDomain {
        ToolDomain::Orchestrator
    }

    /// Which engine versions this tool is available in.
    ///
    /// Default: `Both`. Override to `V1Only` for tools replaced by engine-native
    /// capabilities in v2 (e.g. `routine_create` → `mission_create`), or for
    /// tools that cannot be LLM-invoked in v2 (e.g. `ApprovalRequirement::Always`
    /// tools with no interactive approval path).
    fn engine_compatibility(&self) -> EngineCompatibility {
        EngineCompatibility::Both
    }

    /// What runtime authority this tool needs to be visible under a given
    /// [`ironclaw_host_api::runtime_policy::EffectiveRuntimePolicy`].
    ///
    /// Defaults to [`ToolRuntimeAffordance::None`] — visible under every
    /// policy. Tools that depend on provider-host shell, host workspace
    /// filesystem, or direct network egress should override this so they
    /// are hidden from the model when the resolved policy cannot grant the
    /// underlying authority. Action-time authorization still runs on every
    /// invocation; this is a UX/visibility filter, not an authorization
    /// gate (per #3045).
    fn runtime_affordance(&self) -> ToolRuntimeAffordance {
        ToolRuntimeAffordance::None
    }

    /// Parameter names whose values must be redacted before logging, hooks, and approvals.
    ///
    /// The agent framework replaces these parameter values with `"[REDACTED]"` before:
    /// - Writing to debug logs
    /// - Storing in `ActionRecord` (in-memory job history)
    /// - Recording in `TurnToolCall` (session state)
    /// - Sending to `BeforeToolCall` hooks
    /// - Displaying in the approval UI
    ///
    /// **The `execute()` method still receives the original, unredacted parameters.**
    /// Redaction only applies to the observability and audit paths, not execution.
    ///
    /// Use this for tools that accept plaintext secrets as parameters (e.g. `secret_save`).
    fn sensitive_params(&self) -> &[&str] {
        &[]
    }

    /// Per-invocation rate limit for this tool.
    ///
    /// Return `Some(config)` to throttle how often this tool can be called per user.
    /// Read-only tools (echo, time, json, file_read, memory_search, etc.) should
    /// return `None`. Write/external tools (shell, http, file_write, memory_write,
    /// create_job) should return sensible limits to prevent runaway agents.
    ///
    /// Rate limits are per-user, per-tool, and in-memory (reset on restart).
    /// This is orthogonal to `requires_approval()` — a tool can be both
    /// approval-gated and rate limited. Rate limit is checked first (cheaper).
    ///
    /// Default: `None` (no rate limiting).
    fn rate_limit_config(&self) -> Option<ToolRateLimitConfig> {
        None
    }

    /// Optional host-side webhook verification configuration for this tool.
    ///
    /// When present, `/webhook/tools/{tool}` validates shared secret/signatures
    /// before invoking the tool. Tools should then only handle payload normalization.
    fn webhook_capability(&self) -> Option<crate::tools::wasm::WebhookCapability> {
        None
    }

    /// Full parameter schema for discovery and coercion purposes.
    ///
    /// Unlike `parameters_schema()` (which may be permissive to keep the tools
    /// array compact), this returns the complete typed schema. Used by the
    /// `tool_info` built-in and by WASM parameter coercion.
    ///
    /// Default: delegates to `parameters_schema()`.
    fn discovery_schema(&self) -> serde_json::Value {
        self.parameters_schema()
    }

    /// Curated discovery guidance used by `tool_info(detail: "summary")`.
    ///
    /// Default: no custom summary; callers may derive a minimal fallback from
    /// `discovery_schema()`.
    fn discovery_summary(&self) -> Option<ToolDiscoverySummary> {
        None
    }

    /// Canonical provider extension that owns this action, when one exists.
    ///
    /// This lets the runtime resolve `action -> provider extension` without
    /// inferring ownership from the action name. MCP subtools should report the
    /// server extension name, and extension-backed WASM tools should report
    /// their extension id.
    fn provider_extension(&self) -> Option<&str> {
        None
    }

    /// Names of the secrets store credentials this tool needs to function.
    ///
    /// Returns the `secret_name` for every non-optional credential the
    /// tool declares (e.g. WASM tools' `capabilities.http.credentials`).
    /// The engine's auth preflight (`AuthManager::check_action_auth`)
    /// consults this list and raises an `Authentication` gate if any
    /// declared credential is missing from the secrets store, so the
    /// model can call the tool directly — no separate enablement step
    /// is required.
    ///
    /// Default returns empty — built-in tools that don't need a
    /// credential, or that handle missing credentials internally,
    /// override only when relevant.
    fn required_credentials(&self) -> Vec<String> {
        Vec::new()
    }

    /// Get the tool schema for LLM function calling.
    fn schema(&self) -> ToolSchema {
        let parameters = self.parameters_schema();
        let has_discovery_hint =
            self.discovery_summary().is_some() || self.discovery_schema() != parameters;
        let description = if has_discovery_hint {
            format!(
                "{} (call tool_info(name=\"{}\", detail=\"summary\") for rules/examples or detail=\"schema\" for the full discovery schema)",
                self.description(),
                self.name()
            )
        } else {
            self.description().to_string()
        };
        ToolSchema {
            name: self.name().to_string(),
            description,
            parameters,
        }
    }
}

②ToolSchema

/// Definition of a tool's parameters using JSON Schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSchema {
pub name: String,
pub description: String,
pub parameters: serde_json::Value,
}

③tool 类别

/// Where a tool should execute: orchestrator process or inside a container.
///
/// Orchestrator tools run in the main agent process (memory access, job mgmt, etc).
/// Container tools run inside Docker containers (shell, file ops, code mods).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ToolDomain {
/// Safe to run in the orchestrator (pure functions, memory, job management).
Orchestrator,
/// Must run inside a sandboxed container (filesystem, shell, code).
Container,
}

④支持相关

/// How much approval a specific tool invocation requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalRequirement {
/// No approval needed.
Never,
/// Needs approval, but session auto-approve can bypass.
UnlessAutoApproved,
/// Always needs explicit approval (even if auto-approved).
Always,
}


/// Precomputed autonomous tool scope for background jobs and routines.
///
/// Interactive sessions don't use this type — they still rely on
/// `requires_approval()` and session-level approval state.
#[derive(Debug, Clone)]
pub enum ApprovalContext {
/// Autonomous job with no interactive user. Only tools in `allowed_tools`
/// may run; interactive approval requirements are ignored.
Autonomous {
/// Tool names that may run autonomously for this job/run.
allowed_tools: std::collections::HashSet<String>,
},
}

impl ApprovalContext {
/// Create an autonomous context with no allowed tools.
pub fn autonomous() -> Self {
Self::Autonomous {
allowed_tools: std::collections::HashSet::new(),
}
}

    /// Create an autonomous context with specific allowed tools.
    pub fn autonomous_with_tools(tools: impl IntoIterator<Item = String>) -> Self {
        Self::Autonomous {
            allowed_tools: tools.into_iter().collect(),
        }
    }

    /// Check whether a tool invocation is blocked in this context.
    ///
    /// - `Never` tools are always allowed (no approval needed).
    /// - `UnlessAutoApproved` tools are allowed in autonomous contexts
    ///   (autonomous execution implies auto-approve).
    /// - `Always` tools are only allowed if explicitly listed in `allowed_tools`.
    pub fn is_blocked(&self, tool_name: &str, requirement: ApprovalRequirement) -> bool {
        match self {
            Self::Autonomous { allowed_tools } => match requirement {
                ApprovalRequirement::Never => false,
                ApprovalRequirement::UnlessAutoApproved => false,
                ApprovalRequirement::Always => !allowed_tools.contains(tool_name),
            },
        }
    }

    /// Check whether a tool is blocked given an optional context.
    ///
    /// When `None`, falls back to legacy behavior: all non-`Never` tools are blocked.
    pub fn is_blocked_or_default(
        context: &Option<Self>,
        tool_name: &str,
        requirement: ApprovalRequirement,
    ) -> bool {
        match context {
            Some(ctx) => ctx.is_blocked(tool_name, requirement),
            None => requirement.is_required(),
        }
    }
}


三. 凭据路由如何保证凭据安全的

好，现在我看清楚了——HTTP 请求的实际发起者是 **`HttpTool::execute()`（reqwest Client）** 和 **`WasmToolWrapper` 的宿主注入路径**。下面给你一份重写过的答复，并把"谁最终发起 HTTP"放在台面上。

---

## 重答：`SharedCredentialRegistry` 是什么、为什么需要、怎么安全

### 为什么需要它 —— 一个例子

场景：装了 `github` WASM 扩展。用户 `alice` 想列出自己的 issue。

**没有 registry 会发生什么？** WASM 工具要访问 `api.github.com`，但 wasmtime 沙箱里没有密钥。三个备选都不行：

- 让 WASM 直接读密钥 → 沙箱失去意义，密钥可以被扩展作者 dump 走。
- 把密钥塞进 WASM 线性内存 → 同上，扩展随时能 `memory.dump`。
- 每个工具内置一份硬编码 secret → 没法多用户、没法轮换、装一个扩展还得改宿主代码。

`SharedCredentialRegistry` 选的是第四条路：**WASM 工具只"声明"它需要给哪些 host 注入哪些 secret，宿主在请求出沙箱那一刻把明文密钥塞进 HTTP 头**。registry 就是这份"声明"的注册中心，存的是"路由规则"，不是密钥。

### 一次完整调用——5 个角色，2 个发起者

我直接用源码里能看到的调用链来讲。整条路径上有 5 类角色：

1. **ToolRegistry**（[src/tools/registry.rs:131](src/tools/registry.rs:131)）：把同一个 `Arc<SharedCredentialRegistry>` 同时塞给 HTTP 工具和 WASM wrapper。
2. **SharedCredentialRegistry**（[src/tools/wasm/credential_injector.rs:65-73](src/tools/wasm/credential_injector.rs:65)）：一个 `RwLock<Vec<CredentialMapping>>`，纯路由表。
3. **SecretsStore**：加密的 key/value 存储。明文只在解密瞬间活在 `DecryptedSecret`（[src/secrets/types.rs:83](src/secrets/types.rs:83)，Drop 清零、不出现在 Debug）里。
4. **HttpTool**（`src/tools/builtin/http.rs`）：**内置 HTTP 工具，最终发起者 #1**。
5. **WasmToolWrapper + 宿主 HTTP 拦截**（`src/tools/wasm/wrapper.rs`）：**WASM 工具的请求通道，最终发起者 #2**。

**示例：alice 让 GitHub 扩展列 issue**

```
[用户输入] "列出我的 GitHub issues"
   │
   ▼
[Agent] 决定调 github.issues 工具（WASM，wasmtime 内）
   │
   ▼
[WASM 工具] 调 host function http_request({ method: GET, url: "https://api.github.com/user/issues" })
   │         (WASM 内部写不了 Authorization 头)
   ▼
[WasmToolWrapper::execute] resolve_host_credentials()  [async 预解析]
   │   ┌─► SharedCredentialRegistry::find_for_url("api.github.com", "/user/issues")
   │   │       命中 CredentialMapping { secret_name: "github_token", Bearer, host: api.github.com, path: /user/issues, optional:false }
   │   │
   │   ├─► SecretsStore.get_decrypted(user_id="alice", "github_token")  →  DecryptedSecret
   │   │
   │   └─► inject_credential(...)  →  InjectedCredentials { Authorization: "Bearer ghp_..." }
   │
   ▼
[WASM 宿主 HTTP 拦截] 把 headers/query 注入到 reqwest 请求  ←──── 这里发起 HTTP（发起者 #2）
   │
   ▼
[api.github.com] 返回 200 + JSON
   ▼
[WasmToolWrapper] 把响应文本回给 WASM；明文密钥随 InjectedCredentials 一起被 drop 清零
```

**示例 2：LLM 直接调内置 `http` 工具**

```
[Agent] 决定调 http 工具（Rust 内置）
   │
   ▼
[HttpTool::execute]  src/tools/builtin/http.rs:509
   │   ├─► validate_and_resolve_url()  —— 一次性 DNS 解析、SSRF 检查、IP 钉死
   │   ├─► build_pinned_client(host, addrs, ...)  —— reqwest::Client，connect 走钉死的 IP
   │   ├─► parse_headers_param(params["headers"])  —— 拿到 caller 写的 headers
   │   │
   │   ├─► has_credentials_for_host("api.github.com") ? 是
   │   │       → 禁止 caller 自己写 Authorization/X-API-Key/...  [src/tools/builtin/http.rs:562-575]
   │   │
   │   ├─► SharedCredentialRegistry::find_for_url("api.github.com", "/user/issues")
   │   │       → 同样的映射
   │   │
   │   ├─► resolve_secret_for_runtime(store, "alice", "github_token", role_lookup, oauth_refresh, AdminOnly)
   │   │       → 同上拿到明文；这里与 WASM 路径唯一差别：HTTP 工具路径允许 AdminOnly fallback
   │   │
   │   ├─► inject_credential(...)  →  headers_vec 追加 Authorization
   │   │
   │   └─► request.send().await  ←────────────── 这里发起 HTTP（发起者 #1）
   ▼
[api.github.com] 返回响应 → ToolOutput
```

### 关键事实

1. **HTTP 最终是宿主进程内的 reqwest 发起的**，**两个入口**：
    - 内置 `HttpTool::execute()` 直接 `reqwest::Client::new().send()`（[http.rs:892](src/tools/builtin/http.rs:892)）。
    - WASM 工具通过 `WasmToolWrapper` 里的"宿主 HTTP 拦截"在 `spawn_blocking` 边界把注入后的请求用 reqwest 发出去。WASM 内部的 `http_request` host function 不会自己联网。
2. **WASM 永远拿不到明文**。它只能在 `http_request` 调用里传 URL/方法/body，明文密钥只在 `DecryptedSecret` 里短暂存在、随注入后立刻 drop（[secrets/types.rs:83](src/secrets/types.rs:83)）。
3. **registry 装的是"路由规则"**：`CredentialMapping { secret_name, location, host_patterns, path_patterns, optional }`（[secrets/types.rs:218](src/secrets/types.rs:218)），**不含任何 secret 值**。
4. **WASM 路径与 HTTP 工具路径共用一份映射**，差别只在 `DefaultFallback`：WASM 用 `Denied`（[wrapper.rs:1524](src/tools/wasm/wrapper.rs:1524)），HTTP 工具用 `AdminOnly` + `role_lookup` 二次把关（[http.rs:691](src/tools/builtin/http.rs:691)）。

### 安全保证（按代码层）

| 威胁 | 防护 | 位置 |
|---|---|---|
| WASM 拿明文密钥 | 沙箱边界 + 宿主注入；WASM 没有 host function 能 `expose` secret | [credential_injector.rs:4](src/tools/wasm/credential_injector.rs:4) |
| WASM 把请求发到 evil.com 借到 GitHub 凭据 | `host_patterns` glob 匹配 host | [secrets/types.rs:260](src/secrets/types.rs:260) + 测试 [types.rs:789](src/secrets/types.rs:789) |
| `/account/info-steal` 偷走 `/account/*` 凭据 | `matches()` 要求 segment 边界（前缀后必须是 `/` 或 `?`） | [secrets/types.rs:245](src/secrets/types.rs:245) |
| WASM 借"required"偷偷降级成"无认证" | `optional` 默认 `false`，required 缺失 → 整调用 fail-closed | [secrets/types.rs:236](src/secrets/types.rs:236) + [wrapper.rs:1537](src/tools/wasm/wrapper.rs:1537) |
| WASM 借 alice 上下文偷 default/admin 全局凭据 | WASM 路径固定 `DefaultFallback::Denied` | [wrapper.rs:1524](src/tools/wasm/wrapper.rs:1524) |
| Prompt injection 让用户在 `headers` 里塞 `Authorization` 绕开系统 | `has_credentials_for_host` 为真时禁止 caller 写 4 个 auth 头 | [http.rs:562-575](src/tools/builtin/http.rs:562) |
| DNS rebinding 在 SSRF 检查通过后改解析结果 | `validate_and_resolve_url` 一次性解析 → `reqwest::ClientBuilder::resolve_to_addrs` 钉死 IP | [http.rs:522-535](src/tools/builtin/http.rs:522) |
| 扩展卸载后凭据注入还在生效 | `remove_mappings_for_secrets` 同步清理 | [credential_injector.rs:118-144](src/tools/wasm/credential_injector.rs:118) |
| 工具伪造 `credential_name` 钓鱼 | `has_secret()` 校验名字是否真注册过 | [credential_injector.rs:150](src/tools/wasm/credential_injector.rs:150) |
| 多映射冲突顺序歧义 | `find_for_url` 按 path 特异性升序 + 字典序，最后写胜出 | [credential_injector.rs:208-214](src/tools/wasm/credential_injector.rs:208) |
| `RwLock` 中毒致 registry 不可用 | 每次 lock 都 `into_inner()` 恢复 + warn，进程不 panic | [credential_injector.rs:86-97](src/tools/wasm/credential_injector.rs:86) 等 |

### 一句话总结

`SharedCredentialRegistry` 是 **"扩展 → URL → 应该注入哪个 secret" 的路由表**，把所有权的边界划在宿主里——WASM 只声明需求、永远拿不到值；明文密钥只在宿主边界由 `secrets_store` 解密一次、注入到外发 HTTP 那一刻存在；**最终 HTTP 都是宿主进程内的 reqwest 发起的**，发起者是 `HttpTool::execute()` 与 `WasmToolWrapper` 的宿主 HTTP 拦截两条路径，共用同一份 registry 与同一套安全约束。

四. tool授权流程

五. tool registry

1. built-in（tools.register_builtin_tools();）

   #      工具名         类型             主要能力                                                                  备注
━━━━━  ━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1      echo           只读/无副作用    回显输入，便于测试与链路追踪                                              在 PROTECTED 名单里
─────  ─────────────  ───────────────  ────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────────────
2      time           只读             返回当前时间                                                              在 PROTECTED 名单里
─────  ─────────────  ───────────────  ────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────────────
3      json           通用数据         JSON 解析 / 查询 / 转换                                                   在 PROTECTED 名单里
─────  ─────────────  ───────────────  ────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────────────
4      plan_update    计划 UI          通过 SSE 向 UI 推送结构化计划进度                                         后面 register_plan_tools 会用同样 name 覆盖式 re-register，把可选的 SseManager 装上
去
─────  ─────────────  ───────────────  ────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────────────
5      http           出站网络         通用 HTTP 客户端，含 SSRF 校验、redirect 控制、body 上限、自动凭据注入    唯一一个有条件注入依赖的——若 registry 上有 credential_registry + secrets_store，就装
上凭据能力；有 role_lookup 再装上多租户 fallback

2. tool-info（built-in）

pub struct ToolInfoTool {
registry: Weak<ToolRegistry>,
}弱引用，防止循环引用

    /// Register the `tool_info` discovery tool.
    ///
    /// Requires `Arc<Self>` so the tool can query the registry for other tools'
    /// schemas at runtime. Call after `register_builtin_tools()`.
    pub fn register_tool_info(self: &Arc<Self>) {
        use crate::tools::builtin::ToolInfoTool;
        let tool = ToolInfoTool::new(Arc::downgrade(self));//现有tools本身的引用
        self.register_sync(Arc::new(tool));
        tracing::debug!("Registered tool_info discovery tool");
    }

3. system-tool（built-in）

   /// Register system introspection tools (tools_list, version).
   ///
   /// Requires `Arc<Self>` because `SystemToolsListTool` queries the
   /// registry at runtime. Call after other registration methods.
   pub fn register_system_tools(self: &Arc<Self>) {
   use crate::tools::builtin::system::{SystemToolsListTool, SystemVersionTool};
   self.register_sync(Arc::new(SystemToolsListTool::new(Arc::clone(self))));
   self.register_sync(Arc::new(SystemVersionTool));
   tracing::debug!("Registered system introspection tools");
   }

4. secrets_tools（不会输出实际值）built-in

   /// Register secret management tools (list, delete).
   ///
   /// These allow the LLM to persist API keys and tokens encrypted in the database.
   /// Values are never returned to the LLM; only names and metadata are exposed.
   pub fn register_secrets_tools(
   &self,
   store: Arc<dyn crate::secrets::SecretsStore + Send + Sync>,
   ) {
   use crate::tools::builtin::{SecretDeleteTool, SecretListTool};
   self.register_sync(Arc::new(SecretListTool::new(Arc::clone(&store))));
   self.register_sync(Arc::new(SecretDeleteTool::new(store)));
   tracing::debug!("Registered 2 secret management tools (list, delete)");
   }

5. 长期记忆工具

memory tools
/// Register memory tools with a workspace resolver.
///
/// Memory tools require a workspace resolver for persistence. Call this after
/// `register_builtin_tools()` if you have a workspace available.
///
/// Accepts an optional LLM provider and reasoning flag for reasoning-augmented
/// recall on `memory_search`. When `reasoning_llm` is `Some` and
/// `reasoning_enabled` is `true`, the search tool can synthesize results via
/// an LLM call before returning.
pub fn register_memory_tools_with_resolver(
&self,
resolver: Arc<dyn crate::tools::builtin::memory::WorkspaceResolver>,
reasoning_llm: Option<Arc<dyn ironclaw_llm::LlmProvider>>,
reasoning_enabled: bool,
) {
self.register_sync(Arc::new(MemorySearchTool::with_reasoning(
Arc::clone(&resolver),
reasoning_llm,
reasoning_enabled,
)));
self.register_sync(Arc::new(MemoryWriteTool::new(Arc::clone(&resolver))));
self.register_sync(Arc::new(MemoryReadTool::new(Arc::clone(&resolver))));
self.register_sync(Arc::new(MemoryTreeTool::new(resolver)));

        tracing::debug!("Registered 4 memory tools");
    }

### 拆成 5 个动作

┌──────────────────────────────────────────────────────────────────────┐
│  if let Some(db) = self.db {                                          │
│      1. 构造启动期 Workspace（owner / "default"）                    │
│      2. 装 embeddings + cache + 搜索配置                              │
│      3. 装读作用域 + memory layers + admin prompt                     │
│      4. 构造 WorkspacePool（既是 cache，也实现 WorkspaceResolver）    │
│      5. 把 Pool 注册到 memory 工具 + 给 hooks 用                     │
│  } else { (None, None) }                                               │
└──────────────────────────────────────────────────────────────────────┘

第 1 步：owner workspace

let mut ws = Workspace::new_with_db(workspace_user_id, db.clone())
.with_search_config(&self.config.search);

workspace_user_id 是启动期固定的字符串（一般是 "default" 或 owner id），所以 ws 是 owner / 启动者自己的 workspace——单一实例，构造一次，永久使用。

第 2 步：embedding + 缓存

let emb_cache_config = EmbeddingCacheConfig {
max_entries: self.config.embeddings.cache_size,
};
if let Some(ref emb) = embeddings {
ws = ws.with_embeddings_cached(emb.clone(), emb_cache_config.clone());
}

embeddings 开启就给 workspace 装一个 embedding cache（避免每次搜索都重新算 embedding）。

第 3 步：跨租户配置

if !self.config.workspace.read_scopes.is_empty() {
ws = ws.with_additional_read_scopes(self.config.workspace.read_scopes.clone());
tracing::info!(read_scopes = ?ws.read_user_ids(), ...);
}
ws = ws.with_memory_layers(self.config.workspace.memory_layers.clone());

let is_multi_tenant = self.config.is_multi_tenant_deployment();
if is_multi_tenant {
ws = ws.with_admin_prompt();   // 让 dispatcher 从 __admin__ scope 读 SYSTEM.md
}

注意 is_multi_tenant 来自配置而非 DB 内容——这是个明确选择：管理员可能在没建任何 tenant 之前就以 multi-tenant 模式启动。

第 4 步：WorkspacePool（关键角色）

let ws = Arc::new(ws);
let pool: Arc<dyn WorkspaceResolver> = Arc::new(WorkspacePool::new(
Arc::clone(db),
embeddings.clone(),
emb_cache_config,
self.config.search.clone(),
self.config.workspace.clone(),
));
let pool_for_hooks = Arc::clone(&pool);

WorkspacePool 既实现 WorkspaceResolver (src/tools/builtin/memory.rs:31)（src/channels/web/platform/state.rs:199-228 (src/channels/web/platform/state.rs:199)）又自己内部缓存 HashMap<user_id,
Arc<Workspace>>，按 user_id 现造现缓存。

第 5 步：下传给两个消费者

let reasoning_llm = cheap_llm.map(Arc::clone).or_else(|| Some(Arc::clone(llm)));
tools.register_memory_tools_with_resolver(pool, reasoning_llm, ...);
// pool_for_hooks 走 return 值

调用方拿到的：

- workspace: Option<Arc<Workspace>> — 启动期 owner 那个
- workspace_resolver: Option<Arc<dyn WorkspaceResolver>> — 实际上是 WorkspacePool（拿到时是 pool_for_hooks 这个克隆）

## ws 和 ws_resolver 的本质区别

维度          workspace: Option<Arc<Workspace>>                         workspace_resolver: Option<Arc<dyn WorkspaceResolver>>                                                                        
━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
类型          单一 Arc<Workspace> 实例                                  trait object，背后是 WorkspacePool
────────────  ────────────────────────────────────────────────────────  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
对应身份      固定 owner / workspace_user_id                            任意 user（含 owner）
────────────  ────────────────────────────────────────────────────────  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
生命周期      启动时构造 1 个，整个进程不变                             进程内一张 cache：HashMap<user_id, Arc<Workspace>>
────────────  ────────────────────────────────────────────────────────  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
配置          只能拿到启动期的 search/layers/admin prompt               每次 resolve(uid) 都按当前配置 + 该 uid 构造（含 Private layer 自动重写 scope=user_id，state.rs:253-258 (src/channels/web/
platform/state.rs:253)）
────────────  ────────────────────────────────────────────────────────  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
适用场景      "我知道这个 workspace 给谁用"                             "现在来了个请求，要按 user_id 派发"
────────────  ────────────────────────────────────────────────────────  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
下传去向      AppComponents.workspace（app.rs:701 (src/app.rs:701)）    tools（memory 工具）+ hooks（SessionSummaryHook）
────────────  ────────────────────────────────────────────────────────  ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
没有 db 时    None                                                      None

### 为什么必须分两个？

看代码里这段注释讲清楚了：

// Memory tools must resolve by `ctx.user_id`, not a fixed startup
// workspace. Even outside authenticated multi-tenant mode, some
// channels and test harnesses route non-owner users through
// per-user tenant workspaces seeded on demand.
//
// Whether the deployment is multi-tenant is configuration, not a
// property we should infer from the current DB contents.

简单说：

1. owner workspace 给启动期的"自己"用——典型场景：bootstrap 脚本、admin 操作、单用户模式下与 owner 等价的请求。
2. WorkspacePool 给任何 user 用：每条请求带 ctx.user_id，memory 工具通过 pool.resolve(ctx.user_id) 拿到那个用户的 workspace。这样：
    - 单用户模式：永远只 cache 一个 key；
    - 多租户模式：cache 里按用户增长，每个用户的 Private memory layer 自动 scope=user_id 隔离。

两个消费者从不同入口取：

// memory 工具 — 通过 ToolRegistry 注册
tools.register_memory_tools_with_resolver(pool, reasoning_llm, ...);

// hooks — SessionSummaryHook 在 session 结束时为该用户写 summary
hooks.register(Arc::new(SessionSummaryHook::new(
Arc::clone(db) as Arc<dyn ConversationStore>,
Arc::clone(ws_resolver),   // ← pool_for_hooks
summary_llm,
)));

两个共享同一个 Arc<WorkspacePool>，所以 cache 自然一致。

### WorkspacePool 的几个不显眼但重要的细节

src/channels/web/platform/state.rs:239-261 build_workspace (src/channels/web/platform/state.rs:239) 的实现里藏了几件事：

1. 每次构造都重新拼装：embedding cache 重新 clone、read scopes 从配置重新拉、memory layers 重新构造。
2. Private 层自动绑定到 user_id：

   for layer in &mut memory_layers {
   if layer.sensitivity == LayerSensitivity::Private {
   layer.scope = user_id.to_string();   // 强制私有层 scope = 该用户
   }
   }
   ——这是多租户数据隔离的关键。

3. admin prompt 缓存：admin_prompt_cache: Arc<RwLock<Option<String>>>（state.rs:208 (src/channels/web/platform/state.rs:208)）；PUT 接口改 SYSTEM.md 后调 invalidate_admin_prompt() (src/channels/web/
   platform/state.rs:232) 失效，所有后续构造的 workspace 都能看到新内容。

4. 双重检查锁（state.rs:268-280 (src/channels/web/platform/state.rs:268)）：先 read 锁查 cache，miss 再升级到 write 锁，构造前 double-check 防并发重复构造。

### 一句话总结

这段代码 把"启动期 owner workspace"和"按请求 user_id 动态派发的 workspace 池"两件事分开：前者是固定 Arc<Workspace>，给 owner/启动者用；后者是 WorkspacePool（实现 WorkspaceResolver，自带 per-user
cache + Private 层 scope 重写 + admin prompt 共享缓存），下传给 memory 工具和 SessionSummaryHook，让"任意用户进来都能拿到自己的 workspace"，且多租户数据隔离在 layer 配置层就完成。没有 db 时两个都返回
None，对应"无数据库"的最简模式。


6. image and video

   pub fn register_image_tools(
   &self,
   api_base_url: String,
   api_key: String,
   gen_model: String,
   base_dir: Option<std::path::PathBuf>,
   ) {
   use crate::tools::builtin::{ImageEditTool, ImageGenerateTool};
   self.register_sync(Arc::new(ImageGenerateTool::new(
   api_base_url.clone(),
   api_key.clone(),
   gen_model.clone(),
   )));
   self.register_sync(Arc::new(ImageEditTool::new(
   api_base_url,
   api_key,
   gen_model,
   base_dir,
   )));
   tracing::debug!("Registered 2 image tools (generate, edit)");
   }

   /// Register vision/image analysis tools.
   ///
   /// These tools allow the LLM to analyze images using a vision-capable model.
   pub fn register_vision_tools(
   &self,
   api_base_url: String,
   api_key: String,
   vision_model: String,
   base_dir: Option<std::path::PathBuf>,
   ) {
   use crate::tools::builtin::ImageAnalyzeTool;
   self.register_sync(Arc::new(ImageAnalyzeTool::new(
   api_base_url,
   api_key,
   vision_model,
   base_dir,
   )));
   tracing::debug!("Registered 1 vision tool (analyze)");
   }

7. builder tool

       // Register builder tool if enabled
        let builder = if self.config.builder.enabled
            && (self.config.agent.allow_local_tools || !self.config.sandbox.enabled)
        {生产默认关，不允许不安全工具逃逸

六. 录制回放

好，现在我把链路看清楚了。`http_interceptor` 来自 `recording_handle` 内部的 `RecordingHttpInterceptor`，并且会被 `CompositeHttpInterceptor` 包起来——而测试 remap 也是这个 trait。回答你的问题：

---

## 两者区别 & 为什么都出现

### 一句话总结

- **录制用 `HttpInterceptor`**：在 [`crates/ironclaw_llm/src/recording.rs:154`](crates/ironclaw_llm/src/recording.rs:154)，LLM 端 HTTP 出站时拦下来"录/播"，是面向**功能测试 / 回放**。
- **测试用 `remap_from_env`**：在 [`src/http_intercept.rs`](src/http_intercept.rs)，只是**特定场景**下"换一台 loopback 监听"，是面向**集成测试**。
- 两者实现的是**同一个 trait**，所以最终被 `CompositeHttpInterceptor` 链在一起（[http_intercept.rs:9](src/http_intercept.rs:9)）。

### 详细对比

| 维度 | `RecordingHttpInterceptor` (recording.rs) | `HostRemapHttpInterceptor` (http_intercept.rs) |
|---|---|---|
| 出处 | `crates/ironclaw_llm/src/recording.rs:154-211` | `src/http_intercept.rs:45-131` |
| 实现接口 | `HttpInterceptor::before_request` / `after_response` | 同上 |
| 触发来源 | `RecordingLlm` 启动后挂上（默认就在 `RecordingLlm` 内部） | 环境变量 `IRONCLAW_TEST_HTTP_REMAP=host=http://127.0.0.1:port`，仅在 `cfg(test, debug_assertions)` 下读取（[app.rs:535-539](src/app.rs:535)） |
| 用途 | 给 LLM HTTP 出站做录制与回放（fixture） | 把对外的 host 替换成本地 loopback 监听，e2e 时让本机起一个 mock server 替代第三方 |
| 行为 | recording：`before_request` 返回 `None` 放行 → 真实请求 → `after_response` 落盘（先 redact）；replaying：`before_request` 直接返回录好的响应短路 | `before_request` 直接把 URL 改写到 loopback 监听，自己发 reqwest 请求并把响应作为"短路响应"返回 |
| 是否触碰真实网络 | recording：**是**；replaying：**否** | remap：**是**，但目标只能是 `localhost` / `127.0.0.0/8`（[http_intercept.rs:71-85](src/http_intercept.rs:71)） |
| 是否记录 | 是 → 写到 fixture 文件，提交进 repo（先 redact） | 否 |
| 安全约束 | 录制时强制 redact auth headers/cookies/api keys（[recording.rs:227-251](crates/ironclaw_llm/src/recording.rs:227)） | loopback 目标强校验（外部 IP 一律拒绝注册） + `cfg(test, debug_assertions)` 编译门 + remap 包内警告 |
| 注释里写的"残余威胁模型" | 落盘文件被提交到公共仓库 → 走 redact | "攻击者同时拥有 debug build 的环境变量控制权 + loopback 上的监听"（[http_intercept.rs:64-70](src/http_intercept.rs:64)） |

### 为什么 `ToolRegistry` 上有 `http_interceptor` 字段？

因为 WASM 工具发起的 HTTP 也需要走拦截路径。看一下链路就明白：

1. **LLM 出站 HTTP**（OpenAI/Anthropic/Stripe 等）：由 `RecordingLlm` 内部已挂的 interceptor 拦截，**不走** `ToolRegistry.http_interceptor`。
2. **WASM 工具出站 HTTP**：由 [`src/tools/wasm/wrapper.rs:1018`](src/tools/wasm/wrapper.rs:1018) 在注册 WASM wrapper 时把 `ToolRegistry.http_interceptor` 透传给 `WasmToolWrapper`；WASM 工具每次发出 HTTP 时宿主会先过它一遍。
3. **测试 remap** 既然是 `HttpInterceptor` trait 实现，自然必须走和录制/回放同一套接入点，于是被装到 `ToolRegistry.http_interceptor`，再随 WASM 注册渗到 wrapper。`HttpTool` 内置 HTTP 工具本身不用这套（自带 reqwest + SSRF）。

### 为什么 `http_interceptor` 字段是 `Option`？

通常为 `None`。在 [`app.rs:535-542`](src/app.rs:535) 看得很清楚：

```rust
let http_interceptor = if cfg!(any(test, debug_assertions)) {
    crate::http_intercept::remap_from_env()    // 测试 build 且环境变量存在时才非 None
} else {
    None                                        // release build 必为 None
};
```

只有当 `cfg(test)` 或 `cfg(debug_assertions)` 为真 **且** `IRONCLAW_TEST_HTTP_REMAP` 环境变量存在且至少含一个合法 loopback 映射时，才会返回 `Some(...)`。所以这个字段 99% 的运行时是 `None`，但要预留这个槽位给测试 / `cargo run` 调试时使用。

### 真正的混合运行：CompositeHttpInterceptor

`http_interceptor` 是 `Arc<dyn HttpInterceptor>`，可以装一个或多个。在 [`http_intercept.rs:9-43`](src/http_intercept.rs:9)：

```rust
pub struct CompositeHttpInterceptor {
    interceptors: Vec<Arc<dyn HttpInterceptor>>,
}
impl HttpInterceptor for CompositeHttpInterceptor {
    async fn before_request(&self, request) -> Option<HttpExchangeResponse> {
        for interceptor in &self.interceptors {
            if let Some(response) = interceptor.before_request(request).await {
                return Some(response);   // 任一拦截器短路就返回
            }
        }
        None                            // 全部放行 → 真实网络
    }
    async fn after_response(&self, request, response) {
        for interceptor in &self.interceptors {
            interceptor.after_response(request, response).await;   // 全部调用，recording 拿到
        }
    }
}
```

注释明确：短路时**不**调用 `after_response`（[http_intercept.rs:21-29](src/http_intercept.rs:21)），因为响应是合成的、不是"真实请求"，调用会重复记录。所以测试里如果 remap + recording 同时挂：

- `before_request`：先 remap → loopback 命中、返回合成响应 → recording 不参与 `before_request`、自然也不会调 `after_response`。
- 但如果请求没命中 remap mapping：放行到真实网络 → recording 在 `after_response` 录下来。

### 一句话总结（再压缩）

`ToolRegistry` 的 `http_interceptor` 字段是 **"WASM 出站 HTTP 的可插拔拦截点"**，recording/remap/replay 都实现同一个 `HttpInterceptor` trait，按需组合在 `CompositeHttpInterceptor` 里。remap 是给特定集成场景（本地 mock 服务）开的逃生口，因此用环境变量 + loopback 校验 + `cfg(test, debug_assertions)` 三道闸门锁死；recording 是 fixture 录制机制，重点在 redact 而非 loopback。两者目的不同，但走同一接入点，所以你看到它们都"出现"。

