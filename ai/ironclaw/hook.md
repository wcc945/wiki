一. 类别和注册时机

具体来说，向这个 Arc<HookRegistry> 注册 hook 的路径有 4 条，每条的发生时机不同：

┌─────┬────────────────────────────────────┬────────────────────────────────────────────────┬────────────────────────────────────────────────────────────┬────────────────────────────────────────┐
│  #  │                路径                │                      时机                      │                            来源                            │                是否必出                │
├─────┼────────────────────────────────────┼────────────────────────────────────────────────┼────────────────────────────────────────────────────────────┼────────────────────────────────────────┤
│ 1   │ hooks.register(SessionSummaryHook) │ 进程启动 AppBuilder::build_all                 │ 代码硬编码                                                 │ 必出（有 db+workspace 时）             │
│     │                                    │ 时（src/app.rs:1172-1190）                     │                                                            │                                        │
├─────┼────────────────────────────────────┼────────────────────────────────────────────────┼────────────────────────────────────────────────────────────┼────────────────────────────────────────┤
│ 2   │ register_bundled_hooks(registry)   │ 启动期 bootstrap_hooks                         │ 硬编码                                                     │ 必出 1 个：AuditLogHook（priority      │
│     │                                    │ 内（src/hooks/bundled.rs:136-146）             │                                                            │ 25，挂全部 6 个点）                    │
├─────┼────────────────────────────────────┼────────────────────────────────────────────────┼────────────────────────────────────────────────────────────┼────────────────────────────────────────┤
│ 3   │ register_plugin_bundles(...)       │ 启动期 bootstrap_hooks                         │ 扫描 wasm_tools_dir / wasm_channels_dir / dev_tools        │ 看装了哪些扩展                         │
│     │                                    │ 内（src/hooks/bootstrap.rs:77-102）            │ 下所有"激活的"扩展的 capabilities.json 里的 hooks 段       │                                        │
├─────┼────────────────────────────────────┼────────────────────────────────────────────────┼────────────────────────────────────────────────────────────┼────────────────────────────────────────┤
│ 4   │ register_workspace_bundles(...)    │ 启动期 bootstrap_hooks                         │ 读 workspace 里的 hooks/hooks.json 和 hooks/*.hook.json    │ 看用户/owner 在 workspace 里放了什么   │
│     │                                    │ 内（src/hooks/bootstrap.rs:254-309）           │                                                            │                                        │
└─────┴────────────────────────────────────┴────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────┘

bootstrap_hooks 的总入口在 src/main.rs:837，它在 main 启动序列里被调用；它把 4 条路径的注册数累加进 HookBootstrapSummary，所以现在一个干净的 fresh start 至少会跑出 2 个 hook：session_summary +
builtin.audit_log，再加上任何已安装扩展 capabilities 里声明的 hook，再加 workspace 里 hooks/ 下的 declarative bundle。

  ---
4 条路径里出来的 hook 分别是哪几类

bundled.rs:81-187 把它们分成两类：

A. Rule hook（声明式 regex/prepend/append/reject）

直接读 HookRuleConfig，运行时编译成 RuleHook（bundled.rs:290-426）。支持：

- when_regex —— 守门正则，不匹配就 no-op
- reject_reason —— 匹配上时直接 Reject
- replacements[] —— 链式 regex 改写
- prepend / append —— 头尾加串

▎ 注意：这种 hook 会实际改写主内容（走 HookOutcome::modify，被下一个 hook 看到改写后的版本）。一个典型例子就是 bundled.rs:986-997 的测试用例 block-forbidden：在 BeforeInbound 上把含 "forbidden"
▎ 的消息直接拒掉。

B. Outbound webhook hook（fire-and-forget HTTP 通知）

OutboundWebhookHook（bundled.rs:430-592）：只对做了 summarize_webhook_event 的轻量摘要（channel、tool_name、content_length、parameter_count 等元信息）发一个 POST 到配置里的 https URL。它本身不改写
HookOutcome，永远是 ok()，所以不会污染后续 hook 的输入。

它的安全约束做得很重（bundled.rs:640-859）：

- 必须 https（http:// 拒绝）
- 禁 host 列表：localhost / *.localhost / host.docker.internal / 云 metadata 服务
- 禁 IP 列表：私网、回环、link-local、CGN 100.64.0.0/10、benchmarking 198.18.0.0/15、IPv4-mapped IPv6
- 运行时再做一次 DNS 解析 + 重新校验 IP（dispatch_client_for_target，bundled.rs:681-727）—— 防止声明时是公网 IP、运行时 DNS rebind 到 127.0.0.1
- 禁 header 列表：Host / Authorization / Cookie / X-Forwarded-* 等
- max_in_flight 限流（默认 32 并发，满了就 drop 这条事件并 warn）

▎ 这套限制等于把 "webhook 偷数据" / "SSRF 到 169.254.169.254 偷云凭据" / "DNS rebind 攻击" 这三条攻击路径堵掉了。

C. 还有一个必出的 AuditLogHook

struct AuditLogHook;
// bundled.rs:253-280
// priority 25, hook_points = ALL_HOOK_POINTS

它就是个 tracing::debug! 把每个事件打到 hooks::audit target 上，不消费 payload，只记元信息（user_id、point 名）。所以你 RUST_LOG=hooks::audit=debug 就能看到全部 6
个点的触发流水。它存在的意义是给操作员一个统一的"hook 到底跑了没"的探针——尤其当 Rule hook 用 reject_reason 改写内容时，看 audit log 才知道哪条 hook 拦了哪条消息。

二. 相关结构体

1. manager

/// Registry that manages hooks and executes them at lifecycle points.
///
/// Hooks are executed in priority order (lower number = higher priority).
/// A `Reject` outcome stops the chain immediately.
/// A `Modify` outcome chains through subsequent hooks.
pub struct HookRegistry {
hooks: RwLock<Vec<HookEntry>>,
}

2. entry

struct HookEntry {
hook: Arc<dyn Hook>,
priority: u32,
}

/// Trait for implementing lifecycle hooks.
///
/// Hooks intercept and can modify agent operations at well-defined points.
#[async_trait]
pub trait Hook: Send + Sync {
/// A unique name for this hook.
fn name(&self) -> &str;

    /// The lifecycle points this hook should be called at.
    fn hook_points(&self) -> &[HookPoint];

    /// How to handle failures in this hook.
    ///
    /// Default: `FailOpen` (continue on error).
    fn failure_mode(&self) -> HookFailureMode {
        HookFailureMode::FailOpen
    }

    /// Maximum time this hook is allowed to run.
    ///
    /// Default: 5 seconds.
    fn timeout(&self) -> Duration {
        Duration::from_secs(5)
    }

    /// Execute the hook.
    async fn execute(&self, event: &HookEvent, ctx: &HookContext)
    -> Result<HookOutcome, HookError>;
}


pub enum HookPoint {
/// Before processing an inbound user message.
BeforeInbound,
/// Before executing a tool call.
BeforeToolCall,
/// Before sending an outbound response.
BeforeOutbound,
/// When a new session starts.
OnSessionStart,
/// When a session ends (pruned or expired).
OnSessionEnd,
/// Transform the final response before completing a turn.
TransformResponse,
}

pub enum HookEvent {
/// An inbound user message about to be processed.
Inbound {
user_id: String,
channel: String,
content: String,
thread_id: Option<String>,
},
/// A tool call about to be executed.
ToolCall {
tool_name: String,
parameters: serde_json::Value,
user_id: String,
/// "chat" for interactive, or a job ID string for autonomous jobs.
context: String,
},
/// An outbound response about to be sent.
Outbound {
user_id: String,
channel: String,
content: String,
thread_id: Option<String>,
},
/// A new session was created.
SessionStart { user_id: String, session_id: String },
/// A session was ended (pruned).
SessionEnd {
user_id: String,
session_id: String,
/// Thread IDs (= conversation IDs) that belonged to this session.
/// Used by hooks like SessionSummaryHook to summarize the correct
/// conversation rather than guessing via recency.
#[serde(default)]
thread_ids: Vec<uuid::Uuid>,
},
/// The final response is being transformed before completing a turn.
ResponseTransform {
user_id: String,
thread_id: String,
response: String,
},
}

/// The result of executing a hook.
#[derive(Debug, Clone)]
pub enum HookOutcome {
/// Continue processing, optionally with modified content.
Continue {
/// If `Some`, replace the event's primary content with this value.
modified: Option<String>,
},
/// Reject the event entirely.
Reject {
/// Human-readable reason for the rejection.
reason: String,
},
}

/// How to handle hook execution failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookFailureMode {
/// On error/timeout, continue processing as if the hook returned `ok()`.
FailOpen,
/// On error/timeout, reject the event.
FailClosed,
}
● HookFailureMode 含义

这个枚举说的是"当这个 hook 自己在执行时出错或超时了，框架（HookRegistry::run）该怎么收尾"。它不影响正常返回的 HookOutcome——那是 hook 自己决定 Continue 还是 Reject。

只影响两种异常路径（registry.rs:122-150）：

┌───────────────┬────────────────────────────────────────────────┐
│     异常      │                      含义                      │
├───────────────┼────────────────────────────────────────────────┤
│ Ok(Err(e))    │ hook 的 execute 主动返回了 Err(HookError::...) │
├───────────────┼────────────────────────────────────────────────┤
│ Err(_elapsed) │ hook 超过它自己声明的 timeout() 还没返回       │
└───────────────┴────────────────────────────────────────────────┘

  ---
FailOpen —— "静默放过"

HookFailureMode::FailOpen => {
tracing::warn!(hook = hook.name(), "Hook failed (fail-open): {}", err);
// 继续链上的下一个 hook，最终 return HookOutcome::ok()
}

行为：这个 hook 死了没事，主流程继续。它的"占位"等于 HookOutcome::ok()，不会把它之前的 modify 撤掉、也不会 Reject 整条链。

▎ 这就是为什么 SessionSummaryHook 选 FailOpen（session_summary.rs:58-60）——摘要写失败不应该让会话结束流程崩，主流程照常结束、用户无感。

FailClosed —— "拒绝整个事件"

HookFailureMode::FailClosed => {
tracing::warn!(hook = hook.name(), "Hook failed (fail-closed): {}", err);
return Err(HookError::ExecutionFailed { ... });
// 立即返回, 不再调后续 hook
}

行为：hook 异常 = 等价于 Reject。整个 HookRegistry::run 返回 Err，链上剩余 hook 一律不再执行。

▎ 适合挂在安全敏感的 hook 上。比如一个 BeforeToolCall 的 "审批 hook"——它超时了 = 没人审批 = 不能放行工具调用。

  ---
三种"对主流程的最终影响"对比

┌─────────────────────────────────────┬────────────────────────────────────┬────────────────────┐
│            Hook 怎么走的            │             主流程收到             │        说明        │
├─────────────────────────────────────┼────────────────────────────────────┼────────────────────┤
│ hook 正常返回 Continue              │ 继续下一个 hook                    │ 任何 mode 都一样   │
├─────────────────────────────────────┼────────────────────────────────────┼────────────────────┤
│ hook 正常返回 Reject                │ 立即 Err(HookError::Rejected)      │ 任何 mode 都一样   │
├─────────────────────────────────────┼────────────────────────────────────┼────────────────────┤
│ hook 正常返回 Continue { modified } │ 用改写后的 content 继续下一个 hook │ 任何 mode 都一样   │
├─────────────────────────────────────┼────────────────────────────────────┼────────────────────┤
│ hook 返回 Err（异常）               │ FailOpen = 继续；FailClosed = 拒绝 │ 这里 mode 才有区别 │
├─────────────────────────────────────┼────────────────────────────────────┼────────────────────┤
│ hook 超时                           │ FailOpen = 继续；FailClosed = 拒绝 │ 同上               │
└─────────────────────────────────────┴────────────────────────────────────┴────────────────────┘

