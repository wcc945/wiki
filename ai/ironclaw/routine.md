一. 相关结构

1. RoutineEngine

● RoutineEngine 是 src/agent/routine_engine.rs 里那个组件——周期 tick cron ticker + 事件匹配器，按 Routine（cron/event/system_event/manual trigger）匹配时间或事件，触发对应的 RoutineAction（轻量 inline
跑，或 dispatch 给 Scheduler 跑完整 job）。它是把"用户设的自动化规则"真正按节奏/事件跑起来的执行器，本身不干活、只调度。
/// The routine execution engine.
pub struct RoutineEngine {
config: RoutineConfig,
store: SystemScope,
llm: Arc<dyn LlmProvider>,
workspace: Arc<Workspace>,
/// Sender for notifications (routed to channel manager).
notify_tx: mpsc::Sender<OutgoingResponse>,
/// Currently running routine count (across all routines).
running_count: Arc<AtomicUsize>,
/// Cached matchers for all event-driven routines.
event_cache: Arc<RwLock<Vec<EventMatcher>>>,
/// Scheduler for dispatching jobs (FullJob mode).
scheduler: Option<Arc<Scheduler>>,
/// Owner-scoped extension activation state for autonomous tool resolution.
extension_manager: Option<Arc<ExtensionManager>>,
/// Tool registry for lightweight routine tool execution.
tools: Arc<ToolRegistry>,
/// Safety layer for tool output sanitization.
safety: Arc<SafetyLayer>,
/// Sandbox readiness state — only `DockerUnavailable` blocks full-job dispatch.
sandbox_readiness: SandboxReadiness,
/// Optional global HTTP interceptor (e.g., the
/// `IRONCLAW_TEST_HTTP_REMAP` debug-only host remapper installed
/// in `src/app.rs`). Threaded through to every spawned
/// `EngineContext` and on into the `JobContext` that drives
/// Lightweight tool dispatches, so routine-fired tools see the
/// same interceptor the chat path does.
http_interceptor: Option<Arc<dyn ironclaw_llm::recording::HttpInterceptor>>,
/// Resolved runtime policy threaded through to spawned
/// `EngineContext`s so routine-driven LLM iterations apply the
/// model-facing tool list filter (#3243 HIGH iteration-2 gap).
runtime_policy: Option<ironclaw_host_api::runtime_policy::EffectiveRuntimePolicy>,
/// Timestamp when this engine instance was created. Used by
/// `sync_dispatched_runs` to distinguish orphaned runs (from a previous
/// process) from actively-watched runs (from this process).
boot_time: chrono::DateTime<Utc>,
}
# `RoutineEngine` 字段逐项

```rust
pub struct RoutineEngine {
    config:            RoutineConfig,                          // 配置（enabled、interval、limits…）
    store:             SystemScope,                            // 系统级 DB scope
    llm:               Arc<dyn LlmProvider>,                   // 跑 lightweight LLM 调用
    workspace:         Arc<Workspace>,                         // 读/写工作区（HEARTBEAT 同款）
    notify_tx:         mpsc::Sender<OutgoingResponse>,         // 通知上行，发到 ChannelManager
    running_count:     Arc<AtomicUsize>,                       // 当前在跑的 routine 计数（并发限制用）
    event_cache:       Arc<RwLock<Vec<EventMatcher>>>,         // 事件型 routine 预编译的匹配器
    scheduler:         Option<Arc<Scheduler>>,                 // FullJob 模式转给后台调度器
    extension_manager: Option<Arc<ExtensionManager>>,         // owner-scoped extension 激活状态
    tools:             Arc<ToolRegistry>,                      // lightweight 模式下的工具执行
    safety:            Arc<SafetyLayer>,                       // 工具输出脱敏
    sandbox_readiness: SandboxReadiness,                       // Docker 状态（决定能否 FullJob）
}
```

| 字段                                               | 作用                                                                                                                                                          |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `config: RoutineConfig`                            | 引擎的总配置：是否启用、tick 间隔、最大并发等。从 `rt_config` 传进来。                                                                                        |
| `store: SystemScope`                               | **系统级** DB scope（不是用户级）——routines 列表存在 system 命名空间里。                                                                                      |
| `llm: Arc<dyn LlmProvider>`                        | 跑"轻量 LLM 任务"用的 provider，比如 `RoutineAction::Lightweight` 模式调一次 LLM 完成事。                                                                     |
| `workspace: Arc<Workspace>`                        | 读 `HEARTBEAT.md` / daily logs / 写入发现结果——和 heartbeat 共享同一份 workspace。                                                                            |
| `notify_tx: mpsc::Sender<OutgoingResponse>`        | 通知上行通道：routines 跑完想告诉用户就走这里，绕开业务消息流，单独路由到 `ChannelManager`。                                                                  |
| `running_count: Arc<AtomicUsize>`                  | **当前正在执行的 routine 数量**。配 `config` 里的 `max_concurrent` 一起做并发限流——超过上限就 skip 这一 tick。                                                |
| `event_cache: Arc<RwLock<Vec<EventMatcher>>>`      | **预编译**的事件匹配器列表。每个 event-driven routine 在加载时编译一次匹配规则（pattern、tag 等），tick 时只跑这些 matcher 即可，省掉每 tick 重新解析的开销。 |
| `scheduler: Option<Arc<Scheduler>>`                | FullJob 模式委托：routines 触发后调 `Scheduler::dispatch_job` 跑后台任务，`RoutineEngine` 自己不直接执行。`None` 表示引擎不能跑 full-job。                    |
| `extension_manager: Option<Arc<ExtensionManager>>` | 自治场景下让 routine 也能解析"这个用户装了什么 extension"——决定能调哪些工具/扩展。`None` 时退化为只跑 builtin tools。                                         |
| `tools: Arc<ToolRegistry>`                         | **轻量模式**下要执行的工具集合（不同于 `Scheduler` 跑 job 时那套）。                                                                                          |
| `safety: Arc<SafetyLayer>`                         | 工具输出回 LLM 前必须过的安全层（sanitizer + leak detector 等）。                                                                                             |
| `sandbox_readiness: SandboxReadiness`              | 沙箱可用性。`DockerUnavailable` 状态**显式阻止** FullJob 派发——避免 engine 强派发然后 sandbox 拒绝。                                                          |

# 几个隐藏的小细节

- `notify_tx` 是**`mpsc::Sender` 端**——RoutineEngine 是发送方，接收方在 `app.rs`/agent 启动那段把 rx 桥接到 `ChannelManager`（不在这文件里）。
- `store: SystemScope`（不是 `UserScope`）——routines 本身是系统级配置，不属于任何单用户。
- `running_count: Arc<AtomicUsize>`——`Arc` 是因为 routine 触发的闭包要 clone 一份带进 spawned task 里做"结束减一"。
- `event_cache` 的存在意味着**热路径性能敏感**——event matcher 编译是按 routine 一次性、tick 多次复用的。
- `sandbox_readiness: SandboxReadiness` 这个**`Option` 缺席**——总是存在（`#[derive(Default)]`），但枚举值 `DockerUnavailable` 是触发"跳过 FullJob"的信号。

2. Routine

/// A routine is a named, persistent, user-owned task with a trigger and an action.
//! ```text
//! ┌──────────┐     ┌─────────┐     ┌──────────────────┐
//! │  Trigger  │────▶│ Engine  │────▶│  Execution Mode  │
//! │ cron/event│     │guardrail│     │lightweight│full_job│
//! │ system    │     │ check   │     └──────────────────┘
//! │ manual    │     └─────────┘              │
//! └──────────┘                               ▼
//!                                     ┌──────────────┐
//!                                     │  Notify user │
//!                                     │  if needed   │
//!                                     └──────────────┘
//! ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Routine {
pub id: Uuid,
pub name: String,
pub description: String,
pub user_id: String,
pub enabled: bool,
pub trigger: Trigger,
pub action: RoutineAction,
pub guardrails: RoutineGuardrails,
pub notify: NotifyConfig,

    // Runtime state (DB-managed)
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub run_count: u64,
    pub consecutive_failures: u32,
    pub state: serde_json::Value,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// When a routine should fire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
/// Fire on a cron schedule (e.g. "0 9 * * MON-FRI" or "every 2h").
Cron {
schedule: String,
#[serde(default)]
timezone: Option<String>,
},
/// Fire when a channel message matches a pattern.
Event {
/// Optional channel filter (e.g. "telegram", "slack").
channel: Option<String>,
/// Regex pattern to match against message content.
pattern: String,
},
/// Fire when a structured system event is emitted.
SystemEvent {
/// Event source namespace (e.g. "github", "workflow", "tool").
source: String,
/// Event type within the source (e.g. "issue.opened").
event_type: String,
/// Optional exact-match filters against payload top-level fields.
#[serde(default)]
filters: std::collections::HashMap<String, String>,
},
/// Fire on incoming webhook POST to /api/webhooks/{path}.
Webhook {
/// Optional webhook path suffix (defaults to routine id).
path: Option<String>,
/// Optional shared secret for HMAC validation.
secret: Option<String>,
},
/// Only fires via tool call or CLI.
Manual,
}

/// What happens when a routine fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RoutineAction {
/// Single LLM call (optionally with tools). Cheap and fast.
Lightweight {
/// The prompt sent to the LLM.
prompt: String,
/// Workspace paths to load as context (e.g. ["context/priorities.md"]).
#[serde(default)]
context_paths: Vec<String>,
/// Max output tokens (default: 4096).
#[serde(default = "default_max_tokens")]
max_tokens: u32,
/// Enable tool access (default: false for backward compatibility).
/// When true, the LLM can call tools during execution.
/// Tools requiring approval are automatically filtered out.
#[serde(default)]
use_tools: bool,
/// Max tool call rounds (default: 3). Only used when use_tools is true.
#[serde(default = "default_max_tool_rounds")]
max_tool_rounds: u32,
},
/// Full multi-turn worker job with tool access.
FullJob {
/// Job title for the scheduler.
title: String,
/// Job description / initial prompt.
description: String,
/// Max reasoning iterations (default: 10).
#[serde(default = "default_max_iterations")]
max_iterations: u32,
},
}

┌───────────┬──────────────────────────────────────────────────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────┐   
│   维度    │                            RoutineAction::Lightweight                            │                                      RoutineAction::FullJob                                       │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 执行实体  │ RoutineEngine 内部 inline                                                        │ 委托给 Scheduler                                                                                  │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 跑在哪    │ 引擎 spawn 的轻量 task，跟 routine run 同一 task                                 │ 独立 worker job（在 jobs map 里占一个 slot，受 max_parallel_jobs 限制）                           │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ LLM 调用  │ 单次 llm.chat（可选用工具走 N 轮小循环）                                         │ 走完整 agentic loop，多轮推理（默认 25 次 max_iterations，RoutineEngine 字段定义为 10 但实际默认  │   
│           │                                                                                  │ 25，routine.rs:330）                                                                              │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 工具      │ engine.tools（lightweight 集），默认关（use_tools: false），开启时需走           │ 完整 ToolDispatcher::dispatch，包含 ActionRecord 审计、参数脱敏、超时、输出脱敏                   │   
│           │ autonomous_allowed_tool_names 过滤（带"approval"标记的工具自动剔除）             │                                                                                                   │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 输出上限  │ max_tokens（默认 4096）                                                          │ max_iterations（默认 25）—— 整轮推理循环上限                                                      │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 会话/线程 │ 临时会话，无独立 thread                                                          │ 创建独立 JobContext、有独立 UserId、在 ContextManager 里持久化                                    │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 通知      │ 通过 engine.notify_tx mpsc 直接 send OutgoingResponse 到 ChannelManager          │ job 完成后经 JobMonitor/SSE 灌回主 loop                                                           │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 沙箱      │ 不需要（轻量，本机直跑）                                                         │ 看 sandbox_readiness：Docker 不可用 → 拒绝派发（fail-closed）；DisabledByConfig → 退化为 host     │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 完成监控  │ 直接 await，结果写到 run.result_summary                                          │ FullJobWatcher::wait_for_completion（routine_engine.rs:1538），5s 间隔轮询 DB，routine run 必须   │   
│           │                                                                                  │ link 到 job_id（不 link 永久卡 "running" 状态）                                                   │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ DB 持久化 │ 仅写 routine_runs 表                                                             │ 写 routine_runs + 独立的 jobs 行 + job_actions / llm_calls（FK 引用）                             │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 重试语义  │ 失败重试有 token 累计保留                                                        │ 失败有专门的 JobState::Stuck → SelfRepair 路径                                                    │   
├───────────┼──────────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤   
│ 适用场景  │ "每 N 分钟看一眼 HEARTBEAT.md，通知我" "扫一下邮箱标题，挑重要的"                │ "每天早上跑一个完整调研、生成报告、写到 Notion"                                                   │   
└───────────┴──────────────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────┘ 

