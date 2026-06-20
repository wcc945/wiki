一. 相关结构

1. SessionManager (会话管理session-thread-turn)

pub struct SessionManager {
sessions: RwLock<HashMap<String, Arc<Mutex<Session>>>>,
thread_map: RwLock<HashMap<ThreadKey, Uuid>>,
undo_managers: RwLock<HashMap<Uuid, Arc<Mutex<UndoManager>>>>,
hooks: Option<Arc<HookRegistry>>,
}
这是 agent 模块的"会话账本"。它本身不持有对话内容——对话内容在 Session { threads: HashMap<Uuid, Thread> { turns: Vec<Turn> } } 里——它只负责索引和外挂能力的查找表。

1. sessions: RwLock<HashMap<String, Arc<Mutex<Session>>>>

▎ Key = user_id，Value = 共享会话句柄

这是身份到会话的映射。 多用户多会话是核心：

- 同一个 user_id（不分 channel）→ 同一个 Arc<Mutex<Session>>。test_get_or_create_session 验证了这一点。
- 不同 user_id → 不同 Session。

sessions:
"user-alice"  -> Arc<Mutex<Session{id: S1, threads: {T1, T2, T3}}>>
"user-bob"    -> Arc<Mutex<Session{id: S2, threads: {T4}}>>

几个值得注意的设计点：

- 双重检查锁（DCL）：get_or_create_session 先 read lock 找；找不到再 write lock；拿到写锁后再查一次（session_manager.rs:65）。这是标准 Java/C++ 风格 DCL，在 Rust 里也是必要的——光靠 if let Some
  然后直接 insert 会因为两个并发请求都 miss read lock 而重复创建。concurrent_get_or_create_same_user_returns_same_session 这个测试就是为这个不变量存在的。
- Arc<Mutex<Session>> 而不是 Arc<RwLock<Session>>：因为 Session 几乎所有操作都是 read-modify-write（加 turn、改 active_thread、改 last_active_at），所以读多写少这个 RwLock 优势拿不到，反而 Mutex
  更简单、不会有人写出"读锁里 await 别的写锁"这种死锁模式。
- 1000 阈值警告（session_manager.rs:74-80）：超过 1000 个活跃 session 时，每 100 个打一条 warn——这是廉价的对运维友好的信号，不是 hard cap。
- Prune：prune_stale_sessions 遍历 read lock，对每个 session 用 try_lock() 试一下（拿不到就跳过——有人在用就保留），按 last_active_at < cutoff 过滤，然后批量
  remove。这是一个"尽力而为的清理器"——永远不会把正在用的 session 杀掉。

1:1 不是 1:N：sessions 这层不分 channel，分 channel 的事是下一个字段的活。

  ---
2. thread_map: RwLock<HashMap<ThreadKey, Uuid>>

▎ Key = (user_id, channel, external_thread_id)，Value = 内部 thread UUID

这是 channel 维度的 thread 索引。 同一个 user_alice 在 Telegram、Slack、Web Gateway 上各自有独立的对话线——它们都活在 alice 的同一个 Session 里（因为 sessions 是按 user_id 分），但 thread_map 让每个
channel 路由到不同的 Thread。

thread_map:
("alice", "telegram", Some("chat-123")) -> T1
("alice", "slack",    Some("C123.456")) -> T2
("alice", "gateway",  None)             -> T3   // 没显式 thread_id 的默认线
("alice", "telegram", None)             -> T4   // telegram 默认线（独立于 gateway 默认线）

几个关键设计点：

- ThreadKey 三元组：(user_id, channel, external_thread_id)。
    - user_id：多用户隔离。
    - channel：多端隔离——同一个用户在 web 和 telegram 是不同的 thread。
    - external_thread_id: Option：None 也是一个合法的 key（默认线），而且 None ≠ Some("")，这是有意为之（test_resolve_thread_none_vs_some_external_id 验证）。
- None 不触发 UUID adoption（session_manager.rs:170-174 + test_resolve_thread_with_none_external_thread_id_does_not_adopt）：这是为了保护一个不变量——"没传 external_id 的调用方永远拿到它自己创建的默认
  thread，不会被某个并行 hydrate 出来的 UUID 拐走"。这是个微妙的安全洞的修补，注释里点名了。
- register_thread（:246）：这是反向操作——给一个已经存在但没在 map 里的 thread（比如从 DB hydrate 出来的、chat_new_thread_handler 直接 sess.threads.insert(...) 的）建立 map 入口。or_insert_with
  让它幂等（test_register_thread_idempotent），用 entry().or_insert_with 而不是 insert 是因为后者会覆盖已有的 undo manager（覆盖就丢数据了）。
- UUID adoption 路径（:152-198）：一个相对复杂的兜底——如果 external_thread_id 是个合法 UUID 且 thread_map 里没人认领它、且 session.threads 里确实有这个 UUID 的 Thread，那 map
  把它认领回去。test_register_thread_preserves_uuid_on_resolve 这条测试的注释说"this was the root cause of the wrong conversation
  bug"——就是说之前漏了这个路径导致回复发到错的对话去了。resolve_thread_with_parsed_uuid 是为审批路由优化而加的：approval 路径已经验证过 UUID 是合法的，不需要重复 parse。
- Prune 时按 user 整体抹掉（:386）：thread_map.retain(|key, _| !stale_users.contains(&key.user_id))——不需要逐 thread 判，因为它和 sessions 里的用户生命周期一致。

  ---
3. undo_managers: RwLock<HashMap<Uuid, Arc<Mutex<UndoManager>>>>

▎ Key = thread Uuid，Value = 该 thread 的 undo/redo 状态机

这是 thread 维度的 undo 能力外挂。 为什么单独拎出来而不是塞进 Session.threads[uuid].undo_manager？

- Session 在锁内，UndoManager 也需要锁——把两个 mutex 字段放在同一个 struct 上 = 经典的可重入/借锁死锁陷阱。
- prune 时独立 remove（:389-394）：cleanup 路径需要按 thread_id 直接抹掉，外部 map 拿得到 key（stale_thread_ids），不用锁整个 Session。
- UndoManager 维护一个 checkpoints 栈（不是简单一个 prev）：最多 20 个快照，超出 FIFO。undo/redo 是栈操作。这个细节不在字段层面暴露，但 test_undo_manager 验证了 get_undo_manager(thread_id)
  两次返回同一个 Arc。

get_undo_manager 也有 DCL（:279-298）：read lock 查 miss → write lock → 再查 miss → insert。和 sessions 一样的并发安全模式。

一个隐含事实：thread_map 的 key 是 (user, channel, ext_id)，undo_managers 的 key 是 Uuid（内部 thread id）。这两个 map 的生命周期是独立的——thread_map 按 user 整体 prune；undo_managers 按 thread_id
逐个 prune。所以 prune 一个用户时是"先把 thread_id 收齐再批量删 undo_managers"——见 :332-395 的两段循环。

  ---
4. hooks: Option<Arc<HookRegistry>>

▎ None 意味着不挂任何 hook；Some 意味着触发 OnSessionStart / OnSessionEnd

这是给 SessionManager 接钩子的插槽。

- Option 是有意的（不是 architecture.md 规则 2 警告的那种"运行时假装可缺"的 smell）：测试构造的 SessionManager::new() 不需要 hooks；只有 AppBuilder 把它组装进 AgentComponents 时才通过 with_hooks()
  注入。这是真正的可选依赖——测试代码大量不挂 hook，照样能跑。
- fire-and-forget 触发（:83-97 和 :351-371）：
    - get_or_create_session 创建完 session 后，tokio::spawn 一个异步任务去 hooks.run(&HookEvent::SessionStart { ... })——不阻塞 session 创建。
    - prune_stale_sessions 删除前，对每个要清理的 session 同样 spawn 触发 HookEvent::SessionEnd { ..., thread_ids }——让 SessionSummaryHook 有机会写完摘要再死。
    - 两处都 if let Err(e) = ... { tracing::warn!(...) }，fail-open 行为——hook 出错不会拖垮会话流程，这呼应 HookFailureMode 的讨论。
- 不在普通 session 查找路径触发（:53-67）：只有"创建新 session"才触发 SessionStart；只是 read lock 命中已存在 session 时不触发——否则每次发消息都会 spawn 一个新 hook 任务。这是个有意识的状态机设计。
- 没有 OnInbound / OnOutbound：那些挂在消息层（Agent/ChatDelegate），不是 SessionManager 的职责。SessionManager 只关心生命周期事件。

  ---
字段间的总体关系

          ┌─────────────────────────────────────────────┐
          │              SessionManager                 │
          │                                             │
          │   sessions                                  │
          │   ┌──────────────────────────────────────┐  │
          │   │ "alice" -> Arc<Mutex<Session>>       │  │
          │   │            threads: {                │  │
          │   │              T1: { turns: [...] }    │  │  ← 对话内容在这里
          │   │              T2: { turns: [...] }    │  │
          │   │              ...                     │  │
          │   │            }                          │  │
          │   └──────────────────────────────────────┘  │
          │                                             │
          │   thread_map                                │
          │   ┌──────────────────────────────────────┐  │
          │   │ (alice,telegram,"chat-123") -> T1    │  │  ← 路由索引
          │   │ (alice,slack,"C123.456") -> T2       │  │
          │   └──────────────────────────────────────┘  │
          │                                             │
          │   undo_managers                             │
          │   ┌──────────────────────────────────────┐  │
          │   │ T1 -> Arc<Mutex<UndoManager{ckpts[]}>│ │  ← thread 维度的 undo 状态
          │   │ T2 -> Arc<Mutex<UndoManager{...}>   │  │
          │   └──────────────────────────────────────┘  │
          │                                             │
          │   hooks: Some(Arc<HookRegistry>)            │  ← 生命周期信号
          └─────────────────────────────────────────────┘

四个字段的 key 空间是不重叠的：
- sessions —— 主体（user 维度）
- thread_map —— 路由（user × channel × ext_id → 主体内的 thread）
- undo_managers —— 能力外挂（thread 维度）
- hooks —— 全局共享信号

这种"主体一份、索引若干、能力外挂、全局信号"的拆分让 prune/resolve/route/notify 各走各的锁，不会因为一个高频路径（比如 undo）抢锁导致消息路径（比如 resolve_thread）卡死。

/// Key for mapping external thread IDs to internal ones.
#[derive(Clone, Hash, Eq, PartialEq)]
struct ThreadKey {
user_id: String,
channel: String,
external_thread_id: Option<String>,
}

2. Session(一个用户一个会话)

//! Session and thread model for turn-based agent interactions.
//!
//! A Session contains one or more Threads. Each Thread represents a
//! conversation/interaction sequence with the agent. Threads contain
//! Turns, which are request/response pairs.
//!
//! This model supports:
//! - Undo: Roll back to a previous turn
//! - Interrupt: Cancel the current turn mid-execution
//! - Compaction: Summarize old turns to save context
//! - Resume: Continue from a saved checkpoint


/// A session containing one or more threads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
/// Unique session ID.
pub id: Uuid,
/// User ID that owns this session.
pub user_id: String,
/// Active thread ID.
pub active_thread: Option<Uuid>,
▎ active_thread 是 SessionManager 不维护的、"用户当前焦点"这个用户态概念的内存表示。 它不是路由优化，是用户隐式语义的承载——"用户没指定 thread_id 时该往哪发"这个问题的答案。SessionManager 的
▎ thread_map 处理显式路由，Session 内部的 active_thread 处理隐式焦点。
/// All threads in this session.
pub threads: HashMap<Uuid, Thread>,//threadId对应的具体会话内容
/// When the session was created.
pub created_at: DateTime<Utc>,
/// When the session was last active.
pub last_active_at: DateTime<Utc>,
/// Session metadata.
pub metadata: serde_json::Value,
/// Tools that have been auto-approved for this session ("always approve").
#[serde(default)]
pub auto_approved_tools: HashSet<String>,
}

3. thread(每个用户session可能会有多个会话)

/// A conversation thread within a session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thread {
/// Unique thread ID.
pub id: Uuid,
/// Parent session ID.
pub session_id: Uuid,
/// Current state.
pub state: ThreadState,
/// Turns in this thread.
pub turns: Vec<Turn>,//所有消息的存储，有多轮
/// When the thread was created.
pub created_at: DateTime<Utc>,
/// When the thread was last updated.
pub updated_at: DateTime<Utc>,
/// Thread metadata (e.g., title, tags).
pub metadata: serde_json::Value,
/// Pending approval request (when state is AwaitingApproval).
#[serde(default)]
pub pending_approval: Option<PendingApproval>,
/// Pending auth token request (thread is in auth mode).
#[serde(default)]
pub pending_auth: Option<PendingAuth>,
/// Messages queued while the thread was processing a turn.
#[serde(default, skip_serializing_if = "VecDeque::is_empty")]
pub pending_messages: VecDeque<String>,pending_messages 是 agent 正在处理 turn 时新到达消息的 FIFO 队列，
避免用户在思考期间发的消息被丢弃——按到达顺序在当前 turn 完成后依次启动新 turn。
/// Channel that created this thread (for approval authorization).
#[serde(default)]
pub source_channel: Option<String>,
}

/// State of a thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreadState {
/// Thread is idle, waiting for input.
Idle,
/// Thread is processing a turn.
Processing,
/// Thread is waiting for user approval.
AwaitingApproval,
/// Thread has completed (no more turns expected).
Completed,
/// Thread was interrupted.
Interrupted,
}

/// Pending tool approval request stored on a thread.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingApproval {
/// Unique request ID.
pub request_id: Uuid,
/// Tool name requiring approval.
pub tool_name: String,
/// Tool parameters (original values, used for execution).
pub parameters: serde_json::Value,
/// Redacted tool parameters (sensitive values replaced with `[REDACTED]`).
/// Used for display in approval UI, logs, and SSE broadcasts.
#[serde(default)]
pub display_parameters: serde_json::Value,
/// Description of what the tool will do.
pub description: String,
/// Tool call ID from LLM (for proper context continuation).
pub tool_call_id: String,
/// Context messages at the time of the request (to resume from).
pub context_messages: Vec<ChatMessage>,
/// Remaining tool calls from the same assistant message that were not
/// executed yet when approval was requested.
#[serde(default)]
pub deferred_tool_calls: Vec<ToolCall>,
/// First actionable auth prompt already discovered in this turn. Persisted
/// so approval pauses do not drop the prompt before it can be surfaced.
#[serde(default)]
pub selected_auth_prompt: Option<PendingAuthPrompt>,
/// User timezone at the time the approval was requested, so it persists
/// through the approval flow even if the approval message lacks timezone.
#[serde(default)]
pub user_timezone: Option<String>,
/// Whether the "always" auto-approve option should be offered to the user.
/// `false` when the tool returned `ApprovalRequirement::Always` (e.g.
/// destructive shell commands), meaning every invocation must be confirmed.
#[serde(default = "default_true")]
pub allow_always: bool,
}

/// When `tool_auth` returns `awaiting_token`, the thread enters auth mode.
/// The next user message is intercepted before entering the normal pipeline
/// (no logging, no turn creation, no history) and routed directly to the
/// credential store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingAuth {
/// Extension name to authenticate.
pub extension_name: ExtensionName,
/// When this auth mode was entered. Used for TTL expiry.
#[serde(default = "Utc::now")]
pub created_at: DateTime<Utc>,
}

4. turn

/// A single turn (request/response pair) in a thread.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Turn {
/// Turn number (0-indexed).
pub turn_number: usize,
/// Persisted user message ID when this turn has been written to the DB.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub user_message_id: Option<Uuid>,
/// User input that started this turn.
pub user_input: String,
/// Agent response (if completed).
pub response: Option<String>,
/// Tool calls made during this turn.
pub tool_calls: Vec<TurnToolCall>,
/// Turn state.
pub state: TurnState,Thread.state（不是 turn）是另一组状态：
Idle | Processing | AwaitingApproval | Completed | Interrupted——管的是整个 thread 当前能不能接受新 turn。
/// When the turn started.
pub started_at: DateTime<Utc>,
/// When the turn completed.
pub completed_at: Option<DateTime<Utc>>,
/// Error message (if failed).
pub error: Option<String>,
/// Agent's reasoning narrative for this turn.
/// Cleaned via `clean_response` and sanitized through `SafetyLayer` before storage.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub narrative: Option<String>,
/// Transient image content parts for multimodal LLM input.
/// Not serialized — images are only needed for the current LLM call.
/// The text description in `user_input` persists for compaction/context.
#[serde(skip)]
pub image_content_parts: Vec<ironclaw_llm::ContentPart>,
}

一个 Turn = 一轮完整的人机交互，从用户输入到 agent 给出最终回复。

turns[0] = Turn {
user_input: "帮我查东京天气",
response: Some("东京明天 25°C，晴..."),
tool_calls: vec![{tool: weather_lookup, args: {city: "tokyo"}, result: Ok(...)}],
state: TurnState::Complete,
}

turns[1] = Turn {
user_input: "还有大阪",
response: Some("大阪明天 22°C..."),
tool_calls: vec![{tool: weather_lookup, args: {city: "osaka"}, result: Ok(...)}],
state: TurnState::Complete,
}

/// State of a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TurnState {
/// Turn is being processed.
Processing,
/// Turn completed successfully.
Completed,
/// Turn failed with an error.
Failed,
/// Turn was interrupted.
Interrupted,
}

和 ContextWindow 的关系

LLM 每次调用要看完整历史——直接把 turns 整个序列化塞进去。但 turn 数多了会超出 context window，所以：

[ turns[0..10] ]   ← 早期对话，被 compaction 压缩成 summary 写入 workspace
[ turns[11..50] ]  ← 中期
[ turns[51..N] ]   ← 最近 10 个 turn，保留原文

compaction.rs 的三种策略（MoveToWorkspace / Summarize / Truncate）都在操作这个 turns Vec——把旧的 turn 摘掉、生成 summary。

turns 是 LLM 上下文的直接来源——没有 turns 就没法继续对话（agent 失忆）。

  ---
和 UndoManager 的关系

UndoManager 在 SessionManager 层、不在 Thread 里。它存的 checkpoints 是 turns Vec 的快照（按 CLAUDE.md："Checkpoints store message lists (max 20 by default)"）：

UndoManager {
checkpoints: VecDeque<Vec<Turn>>,  // 历史快照栈
}

/undo → 弹一个 checkpoint → 把 Thread.turns 整个替换成那个 checkpoint 的 turns。

turns 是 undo 的最小单位——不是按 turn 撤销，是按"这一批 turn 一起回滚"。

  ---
一句话总结

▎ Thread.turns 是这个对话线程的完整历史，按时间顺序存所有用户输入、agent 回复、调用的工具、turn 状态——既是 LLM 上下文的来源，也是 undo 的最小单位，也是 compaction 处理的目标。

