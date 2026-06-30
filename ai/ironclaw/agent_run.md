一. agent_loop.rs

run loop 层面：是串行 await handle_message 后再读下一条。

pending_messages 层面：防止同一个 thread 在其它入口/并发任务/非空闲状态下收到普通输入时丢消息或并发开 turn。

你可以把它理解成 thread 级别的“二道防线”。外层 loop 提供了一层串行消费，但不能假设所有未来/现有入口都严格只靠这一层。
****************
我看了代码，结论是：**在当前主路径下，普通用户消息自然触发 `pending_messages` 队列的可能性很低，基本可以认为这套队列机制现在不是靠正常串行 `Agent::run` 触发的。**

关键证据：

1. `Agent::run` 是串行消费 merged stream

在 [src/agent/agent_loop.rs](E:/codes/Rust/projects/ironclaw/src/agent/agent_loop.rs:1070) 的主循环里：

```rust
msg = message_stream.next() => { ... }

...

match self.handle_message(&message).await {
```

这里确实是：

```text
读一条 message
→ await handle_message 完成
→ 再读下一条 message
```

所以只要消息都通过这个 `message_stream/msg_tx` 进来，同一个 agent loop 不会并发处理两条消息。

2. Web/chat/ws/responses_api 也基本都是发进 `msg_tx`

例如 chat handler 在 [src/channels/web/features/chat/mod.rs](E:/codes/Rust/projects/ironclaw/src/channels/web/features/chat/mod.rs:137)：

```rust
tx.send(msg).await
```

Responses API 也是先 `send_to_agent`，本质也是送到 agent loop。也就是说它们不会直接并发调用 `handle_message`。

3. `pending_messages` 的唯一正常入队点是 thread 已经 `Processing`

在 [src/agent/thread_ops.rs](E:/codes/Rust/projects/ironclaw/src/agent/thread_ops.rs:579)：

```rust
match thread_state {
    ThreadState::Processing => {
        ...
        if thread.state == ThreadState::Processing {
            ...
            if !thread.queue_message(content.to_string()) {
                ...
            }
            return Ok(SubmissionResult::Ok {
                message: Some("Message queued — will be processed after the current turn.".into()),
            });
        }
    }
}
```

而 `Processing` 是在真正开始处理 turn 时设置的：

[src/agent/session.rs](E:/codes/Rust/projects/ironclaw/src/agent/session.rs:440)

```rust
self.state = ThreadState::Processing;
```

正常处理完会 `conclude_turn(...)` 变回 `Idle`，或者 `await_approval(...)` 变成 `AwaitingApproval`。所以在单一 `run` loop 串行处理下，下一条普通消息进来时，通常不会看到 `Processing`。

4. 测试里也不是自然并发触发，而是手动塞队列

在 [tests/e2e_advanced_traces.rs](E:/codes/Rust/projects/ironclaw/tests/e2e_advanced_traces.rs:791) 这个测试里，注释写得很直白：

```rust
// Pre-populate the thread's pending_messages queue.
// This simulates what happens when a concurrent request ...
```

然后测试直接：

```rust
thread.queue_message("What is 2+2?".to_string());
```

也就是说测试覆盖的是 drain loop 能处理队列，但没有真正通过并发入口自然触发 `ThreadState::Processing => queue_message`。

还有一个很强的旁证：`bridge/router.rs` 里专门说过，如果 engine 在 approval gate 里挂住，agent loop 会卡在 `handle_message(...).await`，所以通过 `msg_tx` 发来的 approval 会排在后面，无法被处理。这段注释在 [src/bridge/router.rs](E:/codes/Rust/projects/ironclaw/src/bridge/router.rs:2945)：

```text
agent loop is blocked at match self.handle_message(...).await and cannot drain new submissions from msg_tx
```

这正好说明当前架构确实是串行卡住的，不是并发处理同一个 thread。

所以判断是：

```text
当前普通生产路径：
  message_stream/msg_tx → Agent::run → handle_message await
  基本不会自然触发 pending_messages 入队

pending_messages 目前更像是：
  1. 早期/预期并发入口留下的防御机制
  2. 给未来多入口并发处理准备的 thread 级保护
  3. 测试里可手动预填，验证 drain loop 行为
  4. 极端情况下如果某个 thread 被异常留在 Processing，下一条消息会入队
```

如果你是在评估这块代码是否“实际有用”，我的结论是：**drain loop 本身能工作，但当前主架构下，自动入队路径大概率是不可达或接近不可达的。**这块注释说“queued during processing”在设计意图上成立，但和现在 `Agent::run` 的串行消费模型有张力。
*********************************
respond 是“回复用户的正式答案”，send_status 是“告诉前端/通道当前处理状态或特殊事件”。它们可能最终都显示给用户，但不是同一种消息，也不是同一条发送链路。

1. 整体流程

   /// Run the agent main loop.
   pub async fn run(self) -> Result<(), Error> {

        // Start channels
        let mut message_stream = self.channels.start_all().await?;开启所有channel，得到接受流

   Agent 启动时 spawn 出来的一个后台定时清理任务，负责周期性地把"长时间无活动的 session"从内存中清理掉，避免 SessionManager 里的几张 in-memory map 无限膨胀。
   // Spawn session pruning task
   let session_mgr = self.session_manager.clone();
   let session_idle_timeout = self.config.session_idle_timeout;
   let pruning_handle = tokio::spawn(async move {
   let mut interval = tokio::time::interval(std::time::Duration::from_secs(600)); // Every 10 min
   interval.tick().await; // Skip immediate first tick
   loop {
   interval.tick().await;
   session_mgr.prune_stale_sessions(session_idle_timeout).await;
   }
   });
   每 10 分钟一次：
    1. 找 last_active_at < (now - session_idle_timeout) 且 try_lock 成功的 sessions
    2. 收集它们的 thread_ids
    3. 对每个 stale session spawn 一个 OnSessionEnd hook（不等）
    4. 从 sessions / thread_map / undo_managers 三个 map 里清掉相关条目
    5. 打 info 日志，返回清理数量

Agent 启动时 spawn 的一个 5 分钟一次的后台 worker，负责把本地排队中的脱敏对话 trace 推送到 Trace Commons 远端，并把"贡献被引用"的致谢通知通过用户可用的 channel 投递出去。
脱敏对话 trace = agent 跑完一轮对话后，把这次对话的 turns 打包成 envelope，过一遍 deterministic redactor（移除/替换 PII、密钥、邮箱、IP、内部路径等敏感字段），再排进本地队列、最终推到 Trace Commons  
远端的那份"对话记录"。

它是 Trace Commons 共享语料库的基本单位——别人可以下载、研究、微调，但永远看不到原始用户名、token、个人信息。
let trace_queue_worker_handle = spawn_trace_queue_flush_worker(
self.owner_id().to_string(),
self.deps.store.clone(),
self.channels.clone(),
);

        // Spawn heartbeat if enabled
        let heartbeat_handle
Agent 启动时 spawn 的心跳后台任务，周期性读取 HEARTBEAT.md 让 agent 主动检查用户/工作区状态并把发现通过 channel 通知用户——也就是"在用户没说话时也保持主动"的定时器。默认 30 分钟一次，频率由 hb_config
控制。

Agent 启动时 spawn 的定时/事件触发引擎，按 Routine 配置里的 cron、事件或系统信号去自动执行用户预设的轻量或完整任务——比如"每天早上 9 点整理邮件"、"收到某类事件时跑一次全 agent 流程"。
// Spawn routine engine if enabled
let routine_handle

这块是在 `Agent::run()` 启动时初始化 **Routine 自动化引擎**。可以理解为：如果配置里启用了 routines，就把“定时任务 / 事件触发任务 / 系统事件任务”的后台运行能力挂到当前 agent 上。

入口在 [src/agent/agent_loop.rs](E:/codes/Rust/projects/ironclaw/src/agent/agent_loop.rs:1341)：

```rust
// Spawn routine engine if enabled
let routine_handle = if let Some(ref rt_config) = self.routine_config {
```

它大致做这几件事：

1. **判断是否启用 routine**
    - `self.routine_config` 存在；
    - `rt_config.enabled == true`；
    - 同时必须有 `store` 和 `workspace`。
    - 没有 DB 或 workspace 时会打 warn，然后不启动。

2. **创建 `RoutineEngine`**
   这里把 routine 运行需要的依赖都塞进去：
    - DB scope：读写 routine、run 状态；
    - LLM：routine 执行时可能要调用模型；
    - workspace：访问记忆/上下文；
    - scheduler：派发后台 job；
    - extension manager、tools、safety、sandbox、HTTP interceptor 等运行时依赖。

3. **应用 runtime policy**
   ```rust
   engine.set_runtime_policy(policy.clone());
   ```
   这保证 routine 里模型能看到的工具列表也遵守当前 runtime policy，不会和普通 agent turn 的工具暴露规则不一致。

4. **注册 routine 相关内置工具**
   ```rust
   self.deps.tools.register_routine_tools(...)
   ```
   这样模型或系统路径可以使用 routine 工具，比如创建/更新/触发 routine 之类。

5. **刷新事件触发缓存**
   ```rust
   engine.refresh_event_cache().await;
   ```
   event/system_event 类型的 routine 需要快速匹配，不可能每条消息都从 DB 全量扫，所以这里先加载一份内存缓存。

6. **启动通知转发任务**
   这段 `tokio::spawn(async move { ... })` 监听 `notify_rx`。

   routine 执行完成、失败或需要通知用户时，会通过 `notify_tx` 发出 `OutgoingResponse`。这个转发任务负责：
    - 从 metadata 里读 `notify_channel`、`notify_user`、`owner_id`；
    - 解析真正要通知的用户；
    - 优先发到指定 channel；
    - 如果指定 channel 失败且允许 fallback，就广播到所有 channel。

7. **启动 cron ticker**
   ```rust
   let cron_handle = spawn_cron_ticker(Arc::clone(&engine), cron_interval);
   ```
   对应 [src/agent/routine_engine.rs](E:/codes/Rust/projects/ironclaw/src/agent/routine_engine.rs:2185)。

   这个后台任务会：
    - 启动时恢复 crash 前遗留的 dispatched runs；
    - 启动时立即检查一次 cron routine；
    - 之后按 `cron_check_interval_secs` 周期检查 cron trigger；
    - 每 60 秒刷新一次 event cache。

8. **把 engine 暴露给主循环和 web gateway**
   ```rust
   *self.routine_engine_slot.write().await = Some(Arc::clone(&engine));
   ```

   这个 slot 很关键。后面普通用户消息处理时会读它：

   [src/agent/agent_loop.rs](E:/codes/Rust/projects/ironclaw/src/agent/agent_loop.rs:2113)

   ```rust
   if !message.is_internal
       && let Submission::UserInput { ref content } = submission
       && let Some(engine) = self.routine_engine().await
   {
       let fired = engine.check_event_triggers(...).await;
       ...
   }
   ```

   也就是说：用户发一条普通消息后，agent 会先检查有没有匹配的 `message_event` routine。如果有匹配的 routine，这条消息可能会被 routine 消费掉，不再走普通聊天回复。

9. **保存 `routine_handle` 用于 shutdown**
   退出 agent 时：

   [src/agent/agent_loop.rs](E:/codes/Rust/projects/ironclaw/src/agent/agent_loop.rs:1637)

   ```rust
   if let Some((cron_handle, _)) = routine_handle {
       cron_handle.abort();
   }
   ```

   也就是 agent 退出时停止 cron 后台 ticker。

一句话总结：这块代码是在 agent 启动阶段挂载 routine 自动化系统，让它能响应 cron 定时器、用户消息事件、系统事件，并把 routine 的通知正确发回用户所在 channel。

        loop {
            let message = tokio::select! {//多路监听，看哪个先到
                biased;
                _ = tokio::signal::ctrl_c() => {
                    tracing::debug!("Ctrl+C received, shutting down...");
                    break;
                }
                msg = message_stream.next() => {
                    match msg {
                        Some(m) => m,
                        None => {
                            tracing::debug!("All channel streams ended, shutting down...");
                            break;
                        }
                    }
                }
            };

            // Apply transcription middleware to audio attachments
            let mut message = message;
            if let Some(ref transcription) = self.deps.transcription {//语音转文字
                transcription
                    .process(&mut message.attachments, &mut message.content)
                    .await;
            }

            // Apply document extraction middleware to document attachments
            if let Some(ref doc_extraction) = self.deps.document_extraction {//提取文档
                doc_extraction.process(&mut message).await;
            }

            // Store successfully extracted document text in workspace for indexing
            self.store_extracted_documents(&message).await;//存储文档
*****************
            match self.handle_message(&message).await {//处理消息核心逻辑
                Ok(HandleOutcome::Respond(response)) => {
                    // Hook: BeforeOutbound — allow hooks to modify or suppress outbound
                    let event = crate::hooks::HookEvent::Outbound {
                        user_id: message.user_id.clone(),
                        channel: message.channel.clone(),
                        content: response.content.clone(),
                        thread_id: message.thread_id.as_ref().map(|t| t.as_str().to_string()),
                    };
                    match self.hooks().run(&event).await {//跑一些钩子，然后返回
                        Err(err) => {
                            tracing::warn!("BeforeOutbound hook blocked response: {}", err);
                            crate::generated_images::remove_staged_generated_image_attachments(
                                &response.attachments,
                            );
                            // Still send Done so the client knows the turn is complete
                            // even though the response was suppressed by the hook.
                            self.send_done(&message).await;//注意
                        }
                        Ok(crate::hooks::HookOutcome::Continue {
                            modified: Some(new_content),
                        }) => {
                            let mut response = response;
                            response.content = new_content;
                            if let Err(e) = self.respond_then_done(&message, response).await {
                                tracing::error!(
                                    channel = %message.channel,
                                    error = %e,
                                    "Failed to send response to channel"
                                );
                            }
                        }
                        _ => {
                            if let Err(e) = self.respond_then_done(&message, response).await {
                                tracing::error!(
                                    channel = %message.channel,
                                    error = %e,
                                    "Failed to send response to channel"
                                );
                            }
                        }
                    }
                }
                Ok(HandleOutcome::NoResponse) => {
                    tracing::debug!(
                        channel = %message.channel,
                        user = %message.user_id,
                        "Suppressed empty response (not sent to channel)"
                    );
                    self.send_done(&message).await;
                }
                Ok(HandleOutcome::Pending) => {//等待审批的都会落在这
                    tracing::debug!(
                        channel = %message.channel,
                        user = %message.user_id,
                        "Turn paused (Pending); suppressing Done"
                    );
                }
                Ok(HandleOutcome::Shutdown) => {
                    tracing::debug!("Shutdown command received, exiting...");
                    break;
                }
                Err(e) => {
                    tracing::error!("Error handling message: {}", e);
                    if let Err(send_err) = self
                        .respond_then_done(
                            &message,
                            OutgoingResponse::text(format!("Error: {}", e)),
                        )
                        .await
                    {
                        tracing::error!(
                            channel = %message.channel,
                            error = %send_err,
                            "Failed to send error response to channel"
                        );
                    }
                }
            }

        // Cleanup
        tracing::debug!("Agent shutting down...");
        repair_handle.abort();
        pruning_handle.abort();
        trace_queue_worker_handle.abort();
        if let Some(handle) = heartbeat_handle {
            handle.abort();
        }
        if let Some((cron_handle, _)) = routine_handle {
            cron_handle.abort();
        }
        self.scheduler.stop_all().await;
        self.channels.shutdown_all().await?;

        Ok(())最终清理

2. Submission

Submission 是 agent loop 的“输入事件类型系统”，负责把各种来源的消息统一成一组明确的内部动作
pub enum Submission {
/// User text input (starts a new turn).
UserInput {
/// The user's message content.
content: String,
},

    /// Response to an execution approval request (with explicit request ID).
    ExecApproval {
        /// ID of the approval request being responded to.
        request_id: Uuid,
        /// Whether the execution was approved.
        approved: bool,
        /// If true, auto-approve this tool for the rest of the session.
        always: bool,
    },

    /// External system resolved a pending gate (for example an OAuth
    /// callback or a caller-executed tool result).
    ExternalCallback {
        /// ID of the pending gate request being resolved.
        request_id: Uuid,
        /// Optional payload supplied alongside the resolution. OAuth-style
        /// callbacks (the original use case) pass `None`; caller-executed
        /// external tool calls in the Responses API pass a JSON object
        /// describing the tool outputs.
        ///
        /// Defaults to `None` on the wire so existing OAuth callers don't
        /// need to be updated.
        #[serde(default)]
        payload: Option<serde_json::Value>,
    },

    /// Resolve an authentication gate with an exact request id.
    GateAuthResolution {
        /// ID of the pending authentication gate being resolved.
        request_id: Uuid,
        /// Credential submission or cancellation for the gate.
        resolution: AuthGateResolution,
    },

    /// Simple approval response (yes/no/always) for the current pending approval.
    ApprovalResponse {
        /// Whether the execution was approved.
        approved: bool,
        /// If true, auto-approve this tool for the rest of the session.
        always: bool,
    },

    /// Interrupt the current turn.
    Interrupt,

    /// Request context compaction.
    Compact,

    /// Undo the last turn.
    Undo,

    /// Redo a previously undone turn (if available).
    Redo,

    /// Resume from a specific checkpoint.
    Resume {
        /// ID of the checkpoint to resume from.
        checkpoint_id: Uuid,
    },

    /// Clear the current thread and start fresh.
    Clear,

    /// Switch to a different thread.
    SwitchThread {
        /// ID of the thread to switch to.
        thread_id: Uuid,
    },

    /// Create a new thread.
    NewThread,

    /// List threads for the interactive resume picker.
    ListThreads,

    /// Trigger a manual heartbeat check.
    Heartbeat,

    /// Summarize the current thread.
    Summarize,

    /// Suggest next steps based on the current thread.
    Suggest,

    /// User-provided expected behavior for the last interaction.
    /// Fires into the self-improvement pipeline with conversation context.
    Expected {
        /// What the user expected to happen.
        description: String,
    },

    /// Check job status. No job_id shows all jobs; with job_id shows a specific job.
    JobStatus {
        /// Optional job ID (UUID or short prefix). If None, shows all jobs.
        job_id: Option<String>,
    },

    /// Cancel a running job.
    JobCancel {
        /// Job ID (UUID or short prefix).
        job_id: String,
    },

    /// Quit the agent. Bypasses thread-state checks.
    Quit,

    /// System command (help, model, version, tools, ping, debug).
    /// Bypasses thread-state checks and safety validation.
    SystemCommand {
        /// The command name (e.g. "help", "model", "version").
        command: String,
        /// Arguments to the command.
        args: Vec<String>,
    },

    /// Plan mode command (/plan).
    /// All subcommands are rewritten to UserInput with [PLAN MODE] prefix
    /// to activate the plan-mode skill.
    Plan {
        /// The plan subcommand.
        sub: PlanSubcommand,
    },

    /// Claim a pairing code from any chat surface (e.g. `approve telegram CODE`).
    ///
    /// The user-facing pairing flow tells users to type exactly this in any
    /// IronClaw chat. The handler delegates to the same pairing store and
    /// extension-manager hooks that `POST /api/pairing/{channel}/approve`
    /// uses, so the TUI/CLI/web/telegram surfaces all behave consistently.
    PairingClaim {
        /// Channel name (e.g. `telegram`, `slack-relay`).
        channel: String,
        /// Pairing code, lowercased by `SubmissionParser::parse`. The
        /// pairing store is case-insensitive, so the wire-format
        /// uppercase shape still matches.
        code: String,
    },
}

3. Message

/// A message received from an external channel.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
/// Unique message ID.
pub id: Uuid,
/// Channel this message came from.
pub channel: String,
/// Resolved owner identity for this message.
///
/// For owner-capable channels this is the stable instance owner ID when the
/// configured owner is speaking; for pairing-aware channels (e.g. WASM) this
/// is the result of `pairing_resolve_identity`; for non-pairing channels
/// (HTTP, web, REPL) it comes directly from the authenticated token.
pub user_id: String,
/// Channel-specific sender/actor identifier.
pub sender_id: String,
/// Optional display name.
pub user_name: Option<String>,
/// Message content.
pub content: String,
/// Structured submission sideband for internal callers that need exact
/// routing semantics without serializing control payloads into `content`.
pub structured_submission: Option<crate::agent::submission::Submission>,
/// Thread/conversation ID for threaded conversations.
///
/// This is the *external* channel-supplied thread identifier (e.g. a
/// Telegram chat id, Slack `thread_ts`, or web-UI UUID string) — **not**
/// the internal engine [`ironclaw_engine::ThreadId`] UUID. Conversion to
/// the internal id happens in `SessionManager::resolve_thread`.
pub thread_id: Option<ExternalThreadId>,
/// Stable channel/chat/thread scope for this conversation.
pub conversation_scope_id: Option<String>,
/// When the message was received.
pub received_at: DateTime<Utc>,
/// Channel-specific metadata.
pub metadata: serde_json::Value,
/// IANA timezone string from the client (e.g. "America/New_York").
pub timezone: Option<String>,
/// File or media attachments on this message.
pub attachments: Vec<IncomingAttachment>,
/// Internal-only flag: message was generated inside the process (e.g. job
/// monitor) and must bypass the normal user-input pipeline. This field is
/// not settable via metadata, so external channels cannot spoof it.
pub(crate) is_internal: bool,
/// `true` when this message represents an *agent broadcast* echoing
/// back through the channel — e.g. an outbound bot message that the
/// channel adapter (Slack, Discord, etc.) re-delivers as an inbound
/// event. Mission `OnEvent` firing skips messages with this flag set
/// to avoid self-recursion: a mission whose pattern matches its own
/// output would otherwise re-trigger forever.
///
/// Channel adapters that have echo behavior MUST set this when
/// re-emitting the agent's own outbound text. Adapters without echo
/// behavior (CLI, REPL, web gateway) leave it `false`.
pub is_agent_broadcast: bool,
/// When set, this message was produced as a side effect of a mission
/// firing — typically the mission's notification text re-entering
/// through a channel adapter. Mission `OnEvent` firing skips messages
/// tagged with a `triggering_mission_id` to bound chain-recursion
/// across distinct missions (mission A → notification → mission B →
/// notification → mission C → ...). The string is the originating
/// `MissionId` for diagnostics.
pub triggering_mission_id: Option<String>,
}


4. 核心self.handle_message(&message).await

        let target = message
            .routing_target()
            .unwrap_or_else(|| message.user_id.clone());//发送目标uid||grouopId
        self.tools()
            .set_message_tool_context(Some(message.channel.clone()), Some(target))//发送消息到channeld的工具，设置默认对象
            .await;

        // Hook: BeforeInbound — allow hooks to modify or reject user input
        if let Submission::UserInput { ref content } = submission {
            let event = crate::hooks::HookEvent::Inbound {
                user_id: message.user_id.clone(),
                channel: message.channel.clone(),
                content: content.clone(),
                thread_id: message.thread_id.as_ref().map(|t| t.as_str().to_string()),
            };
            match self.hooks().run(&event).await {
                Err(crate::hooks::HookError::Rejected { reason }) => {
                    return Ok(HandleOutcome::Respond(OutgoingResponse::text(format!(
                        "[Message rejected: {}]",
                        reason
                    ))));
                }
                Err(err) => {
                    return Ok(HandleOutcome::Respond(OutgoingResponse::text(format!(
                        "[Message blocked by hook policy: {}]",
                        err
                    ))));
                }
                Ok(crate::hooks::HookOutcome::Continue {
                    modified: Some(new_content),
                }) => {
                    submission = Submission::UserInput {
                        content: new_content,
                    };
                }
                _ => {} // Continue, fail-open errors already logged in registry
            }
        }

这个函数是为了在处理前端指定的 thread 前，先把对应 thread 从 DB 恢复到内存，确保后续 agent 使用前端指定的同一个 thread UUID，而不是误创建新 thread，导致消息落到错误会话里。



恢复历史 thread 时，必须用 DB 里记录的原始 source_channel 做审批权限判断。如果查不到原始 channel，就默认不允许审批，避免旧数据或查询失败导致越权审批。

    pub(super) async fn maybe_hydrate_thread(
        &self,
        message: &IncomingMessage,
        external_thread_id: &str,
    ) -> Option<String> {
        // Only hydrate UUID-shaped thread IDs (web gateway uses UUIDs)
        let thread_uuid = match Uuid::parse_str(external_thread_id) {
            Ok(id) => id,
            Err(_) => return None,
        };

        // Check if already in memory
        let session = self
            .session_manager
            .get_or_create_session(&message.user_id)
            .await;
        {
            let sess = session.lock().await;
            if sess.threads.contains_key(&thread_uuid) {
                return None;
            }
        }

        // Load history from DB (may be empty for a newly created thread).
        let mut chat_messages: Vec<ChatMessage> = Vec::new();
        let msg_count;

        if let Some(store) = self.store() {
            // Never hydrate history from a conversation UUID that isn't owned
            // by the current authenticated user.
            let owned = match store
                .conversation_belongs_to_user(thread_uuid, &message.user_id)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        "Failed to verify conversation ownership for hydration {}: {}",
                        thread_uuid,
                        e
                    );
                    if requires_preexisting_uuid_thread(&message.channel) {
                        return Some(FORGED_THREAD_ID_ERROR.to_string());
                    }
                    return None;
                }
            };


            let db_messages = store
                .list_conversation_messages(thread_uuid)
                .await
                .unwrap_or_default();
            msg_count = db_messages.len();
            chat_messages = rebuild_chat_messages_from_db(&db_messages);
        } else {
            msg_count = 0;
        }

        // Create thread with the historical ID and restore messages.
        // Read source_channel from DB so the authorization check uses the
        // original creator's channel, not the requesting message's channel.
        //
        // Fail-closed policy: if the DB lookup fails or the conversation has
        // no stored source_channel (legacy row), the thread is hydrated with
        // source_channel = None.  `is_approval_authorized(None, _)` returns
        // false, so approvals are denied until the conversation is backfilled
        // with a source_channel via an explicit migration or re-creation.
        let db_source_channel = if let Some(store) = self.store() {//最初threadId的channel
            match store.get_conversation_source_channel(thread_uuid).await {one

        let effective_source_channel = db_source_channel.as_deref();

        let session_id = {
            let sess = session.lock().await;
            sess.id
        };

        let mut thread = crate::agent::session::Thread::with_id(
            thread_uuid,
            session_id,
            effective_source_channel,
        );
        if !chat_messages.is_empty() {
            thread.restore_from_messages(chat_messages);
        }

        // Insert into session and register with session manager
        {
            let mut sess = session.lock().await;
            sess.threads.insert(thread_uuid, thread);
            sess.active_thread = Some(thread_uuid);
            sess.last_active_at = chrono::Utc::now();
        }

        self.session_manager
            .register_thread(
                &message.user_id,
                &message.channel,
                thread_uuid,
                Arc::clone(&session),
            )
            .await;


        None
    }


这段是在**为审批/回调类 submission 提前解析目标 thread UUID**，让后面的 session/thread 解析逻辑可以把这类消息路由到一个已经存在的指定线程上。

代码：

```rust
let approval_thread_uuid = if matches!(
    submission,
    Submission::ExecApproval { .. }
        | Submission::ApprovalResponse { .. }
        | Submission::ExternalCallback { .. }
        | Submission::GateAuthResolution { .. }
) {
    message
        .conversation_scope()
        .and_then(|thread_id| Uuid::parse_str(thread_id).ok())
} else {
    None
};
```

逐步解释：

1. 先判断当前 `submission` 是否属于审批/回调类：

```rust
Submission::ExecApproval { .. }
Submission::ApprovalResponse { .. }
Submission::ExternalCallback { .. }
Submission::GateAuthResolution { .. }
```

这些不是普通用户聊天，而是对某个已经挂起的执行、审批、认证 gate、外部 callback 的响应。

2. 如果是这些类型，就从消息里取 conversation scope：

```rust
message.conversation_scope()
```

这个通常来自前端或 channel 携带的 thread/conversation id。

3. 尝试把它解析成 UUID：

```rust
Uuid::parse_str(thread_id).ok()
```

如果成功，就得到：

```rust
Some(uuid)
```

否则是：

```rust
None
```

4. 如果不是审批/回调类 submission，直接是：

```rust
None
```

为什么只给审批/回调类开这个口？

注释里说得很关键：

```rust
Approval submissions are allowed to target an already-loaded owned thread
by UUID across channels
```

也就是：**审批消息允许跨 channel 指向一个已经加载、且归当前 owner 拥有的 thread UUID。**

场景例子：

```text
1. 某个 HTTP / gateway / owner-scoped channel 触发了一项工作
2. agent 在 thread A 里请求工具审批
3. Web UI 显示一个 approval card
4. 用户在 Web UI 点击 approve
5. Web UI 发来的审批请求 channel 是 gateway
6. 原始工作可能来自 HTTP 或别的 owner-scoped channel
```

这时不能简单按“当前消息 channel”去找 thread。Web UI 必须能说：

```text
我要审批 thread A 里那个挂起请求
```

所以这里从 `conversation_scope` 解析出 thread UUID，后续传给 thread/session 解析逻辑。

但为什么普通 `UserInput` 不这么做？

因为普通聊天如果允许任意跨 channel 指定 UUID，风险更大：

```text
Telegram 用户带一个 thread UUID
→ 试图把普通消息塞进 gateway/web 的会话
```

所以这里限定为审批/回调类 submission，并且注释里强调：

```text
already-loaded owned thread
```

不是随便加载任意 thread。它应该只允许目标是当前 owner 已经加载、归属合法的 thread。

一句话总结：

> 这段代码给审批、认证、外部回调这类“响应已有挂起请求”的消息，提取一个可选的目标 thread UUID，使 Web approval UI 能跨 channel 审批其它 channel 发起的工作，同时避免普通聊天滥用 thread UUID 路由。
****************

approval_thread_uuid 本身是内部 thread UUID；
但它来自外部请求携带的 conversation_scope。
// Resolve session and thread. Approval submissions are allowed to
// target an already-loaded owned thread by UUID across channels so the
// web approval UI can approve work that originated from HTTP/other
// owner-scoped channels.
let approval_thread_uuid = if matches!(
submission,
Submission::ExecApproval { .. }
| Submission::ApprovalResponse { .. }
| Submission::ExternalCallback { .. }
| Submission::GateAuthResolution { .. }
) {
message
.conversation_scope()
.and_then(|thread_id| Uuid::parse_str(thread_id).ok())
} else {
None
};

//最终得到内部threadId       
let (session, thread_id) = if let Some(target_thread_id) = approval_thread_uuid {//有内部审批thread_id
let session = self
.session_manager
.get_or_create_session(&message.user_id)
.await;
let mut sess = session.lock().await;
if let Some(thread) = sess.threads.get(&target_thread_id) {
// Block ExecApproval (JSON from the approval UI) when there
// is no pending approval — prevents hijacking a thread by UUID.
// ApprovalResponse (bare keywords) is allowed through so the
// should_route_as_approval guard can downgrade it to UserInput.
if thread.pending_approval.is_none()//是否支持审批
&& matches!(submission, Submission::ExecApproval { .. })
{
drop(sess);
return Ok(HandleOutcome::Respond(OutgoingResponse::text(
"Error: no pending approval on this thread",
)));
}
// ApprovalResponse (bare "yes"/"no"/"always") without a
// pending approval: fall through to normal handling so the
// should_route_as_approval guard can downgrade to UserInput.
// Skip the cross-channel auth check — it only matters for
// actual approvals, not messages about to become UserInput.

                if thread.pending_approval.is_some() {//是否有权限审批
                    let authorized = crate::agent::session::is_approval_authorized(
                        thread.source_channel.as_deref(),
                        &message.channel,
                    );
                    if !authorized {
                        tracing::warn!(
                            %target_thread_id,
                            source_channel = ?thread.source_channel,
                            approval_channel = %message.channel,
                            "Blocked cross-channel approval attempt"
                        );
                        drop(sess);
                        return Ok(HandleOutcome::Respond(OutgoingResponse::text(
                            "Error: approval not authorized for this channel",
                        )));
                    }
                }
                sess.active_thread = Some(target_thread_id);
                sess.last_active_at = chrono::Utc::now();
                drop(sess);
                self.session_manager
                    .register_thread(
                        &message.user_id,
                        &message.channel,
                        target_thread_id,
                        Arc::clone(&session),
                    )
                    .await;
                (session, target_thread_id)
            } else {
                drop(sess);
                self.session_manager
                    .resolve_thread_with_parsed_uuid(//找不到就创建
                        &message.user_id,
                        &message.channel,
                        message.conversation_scope(),
                        approval_thread_uuid,
                    )
                    .await
            }
        } else {//非外部审批请求
            self.session_manager
                .resolve_thread(
                    &message.user_id,
                    &message.channel,
                    message.conversation_scope(),
                )
                .await
        };

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
*******************************       

// Auth mode interception: if the thread is awaiting a token, route
// the message directly to the credential store. Nothing touches
// logs, turns, history, or compaction.
let pending_auth = {
let sess = session.lock().await;
sess.threads
.get(&thread_id)
.and_then(|t| t.pending_auth.clone())
};

        if let Some(pending) = pending_auth {
            if pending.is_expired() {
                // TTL exceeded — clear stale auth mode
                tracing::warn!(
                    extension = %pending.extension_name,
                    "Auth mode expired after TTL, clearing"
                );
                {
                    let mut sess = session.lock().await;
                    if let Some(thread) = sess.threads.get_mut(&thread_id) {
                        thread.pending_auth = None;
                    }
                }
                // If this was a user message (possibly a pasted token), return an
                // explicit error instead of forwarding it to the LLM/history.
                if matches!(submission, Submission::UserInput { .. }) {
                    return Ok(HandleOutcome::Respond(OutgoingResponse::text(format!(
                        "Authentication for **{}** expired. Please try again.",
                        pending.extension_name
                    ))));
                }
                // Control submissions (interrupt, undo, etc.) fall through to normal handling
            } else {
                match &submission {
                    Submission::UserInput { content } => {
                        return self
                            .process_auth_token(message, &pending, content, session, thread_id)
                            .await
                            .map(HandleOutcome::from_legacy);
                    }
                    _ => {
                        // Any control submission (interrupt, undo, etc.) cancels auth mode
                        let mut sess = session.lock().await;
                        if let Some(thread) = sess.threads.get_mut(&thread_id) {
                            thread.pending_auth = None;
                        }
                        // Fall through to normal handling
                    }
                }
            }
        }
*****************
这段是 **auth mode 拦截逻辑**：当某个 thread 正在等用户输入认证 token 时，下一条普通用户消息不会进入 LLM，也不会写入聊天历史，而是直接当作 credential/token 处理。

也就是处理这种流程：

```text
agent 需要访问某个 extension/tool
→ 发现缺少 token
→ thread.pending_auth = Some(...)
→ 提示用户输入 token
→ 用户下一条消息发来 token
→ 这里拦截并保存 token
```

逐段看。

先从当前 thread 里取 pending auth 状态：

```rust
let pending_auth = {
    let sess = session.lock().await;
    sess.threads
        .get(&thread_id)
        .and_then(|t| t.pending_auth.clone())
};
```

这里查的是：

```text
当前 thread 是否正在等待认证凭证
```

如果没有：

```rust
if let Some(pending) = pending_auth {
```

里面就不会执行，直接走后续正常消息处理。

如果有 pending auth，先看是否过期：

```rust
if pending.is_expired() {
```

过期了就清掉：

```rust
thread.pending_auth = None;
```

然后如果当前这条消息是普通用户输入：

```rust
if matches!(submission, Submission::UserInput { .. }) {
```

就直接回复：

```text
Authentication for **xxx** expired. Please try again.
```

并且 `return`，当前消息处理结束。

为什么这里不让它继续进 LLM？

因为用户这条消息很可能是刚粘贴的 token。即使 auth 已经过期，也不能把 token 当普通聊天内容写入历史或发给模型。

注释里的这句：

```rust
// Nothing touches logs, turns, history, or compaction.
```

就是这个安全点：token 不能进入日志、turn、history、上下文压缩。

如果 auth 没过期：

```rust
} else {
    match &submission {
        Submission::UserInput { content } => {
            return self
                .process_auth_token(message, &pending, content, session, thread_id)
                .await
                .map(HandleOutcome::from_legacy);
        }
        _ => { ... }
    }
}
```

如果当前 submission 是普通用户输入：

```rust
Submission::UserInput { content }
```

就把 `content` 当作 token 交给：

```rust
process_auth_token(...)
```

处理。处理完直接 `return`，不会继续走 LLM。

如果当前 submission 不是普通用户输入，比如：

```text
/interrupt
/undo
/clear
approval response
external callback
```

代码会把 auth mode 取消：

```rust
thread.pending_auth = None;
```

然后**不 return**，继续走后面的正常控制命令逻辑。

这句注释解释了意图：

```rust
// Any control submission (interrupt, undo, etc.) cancels auth mode
```

也就是说，用户不输入 token，而是发了控制命令，就认为他不想继续认证了，取消等待 token 的状态。

整体流程可以总结成：

```text
当前 thread 没有 pending_auth
→ 正常处理

当前 thread 有 pending_auth 且未过期：
  普通文本
  → 当 token 保存
  → 不进 LLM/历史
  → 当前 turn 结束

  控制命令
  → 取消 pending_auth
  → 继续按控制命令处理

当前 thread 有 pending_auth 但已过期：
  普通文本
  → 回复“认证已过期”
  → 不进 LLM/历史

  控制命令
  → 清掉 pending_auth
  → 继续按控制命令处理
```

核心目的是两个：

1. **功能上**：让用户下一条普通消息可以作为认证 token。
2. **安全上**：防止 token 进入 LLM、聊天记录、压缩摘要或日志。

        if !message.is_internal
            && let Submission::UserInput { ref content } = submission
            && let Some(engine) = self.routine_engine().await
        {
            let single_message_repl = is_single_message_repl(message);
            // Use post-hook content so that BeforeInbound hooks that rewrite
            // input are respected by event trigger matching.
            let fired = if single_message_repl {
                engine.check_event_triggers_and_wait(message, content).await
            } else {
                engine.check_event_triggers(message, content).await
            };
            if fired > 0 {//如果用户消息触发Routine，则不往后继续agent流程
                return if single_message_repl {
                    Ok(HandleOutcome::Shutdown)
                } else {
                    Ok(HandleOutcome::NoResponse)
                };
            }
        }
*****************************
    /// Check incoming message against event triggers. Returns number of routines fired.
    pub async fn check_event_triggers(&self, message: &IncomingMessage, content: &str) -> usize {
        let triggered = self.matching_event_triggers(message, content).await;
        let fired = triggered.len();
        for triggered in triggered {
            std::mem::drop(self.spawn_fire(triggered.routine, "event", Some(triggered.detail)));//启动一个routine
        }
        fired
    }
******************************
    async fn matching_event_triggers(
        &self,
        message: &IncomingMessage,
        content: &str,
    ) -> Vec<TriggeredRoutine> {
        let cache = self.event_cache.read().await;

        // Early return if there are no message matchers at all.
        if !cache
            .iter()
            .any(|m| matches!(m, EventMatcher::Message { .. }))
        {
            return Vec::new();
        }
        let mut triggered = Vec::new();

        // Collect routine IDs for batch query
        let routine_ids: Vec<Uuid> = cache
            .iter()
            .filter_map(|matcher| match matcher {
                EventMatcher::Message { routine, .. } => Some(routine.id),
                EventMatcher::System { .. } => None,
            })
            .collect();

        if routine_ids.is_empty() {
            return Vec::new();
        }

        // Single batch query instead of N queries
        let concurrent_counts = match self.batch_concurrent_counts(&routine_ids).await {
            Some(counts) => counts,
            None => return Vec::new(),
        };

        for matcher in cache.iter() {
            let (routine, re) = match matcher {
                EventMatcher::Message { routine, regex } => (routine, regex),
                EventMatcher::System { .. } => continue,
            };

            // User ownership + channel filter (extracted for testability).
            if !routine_matches_message(routine, message) {
                // User mismatch is expected for multi-user setups — keep at
                // trace to avoid one log per routine per inbound message.
                continue;
            }

            // Regex match
            if !re.is_match(content) {
                continue;
            }

            // Cooldown check
            if !self.check_cooldown(routine) {
                tracing::debug!(routine = %routine.name, "Skipped: cooldown active");
                continue;
            }

            // Concurrent run check (using batch-loaded counts)
            let running_count = concurrent_counts.get(&routine.id).copied().unwrap_or(0);
            if running_count >= routine.guardrails.max_concurrent as i64 {
                tracing::debug!(routine = %routine.name, "Skipped: max concurrent reached");
                continue;
            }

            // Global capacity check
            if self.running_count.load(Ordering::Relaxed) >= self.config.max_concurrent_routines {
                tracing::warn!(routine = %routine.name, "Skipped: global max concurrent reached");
                continue;
            }

            let detail = truncate(content, 200);
            triggered.push(TriggeredRoutine {//添加到匹配到的
                routine: routine.clone(),
                detail,
            });
        }

        triggered
    }

        // Build per-tenant execution context once; threaded through all handlers.
        let tenant = self.tenant_ctx(&message.user_id).await;
这句是在为当前消息构建一个 **tenant execution context**，也就是“当前用户/租户的执行上下文”，然后后面所有 handler 都用同一个 `tenant`。

代码：

```rust
// Build per-tenant execution context once; threaded through all handlers.
let tenant = self.tenant_ctx(&message.user_id).await;
```

含义：

```text
根据 message.user_id
→ 构建当前 tenant/user scope 的上下文
→ 后续处理命令、工具、LLM、workspace、配置等都带着这个上下文
```

这里的 `tenant` 通常用于多用户/多租户隔离。比如同一个 agent 进程里可能服务多个用户：

```text
user A
user B
user C
```

每个用户应该有自己的：

- workspace scope
- secrets / credentials
- memory
- runtime policy
- permissions
- cost / usage accounting
- possibly user-specific config

所以不能在后续 handler 里随便用全局上下文，而是要用：

```rust
tenant
```

来明确“这次执行属于哪个 user_id”。

注释里的：

```rust
Build per-tenant execution context once
```

意思是：这个上下文只构建一次，避免每个 handler 都重复查。

```rust
threaded through all handlers
```

这里的 `threaded through` 不是 OS thread 的意思，而是“贯穿传递”。也就是：

```text
tenant 被一路作为参数传给后续各个 handler
```

典型用途类似：

```rust
self.handle_system_command(..., &tenant).await
self.handle_user_input(..., &tenant).await
```

为什么用 `message.user_id`？

因为 `message.user_id` 是当前入站消息所属的 owner/user scope。tenant context 必须按这个 scope 创建，不能按 channel sender id 或 metadata 里可伪造字段创建。

一句话总结：

> 这行代码把当前消息所属用户转换成一个租户级执行上下文，后续 agent 处理这条消息时都基于这个上下文做隔离、权限、配置和资源访问。
*********************************
/// Per-request tenant execution context.
///
/// Bundles a [`TenantScope`] (scoped DB access), workspace, cost guard,
/// and per-tenant rate limiting. Constructed once per request via
/// [`AgentDeps::tenant_ctx()`](crate::agent::AgentDeps::tenant_ctx).
///
/// `Clone + Send + Sync` — safe to store on `ChatDelegate` without lifetime issues.
#[derive(Clone)]
pub struct TenantCtx {
identity: crate::ownership::UserId,
store: Option<TenantScope>,
workspace: Option<Arc<Workspace>>,
cost_guard: Arc<CostGuard>,
rate: Arc<TenantRateState>,
}

        // Process based on submission type
        let result = match submission {
            Submission::UserInput { content } => {
                let mut result = self
                    .process_user_input(
                        message,
                        tenant.clone(),
                        session.clone(),
                        thread_id,
                        &content,
                    )
                    .await;

                // Drain any messages queued during processing.
                // Messages are merged (newline-separated) so the LLM receives
                // full context from rapid consecutive inputs instead of
                // processing each as a separate turn with partial context (#259).
                //
                // Only `Response` continues the drain — the user got a normal
                // reply and there may be more queued messages to process.
                //
                // Everything else stops the loop:
                // - `NeedApproval`: thread is blocked on user approval
                // - `Interrupted`: turn was cancelled
                // - `Ok`: control-command acknowledgment (including the "queued"
                //    ack returned when a message arrives during Processing)
                // - `Error`: soft error — draining more messages after an error
                //    would produce confusing interleaved output
                // - `Err(_)`: hard error
                while let Ok(SubmissionResult::Response {
                    content: outgoing,
                    attachments,
                }) = &result
                {
                    let merged = {
                        let mut sess = session.lock().await;
                        sess.threads
                            .get_mut(&thread_id)
                            .and_then(|t| t.drain_pending_messages())
                    };
                    let Some(next_content) = merged else {
                        break;
                    };

                    tracing::debug!(
                        thread_id = %thread_id,
                        merged_len = next_content.len(),
                        "Drain loop: processing merged queued messages"
                    );

                    // Send the completed turn's response before starting the next.
                    //
                    // Known limitations:
                    // - One-shot channels (HttpChannel) consume the response
                    //   sender on the first respond() call keyed by msg.id.
                    //   Subsequent calls (including the outer handler's final
                    //   respond) are silently dropped. For one-shot channels
                    //   only this intermediate response is delivered.
                    // - All drain-loop responses are routed via the original
                    //   `message`, so channels that key routing on message
                    //   identity will attribute every response to the first
                    //   message. This is acceptable for the current
                    //   single-user-per-thread model.
                    let response = build_outgoing_response_for_thread(
                        &session,
                        thread_id,
                        outgoing.clone(),
                        attachments.clone(),
                    )
                    .await;
                    if let Err(e) = self.respond_then_done(message, response).await {
                        tracing::warn!(
                            thread_id = %thread_id,
                            "Failed to send intermediate drain-loop response: {e}"
                        );
                    }

                    // Process merged queued messages as a single turn.
                    // Use a message clone with cleared attachments so
                    // augment_with_attachments doesn't re-apply the original
                    // message's attachments to unrelated queued text.
                    let mut queued_msg = message.clone();
                    queued_msg.attachments.clear();
                    result = self
                        .process_user_input(
                            &queued_msg,
                            tenant.clone(),
                            session.clone(),
                            thread_id,
                            &next_content,
                        )
                        .await;

                    // If processing failed, re-queue the drained content so it
                    // isn't lost. It will be picked up on the next successful turn.
                    if !matches!(&result, Ok(SubmissionResult::Response { .. })) {
                        let mut sess = session.lock().await;
                        if let Some(thread) = sess.threads.get_mut(&thread_id) {
                            thread.requeue_drained(next_content);
                            tracing::debug!(
                                thread_id = %thread_id,
                                "Re-queued drained content after non-Response result"
                            );
                        }
                    }
                }

                result
            }
            Submission::SystemCommand { command, args } => {
                tracing::debug!(
                    "[agent_loop] SystemCommand: command={}, channel={}",
                    command,
                    message.channel
                );
                // /reasoning is special-cased here (not in handle_system_command)
                // because it needs the session + thread_id to read turn reasoning
                // data, which handle_system_command's signature doesn't provide.
                if command == "reasoning" {
                    let result = self
                        .handle_reasoning_command(&args, &session, thread_id)
                        .await;
                    return match result {
                        SubmissionResult::Response {
                            content,
                            attachments,
                        } => Ok(HandleOutcome::Respond(
                            build_outgoing_response_for_thread(
                                &session,
                                thread_id,
                                content,
                                attachments,
                            )
                            .await,
                        )),
                        SubmissionResult::Ok { message } => Ok(HandleOutcome::from_legacy(message)),
                        SubmissionResult::Error { message } => Ok(HandleOutcome::Respond(
                            OutgoingResponse::text(format!("Error: {}", message)),
                        )),
                        _ => {
                            if is_single_message_repl(message) {
                                Ok(HandleOutcome::Shutdown)
                            } else {
                                Ok(HandleOutcome::NoResponse)
                            }
                        }
                    };
                }
                // Authorization checks (including restart channel check) are enforced in handle_system_command
                self.handle_system_command(&command, &args, &message.channel, &tenant)
                    .await
            }
            Submission::Undo => self.process_undo(session.clone(), thread_id).await,
            Submission::Redo => self.process_redo(session.clone(), thread_id).await,
            Submission::Interrupt => self.process_interrupt(session.clone(), thread_id).await,
            Submission::Compact => self.process_compact(session.clone(), thread_id).await,
            Submission::Clear => self.process_clear(session.clone(), thread_id).await,
            Submission::NewThread => self.process_new_thread(message).await,
            Submission::Heartbeat => self.process_heartbeat(&message.user_id).await,
            Submission::Summarize => self.process_summarize(session.clone(), thread_id).await,
            Submission::Suggest => self.process_suggest(session.clone(), thread_id).await,
            Submission::Expected { description } => {
                self.process_expected(session.clone(), thread_id, &description, &message.user_id)
                    .await
            }
            Submission::JobStatus { job_id } => {
                self.process_job_status(&tenant, job_id.as_deref()).await
            }
            Submission::JobCancel { job_id } => self.process_job_cancel(&tenant, &job_id).await,
            Submission::Quit => return Ok(HandleOutcome::Shutdown),
            Submission::SwitchThread { thread_id: target } => {
                self.process_switch_thread(message, target).await
            }
            Submission::Resume { checkpoint_id } => {
                self.process_resume(session.clone(), thread_id, checkpoint_id)
                    .await
            }
            Submission::ListThreads => self.process_list_threads(session.clone(), message).await,
            Submission::ExecApproval {
                request_id,
                approved,
                always,
            } => {
                self.process_approval(
                    message,
                    session.clone(),
                    thread_id,
                    Some(request_id),
                    approved,
                    always,
                )
                .await
            }
            Submission::ExternalCallback { .. } => Ok(SubmissionResult::Error {
                message: "External callbacks require ENGINE_V2".to_string(),
            }),
            Submission::GateAuthResolution { .. } => Ok(SubmissionResult::Error {
                message: "Auth gate resolution requires ENGINE_V2".to_string(),
            }),
            Submission::ApprovalResponse { approved, always } => {
                let thread_state = {
                    let sess = session.lock().await;
                    sess.threads
                        .get(&thread_id)
                        .map(|t| t.state)
                        .unwrap_or(ThreadState::Idle)
                };
                // NOTE: TOCTOU possible — state could change between check
                // and process_approval; process_approval handles stale cases.
                if should_route_as_approval(thread_state, &message.content) {
                    self.process_approval(
                        message,
                        session.clone(),
                        thread_id,
                        None,
                        approved,
                        always,
                    )
                    .await
                } else {
                    // Run BeforeInbound hooks for the downgraded content —
                    // the hook check above only fires for UserInput submissions,
                    // and this was parsed as ApprovalResponse.
                    let content = message.content.clone();
                    let hook_event = crate::hooks::HookEvent::Inbound {
                        user_id: message.user_id.clone(),
                        channel: message.channel.clone(),
                        content: content.clone(),
                        thread_id: message.thread_id.as_ref().map(|t| t.as_str().to_string()),
                    };
                    let content = match self.hooks().run(&hook_event).await {
                        Err(crate::hooks::HookError::Rejected { reason }) => {
                            // Match the main UserInput path's rejection behavior.
                            return Ok(HandleOutcome::Respond(OutgoingResponse::text(format!(
                                "[Message rejected: {reason}]"
                            ))));
                        }
                        Err(err) => {
                            // Match the main UserInput path's error behavior.
                            return Ok(HandleOutcome::Respond(OutgoingResponse::text(format!(
                                "[Message blocked by hook policy: {err}]"
                            ))));
                        }
                        Ok(crate::hooks::HookOutcome::Continue {
                            modified: Some(new_content),
                        }) => new_content,
                        _ => content, // Continue — no modification
                    };

                    // Process as user input with the drain loop so queued
                    // messages during processing are merged, matching the
                    // Submission::UserInput arm's behavior.
                    let mut result = self
                        .process_user_input(
                            message,
                            tenant.clone(),
                            session.clone(),
                            thread_id,
                            &content,
                        )
                        .await;

                    while let Ok(SubmissionResult::Response {
                        content: outgoing,
                        attachments,
                    }) = &result
                    {
                        let merged = {
                            let mut sess = session.lock().await;
                            sess.threads
                                .get_mut(&thread_id)
                                .and_then(|thread| thread.drain_pending_messages())
                        };
                        let Some(next_content) = merged else {
                            break;
                        };

                        let response = build_outgoing_response_for_thread(
                            &session,
                            thread_id,
                            outgoing.clone(),
                            attachments.clone(),
                        )
                        .await;
                        if let Err(e) = self.respond_then_done(message, response).await {
                            tracing::warn!(
                                %thread_id,
                                "Failed to send intermediate drain-loop response: {e}"
                            );
                        }

                        let mut queued_msg = message.clone();
                        queued_msg.attachments.clear();
                        result = self
                            .process_user_input(
                                &queued_msg,
                                tenant.clone(),
                                session.clone(),
                                thread_id,
                                &next_content,
                            )
                            .await;

                        if !matches!(&result, Ok(SubmissionResult::Response { .. })) {
                            let mut sess = session.lock().await;
                            if let Some(thread) = sess.threads.get_mut(&thread_id) {
                                thread.requeue_drained(next_content);
                            }
                        }
                    }

                    result
                }
            }
            Submission::PairingClaim { channel, code } => {
                // Pairing approval is independent of engine_v2 — it only
                // touches the pairing store and the extension manager.
                // Reuse the bridge handler so v1 and v2 surfaces behave
                // identically (#3317).
                match crate::bridge::handle_pairing_claim(self, message, &channel, &code).await {
                    Ok(crate::bridge::BridgeOutcome::Respond(text)) => {
                        Ok(SubmissionResult::Response {
                            content: text,
                            attachments: Vec::new(),
                        })
                    }
                    Ok(crate::bridge::BridgeOutcome::NoResponse)
                    | Ok(crate::bridge::BridgeOutcome::Pending) => {
                        Ok(SubmissionResult::Ok { message: None })
                    }
                    Err(e) => Ok(SubmissionResult::Error {
                        message: format!("Pairing approval failed: {e}"),
                    }),
                }
            }
            Submission::Plan { sub } => {
                use crate::agent::submission::PlanSubcommand;
                let rewritten = match sub {
                    PlanSubcommand::Create { description } => {
                        format!("[PLAN MODE] Create a plan for: {description}")
                    }
                    PlanSubcommand::Approve { plan_ref } => {
                        let r = plan_ref.as_deref().unwrap_or("the most recent plan");
                        format!(
                            "[PLAN MODE] Approve and execute plan {r}. \
                             Create a mission from the plan content using mission_create, \
                             then fire it with mission_fire."
                        )
                    }
                    PlanSubcommand::Status { plan_ref } => {
                        let r = plan_ref.as_deref().unwrap_or("the most recent plan");
                        format!(
                            "[PLAN MODE] Show status of plan {r}. \
                             Check the associated mission's thread_history, \
                             current_focus, and approach_history."
                        )
                    }
                    PlanSubcommand::Revise { plan_ref, feedback } => {
                        let r = plan_ref.as_deref().unwrap_or("the most recent plan");
                        format!("[PLAN MODE] Revise plan {r} based on: {feedback}")
                    }
                    PlanSubcommand::List => {
                        "[PLAN MODE] List all plans. Search memory for plan documents \
                         and show their status."
                            .to_string()
                    }
                };
                self.process_user_input(message, tenant, session.clone(), thread_id, &rewritten)
                    .await
            }
        };

        // Convert SubmissionResult to a HandleOutcome.
        match result? {
            SubmissionResult::Response {
                content,
                attachments,
            } => Ok(submission_response_to_handle_outcome(
                &session,
                thread_id,
                content,
                attachments,
            )
            .await),
            SubmissionResult::Ok {
                message: output_message,
            } => {
                let should_exit =
                    if output_message.as_deref() == Some("") && is_single_message_repl(message) {
                        let sess = session_for_empty_exit.lock().await;
                        sess.threads
                            .get(&thread_id)
                            .map(|thread| thread.state != ThreadState::AwaitingApproval)
                            .unwrap_or(true)
                    } else {
                        false
                    };

                if should_exit {
                    Ok(HandleOutcome::Shutdown)
                } else {
                    Ok(HandleOutcome::from_legacy(output_message))
                }
            }
            SubmissionResult::Error { message } => Ok(HandleOutcome::Respond(
                OutgoingResponse::text(format!("Error: {}", message)),
            )),
            SubmissionResult::Interrupted => Ok(HandleOutcome::Respond(OutgoingResponse::text(
                "Interrupted.",
            ))),
            SubmissionResult::AuthPending => {
                // Auth-required status already sent by handle_auth_intercept.
                // Thread is in auth mode — suppress text response and Done.
                Ok(HandleOutcome::Pending)
            }
            SubmissionResult::NeedApproval { .. } => {
                // ApprovalNeeded status was already sent by thread_ops.rs before
                // returning this result. The thread is now in AwaitingApproval —
                // do NOT emit a terminal Done because the turn is paused, not
                // complete. Sending Done here would also trip the web UI's
                // missing-response safety net (see #2079).
                Ok(HandleOutcome::Pending)
            }
        }

5. 执行排队机制

这段注释是在解释 **message drain 机制**：当 agent 正在处理一条用户消息时，如果同一个 thread 又来了新的用户消息，这些消息可能会先进入队列。当前处理完成后，agent 会尝试把队列里的消息继续取出来合并处理，避免快速连续输入被拆成多个上下文不完整的 turn。

原注释可以翻译成：

```text
// 清空处理过程中排队的消息。
// 这些消息会用换行符合并，这样 LLM 能拿到快速连续输入的完整上下文，
// 而不是把每条消息都当成一个独立 turn 来处理，导致上下文不完整。
// 见 #259。
//
// 只有 `Response` 会继续 drain，因为用户已经收到了一次正常回复，
// 队列里可能还有更多消息需要处理。
//
// 其他结果都会停止这个循环：
// - `NeedApproval`：thread 正在等待用户审批，不能继续处理后续消息
// - `Interrupted`：当前 turn 被取消
// - `Ok`：控制命令确认，包括 Processing 状态下新消息到达时返回的 "queued" ack
// - `Error`：软错误；出错后继续 drain 更多消息会让输出交错，难以理解
// - `Err(_)`：硬错误
```

具体它想解决的问题是这种：

```text
用户连续很快发：
1. 帮我改这个函数
2. 顺便加测试
3. 不要改 API
```

如果 agent 收到第 1 条就开始处理，第 2、3 条在处理过程中才到达，那么不做 drain 的话可能变成：

```text
turn 1: 只看到“帮我改这个函数”
→ LLM 开始执行

turn 2: 后面才看到“顺便加测试”
turn 3: 再看到“不要改 API”
```

这样 LLM 第一次处理时上下文不完整，容易做错。

drain 的目标是：

```text
处理过程中发现同 thread 有排队消息
→ 把它们用换行符合并
→ 作为更完整的用户输入继续处理
```

所以注释里说：

```text
Messages are merged (newline-separated)
```

比如变成：

```text
帮我改这个函数
顺便加测试
不要改 API
```

但它不会在任何情况下都继续 drain。只有当前处理结果是：

```rust
SubmissionResult::Response { ... }
```

才继续。

为什么？

因为 `Response` 表示这是一次正常回复，当前 turn 已经完整结束，继续处理排队消息是安全的。

其它情况不能继续：

```rust
NeedApproval
```

agent 正在等用户审批。继续 drain 后面的消息可能绕过审批或改变等待状态。

```rust
Interrupted
```

用户已经取消/中断，不能继续自动处理。

```rust
Ok
```

通常是控制命令或 ack，不一定是一次完整普通聊天回复。尤其当 thread 正在 `Processing` 时，新消息可能只收到一个 “queued” 确认，这时不能再套一层 drain。

```rust
Error
```

发生软错误后，如果继续处理队列，会让用户看到：

```text
Error: ...
然后又突然继续执行别的消息
```

输出会交错，很难理解当前状态。

```rust
Err(_)
```

硬错误更不能继续。

一句话总结：

> 这段注释说明：agent 会在普通回复后继续消化处理期间排队的快速连续消息，并把它们合并成完整上下文；但一旦遇到审批、取消、控制命令或错误，就停止 drain，避免状态混乱或输出交错。

****************************
这块是：**先返回当前已完成 turn 的回复，然后再处理队列里的下一批消息；队列消息处理完后，最后一条结果交给外层统一返回。**

不是“把队列所有消息处理完后一块返回”。

流程大概是：

```text
1. 先处理当前 UserInput
   result = process_user_input(...)

2. 如果 result 是 Response：
   说明当前 turn 已经产生一条正常回复

3. 尝试 drain 队列里的 pending messages
   如果没有排队消息 → 跳出 while，外层返回当前 result

4. 如果有排队消息：
   先把当前 result 的 outgoing 回复发出去
   respond_then_done(message, response).await

5. 再把排队消息合并成 next_content
   process_user_input(..., &next_content)

6. 得到新的 result

7. 如果新的 result 还是 Response，并且队列还有消息：
   再先发出这条 Response
   再处理下一批队列消息

8. 最后一轮 result 留给外层 match 转成 HandleOutcome 返回
```

对应关键代码是这里：

```rust
let response = build_outgoing_response_for_thread(
    &session,
    thread_id,
    outgoing.clone(),
    attachments.clone(),
)
.await;

if let Err(e) = self.respond_then_done(message, response).await {
    ...
}
```

这说明：**在处理下一批 queued messages 之前，会先把上一轮回复发出去。**

然后才继续：

```rust
result = self
    .process_user_input(
        &queued_msg,
        tenant.clone(),
        session.clone(),
        thread_id,
        &next_content,
    )
    .await;
```

所以顺序是：

```text
处理第 1 条 → 得到回复 A
发现队列有消息
先发送回复 A
再处理队列合并内容 → 得到回复 B
如果还有队列
先发送回复 B
再处理下一批 → 得到回复 C
最后回复 C 由外层返回发送
```

为什么最后一条不是这里发？

因为循环条件是：

```rust
while let Ok(SubmissionResult::Response { ... }) = &result
```

只有在发现还有 queued message 时，才需要先把当前 response 发掉，腾出后续处理空间。

如果没有 queued message：

```rust
let Some(next_content) = merged else {
    break;
};
```

就直接 `break`，当前 `result` 会被返回：

```rust
result
```

然后外层统一处理：

```rust
SubmissionResult::Response { content, attachments } => ...
```

也就是说：

```text
中间回复：在 drain loop 里用 respond_then_done 发送
最后回复：跳出 loop 后由外层返回发送
```

举例：

```text
用户快速发三段：
A
B
C
```

假设处理 A 时，B/C 入队。

执行顺序：

```text
process A → response A
drain 得到 B+C
send response A
process B+C → response BC
队列空
跳出循环
外层 send response BC
```

所以用户看到的是：

```text
response A
response BC
```

不是等 B+C 处理完以后才一起返回。

再看一个多轮队列例子：

```text
process A → response A
drain 得到 B
send response A
process B → response B
drain 得到 C
send response B
process C → response C
队列空
外层 send response C
```

总结：

> 这段逻辑是“边处理边返回”：每处理完一轮并发现还有队列消息，就先发送当前回复，再处理队列；最后一轮回复由外层统一返回。
****************************************
          Submission::UserInput { content } => {
                let mut result = self
                    .process_user_input(
                        message,
                        tenant.clone(),
                        session.clone(),
                        thread_id,
                        &content,
                    )
                    .await;

                // Drain any messages queued during processing.
                // Messages are merged (newline-separated) so the LLM receives
                // full context from rapid consecutive inputs instead of
                // processing each as a separate turn with partial context (#259).
                //
                // Only `Response` continues the drain — the user got a normal
                // reply and there may be more queued messages to process.
                //
                // Everything else stops the loop:
                // - `NeedApproval`: thread is blocked on user approval
                // - `Interrupted`: turn was cancelled
                // - `Ok`: control-command acknowledgment (including the "queued"
                //    ack returned when a message arrives during Processing)
                // - `Error`: soft error — draining more messages after an error
                //    would produce confusing interleaved output
                // - `Err(_)`: hard error
                while let Ok(SubmissionResult::Response {
                    content: outgoing,
                    attachments,
                }) = &result
                {
                    let merged = {
                        let mut sess = session.lock().await;
                        sess.threads
                            .get_mut(&thread_id)
                            .and_then(|t| t.drain_pending_messages())
                    };
                    let Some(next_content) = merged else {
                        break;
                    };

                    tracing::debug!(
                        thread_id = %thread_id,
                        merged_len = next_content.len(),
                        "Drain loop: processing merged queued messages"
                    );

                    // Send the completed turn's response before starting the next.
                    //
                    // Known limitations:
                    // - One-shot channels (HttpChannel) consume the response
                    //   sender on the first respond() call keyed by msg.id.
                    //   Subsequent calls (including the outer handler's final
                    //   respond) are silently dropped. For one-shot channels
                    //   only this intermediate response is delivered.
                    // - All drain-loop responses are routed via the original
                    //   `message`, so channels that key routing on message
                    //   identity will attribute every response to the first
                    //   message. This is acceptable for the current
                    //   single-user-per-thread model.
                    let response = build_outgoing_response_for_thread(
                        &session,
                        thread_id,
                        outgoing.clone(),
                        attachments.clone(),
                    )
                    .await;
                    if let Err(e) = self.respond_then_done(message, response).await {
                        tracing::warn!(
                            thread_id = %thread_id,
                            "Failed to send intermediate drain-loop response: {e}"
                        );
                    }

                    // Process merged queued messages as a single turn.
                    // Use a message clone with cleared attachments so
                    // augment_with_attachments doesn't re-apply the original
                    // message's attachments to unrelated queued text.
                    let mut queued_msg = message.clone();
                    queued_msg.attachments.clear();
                    result = self
                        .process_user_input(
                            &queued_msg,
                            tenant.clone(),
                            session.clone(),
                            thread_id,
                            &next_content,
                        )
                        .await;

                    // If processing failed, re-queue the drained content so it
                    // isn't lost. It will be picked up on the next successful turn.
                    if !matches!(&result, Ok(SubmissionResult::Response { .. })) {
                        let mut sess = session.lock().await;
                        if let Some(thread) = sess.threads.get_mut(&thread_id) {
                            thread.requeue_drained(next_content);
                            tracing::debug!(
                                thread_id = %thread_id,
                                "Re-queued drained content after non-Response result"
                            );
                        }
                    }
                }

                result
            }

6. process_user_input

        let augmented =
            crate::agent::attachments::augment_with_attachments(content, &message.attachments);
        let (effective_content, image_parts) = match &augmented {
            Some(result) => (result.text.as_str(), result.image_parts.clone()),
            None => (content, Vec::new()),
        };

        // First check thread state without holding lock during I/O
        let (thread_state, pending_approval) = {
            let sess = session.lock().await;
            let thread = sess
                .threads
                .get(&thread_id)
                .ok_or_else(|| Error::from(crate::error::JobError::NotFound { id: thread_id }))?;
            (thread.state, thread.pending_approval.clone())
        };

        // Check thread state
        match thread_state {
            ThreadState::Processing => {
                let mut sess = session.lock().await;
                match sess.threads.get_mut(&thread_id) {
                    Some(thread) => {
                        // Re-check state under lock — the turn may have completed
                        // between the snapshot read and this mutable lock acquisition.
                        if thread.state == ThreadState::Processing {
                            // Reject messages with attachments — the queue stores
                            // text only, so attachments would be silently dropped.
                            if !message.attachments.is_empty() {
                                return Ok(SubmissionResult::error(
                                    "Cannot queue messages with attachments while a turn is processing. \
                                     Please resend after the current turn completes.",
                                ));
                            }

                            // Run the same safety checks that the normal path applies
                            // so blocked content is never stored in pending_messages.
                            if let Some(rejection) =
                                self.reject_unsafe_inbound_user_message(message, effective_content)
                            {
                                return Ok(rejection);
                            }

                            if !thread.queue_message(content.to_string()) {
                                return Ok(SubmissionResult::error(format!(
                                    "Message queue full ({MAX_PENDING_MESSAGES}). Wait for the current turn to complete.",
                                )));
                            }
                            // Return `Ok` (not `Response`) so the drain loop in
                            // agent_loop.rs breaks — `Ok` signals a control
                            // acknowledgment, not a completed LLM turn.
                            return Ok(SubmissionResult::Ok {
                                message: Some(
                                    "Message queued — will be processed after the current turn."
                                        .into(),
                                ),
                            });
                        }
                        // State changed (turn completed) — fall through to process normally.
                        // NOTE: `sess` (the Mutex guard) is dropped at the end of
                        // this `Processing` match arm, releasing the session lock
                        // before the rest of process_user_input runs. No deadlock.
                    }
                    None => {
                        return Ok(SubmissionResult::error("Thread no longer exists."));
                    }
                }
            }
            ThreadState::AwaitingApproval => {
                tracing::warn!(
                    message_id = %message.id,
                    thread_id = %thread_id,
                    "Thread awaiting approval, rejecting new input"
                );
                if let Some(pending) = pending_approval.as_ref() {
                    let _ = self
                        .channels
                        .send_status(
                            &message.channel,
                            pending_approval_status_update(pending),
                            &message.metadata,
                        )
                        .await;
                }
                let msg = pending_approval_message(pending_approval.as_ref());
                return Ok(SubmissionResult::pending(msg));
            }
            ThreadState::Completed => {
                tracing::warn!(
                    message_id = %message.id,
                    thread_id = %thread_id,
                    "Thread completed, rejecting new input"
                );
                return Ok(SubmissionResult::error(
                    "Thread completed. Use /thread new.",
                ));
            }
            ThreadState::Idle | ThreadState::Interrupted => {
                // Can proceed
            }
        }

        // Validate inbound content before the turn is created. Attachment-only
        // messages are checked after attachment augmentation so extracted text
        // and multimodal metadata go through the same safety pipeline.
        if let Some(rejection) = self.reject_unsafe_inbound_user_message(message, effective_content)
        {
            return Ok(rejection);
        }

        // Handle explicit commands (starting with /) directly
        // Everything else goes through the normal agentic loop with tools
        let temp_message = IncomingMessage {
            content: content.to_string(),
            ..message.clone()
        };

        if let Some(intent) = self.router.route_command(&temp_message) {
            // Explicit command like /status, /job, /list - handle directly
            return self.handle_job_or_command(intent, message, &tenant).await;
        }

/// Strategy for context compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStrategy {
/// Summarize old messages and keep recent ones.
Summarize {
/// Number of recent turns to keep intact.
keep_recent: usize,
},
/// Truncate old messages without summarization.
Truncate {
/// Number of recent turns to keep.
keep_recent: usize,
},
/// Move context to workspace memory.
MoveToWorkspace,
}
***************************
/// Monitors context size and suggests compaction.
pub struct ContextMonitor {
/// Maximum tokens allowed in context.
context_limit: usize,
/// Threshold ratio for triggering compaction.
threshold_ratio: f64,
}
• suggest_compaction(messages)
│
├─ 1. 估算 tokens
│     │
│     ├─ 对每条 message：
│     │     word_count = message.content 按空白分词数量
│     │     message_tokens = word_count * 1.3 + 4
│     │
│     └─ tokens = 所有 message_tokens 相加
│
├─ 2. 计算是否需要压缩
│     │
│     ├─ context_limit = 100000        默认值
│     ├─ threshold_ratio = 0.8         默认值
│     └─ threshold = 100000 * 0.8 = 80000
│
├─ 3. 判断 needs_compaction
│     │
│     ├─ tokens < 80000
│     │     └─ 返回 None
│     │        不压缩
│     │
│     └─ tokens >= 80000
│           └─ 继续选择压缩策略
│
└─ 4. 选择压缩策略
│
├─ 计算 overage
│     overage = tokens / context_limit
│
├─ tokens > 95000
│     overage > 0.95
│     └─ 返回 Truncate { keep_recent: 3 }
│        严重占用：直接截断旧消息，只保留最近 3 条
│
├─ tokens > 85000
│     overage > 0.85
│     └─ 返回 Summarize { keep_recent: 5 }
│        高占用：总结旧消息，保留最近 5 条
│
└─ 80000 <= tokens <= 85000
└─ 返回 MoveToWorkspace
中等占用：把上下文迁移到 workspace


    /// Compact a thread's context using the given strategy.
    pub async fn compact(
        &self,
        thread: &mut Thread,
        strategy: CompactionStrategy,
        workspace: Option<&Workspace>,
    ) -> Result<CompactionResult, Error> {
        let messages = thread.messages();
        let tokens_before = ContextBreakdown::analyze(&messages).total_tokens;

        let result = match strategy {
            CompactionStrategy::Summarize { keep_recent } => {
                self.compact_with_summary(thread, keep_recent, workspace)
                    .await?
            }
            CompactionStrategy::Truncate { keep_recent } => {
                self.compact_truncate(thread, keep_recent)
            }
            CompactionStrategy::MoveToWorkspace => {
                self.compact_to_workspace(thread, workspace).await?
            }
        };

        let messages_after = thread.messages();
        let tokens_after = ContextBreakdown::analyze(&messages_after).total_tokens;

        Ok(CompactionResult {
            turns_removed: result.turns_removed,
            tokens_before,
            tokens_after,
            summary_written: result.summary_written,
            summary: result.summary,
        })
    }
三种策略都在 [src/agent/compaction.rs](E:/codes/Rust/projects/ironclaw/src/agent/compaction.rs:50) 里分发，核心区别是：**是否生成摘要、是否写 workspace、保留多少最近 turns、失败时是否删除旧 turns。**

**1. `Summarize { keep_recent }`**

调用：

```rust
self.compact_with_summary(thread, keep_recent, workspace).await?
```

做的事：

```text
如果 thread.turns.len() <= keep_recent
→ 什么都不做

否则：
→ 取旧 turns：thread.turns[..turns_to_remove]
→ 把旧 turns 转成 ChatMessage
→ 调 LLM 生成 summary
→ 如果有 workspace：
     尝试把 summary 写入 daily/YYYY-MM-DD.md
     写成功：删除旧 turns，只保留最近 keep_recent 个
     写失败：不删除旧 turns，避免上下文丢失
→ 如果没有 workspace：
     直接删除旧 turns，只保留最近 keep_recent 个
→ 返回 summary
```

写 workspace 的格式是：

```md
## Context Summary (HH:MM UTC)

<summary>
```

特点：

```text
保留信息密度最高：
旧上下文先总结，再删除原始旧 turns。
```

失败保护：

```text
LLM summary 失败 → 返回 Err，不改 thread
workspace 写失败 → 保留旧 turns，不压缩
```

---

**2. `Truncate { keep_recent }`**

调用：

```rust
self.compact_truncate(thread, keep_recent)
```

做的事最简单：

```text
记录 turns_before
调用 thread.truncate_turns(keep_recent)
计算 turns_removed
不生成 summary
不写 workspace
返回 summary = None
```

也就是：

```text
直接删旧 turns，只保留最近 keep_recent 个
```

特点：

```text
最快、最激进、信息损失最大。
```

比如：

```rust
Truncate { keep_recent: 3 }
```

就是只保留最近 3 个 turn，前面的直接丢。

---

**3. `MoveToWorkspace`**

调用：

```rust
self.compact_to_workspace(thread, workspace).await?
```

做的事：

```text
如果没有 workspace：
→ fallback 到 compact_truncate(thread, 5)

如果有 workspace：
→ 固定 keep_recent = 10
→ 如果 thread.turns.len() <= 10
     什么都不做
→ 否则：
     取旧 turns
     把旧 turns 格式化成完整文本
     写入 daily/YYYY-MM-DD.md
     写成功：删除旧 turns，只保留最近 10 个
     写失败：不删除旧 turns，避免上下文丢失
```

写 workspace 的格式是：

```md
## Archived Context (HH:MM UTC)

**Turn 1**
User: ...
Agent: ...
Tools: ...

**Turn 2**
...
```

特点：

```text
不摘要，直接把旧 turn 原文归档到 workspace。
比 Summarize 更保真，但占 workspace 更多。
```

失败保护：

```text
workspace 写失败 → 保留旧 turns，不压缩
没有 workspace → 直接退化成 Truncate { keep_recent: 5 }
```

可以直接看成这张表：

```text
策略              生成摘要  写 workspace  删除旧 turns 条件              保留最近
Summarize         是        有则写摘要     无 workspace 或写入成功        keep_recent
Truncate          否        否            直接删除                      keep_recent
MoveToWorkspace   否        是，写原文     有 workspace 且写入成功        10
MoveToWorkspace   否        否            无 workspace 时 fallback 截断   5
```

最终 `compact(...)` 外层还会统一计算：

```rust
tokens_before
tokens_after
turns_removed
summary_written
summary
```

返回 `CompactionResult`。
********************
        // Auto-compact if needed BEFORE adding new turn
        {
            let mut sess = session.lock().await;
            let thread = sess
                .threads
                .get_mut(&thread_id)
                .ok_or_else(|| Error::from(crate::error::JobError::NotFound { id: thread_id }))?;

            let messages = thread.messages();
            if let Some(strategy) = self.context_monitor.suggest_compaction(&messages) {
                let pct = self.context_monitor.usage_percent(&messages);
                tracing::info!("Context at {:.1}% capacity, auto-compacting", pct);

                // Notify the user that compaction is happening
                let _ = self
                    .channels
                    .send_status(
                        &message.channel,
                        StatusUpdate::Status(format!(
                            "Context at {:.0}% capacity, compacting...",
                            pct
                        )),
                        &message.metadata,
                    )
                    .await;

                let compactor = ContextCompactor::new(self.llm().clone());
                let workspace = self.workspace_for_user(&message.user_id);
                if let Err(e) = compactor
                    .compact(thread, strategy, workspace.as_deref())
                    .await
                {
                    tracing::warn!("Auto-compaction failed: {}", e);
                }
            }
        }

        // Create checkpoint before turn
        let undo_mgr = self.session_manager.get_undo_manager(thread_id).await;
        {
            let sess = session.lock().await;
            let thread = sess
                .threads
                .get(&thread_id)
                .ok_or_else(|| Error::from(crate::error::JobError::NotFound { id: thread_id }))?;

            let mut mgr = undo_mgr.lock().await;
            mgr.checkpoint(
                thread.turn_number(),
                thread.messages(),
                format!("Before turn {}", thread.turn_number()),
            );
        }

        // Start the turn and get messages
这段代码把当前用户输入登记为一个新的 turn，标记 thread 正在 Processing，保存图片信息，并生成后续 LLM/tool loop 要用的完整消息上下文，同时拿到 turn 编号和开始时间供 DB 持久化使用。
let (turn_messages, turn_number, turn_started_at) = {
let mut sess = session.lock().await;
let thread = sess
.threads
.get_mut(&thread_id)
.ok_or_else(|| Error::from(crate::error::JobError::NotFound { id: thread_id }))?;
let turn = thread.start_turn(effective_content);
turn.image_content_parts = image_parts;
let turn_number = turn.turn_number;
let turn_started_at = turn.started_at;
(thread.messages(), turn_number, turn_started_at)
};

        // Persist user message to DB immediately so it survives crashes
这段代码把当前用户输入先写进 DB，防止处理过程中崩溃导致消息丢失；如果写入成功，就把数据库生成的 user message id 回填到内存里当前 turn，方便后续 assistant 回复、工具调用和历史记录关联。
tracing::debug!(
message_id = %message.id,
thread_id = %thread_id,
"Persisting user message to DB"
);
let persisted_user_message_id = self
.persist_user_message(
thread_id,
&message.channel,
&message.user_id,
turn_number,
effective_content,
turn_started_at,
)
.await;

        if let Some(user_message_id) = persisted_user_message_id {
            let mut sess = session.lock().await;
            if let Some(thread) = sess.threads.get_mut(&thread_id)
                && let Some(turn) = thread.turns.last_mut()
                && turn.turn_number == turn_number
            {
                turn.user_message_id = Some(user_message_id);
            }
        }

        // Send thinking status
        let _ = self
            .channels
            .send_status(
                &message.channel,
                StatusUpdate::Thinking("Processing...".into()),
                &message.metadata,
            )
            .await;

        // Run the agentic tool execution loop
        let result = self
            .run_agentic_loop(message, tenant, session.clone(), thread_id, turn_messages)
            .await; chat委托者、选skill等等

        // Re-acquire lock and check if interrupted
        let mut sess = session.lock().await;
        let thread = sess
            .threads
            .get_mut(&thread_id)
            .ok_or_else(|| Error::from(crate::error::JobError::NotFound { id: thread_id }))?;

        if thread.state == ThreadState::Interrupted {
            drop(sess);
            self.clear_conversation_live_state(thread_id, &message.channel, &message.user_id)
                .await;
            if let Some(turn_usage) = turn_usage_from_result(&result) {
                self.send_turn_cost_status(&message.channel, &message.metadata, turn_usage)
                    .await;
            }
            let _ = self
                .channels
                .send_status(
                    &message.channel,
                    StatusUpdate::Status("Interrupted".into()),
                    &message.metadata,
                )
                .await;
            return Ok(SubmissionResult::Interrupted);
        }
> 这是 agentic loop 结束后的中断保护：重新检查 thread 是否被标记为 Interrupted，如果是，就释放锁、清理 live state、发送用量和中断状态，然后返回 SubmissionResult::Interrupted，避免继续保存或发送已经被
> 取消的结果。


        // Complete, fail, or request approval
        match result {
            Ok(AgenticLoopResult::Response {
                text: response,
                turn_usage,
            }) => {
                // Extract <suggestions> from response text before user sees it
                let (response, suggestions) =
                    crate::agent::dispatcher::extract_suggestions(&response);

                // Hook: TransformResponse — allow hooks to modify or reject the final response
                let mut response_attachments_allowed = true;
                let mut response = {
                    let event = crate::hooks::HookEvent::ResponseTransform {
                        user_id: message.user_id.clone(),
                        thread_id: thread_id.to_string(),
                        response: response.clone(),
                    };
                    match self.hooks().run(&event).await {
                        Err(crate::hooks::HookError::Rejected { reason }) => {
                            response_attachments_allowed = false;
                            format!("[Response filtered: {}]", reason)
                        }
                        Err(err) => {
                            response_attachments_allowed = false;
                            format!("[Response blocked by hook policy: {}]", err)
                        }
                        Ok(crate::hooks::HookOutcome::Continue {
                            modified: Some(new_response),
                        }) => new_response,
                        _ => response, // fail-open: use original
                    }
                };

                let response_attachments = if response_attachments_allowed {
                    let current_tool_calls = thread
                        .turns
                        .last()
                        .map(|turn| turn.tool_calls.clone())
                        .unwrap_or_default();
                    stage_generated_image_response_attachments(&current_tool_calls)
                } else {
                    Vec::new()
                };
                if !response_attachments.is_empty() {
                    response = strip_markdown_image_lines(&response);
                }

                thread.conclude_turn(TurnOutcome::Completed(response.clone()));
                let (turn_number, tool_calls, narrative) = thread
                    .turns
                    .last()
                    .map(|t| (t.turn_number, t.tool_calls.clone(), t.narrative.clone()))
                    .unwrap_or_default();
                drop(sess);

                // Persist tool calls then assistant response (user message already persisted at turn start)
                self.persist_tool_calls(PersistToolCallsInput {
                    thread_id,
                    channel: &message.channel,
                    user_id: &message.user_id,
                    turn_number,
                    tool_calls: &tool_calls,
                    narrative: narrative.as_deref(),
                    outcome: Some(trace_turn_outcome_success()),
                })
                .await;
                self.persist_assistant_response(
                    thread_id,
                    &message.channel,
                    &message.user_id,
                    &response,
                )
                .await;

                // Send suggestions after response (best-effort, rendered by web gateway)
                if !suggestions.is_empty() {
                    let _ = self
                        .channels
                        .send_status(
                            &message.channel,
                            StatusUpdate::Suggestions { suggestions },
                            &message.metadata,
                        )
                        .await;
                }

                self.send_turn_cost_status(&message.channel, &message.metadata, &turn_usage)
                    .await;

                self.spawn_autonomous_trace_contribution(
                    message.user_id.clone(),
                    thread_id,
                    message.channel.clone(),
                    message.metadata.clone(),
                );

                Ok(SubmissionResult::response_with_attachments(
                    response,
                    response_attachments,
                ))
            }
            Ok(AgenticLoopResult::NeedApproval {
                pending,
                turn_usage,
            }) => {
                // Store pending approval in thread and update state
                let request_id = pending.request_id;
                let tool_name = pending.tool_name.clone();
                let description = pending.description.clone();
                let parameters = pending.display_parameters.clone();
                let allow_always = pending.allow_always;
                thread.await_approval(*pending);
                drop(sess);
                self.clear_conversation_live_state(thread_id, &message.channel, &message.user_id)
                    .await;
                self.send_turn_cost_status(&message.channel, &message.metadata, &turn_usage)
                    .await;
                let _ = self
                    .channels
                    .send_status(
                        &message.channel,
                        StatusUpdate::ApprovalNeeded {
                            request_id: request_id.to_string(),
                            tool_name: tool_name.clone(),
                            description: description.clone(),
                            parameters: parameters.clone(),
                            allow_always,
                        },
                        &message.metadata,
                    )
                    .await;
                Ok(SubmissionResult::NeedApproval {
                    request_id,
                    tool_name,
                    description,
                    parameters,
                    allow_always,
                })
            }
            Ok(AgenticLoopResult::AuthPending { turn_usage }) => {
                // Auth-required card already sent by the dispatcher, and the
                // thread is already in auth mode (enter_auth_mode called in
                // execute_tool_calls). CompletedSilently transitions to Idle
                // without persisting a redundant text response alongside the
                // auth card.
                thread.conclude_turn(TurnOutcome::CompletedSilently);
                //
                // Persist tool calls so history shows what happened, but
                // skip persist_assistant_response — the auth card is the
                // only user-facing signal.
                let (turn_number, tool_calls, narrative) = thread
                    .turns
                    .last()
                    .map(|t| (t.turn_number, t.tool_calls.clone(), t.narrative.clone()))
                    .unwrap_or_default();
                drop(sess);
                self.persist_tool_calls(PersistToolCallsInput {
                    thread_id,
                    channel: &message.channel,
                    user_id: &message.user_id,
                    turn_number,
                    tool_calls: &tool_calls,
                    narrative: narrative.as_deref(),
                    outcome: None,
                })
                .await;
                self.send_turn_cost_status(&message.channel, &message.metadata, &turn_usage)
                    .await;
                Ok(SubmissionResult::auth_pending())
            }
            Ok(AgenticLoopResult::Failed { error, turn_usage }) => {
                self.send_turn_cost_status(&message.channel, &message.metadata, &turn_usage)
                    .await;
                let error_message = error.to_string();
                thread.conclude_turn(TurnOutcome::Failed(error_message.clone()));
                let (turn_number, tool_calls, narrative) = thread
                    .turns
                    .last()
                    .map(|t| (t.turn_number, t.tool_calls.clone(), t.narrative.clone()))
                    .unwrap_or_default();
                self.persist_tool_calls(PersistToolCallsInput {
                    thread_id,
                    channel: &message.channel,
                    user_id: &message.user_id,
                    turn_number,
                    tool_calls: &tool_calls,
                    narrative: narrative.as_deref(),
                    outcome: Some(trace_turn_outcome_failure(&error_message)),
                })
                .await;
                drop(sess);
                self.clear_conversation_live_state(thread_id, &message.channel, &message.user_id)
                    .await;
                self.spawn_autonomous_trace_contribution(
                    message.user_id.clone(),
                    thread_id,
                    message.channel.clone(),
                    message.metadata.clone(),
                );
                Ok(SubmissionResult::error(error_message))
            }
            Err(e) => {
                let error_message = e.to_string();
                thread.conclude_turn(TurnOutcome::Failed(error_message.clone()));
                let (turn_number, tool_calls, narrative) = thread
                    .turns
                    .last()
                    .map(|t| (t.turn_number, t.tool_calls.clone(), t.narrative.clone()))
                    .unwrap_or_default();
                self.persist_tool_calls(PersistToolCallsInput {
                    thread_id,
                    channel: &message.channel,
                    user_id: &message.user_id,
                    turn_number,
                    tool_calls: &tool_calls,
                    narrative: narrative.as_deref(),
                    outcome: Some(trace_turn_outcome_failure(&error_message)),
                })
                .await;
                drop(sess);
                self.clear_conversation_live_state(thread_id, &message.channel, &message.user_id)
                    .await;
                self.spawn_autonomous_trace_contribution(
                    message.user_id.clone(),
                    thread_id,
                    message.channel.clone(),
                    message.metadata.clone(),
                );
                Ok(SubmissionResult::error(error_message))
            }
        }

