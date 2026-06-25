一. 相关结构

1. ChannelManager





管理所有channel的类。通过管道tx发送消息，通过rx接受消息传给agent。所有的channel的rx会合并成一个大流。



有一个注入管道，用于热载入channel和背景任务使用。热载的将会在下次进入正常流程。



除了下面的外，其余操作都是借助具体的channel去使用功能。

/// Manages multiple input channels and merges their message streams.
///
/// Includes an injection channel so background tasks (e.g., job monitors) can
/// push messages into the agent loop without being a full `Channel` impl.
pub struct ChannelManager {
channels: Arc<RwLock<HashMap<String, Arc<dyn Channel>>>>,
inject_tx: mpsc::Sender<IncomingMessage>,
/// Taken once in `start_all()` and merged into the stream.
inject_rx: tokio::sync::Mutex<Option<mpsc::Receiver<IncomingMessage>>>,
}



    /// Hot-add a channel to a running agent.
    ///
    /// Starts the channel, registers it in the channels map for `respond()`/`broadcast()`,
    /// and spawns a task that forwards its stream messages through `inject_tx` into
    /// the agent loop.
    pub async fn hot_add(&self, channel: Box<dyn Channel>) -> Result<(), ChannelError> {
        let name = channel.name().to_string();

        // Shut down any existing channel with the same name to avoid parallel consumers.
        // The old forwarding task will stop when the channel's stream ends after shutdown.
        {
            let channels = self.channels.read().await;
            if let Some(existing) = channels.get(&name) {
                tracing::debug!(channel = %name, "Shutting down existing channel before hot-add replacement");
                let _ = existing.shutdown().await;
            }
        }

        let stream = channel.start().await?;

        // Register for respond/broadcast/send_status
        self.channels
            .write()
            .await
            .insert(name.clone(), Arc::from(channel));

        // Forward stream messages through inject_tx
        let tx = self.inject_tx.clone();//起了一个背景任务，为了将热载入的channel使用注入tx发送到agentLoop
        tokio::spawn(async move {
            use futures::StreamExt;
            let mut stream = stream;
            while let Some(msg) = stream.next().await {
                if tx.send(msg).await.is_err() {
                    tracing::warn!(channel = %name, "Inject channel closed, stopping hot-added channel");
                    break;
                }
            }
            tracing::debug!(channel = %name, "Hot-added channel stream ended");
        });

        Ok(())
    }

    /// Start all channels and return a merged stream of messages. 启动所有channel，然后合并成一个接受流给agent使用
    ///
    /// Also merges the injection channel so background tasks can push messages
    /// into the same stream.
    pub async fn start_all(&self) -> Result<MessageStream, ChannelError> {
        let channels = self.channels.read().await;
        let mut streams: Vec<MessageStream> = Vec::new();

        for (name, channel) in channels.iter() {
            match channel.start().await {
                Ok(stream) => {
                    tracing::debug!("Started channel: {}", name);
                    streams.push(stream);
                }
                Err(e) => {
                    tracing::error!("Failed to start channel {}: {}", name, e);
                    // Continue with other channels, don't fail completely
                }
            }
        }

        if streams.is_empty() {
            return Err(ChannelError::StartupFailed {
                name: "all".to_string(),
                reason: "No channels started successfully".to_string(),
            });
        }

        // Take the injection receiver (can only be taken once)
        if let Some(inject_rx) = self.inject_rx.lock().await.take() {
            let inject_stream = tokio_stream::wrappers::ReceiverStream::new(inject_rx);
            streams.push(Box::pin(inject_stream));
            tracing::debug!("Injection channel merged into message stream");
        }

        // Merge all streams into one
        let merged = stream::select_all(streams);
        Ok(Box::pin(merged))
    }

2. channel trait

pub trait Channel: Send + Sync {
/// Get the channel name (e.g., "cli", "slack", "telegram", "http").
fn name(&self) -> &str;

    /// Start listening for messages.
    ///
    /// Returns a stream of incoming messages. The channel should handle
    /// reconnection and error recovery internally.
    async fn start(&self) -> Result<MessageStream, ChannelError>;

    /// Send a response back to the user.
    ///
    /// The response is sent in the context of the original message
    /// (same channel, same thread if applicable).
    async fn respond(
        &self,
        msg: &IncomingMessage,
        response: OutgoingResponse,
    ) -> Result<(), ChannelError>;

    /// Send a status update (thinking, tool execution, etc.).
    ///
    /// The metadata contains channel-specific routing info (e.g., Telegram chat_id)
    /// needed to deliver the status to the correct destination.
    ///
    /// Default implementation does nothing (for channels that don't support status).
    async fn send_status(
        &self,
        _status: StatusUpdate,
        _metadata: &serde_json::Value,
    ) -> Result<(), ChannelError> {
        Ok(())
    }

    /// Send a proactive message without a prior incoming message.
    ///
    /// Used for alerts, heartbeat notifications, and other agent-initiated communication.
    /// The user_id helps target a specific user within the channel.
    ///
    /// Default implementation does nothing (for channels that don't support broadcast).
    async fn broadcast(
        &self,
        _user_id: &str,
        _response: OutgoingResponse,
    ) -> Result<(), ChannelError> {
        Ok(())
    }

    /// Check if the channel is healthy.
    async fn health_check(&self) -> Result<(), ChannelError>;

    /// Get conversation context from message metadata for system prompt.
    ///
    /// Returns key-value pairs like "sender", "sender_uuid", "group" that
    /// help the LLM understand who it's talking to.
    ///
    /// Default implementation returns empty map.
    fn conversation_context(&self, _metadata: &serde_json::Value) -> HashMap<String, String> {
        HashMap::new()
    }

    /// Gracefully shut down the channel.
    async fn shutdown(&self) -> Result<(), ChannelError> {
        Ok(())
    }
}

3. gateway channel

/// Web gateway channel implementing the Channel trait.
pub struct GatewayChannel {
config: GatewayConfig,
state: Arc<GatewayState>,
/// Combined auth state: env-var tokens + optional DB-backed tokens.
auth: CombinedAuthState,
}

/// Shared state for all gateway handlers.
pub struct GatewayState {
/// Channel to send messages to the agent loop.
pub msg_tx: tokio::sync::RwLock<Option<mpsc::Sender<IncomingMessage>>>,//参与进agentLoop循环，发送管道
/// SSE broadcast manager (Arc-wrapped so extension manager can hold a reference).
pub sse: Arc<SseManager>,//sse链接处理
/// Workspace for memory API (single-user fallback).
pub workspace: Option<Arc<Workspace>>,
/// Optional per-user workspace resolver/pool.
///
/// This is independent of `multi_tenant_mode`: the runtime may provide a
/// per-user workspace pool even in single-user mode for plumbing or test
/// harnesses.
pub workspace_pool: Option<Arc<WorkspacePool>>,
/// Whether the gateway started in multi-tenant mode.
///
/// This is intentionally separate from `workspace_pool.is_some()`: the
/// runtime may still use a per-user workspace resolver in single-user mode,
/// but the unauthenticated bootstrap routes (`/`, `/style.css`) only need
/// to suppress workspace-driven frontend customizations when startup
/// actually determined that multiple tenants exist.
pub multi_tenant_mode: bool,
/// Session manager for thread info.
pub session_manager: Option<Arc<SessionManager>>,
/// Log broadcaster for the logs SSE endpoint.
pub log_broadcaster: Option<Arc<LogBroadcaster>>,
/// Handle for changing the tracing log level at runtime.
pub log_level_handle: Option<Arc<crate::channels::web::log_layer::LogLevelHandle>>,
/// Extension manager for extension management API.
pub extension_manager: Option<Arc<ExtensionManager>>,
/// Tool registry for listing registered tools.
pub tool_registry: Option<Arc<ToolRegistry>>,
/// Database store for sandbox job persistence.
pub store: Option<Arc<dyn Database>>,
/// Cached settings store. When present, settings reads/writes go through
/// the cache layer for consistency with the agent loop. Concrete type so
/// handlers can also call `invalidate_user()` / `flush()`.
pub settings_cache: Option<Arc<crate::db::cached_settings::CachedSettingsStore>>,
/// Container job manager for sandbox operations.
pub job_manager: Option<Arc<ContainerJobManager>>,
/// Prompt queue for Claude Code follow-up prompts.
pub prompt_queue: Option<PromptQueue>,
/// Durable owner scope for persistence and unauthenticated callback flows.
pub owner_id: String,
/// Shutdown signal sender.
pub shutdown_tx: tokio::sync::RwLock<Option<oneshot::Sender<()>>>,
/// WebSocket connection tracker.
pub ws_tracker: Option<Arc<crate::channels::web::ws::WsConnectionTracker>>,
/// LLM provider for OpenAI-compatible API proxy.
pub llm_provider: Option<Arc<dyn ironclaw_llm::LlmProvider>>,
/// Hot-reload controller for the LLM provider chain. Populated at
/// startup when the chain is built from config (not in test harnesses
/// that inject a provider directly).
pub llm_reload: Option<Arc<ironclaw_llm::LlmReloadHandle>>,
/// LLM session manager handed through to `LlmReloadHandle::reload` so
/// the rebuilt chain keeps using the same (potentially authenticated)
/// NEAR AI / OAuth session without forcing a re-login.
pub llm_session_manager: Option<Arc<ironclaw_llm::SessionManager>>,
/// Optional TOML config path that produced the current `LlmConfig`.
/// Needed so a hot-reload reads the same precedence layers
/// (TOML → DB overlay) as startup.
pub config_toml_path: Option<std::path::PathBuf>,
/// Skill registry for skill management API.
pub skill_registry: Option<Arc<std::sync::RwLock<ironclaw_skills::SkillRegistry>>>,
/// Skill catalog for searching the ClawHub registry.
pub skill_catalog: Option<Arc<ironclaw_skills::catalog::SkillCatalog>>,
/// Shared auth manager for gateway auth submission and readiness checks.
pub auth_manager: Option<Arc<crate::auth::extension::AuthManager>>,
/// Scheduler for sending follow-up messages to running agent jobs.
pub scheduler: Option<crate::tools::builtin::SchedulerSlot>,
/// Per-user rate limiter for chat endpoints (30 messages per 60 seconds per user).
pub chat_rate_limiter: PerUserRateLimiter,
/// Per-IP rate limiter for OAuth/auth endpoints (20 requests per 60 seconds per IP).
pub oauth_rate_limiter: PerUserRateLimiter,
/// Rate limiter for webhook trigger endpoints (10 requests per 60 seconds).
pub webhook_rate_limiter: RateLimiter,
/// Registry catalog entries for the available extensions API.
/// Populated at startup from `registry/` manifests, independent of extension manager.
pub registry_entries: Vec<crate::extensions::RegistryEntry>,
/// Cost guard for token/cost tracking.
pub cost_guard: Option<Arc<crate::agent::cost_guard::CostGuard>>,
/// Routine engine slot for manual routine triggering (filled at runtime).
pub routine_engine: RoutineEngineSlot,
/// Server startup time for uptime calculation.
pub startup_time: std::time::Instant,
/// Snapshot of active (resolved) configuration for the frontend.
pub active_config: Arc<tokio::sync::RwLock<ActiveConfigSnapshot>>,
/// Secrets store for admin secret provisioning.
pub secrets_store: Option<Arc<dyn crate::secrets::SecretsStore + Send + Sync>>,
/// DB auth cache for invalidation on security-critical actions.
pub db_auth: Option<Arc<crate::channels::web::auth::DbAuthenticator>>,
/// Shared pairing store (one instance per server, not per request).
pub pairing_store: Option<Arc<crate::pairing::PairingStore>>,
/// OAuth providers for social login (None when OAuth is disabled).
pub oauth_providers: Option<
Arc<
std::collections::HashMap<
String,
Arc<dyn crate::channels::web::oauth::providers::OAuthProvider>,
>,
>,
>,
/// In-memory store for pending OAuth flows (CSRF + PKCE state).
pub oauth_state_store: Option<Arc<crate::channels::web::oauth::state_store::OAuthStateStore>>,
/// Base URL for constructing OAuth callback URLs.
pub oauth_base_url: Option<String>,
/// Email domains allowed for OAuth/OIDC login. Empty means allow all.
pub oauth_allowed_domains: Vec<String>,
/// NEAR wallet auth nonce store (None when NEAR auth is disabled).
pub near_nonce_store: Option<Arc<crate::channels::web::oauth::near::NearNonceStore>>,
/// NEAR RPC endpoint URL for access key verification.
pub near_rpc_url: Option<String>,
/// NEAR network name (mainnet/testnet) for the frontend wallet connector.
pub near_network: Option<String>,
/// Shutdown signal for OAuth/NEAR sweep background tasks.
/// When this sender is dropped, the sweep loops exit gracefully.
#[allow(dead_code)]
pub oauth_sweep_shutdown: Option<tokio::sync::watch::Sender<()>>,
/// Cache for the assembled frontend HTML served from `/`.
///
/// The cache key is derived from the `updated_at` of
/// `.system/gateway/layout.json` and the `.system/gateway/widgets/`
/// directory — both returned by a single cheap `list(".system/gateway/")`
/// call. A hit skips reading the layout, every widget manifest, every
/// widget JS file, and every widget CSS file. A miss (or absent cache)
/// falls through to the full `build_frontend_html()` path.
pub frontend_html_cache: Arc<tokio::sync::RwLock<Option<FrontendHtmlCache>>>,
/// Channel-agnostic tool dispatcher for routing handler operations through
/// the tool pipeline with audit trail.
pub tool_dispatcher: Option<Arc<crate::tools::dispatch::ToolDispatcher>>,
}

    async fn start(&self) -> Result<MessageStream, ChannelError> {
        let (tx, rx) = mpsc::channel(256);
        *self.state.msg_tx.write().await = Some(tx);

        let addr: SocketAddr = format!("{}:{}", self.config.host, self.config.port)
            .parse()
            .map_err(|e| ChannelError::StartupFailed {
                name: "gateway".to_string(),
                reason: format!(
                    "Invalid address '{}:{}': {}",
                    self.config.host, self.config.port, e
                ),
            })?;

        // The warning bridge forwards WARN/ERROR log lines into the
        // shared SSE stream as verbose-only `AppEvent::Warning` frames.
        // The `tracing` layer that feeds it captures log context at the
        // global subscriber scope, not at request scope — a WARN
        // emitted inside tenant A's request handler is indistinguishable
        // from a global gateway warning. Scoping the whole bridge to
        // the gateway `owner_id` in multi-tenant mode would deliver
        // tenant A's warnings to the admin/owner account instead of
        // tenant A, misrouting per-request diagnostics across
        // accounts. Until per-request provenance is threaded through
        // every `warn!` / `error!` call site, keep the bridge off in
        // multi-tenant deployments entirely.
        if let Some(log_broadcaster) = self.state.log_broadcaster.as_ref() {
            if self.state.multi_tenant_mode {
                tracing::debug!(
                    "warning bridge disabled in multi-tenant mode: \
                     WARN/ERROR log forwarding to debug panel requires \
                     per-request tenant provenance that is not yet \
                     wired through the tracing layer"
                );
            } else {
                log_layer::spawn_warning_bridge(
                    Arc::clone(log_broadcaster),
                    Arc::clone(&self.state.sse),
                    None,
                );
            }
        }

        platform::router::start_server(addr, self.state.clone(), self.auth.clone()).await?;//启动一个web服务去监听端口：http请求
        通过tx参与agentLoop

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    async fn respond(
        &self,
        msg: &IncomingMessage,
        response: OutgoingResponse,
    ) -> Result<(), ChannelError> {
        let thread_id = match &msg.thread_id {//外部channel对话id
            Some(tid) => tid.as_str().to_string(),
            None => {
                return Err(ChannelError::MissingRoutingTarget {
                    name: "gateway".to_string(),
                    reason: "respond() requires a thread_id on the incoming message".to_string(),
                });
            }
        };

        self.state.sse.broadcast_for_user(//使用sse管理器广播回去
            &msg.user_id,
            AppEvent::Response {
                content: response.content,
                thread_id,
            },
        );

        Ok(())
    }

链路
GatewayChannel 完整使用链路

下面是从浏览器请求到响应回流的整条链路，所有引用均来自实际读到的代码（src/channels/web/、src/channels/、src/agent/、src/bridge/、src/main.rs）。

1. 启动阶段：GatewayChannel::new + with_* + Channel::start

src/main.rs:887 调用 GatewayChannel::new(gw_config, owner_id)，随后大量 with_* 链式注入依赖：

GatewayChannel::new
└─ 创建 state: Arc<GatewayState>
├─ sse: SseManager (broadcast::Sender<ScopedEvent>)
├─ msg_tx: RwLock<Option<mpsc::Sender<IncomingMessage>>>
├─ shutdown_tx, ws_tracker, log_broadcaster...
└─ 各 subsystem 占位 Option
└─ 创建 auth: CombinedAuthState (env_auth + 可选 db_auth + 可选 OIDC)

mod.rs:115-207。每个 with_*（with_workspace、with_session_manager、with_tool_registry、with_db_auth、with_job_manager、with_skill_registry、with_llm_provider、with_oauth 等）通过 rebuild_state() 重建 Arc<GatewayState>（注意：必须 start() 前调用，已连接后不能改 sse sender）。

Channel::start() (mod.rs:697-743) 流程：

1. 创建 mpsc 通道 (tx, rx)，把 tx 写入 state.msg_tx —— 之后所有 HTTP handler 拿这个 sender 往 agent 推消息。
2. 解析 SocketAddr，绑定 TcpListener。
3. 在多租户模式下关闭 warning bridge（注释说清楚原因：tracing 层不带请求级租户信息）。
4. 调用 platform::router::start_server(addr, state, auth) 启动 axum。
5. 返回 Box::pin(ReceiverStream::new(rx)) 给 ChannelManager。

2. axum 路由组合（platform/router.rs::start_server）

四组 router，最后 .with_state(state.clone())：

┌─────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬───────────────┐
│ Router  │                                                                                 路由                                                                                  │     鉴权      │
├─────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────┤
│ public  │ /api/health、/oauth/callback、/auth/*、/api/webhooks/*、/relay/events                                                                                                 │ 无            │
├─────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────┤
│ protect │ 全部 /api/chat/*、/api/memory/*、/api/jobs/*、/api/extensions/*、/api/skills/*、/api/settings/*、/api/admin/*、/api/profile、/api/tokens、/v1/chat/completions、/api/ │ auth_middlewa │
│ ed      │ v1/responses、/v1/responses 等                                                                                                                                        │ re            │
├─────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────┤
│ statics │ /、/app.js、/style.css、/theme.css、/admin*、/i18n/*、/favicon.ico、/theme-init.js 等                                                                                 │ 无            │
├─────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────┤
│ project │ /projects/{id}/*                                                                                                                                                      │ auth_middlewa │
│ s       │                                                                                                                                                                       │ re            │
└─────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴───────────────┘

跨切层（按从外到内）：
- DefaultBodyLimit::max(14 MiB)
- catch_panic（500 兜底，截断到 200 字节）
- CorsLayer（同源 + localhost 跨域）
- X-Content-Type-Options: nosniff
- X-Frame-Options: DENY
- Content-Security-Policy: BASE_CSP_HEADER

auth_middleware 来自 platform/auth.rs::CombinedAuthState（env_token / DbAuthenticator / OIDC JWT 任一通过即可）。

3. 入站：浏览器 → 鉴权 → 处理器

举最常用例子：用户发消息到 POST /api/chat/send。

features/chat/mod.rs:82-164 chat_send_handler：

1. 提取 State<Arc<GatewayState>> + AuthenticatedUser(user) + Json<SendMessageRequest>。
2. 速率限制：state.chat_rate_limiter.check(&user.user_id) —— 30 req/60s 滑动窗口（state.rs::PerUserRateLimiter::new(30, 60)）。
3. web_incoming_message("gateway", &user.user_id, content, thread_id) 构造 IncomingMessage（自动注入 metadata.user_id），关联附件、时区。
4. 从 state.msg_tx 克隆 sender（短锁，避免跨 await 持锁）。
5. tx.send(msg).await 把消息推进 agent 主循环。
6. 返回 202 ACCEPTED + SendMessageResponse { message_id, status: "accepted" }。

▎ 关键：HTTP 入口到这一步就 返回了 202。真正的 agent 工作是异步的。

ChannelManager 同样把 GatewayChannel::start() 返回的 MessageStream 合并到统一的 mpsc::Receiver<IncomingMessage> 流（manager.rs:99-133 start_all + inject_rx 合并）。

4. agent 主循环消费（src/agent/agent_loop.rs::run）

agent_loop.rs:1080 self.channels.start_all().await? 拿到合并流，然后进入主循环 loop：

loop {
select! {
Ctrl-C => break,
msg = message_stream.next() => { message = msg }
}
// 中间件：音频转写、文档提取
match self.handle_message(&message).await { ... }
}

agent_loop.rs:1486-1502（run() 主循环）。

handle_message（agent_loop.rs:1713）分发到：

- v1 路径：dispatcher.rs::ChatDelegate —— run_agentic_loop() 共享引擎；send_status() 大量 StatusUpdate::Thinking / ToolStarted / ToolCompleted / ToolResult / ReasoningUpdate / TurnMetrics / ImageGenerated（见 dispatcher.rs:210, 641, 789, 872, 903, 1086, 1108, 1139, 1163, 1261, 1288, 1312, 1625）。
- v2 路径：bridge::router::handle_with_engine() —— 走 ironclaw_engine 的 ConversationManager / ThreadManager。

5. 出站：agent → ChannelManager → GatewayChannel

respond_then_done（agent_loop.rs:686-714）保证顺序：

self.channels.respond(message, response).await;     // 1. 先发正文
self.channels.send_status(channel, StatusUpdate::Status("Done"), &metadata).await; // 2. 再发 Done

ChannelManager::respond (manager.rs:136-150) 通过 msg.channel 字段（这里就是 "gateway"）找到注册过的 channel 实例，转发到 GatewayChannel::respond()（mod.rs:745-769）：

self.state.sse.broadcast_for_user(
&msg.user_id,
AppEvent::Response { content: response.content, thread_id },
);

ChannelManager::send_status 类似地路由到 GatewayChannel::send_status（mod.rs:771-1003）—— 这是个超长 match，把 30+ 个 StatusUpdate 变体转成 AppEvent（如 Thinking → AppEvent::Thinking、ApprovalNeeded → AppEvent::ApprovalNeeded 等），再调用 dispatch_status_event（mod.rs:664-689）。

dispatch_status_event 是 SSE 路由策略核心：

- Some(user_id) → sse.broadcast_for_user(uid, event)（带 // projection-exempt: bridge dispatcher, scoped status update 注释，符合 .claude/rules/gateway-events.md）。
- None + multi_tenant_mode → WARN 并丢弃（不能跨租户广播）。
- None + 单租户 → sse.broadcast(event)（全局广播）。

6. SSE / WebSocket 推送到浏览器

SseManager（platform/sse.rs:44-148）内部用 tokio::sync::broadcast::Sender<ScopedEvent>：

pub fn broadcast_for_user(&self, user_id: &str, event: AppEvent) {
let _ = self.tx.send(self.next_scoped_event(Some(user_id.into()), event));
}

每个 ScopedEvent 带 id（{boot_uuid}:{seq}）、user_id、AppEvent。

订阅侧（/api/chat/events → chat_events_handler，mod.rs:474-499）：

let sse = state.sse.subscribe(Some(user.user_id), verbose, last_event_id)?;

subscribe 内部 tx.subscribe() 得到 BroadcastStream，过滤规则（sse.rs:188-200）：

- user_id=None（全局事件）→ 所有订阅者收。
- 订阅者 user_id=None → 收全部。
- 订阅者 user_id=Some(sub)，事件 user_id=Some(ev)，且 sub == ev → 收。
- 其他 → 跳过。
- verbose-only 事件（ToolResultFull、TurnMetrics）只发给 verbose=true 的连接（防止每条 tool call 复制 50KB）。

axum 把流装成 SSE 响应（30s keepalive，header X-Accel-Buffering: no、Cache-Control: no-cache），event: 字段 = AppEvent 变体名，id: 字段 = <boot>:<seq>，data: 字段 = JSON（#[serde(tag = "type")]）。

WebSocket 路径（/api/chat/ws → chat_ws_handler，mod.rs:501-547）：

1. 校验 Origin 必须是 localhost / 127.0.0.1 / [::1]（is_local_origin）。
2. axum WebSocketUpgrade::on_upgrade → platform::ws::handle_ws_connection。
3. 双任务：sender 把 subscribe_raw() 拿到的 AppEvent 流编码成 {"type":"event","event_type":"...","data":...} 推给客户端；receiver 把客户端消息解码为 WsClientMessage（message / approval），再走和 chat_send_handler 一样的路径（写 msg_tx 或调 inline gate resolver）。

7. v2 引擎路径的 SSE 投影

当 ENGINE_V2=true 时（bridge/router.rs:64-68 is_engine_v2_enabled），agent 调 handle_with_engine()，用 ironclaw_engine 的事件日志作为唯一可信源。投影层有两路：

1. 主对话：bridge/router.rs:6418 thread_event_to_app_events(event, thread_id) -> Vec<AppEvent> —— 唯一把 ThreadEvent 翻成 AppEvent 的函数（EventKind::StepStarted → Thinking，ActionExecuted → ToolStarted + ToolCompleted 等）。
2. gate 暂停后：spawn_post_park_continuation（bridge/router.rs:4922-5060）订阅 thread_manager.subscribe_events()，对匹配 thread_id 的事件跑 thread_event_to_app_events + redact_code_executed_secrets，再 sse.broadcast_for_user(&user_id, app_event)，带 // projection-exempt: bridge dispatcher, post-park event forwarding 注释。

这条规则被 .claude/rules/gateway-events.md 强制：除了源日志（bridge dispatcher / sandbox JobEvent / channel-lifecycle）和 transport-only 允许列表（Heartbeat、StreamChunk），其他 sse.broadcast* 调用必须带 // projection-exempt: <category>, <detail> 注释。

8. 子系统的 state.broadcast / state.sse 路径

GatewayChannel::broadcast(user_id, response)（mod.rs:1005-1045）用于主动推送（心跳、自修复、任务结果），如果 response.thread_id 缺失就用 store.get_or_create_assistant_conversation(user_id, "gateway") 兜底。HealthCheck / Shutdown 看 state.msg_tx.read().await / state.shutdown_tx。

9. 端到端时序

Browser POST /api/chat/send
└─ axum 路由 → auth_middleware → chat_send_handler
└─ 速率检查
└─ 构造 IncomingMessage (带 metadata.user_id, thread_id, attachments)
└─ tx.send(msg) → state.msg_tx
└─ 返回 202 ACCEPTED + message_id
[agent_loop.rs 主循环]
└─ message_stream.next() → 拿到 msg
└─ 中间件 (转写/文档提取)
└─ handle_message(msg)
├─ v1: ChatDelegate.run_agentic_loop()
│     └─ send_status(Thinking/ToolStarted/ToolResult/...)
│     └─ LLM 调用 + 工具执行
│     └─ respond_then_done(response, Status("Done"))
└─ v2: handle_with_engine()
└─ 投影 ThreadEvent → AppEvent → sse.broadcast_for_user
[ChannelManager 路由]
└─ respond / send_status → GatewayChannel.respond / send_status
└─ sse.broadcast_for_user(user_id, AppEvent::Response/...)
[SSE/WS 订阅者]
└─ SseManager.subscribe() 的 BroadcastStream 过滤
└─ axum SSE / WebSocket 帧发给浏览器
Browser EventSource / WebSocket 收到 frame → 渲染

10. 关键不变量（与 .claude/rules/*.md 对齐）

- with_* 必须在 start() 前——rebuild_state 替换 Arc<GatewayState>，已建连的 SSE sender 仍然有效（保留 sse 字段）但其他字段对运行中行为无意义。
- is_internal 不可伪造（channel.rs:62 注释），但这个 channel 不依赖它。
- 多租户隔离：dispatch_status_event + SseManager 的 user_id 过滤 + with_metadata 的 user_id 字符串覆写（防止 WASM channel 伪造 owner）。
- 审计优先：UI 启动的 mutation 走 ToolDispatcher（不在 GatewayChannel 主路径上，是 handlers 的职责）。
- Agent/UI 一致：state.sse.broadcast* 只能从源日志（bridge dispatcher、sandbox JobEvent、channel-lifecycle）发，注释豁免要写明 category。
- 顺序保证：respond_then_done 保证 Response 帧在 Done 之前到达（修复 #2079 SSE 早关闭 turn 的 bug）。

11. 关键文件索引

┌──────────────────────────────────┬──────────────────────────────────────────────────────┐
│              关注点              │                         文件                         │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ Channel 抽象                     │ src/channels/channel.rs:894-960                      │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ ChannelManager                   │ src/channels/manager.rs                              │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ GatewayChannel 定义 + trait impl │ src/channels/web/mod.rs:103-1064                     │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ axum 路由组合                    │ src/channels/web/platform/router.rs:113-624          │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ SSE 广播/订阅                    │ src/channels/web/platform/sse.rs:44-280              │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ WebSocket                        │ src/channels/web/platform/ws.rs:64-                  │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ 鉴权中间件                       │ src/channels/web/platform/auth.rs                    │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ Chat HTTP handlers               │ src/channels/web/features/chat/mod.rs                │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ 事件源日志投影                   │ src/bridge/router.rs:6418 thread_event_to_app_events │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ Agent 主循环                     │ src/agent/agent_loop.rs:1070-1620                    │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ Agentic loop                     │ src/agent/agentic_loop.rs、src/agent/dispatcher.rs   │
├──────────────────────────────────┼──────────────────────────────────────────────────────┤
│ 启动 wiring                      │ src/main.rs:887+                                     │
└──────────────────────────────────┴─────────────────────────────────────

是的。`chat_events_handler` 处理的就是浏览器发起 `GET /api/chat/events` 长连接订阅。

## 完整订阅流程

### 1. 浏览器发起 GET 请求

```js
// 前端（用 EventSource，因为 SSE 协议不能自定义 header）
const es = new EventSource('/api/chat/events?token=<gateway_token>');
es.addEventListener('response',      e => { /* 最终回复 */ });
es.addEventListener('stream_chunk',  e => { /* 流式 token */ });
es.addEventListener('tool_started',  e => { /* 工具开始 */ });
es.addEventListener('tool_completed',e => { /* 工具结束 */ });
es.addEventListener('thinking',      e => { /* 思考中 */ });
es.addEventListener('status',        e => { /* 状态 */ });
es.addEventListener('error',         e => { /* 错误，含 reconnect 触发 */ });
```

也可以走 WebSocket（`/api/chat/ws`），语义一样。

### 2. 路由命中 `chat_events_handler`

`platform/router.rs:187` 注册：

```rust
.route("/api/chat/events", get(chat_events_handler))
```

`features/chat/mod.rs:474-499` 处理器做的事：

```rust
let verbose = params.debug && user.role == "admin";   // 1. 决定能否看 verbose 流
let sse = state.sse.subscribe(
    Some(user.user_id),                               // 2. 按 user_id 过滤
    verbose,
    extract_last_event_id(&params, &headers),         // 3. 增量补帧
).ok_or((503, "Too many connections"))?;

Ok((
    [("X-Accel-Buffering", "no"), ("Cache-Control", "no-cache")],
    sse,                                              // 4. 装好的 axum::response::Sse 流
))
```

### 3. 鉴权

`/api/chat/events` 在 `protected` router 下，挂有 `auth_middleware`（`router.rs:486-489`）。中间件按顺序查：

1. `Authorization: Bearer <token>`（env-var token）
2. DB token（`DbAuthenticator`）
3. OIDC JWT header

`EventSource` 不能设 header，所以这个端点额外支持 `?token=xxx` 查询串（`platform/auth.rs::allows_query_token_auth()` 白名单）。

### 4. axum 写出 SSE 响应

`axum::response::Sse` 把流变成：

```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
X-Accel-Buffering: no

event: stream_chunk
id: <boot_uuid>:<seq>
data: {"type":"stream_chunk","content":"Hel","thread_id":"..."}

event: tool_started
id: <boot_uuid>:<seq>
data: {"type":"tool_started","name":"http","detail":"GET https://...","call_id":"...","thread_id":"..."}

event: response
id: <boot_uuid>:<seq>
data: {"type":"response","content":"Here is the answer.","thread_id":"..."}

（30s 没事件就发 : keepalive 注释行）
```

`event:` 字段 = `AppEvent` 变体名（`#[serde(tag = "type")]`），`id:` 字段 = `<boot>:<seq>`，让 `EventSource` 断线重连时通过 `Last-Event-ID` 头 / `?last_event_id=` 增量补帧。

### 5. 事件怎么流到这里

事件并不是 handler 主动发的。它在 `subscribe()` 那一刻**已经**和 `SseManager.tx` 这条广播总线连上了：

```
上游发布端：
  agent_loop / bridge router / dispatcher
    └─► GatewayChannel.respond() / send_status() / broadcast()
        └─► state.sse.broadcast_for_user(user_id, AppEvent)
            └─► broadcast::Sender.send(ScopedEvent { id, user_id, event })

下游订阅端（多个并存）：
  ┌─ /api/chat/events 浏览器 A    (subscribe → 独立 rx + user_id 过滤)
  ├─ /api/chat/events 浏览器 B    (subscribe → 独立 rx + user_id 过滤)
  ├─ /api/chat/ws     浏览器 C    (subscribe_raw → 独立 rx)
  └─ JobMonitor 内部订阅           (subscribe_raw → 独立 rx)
```

`tokio::sync::broadcast` 保证：
- **每个订阅者有独立的 lag 位置**，慢的浏览器断线/卡顿只会让自己的 `RecvError::Lagged` 丢帧，不会拖累其他订阅者。
- **新连接的浏览器只看从订阅那一刻起的新事件**——它"漏掉"的历史要从 `/api/chat/history` 拉，不在 SSE 这条路补。

## 答你的问题

**是**——`chat_events_handler` 处理的就是浏览器订阅 SSE 的入口。请求到达：

1. axum 路由表匹配到 `GET /api/chat/events`。
2. `auth_middleware` 验证 token。
3. `chat_events_handler` 调 `state.sse.subscribe(...)` 拿到一个**绑定了该用户 user_id 的** `Sse` 流。
4. axum 立刻把响应头（`200 OK` + `text/event-stream`）写回客户端，**连接保持打开**。
5. 之后只要 `broadcast_for_user` 给这个 user_id 发事件，浏览器就会实时收到 `data:` 帧，直到浏览器关闭 / 服务器 shutdown 才结束。

关键点：handler 在 `subscribe()` 之后**就返回了**，真正的数据推送由 `SseManager` + `broadcast::Sender` 在后台异步驱动。

4. WasmChannel

①类型链路相关


pub struct WasmChannel {
/// Channel name.
name: String,

    /// Runtime for WASM execution.
    runtime: Arc<WasmChannelRuntime>,

    /// Prepared module (compiled WASM).
    prepared: Arc<PreparedChannelModule>,

    /// Channel capabilities.
    capabilities: ChannelCapabilities,

    /// Channel configuration JSON (passed to on_start).
    /// Wrapped in RwLock to allow updating before start.
    config_json: RwLock<String>,

    /// Channel configuration returned by on_start.
    channel_config: RwLock<Option<ChannelConfig>>,

    /// Message sender (for emitting messages to the stream).
    /// Wrapped in Arc for sharing with the polling task.
    message_tx: Arc<RwLock<Option<mpsc::Sender<IncomingMessage>>>>,

    /// Pending responses (for synchronous response handling).
    pending_responses: RwLock<HashMap<Uuid, oneshot::Sender<String>>>,

    /// Rate limiter for message emission.
    /// Wrapped in Arc for sharing with the polling task.
    rate_limiter: Arc<RwLock<ChannelEmitRateLimiter>>,

    /// Shutdown signal sender.
    shutdown_tx: RwLock<Option<oneshot::Sender<()>>>,

    /// Polling shutdown signal sender (keeps polling alive while held).
    poll_shutdown_tx: RwLock<Option<oneshot::Sender<()>>>,

    /// Join handle for the active polling task so restarts can wait for the
    /// previous long-poll to exit before starting a replacement.
    poll_task: RwLock<Option<tokio::task::JoinHandle<()>>>,

    /// Websocket runtime shutdown signal sender.
    websocket_shutdown_tx: RwLock<Option<oneshot::Sender<()>>>,

    /// Host-managed websocket outbound sender used by `on_respond` and
    /// `on_poll` to emit protocol-specific websocket frames.
    websocket_outbound_tx: Arc<RwLock<Option<mpsc::Sender<String>>>>,

    /// Serializes websocket-triggered poll executions.
    websocket_poll_lock: Arc<Mutex<()>>,

    /// Registered HTTP endpoints.
    endpoints: RwLock<Vec<RegisteredEndpoint>>,

    /// Injected credentials for HTTP requests (e.g., bot tokens).
    /// Keys are placeholder names like "TELEGRAM_BOT_TOKEN".
    /// Wrapped in Arc for sharing with the polling task.
    credentials: Arc<RwLock<HashMap<String, String>>>,

    /// Background task that repeats typing indicators every 4 seconds.
    /// Telegram's "typing..." indicator expires after ~5s, so we refresh it.
    typing_task: RwLock<Option<tokio::task::JoinHandle<()>>>,

    /// Generated images staged from status updates until the final channel
    /// response can deliver them together with the assistant's text.
    pending_generated_image_attachments: Arc<Mutex<HashMap<String, Vec<String>>>>,

    /// Pairing store for DM pairing (guest access control).
    pairing_store: Arc<PairingStore>,

    /// In-memory workspace store persisting writes across callback invocations.
    /// Ensures WASM channels can maintain state (e.g., polling offsets) between ticks.
    workspace_store: Arc<ChannelWorkspaceStore>,

    /// Serializes callback execution for a single channel instance.
    ///
    /// Some channel state is read-modify-written through the shared workspace
    /// store, so overlapping callbacks can otherwise lose updates.
    callback_lock: Arc<tokio::sync::Mutex<()>>,

    /// Last-seen message metadata (contains chat_id for broadcast routing).
    /// Populated from incoming messages so `broadcast()` knows where to send.
    last_broadcast_metadata: Arc<tokio::sync::RwLock<Option<String>>>,

    /// Settings store for persisting broadcast metadata across restarts.
    settings_store: Option<Arc<dyn crate::db::SettingsStore>>,

    /// Stable owner scope for persistent data and owner-target routing.
    owner_scope_id: String,

    /// Channel-specific actor ID that maps to the instance owner on this channel.
    /// Wrapped in `Arc` so spawned polling/websocket tasks can read the current
    /// value after pairing approval without capturing a stale clone.
    owner_actor_id: Arc<tokio::sync::RwLock<Option<String>>>,

    /// User bound to a single-login channel such as WeChat.
    channel_bound_user_id: Arc<RwLock<Option<String>>>,

    /// Secrets store for host-based credential injection.
    /// Used to pre-resolve credentials before each WASM callback.
    secrets_store: Option<Arc<dyn SecretsStore + Send + Sync>>,
}
---

# WasmChannel 必看字段清单（博客版）

> 文件：`src/channels/wasm/wrapper.rs:782-885`、`mod.rs:120`、`capabilities.rs:24-59`
> 一句话：WasmChannel 是宿主进程内对**一个 WASM 模块实例**的 Rust 侧封装，承载 Channel trait 的全部交互；它的所有字段都围绕"如何把外部消息安全、可控、可观测地喂给一个沙箱里的插件"展开。

## 1. 标识与"是什么 channel"

| 字段           | 类型                         | 用途                                                      | 写博客要点                                                                    |
| -------------- | ---------------------------- | --------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `name`         | `String`                     | channel 名（= WASM 模块名），Channel trait 暴露给上层路由 | 决定 ChannelManager 里按什么 key 注册，Dispatch 时按 `msg.channel` 找到它     |
| `capabilities` | `ChannelCapabilities`        | **核心安全边界**，声明本 channel 能做什么                 | 见 §6                                                                         |
| `prepared`     | `Arc<PreparedChannelModule>` | 编译好的 WASM component                                   | 一次编译、多次实例化；回调时 `new_store` + `instantiate` 拿新实例（每次隔离） |

## 2. 运行时载体（"消息怎么进出"）

| 字段                | 类型                                                 | 用途                                                                                                 |
| ------------------- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `runtime`           | `Arc<WasmChannelRuntime>`                            | wasmtime 引擎 + 链接器 + host functions（logging、http、workspace…）                                 |
| `message_tx`        | `Arc<RwLock<Option<mpsc::Sender<IncomingMessage>>>>` | WASM 内部 `emit_message` 写入 → 转成 IncomingMessage 投到 agent 主循环                               |
| `pending_responses` | `RwLock<HashMap<Uuid, oneshot::Sender<String>>>`     | **同步响应通道**：`emit_message` 配 `request_response` 模式时挂载的 one-shot，等待 host 立即给个回执 |
| `rate_limiter`      | `Arc<RwLock<ChannelEmitRateLimiter>>`                | emit 节流（默认 100/min、5000/h）——防恶意/写崩的 WASM 把 agent 灌爆                                  |

> 写稿角度：这三件套是"一个沙箱插件"和"主进程"**唯一的对话窗口**。`Arc<RwLock<Option<...>>>` 模式是为了让 polling / websocket 任务能 clone 出自己的 sender。

## 3. 长生命周期任务句柄（"后台跑什么"）

| 字段                                                                      | 类型                                  | 启动时机                | 关闭方式                                                                                                     |
| ------------------------------------------------------------------------- | ------------------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------ |
| `shutdown_tx`                                                             | `RwLock<Option<oneshot::Sender<()>>>` | `Channel::start()` 末尾 | `Channel::shutdown()` 时 fire                                                                                |
| `poll_shutdown_tx`                                                        | `RwLock<Option<oneshot::Sender<()>>>` | 启动 polling 时         | 仅在 polling 关闭时                                                                                          |
| `poll_task: RwLock<Option<JoinHandle<()>>>`                               | 轮询任务句柄                          | start_polling 后        | **关键**：替换轮询配置时要先 `await` 旧的退出（避免重叠 #2557 类问题）                                       |
| `websocket_shutdown_tx` / `websocket_outbound_tx` / `websocket_poll_lock` | oneshot + mpsc + Mutex                | 启动 ws runtime 时      | `WebsocketStartDecision::MissingAuth` 时**绝不启动**（注释明说 #2557 教训：Discord 4003 拒后不能死循环重试） |

> 写稿角度：**"installed ≠ active"** 规则的真实落地。discovery 时只装载模块；activation 时才创建这些后台任务句柄；auth 失败就保持 `None`，等用户写完 token 后由 `refresh_active_channel` 重新激活。

## 4. 配置 / 凭据（"channel 自身的状态"）

| 字段                                                | 类型                                                     | 用途                                                                                    |
| --------------------------------------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `config_json: RwLock<String>`                       | 配置 JSON（start 前可改、start 后改不动）                | 传给 `on_start` 的初始配置；启动后可由 `with_runtime_config` 注入 `tunnel_url` 等动态值 |
| `channel_config: RwLock<Option<ChannelConfig>>`     | `on_start` 返回的配置（HTTP endpoints + poll + ws 决策） | `start()` 时拿到，注册 endpoint、起轮询、起 ws                                          |
| `credentials: Arc<RwLock<HashMap<String, String>>>` | URL 注入占位符                                           | 形如 `{TELEGRAM_BOT_TOKEN}` 在 URL / header 中被替换；**WASM 永远拿不到明文**           |
| `secrets_store: Option<Arc<dyn SecretsStore>>`      | 宿主侧的 secrets 存储                                    | `with_secrets_store` 注入；**回调时**按 host_patterns 预解析、自动注入到出站 HTTP       |
| `secrets_store`-类预解析                            | `Vec<ResolvedHostCredential>`                            | host + path 匹配 → header/query 注入；多匹配按 path specificity 排序                    |

> 写稿角度：把"凭据"切成两层。**`credentials`**：插件作者在 WASM 里写 `{TELEGRAM_BOT_TOKEN}` 的占位（XML 模板级别）。**`secrets_store`**：宿主在调用前按 host 匹配自动注入（系统级能力）。这两层是 host **永远不让 WASM 看到明文**的 zero-exposure 模型。

## 5. 路由与状态

| 字段                                                                            | 类型                                        | 用途                                                                                                 |
| ------------------------------------------------------------------------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `endpoints: RwLock<Vec<RegisteredEndpoint>>`                                    | 在 host 注册的 HTTP webhook 路径            | start() 时遍历 `channel_config.http_endpoints`，按 `capabilities.is_path_allowed` 过滤后注册         |
| `workspace_store: Arc<ChannelWorkspaceStore>`                                   | **callback 之间共享的 KV 存储**             | 跨 `on_poll` / `on_http_request` 保存 offset、token 等                                               |
| `callback_lock: Arc<Mutex<()>>`                                                 | 串行化同一个 channel 实例的所有回调         | 防 workspace 读写竞争                                                                                |
| `last_broadcast_metadata: Arc<RwLock<Option<String>>>`                          | 最近一次消息的 chat_id 等元数据             | `Channel::broadcast()` 不知道往哪发时，从这里取上次的目的地                                          |
| `settings_store: Option<Arc<dyn SettingsStore>>`                                | 把 broadcast metadata 持久化                | 跨重启恢复                                                                                           |
| `owner_scope_id: String`                                                        | 稳定的所有者 scope                          | 决定 settings key、broadcast 路由                                                                    |
| `owner_actor_id: Arc<RwLock<Option<String>>>`                                   | channel 侧 actor id ↔ 宿主 owner            | **pairing 通过后会更新**；用 `Arc<RwLock<...>>` 是为了让 polling/WS 长任务读到新值而不是捕获旧 clone |
| `channel_bound_user_id: Arc<RwLock<Option<String>>>`                            | 单点登录 channel（如 WeChat）绑定的 user    | 来自 config_json 解析                                                                                |
| `pending_generated_image_attachments: Arc<Mutex<HashMap<String, Vec<String>>>>` | 工具生成的图片按"最终回复"打包发出          | 见 §7 注释                                                                                           |
| `pairing_store: Arc<PairingStore>`                                              | DM 配对（guest 访问控制）                   | WebChat 可走 `/api/pairing/{channel}/approve` 批准                                                   |
| `typing_task: RwLock<Option<JoinHandle<()>>>`                                   | 每 4s 重发 typing 指示（Telegram ~5s 过期） | `send_status(Thinking)` 时启动；terminal 状态时取消                                                  |

> 写稿角度：这一组是"channel 自洽性"的全部——polling 拿到一条消息、emit 给 agent、agent 回信、回到 on_respond、可能要发图。**`workspace_store` + `callback_lock` + `owner_actor_id: Arc<RwLock>`** 是最容易写错的三个。

## 6. ChannelCapabilities（最该截图贴博客）

`capabilities.rs:24-59`。**这一段决定整个 WASM channel 系统的安全姿态**，建议博客里整个截图：

| 字段                      | 默认                          | 含义                                                                                                                   |
| ------------------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `tool_capabilities`       | `ToolCapabilities::default()` | 复用的工具能力（HTTP 白名单、secrets 访问、workspace_read…）                                                           |
| `allowed_paths`           | `[]`                          | HTTP webhook 路径白名单。`is_path_allowed` 严格相等匹配，**默认 0 个**意味着不开放 webhook                             |
| `allow_polling`           | `false`                       | 是否允许长轮询                                                                                                         |
| `min_poll_interval_ms`    | `30_000`（30s）               | 轮询最小间隔。`validate_poll_interval` 会强制 clamp，**不能低于 30s**（防失控）                                        |
| `workspace_prefix`        | `""` / `channels/<name>/`     | 命名空间：所有写入自动加前缀                                                                                           |
| `durable_workspace_paths` | `[]`                          | **跨重启持久化**的白名单。默认全空，意思是 callback workspace 全在内存，重启就丢（设计意图：防 token/secret 误持久化） |
| `emit_rate_limit`         | 100/min, 5000/h               | emit_message 节流                                                                                                      |
| `max_message_size`        | 64 KB                         | 单条 emit 上限                                                                                                         |
| `callback_timeout`        | 30s                           | 每次 WASM 回调的最大执行时间                                                                                           |

> **写稿金句**：`validate_workspace_path` 三个拒绝规则（绝对路径、`..`、NUL 字节） + `is_durable_workspace_path` 白名单 = "默认 deny、显式 allow"。

## 7. Channel trait 实现的 5 个方法（行为骨架）

`wrapper.rs:3681-3930`：

| 方法            | 关键步骤                                                                                                                                                                                                                      | 设计点                                                                              |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `name()`        | 返回 `&self.name`                                                                                                                                                                                                             | 简单                                                                                |
| `start()`       | **先**调 `on_start`（失败直接返回，**不创建** message_tx）→ 再 `mpsc::channel(256)` → 写 message_tx、shutdown_tx → 注册 endpoints（按 capabilities 过滤）→ 起 polling → 决定 ws（`MissingAuth` **不启**）→ 返回 MessageStream | **顺序很关键**：`on_start` 失败时不能留 orphan tx（注释 3693-3695 明说）            |
| `respond()`     | 包装 `call_on_respond`                                                                                                                                                                                                        | 收到 agent 回复                                                                     |
| `broadcast()`   | 包装 `call_on_broadcast`                                                                                                                                                                                                      | 主动推送，目的地从 `last_broadcast_metadata` 推                                     |
| `send_status()` | 包装 `call_on_status`                                                                                                                                                                                                         | Thinking → 起 4s typing 重发；terminal → 取消 typing；StreamChunk → no-op（防刷屏） |
| `shutdown()`    | fire `shutdown_tx` / `websocket_shutdown_tx` / `poll_shutdown_tx` → await 任务退出                                                                                                                                            |                                                                                     |

## 8. "必看" 速记表（直接贴博客）

```
┌─ 身份 ───────────┐
│ name             │ ChannelManager 的 key
│ capabilities     │ 安全边界（必看 §6）
│ prepared/runtime │ WASM 引擎 + 编译产物
└──────────────────┘
┌─ 对话窗口 ───────┐
│ message_tx       │ WASM → agent
│ pending_responses│ 同步回执
│ rate_limiter     │ 流量护栏
└──────────────────┘
┌─ 长任务 ─────────┐
│ *_shutdown_tx    │ 三类 one-shot
│ poll_task        │ 替换时必须 await 旧
│ websocket_*      │ MissingAuth 不启
└──────────────────┘
┌─ 凭据 ───────────┐
│ credentials      │ 占位符替换（XML 模板级）
│ secrets_store    │ host 按 host_patterns 注入
└──────────────────┘
┌─ 自洽状态 ───────┐
│ workspace_store + callback_lock  │ 跨回调串行 KV
│ last_broadcast_metadata          │ 主动推送给谁
│ owner_actor_id: Arc<RwLock>      │ pairing 后可变
│ typing_task                      │ Telegram 4s 重发
│ durable_workspace_paths          │ 默认空 = 不持久化
└──────────────────┘
```

启动    
async fn start(&self) -> Result<MessageStream, ChannelError> {
// Restore broadcast metadata from settings (survives restarts)
self.load_broadcast_metadata().await;
恢复上次运行记住的"我最后把消息发到哪个 chat_id"。这是个跨重启的细节：

- 主动推送（broadcast）时 WASM 插件不知道自己该往哪发。
- 解决方法：每收到一条入站消息时把"目标 chat_id"存进 last_broadcast_metadata，并持久化到 settings_store。
- 重启后从 settings_store 读回来，这样新一次启动就能直接 broadcast 到对的地方。


        // Call on_start BEFORE creating message_tx so a failed start
        // doesn't leave an orphaned sender with a dropped receiver.
        // (If on_start fails, the rx would be dropped on error return
        // but message_tx would keep the tx alive — causing is_closed=true
        // on any subsequent polling attempt.)
        let config = self
            .call_on_start()
            .await
            .map_err(|e| ChannelError::StartupFailed {
                name: self.name.clone(),
                reason: e.to_string(),
            })?;
这是整个 start 流程里最重的一步：

- 新建一个 wasmtime Store（隔离的线性内存 + HostState）。
- 加载 host functions（日志、http、workspace…）。
- 实例化 prepared 模块（编译过的 artifact）。
- 传 config_json 进 WASM 调它的 on_start 导出函数。
- 拿回 ChannelConfig { http_endpoints, poll, websocket, display_name, ... }。



        // Create message channel — only after on_start succeeds
        let (tx, rx) = mpsc::channel(256);
        *self.message_tx.write().await = Some(tx);
3. 创建 mpsc 并写入 self.message_tx

let (tx, rx) = mpsc::channel(256);                                                                                                                                                                     
*self.message_tx.write().await = Some(tx);

- tx 存到 WasmChannel 字段里，给 polling 任务 / websocket 任务用。
- rx 留在本地，包成 ReceiverStream 后面返回给 ChannelManager。
- buffer 256：积压超 256 条未消费消息就阻塞 emit（背压）。
- message_tx 字段是 Arc<RwLock<Option<...>>> 的原因就在这里：polling 任务、websocket 任务、未来 refresh_active_channel 都要能 clone。




        // Create shutdown channel
        let (shutdown_tx, _shutdown_rx) = oneshot::channel();
        *self.shutdown_tx.write().await = Some(shutdown_tx);

        // Store the config
        *self.channel_config.write().await = Some(config.clone());

        // Register HTTP endpoints
        let mut endpoints = Vec::new();
        for endpoint in &config.http_endpoints {
            // Validate path is allowed
            if !self.capabilities.is_path_allowed(&endpoint.path) {
                tracing::warn!(
                    channel = %self.name,
                    path = %endpoint.path,
                    "HTTP endpoint path not allowed by capabilities"
                );
                continue;
            }

            endpoints.push(RegisteredEndpoint {
                channel_name: self.name.clone(),
                path: endpoint.path.clone(),
                methods: endpoint.methods.clone(),
                require_secret: endpoint.require_secret,
            });
        }
        *self.endpoints.write().await = endpoints;
5. 注册 HTTP endpoints（按 capabilities 过滤）

for endpoint in &config.http_endpoints {                                                                                                                                                               
if !self.capabilities.is_path_allowed(&endpoint.path) {                                                                                                                                            
tracing::warn!(...);                                                                                                                                                                           
continue;          // 不在白名单 → 跳过                                                                                                                                                        
}                                                                                                                                                                                                  
endpoints.push(RegisteredEndpoint { ... });                                                                                                                                                        
}

WASM 插件在 on_start 里声明想注册哪些路径，但 host 端按 capabilities.allowed_paths 强制二次过滤。

- 插件不能偷偷注册 /admin/*、/internal/*。
- 没在白名单里的路径只 warn，不报错（设计意图：插件可以"声明宽、用得窄"，但 host 真正暴露的只能是 cap 允许的那批）。

注册结果存进 endpoints: RwLock<Vec<RegisteredEndpoint>>，host 的 HTTP server 启动时（platform/router.rs 的 webhook 路由）会查这张表来分发请求到对应 channel。






        // Start polling if configured
        if let Some(poll_config) = &config.poll
            && poll_config.enabled
        {
            let interval = self
                .capabilities
                .validate_poll_interval(poll_config.interval_ms)
                .map_err(|e| ChannelError::StartupFailed {
                    name: self.name.clone(),
                    reason: e,
                })?;

            // Create shutdown channel for polling and store the sender to keep it alive
            let (poll_shutdown_tx, poll_shutdown_rx) = oneshot::channel();
            *self.poll_shutdown_tx.write().await = Some(poll_shutdown_tx);

            let handle = self.start_polling(
                Duration::from_millis(interval as u64),
                poll_shutdown_rx,
                Arc::clone(&self.owner_actor_id),
            );
            *self.poll_task.write().await = Some(handle);
        }

        match websocket_start_decision(
            &self.capabilities,
            self.secrets_store.as_deref(),
            &self.owner_scope_id,
        )
        .await
        {
            WebsocketStartDecision::NotConfigured => {}
            WebsocketStartDecision::Spawn(websocket_config) => {
                let (websocket_shutdown_tx, websocket_shutdown_rx) = oneshot::channel();
                *self.websocket_shutdown_tx.write().await = Some(websocket_shutdown_tx);
                self.start_websocket_runtime(
                    websocket_config,
                    websocket_shutdown_rx,
                    Arc::clone(&self.owner_actor_id),
                );
            }
            WebsocketStartDecision::MissingAuth { credential_name } => {
                // Skip the spawn entirely: the runtime would connect, be
                // rejected (e.g. Discord 4003), and retry forever (#2557).
                // Recovery happens through the normal activation/restart
                // path once the credential is written.
                tracing::warn!(
                    channel = %self.name,
                    credential_name = %credential_name,
                    "Skipping websocket runtime start: required credential is not present"
                );
            }
            WebsocketStartDecision::MalformedConfig { reason } => {
                tracing::warn!(
                    channel = %self.name,
                    reason = %reason,
                    "Skipping websocket runtime start: malformed capability configuration"
                );
            }
        }

        tracing::info!(
            channel = %self.name,
            display_name = %config.display_name,
            endpoints = config.http_endpoints.len(),
            "WASM channel started"
        );

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

够了。三种"消息从外部世界到 agent"机制讲清楚。

# Webhook / Polling / WebSocket：WASM channel 的三种入站方式

> 全部从 `src/channels/wasm/` 读出来。这三种机制**不是给宿主用的**，是给 WASM channel 插件用的"如何从外部 IM 平台拿到消息"的三种通讯模型。

## 一句话对比

| 机制          | 触发方向    | 适用平台                                                                | 核心特征                               |
| ------------- | ----------- | ----------------------------------------------------------------------- | -------------------------------------- |
| **Webhook**   | 外部 → 宿主 | 平台支持回调 URL 时（Slack、Telegram、GitHub）                          | 宿主**暴露 HTTP 路径**，平台主动推过来 |
| **Polling**   | 宿主 → 平台 | 平台**不**支持回调时（一些无 webhook 的老 IM）                          | 宿主**定时调**平台的 list API          |
| **WebSocket** | 双向长连接  | 平台提供 ws gateway 时（Discord v10、Slack Apps 用 Events API ws mode） | 宿主**保持一条长连接**，双向推         |

## 1. Webhook：宿主开门让外面"敲门"

### 流程

```
用户发消息给 Telegram bot
        │
        ▼
Telegram 服务器主动 POST https://你的域名/webhook/telegram
        │
        ▼
WasmChannelRouter 收到请求
  ├─ 查 path_to_channel 找到 "telegram" channel
  ├─ 验 X-Telegram-Bot-Api-Secret-Token 是否匹配
  └─ call_on_http_request(channel, body) ──► WASM 插件处理
                                                  │
                                                  ▼
                                          emit_message(...) ──► 投到 mpsc
```

### 关键代码

`src/channels/wasm/wrapper.rs:3715-3735` (`start()` 里的 endpoint 注册)：

```rust
for endpoint in &config.http_endpoints {
    if !self.capabilities.is_path_allowed(&endpoint.path) {  // ← 二次过滤
        continue;                                              //    WASM 声明不算数
    }
    endpoints.push(RegisteredEndpoint { ... });
}
```

`src/channels/wasm/router.rs:24-33` 的 `RegisteredEndpoint`：

```rust
pub struct RegisteredEndpoint {
    pub channel_name: String,
    pub path: String,                // e.g., "/webhook/slack"
    pub methods: Vec<String>,        // GET / POST
    pub require_secret: bool,        // ← 必须验签
}
```

**两层 secret 校验**：
- 普通 channel：`X-Webhook-Secret: <shared_secret>` 头校验。
- Telegram：`X-Telegram-Bot-Api-Secret-Token: <token>` 头校验（`router.rs:62-67, 91, 154` 注释里明说 Telegram 用专用头）。
- Slack：用 `signature_keys` 做 Ed25519 验签（`router.rs:64`）或 `hmac_secrets` 做 HMAC-SHA256 验签（`router.rs:67`）。

### WASM 插件里要做什么

在 `on_start` 的 `ChannelConfig.http_endpoints` 里**声明**：

```rust
ChannelConfig {
    http_endpoints: vec![HttpEndpointConfig {
        path: "/webhook/slack".into(),
        methods: vec!["POST".into()],
        require_secret: true,
    }],
    ..Default::default()
}
```

然后实现 `on_http_request(payload) -> Vec<EmitMessage>`，把 webhook 原始 JSON 解析成 IncomingMessage。

### 限制

- 需要**公网可达**的域名（生产配 `tunnel.rs::start_managed_tunnel` 走 cloudflared/ngrok/tailscale）。
- 路径必须在 `capabilities.allowed_paths` 白名单里。
- 验签**必须开**（`require_secret: true` 是默认）。

## 2. Polling：宿主主动"上门问"

### 流程

```
tokio::time::interval(30s) 触发
        │
        ▼
spawn 的 polling 任务 tick
  ├─ resolve_channel_host_credentials(...)        ← 预解析 host 凭据
  ├─ execute_poll → 新建 wasmtime Store
  │                 实例化 prepared 模块
  │                 调 WASM 导出函数 on_poll
  │                 ← 拿回 Vec<EmitMessage>
  └─ dispatch_emitted_messages → message_tx.send
                                       │
                                       ▼
                                   agent 主循环
```

### 关键代码

`src/channels/wasm/wrapper.rs:3291-3370` `start_polling`：

```rust
tokio::spawn(async move {
    let mut interval_timer = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = interval_timer.tick() => {
                // 1. 预解析 host 凭据
                let host_credentials = resolve_channel_host_credentials(...);
                // 2. 用新实例跑 on_poll
                let result = Self::execute_poll(...).await;
                // 3. 把 emit 出来的消息投到 message_tx
                if !emitted_messages.is_empty() {
                    Self::dispatch_emitted_messages(...);
                }
            }
        }
    }
});
```

### 硬性约束

- `capabilities.allow_polling: false` 是**默认**（`capabilities.rs:33, 67`）。要开 polling 的 channel 必须**显式** `with_polling(min_ms)`。
- `min_poll_interval_ms` 强制 ≥ 30_000ms（`MIN_POLL_INTERVAL_MS = 30s`，`capabilities.rs:14, 96`）。`validate_poll_interval` 强制 clamp。
- **不能高于** capabilities 上限。host 决定节奏，不让 WASM 自己写间隔。
- **每次 tick 都新建 WASM 实例**（注释 `wrapper.rs:3288-3290` 明确说 "fresh instance per callback" 模式）。所以 polling 实例的 workspace 状态**不会**留在 wasmtime linear memory 里，要存到 `ChannelWorkspaceStore`（即 `wrapper.rs:854-856` 的 `workspace_store`）。

### WASM 插件里要做什么

在 `on_start` 的 `ChannelConfig.poll` 里**声明**：

```rust
ChannelConfig {
    poll: Some(PollConfig {
        enabled: true,
        interval_ms: 60_000,
    }),
    ..Default::default()
}
```

实现 `on_poll() -> Vec<EmitMessage>`，自己用 `http_request` host function 去平台拉消息（带 cursor / offset 保存在 workspace 里）。

### 限制

- **延迟高**：最坏 = 30s 才看到消息。
- **费资源**：每次 tick 实例化 WASM 一次。
- 但**最通用**——任何有 list API 的平台都能包。

## 3. WebSocket：双向长连接

### 流程

```
spawn 的 ws runtime 任务启动
        │
        ▼
建 tokio TcpStream → wss://platform/gateway
        │
        ▼
鉴权（HELLO/Identify/握手协议，每平台不同）
        │
   ┌────┴────────────────────────────┐
   │                                 │
   ▼ 平台发来的消息                   ▼ 自己主动发
dispatch_incoming (调 on_respond)    via websocket_outbound_tx ──► ws 帧
   │                                 │
   └─ emit_message ─► message_tx ──► agent
        │
        ▼ (agent 回复)
on_respond(agent_response) ──► websocket_outbound_tx.send(json)
```

### 关键代码

`src/channels/wasm/wrapper.rs:1501-1545` `start_websocket_runtime`：

```rust
tokio::spawn(async move {
    let mut reconnect_attempt = 0u32;
    // ... 持久化未发队列
    // 主循环：建连 → 鉴权 → 收发 → 断线重连
});
```

`wrapper.rs:1527-1536` 注释区明确：

```rust
// websocket_outbound_tx 是 host-managed 双向出站
// 让 on_respond / on_poll 都能 emit protocol-specific 帧
// 而不是只走 agent 主流程
```

### 三个特殊字段（其他机制没的）

| 字段                                                               | 类型    | 作用                                                                |
| ------------------------------------------------------------------ | ------- | ------------------------------------------------------------------- |
| `websocket_shutdown_tx`                                            | oneshot | 单独关闭 ws runtime（不影响 polling）                               |
| `websocket_outbound_tx: Arc<RwLock<Option<mpsc::Sender<String>>>>` | mpsc    | WASM 侧发帧的通道                                                   |
| `websocket_poll_lock: Arc<Mutex<()>>`                              | Mutex   | **串行化** ws 触发的 poll 执行（防同一 channel 实例并发跑 on_poll） |

### 四种 start 决策（`wrapper.rs:3761-3796`）

| 决策                              | 触发条件      | 行为                                              |
| --------------------------------- | ------------- | ------------------------------------------------- |
| `NotConfigured`                   | 插件没声明 ws | 跳过                                              |
| `Spawn(cfg)`                      | 配置 ok       | 起 ws runtime                                     |
| `MissingAuth { credential_name }` | 凭据没写      | **warn 后跳过**（**不能死循环重试**，#2557 教训） |
| `MalformedConfig { reason }`      | 配置坏        | warn 后跳过                                       |

### WASM 插件里要做什么

在 `on_start` 的 `ChannelConfig.websocket` 里声明（具体 schema 见 `capabilities.rs` / `schema.rs`），实现 `on_respond(agent_message)` 用 `websocket_outbound` host function 发帧。

### 限制

- 需要**稳定长连接**（NAT 穿透、proxy timeout 都要算上）。
- 平台侧 ws gateway 可能要求心跳。
- **MissingAuth 不能起**：这是整个 start() 流程里**最反直觉**的设计决策。`wrapper.rs:3778-3788` 注释把教训写死：

```rust
// Skip the spawn entirely: the runtime would connect, be
// rejected (e.g. Discord 4003), and retry forever (#2557).
// Recovery happens through the normal activation/restart
// path once the credential is written.
```

## 三者怎么选（写博客的对比段）

```
                ┌─ 平台支持 webhook？
                │   ├─ 是 ──► Webhook  ← 首选（延迟低、host 不耗资源）
                │   └─ 否 ──► 平台支持 ws？
                │              ├─ 是 ──► WebSocket
                │              └─ 否 ──► Polling  ← 兜底
                ▼
        全部受 capabilities 白名单 + host 二次校验约束
```

| 维度      | Webhook                              | WebSocket                 | Polling                    |
| --------- | ------------------------------------ | ------------------------- | -------------------------- |
| 延迟      | 实时（<1s）                          | 实时（<1s）               | 30s+                       |
| host 资源 | 极低（被动接请求）                   | 中（持长连）              | 中（定时实例化 WASM）      |
| 平台要求  | 公网 URL                             | ws gateway                | list API                   |
| 凭据校验  | host 验签（secret / Ed25519 / HMAC） | 握手时鉴权                | 每次请求带 token           |
| 限流      | 由平台控制                           | 由平台控制                | **host 强制 ≥ 30s**        |
| 错处理    | 5xx 让平台重试                       | 自动重连 + 未发队列持久化 | tick 失败 → 下个 tick 继续 |
| 缺凭据    | endpoint 不暴露                      | **不启 ws**（不重试）     | tick 失败                  |

## 三者共享的 host 侧安全姿态

不管哪种入站方式，host **永远**在中间做这些事（写博客可以列成"host 不变式"）：

1. **WASM 声明什么 ≠ host 给什么**：`capabilities.allowed_paths` / `allow_polling` / ws 决策都是二次过滤。
2. **每个回调都是新 WASM 实例**：线性内存/句柄不残留（`wrapper.rs:3288-3290` 注释），状态走 `workspace_store`。
3. **凭据 zero-exposure**：WASM 拿不到明文，要么是占位符替换、要么是 host 在出站 HTTP 时按 host_patterns 注入。
4. **错误不重试到死**：webhook 失败 → 5xx 让平台重试；ws 缺凭据 → **不启**；polling tick 失败 → 下个 tick 重试（30s 起步）。

## 给博客的小标题

> "WASM channel 的三种入站姿势：Webhook 是开门接客，WebSocket 是长聊专线，Polling 是定时查岗。"

    async fn execute_on_start_with_state(//执行on_start
        &self,
    ) -> Result<(Result<ChannelConfig, WasmChannelError>, ChannelHostState), WasmChannelError> {
        let _callback_guard = self.callback_lock.lock().await;
        self.load_durable_workspace_snapshot().await;

        let runtime = Arc::clone(&self.runtime);
        let prepared = Arc::clone(&self.prepared);
        let capabilities = Self::inject_workspace_reader(&self.capabilities, &self.workspace_store);
        let config_json = self.config_json.read().await.clone();
        let timeout = self.runtime.config().callback_timeout;
        let channel_name = self.name.clone();
        let credentials = self.get_credentials().await;
        let host_credentials = resolve_channel_host_credentials(
            &self.capabilities,
            self.secrets_store.as_deref(),
            &self.owner_scope_id,
        )
        .await;
        let pairing_store = self.pairing_store.clone();
        let workspace_store = self.workspace_store.clone();
        let websocket_outbound_tx = self.websocket_outbound_tx.read().await.clone();

        let (config_result, host_state, committed_paths) =
            tokio::time::timeout(timeout, async move {
                tokio::task::spawn_blocking(move || {
                    let mut store = Self::create_store(
                        &runtime,
                        &prepared,
                        &capabilities,
                        credentials,
                        host_credentials,
                        pairing_store,
                        websocket_outbound_tx,
                    )?;
                    let instance = Self::instantiate_component(&runtime, &prepared, &mut store)?;

                    let channel_iface = instance.near_agent_channel();
                    let config_result = channel_iface
                        .call_on_start(&mut store, &config_json)
                        .map_err(|e| Self::map_wasm_error(e, &prepared.name, prepared.limits.fuel))
                        .and_then(|wasm_result| match wasm_result {
                            Ok(wit_config) => Ok(convert_channel_config(wit_config)),
                            Err(err_msg) => Err(WasmChannelError::CallbackFailed {
                                name: prepared.name.clone(),
                                reason: err_msg,
                            }),
                        });

                    let mut host_state =
                        Self::extract_host_state(&mut store, &prepared.name, &capabilities);
                    let committed_paths =
                        Self::commit_callback_workspace_writes(&mut host_state, &workspace_store);

                    Ok::<_, WasmChannelError>((config_result, host_state, committed_paths))
                })
                .await
                .map_err(|e| WasmChannelError::ExecutionPanicked {
                    name: channel_name.clone(),
                    reason: e.to_string(),
                })?
            })
            .await
            .map_err(|_| WasmChannelError::Timeout {
                name: self.name.clone(),
                callback: "on_start".to_string(),
            })??;

        self.persist_durable_workspace_snapshot_if_needed(&committed_paths)
            .await;

        Ok((config_result, host_state))
    }
execute_on_start_with_state 函数详解

这个函数位于 src\channels\wasm\wrapper.rs:2030，是 WASM 通道（不是工具）的 wrapper。它的作用是：在阻塞线程里实例化 WASM 组件并执行它的 on_start 回调，拿到通道运行期所需的 ChannelConfig（display_name、HTTP 端点、轮询配置等）。

整体流程

拿到互斥锁
↓
加载持久化的 workspace 快照
↓
准备所有回调所需的 Arc / 凭证 / 快照
↓
tokio::time::timeout(callback_timeout, {
spawn_blocking {
创建 Store
实例化 WASM Component
调用 on_start(config_json) → wit_config
转 wit_config → ChannelConfig
抽取 host_state
提交回调期间产生的 workspace 写入
}
})
↓
持久化新的 workspace 快照
↓
返回 (config_result, host_state)

分步解读

1. 互斥与快照恢复（2033–2034 行）

let _callback_guard = self.callback_lock.lock().await;
self.load_durable_workspace_snapshot().await;
- callback_lock 保证同一个 channel 不会并发跑两次回调（on_start / on_http_request 共享）。
- load_durable_workspace_snapshot 把上次持久化进 linear memory 的 workspace 状态恢复出来，让回调看到一致的环境。

2. 准备回调上下文（2036–2051 行）

逐项收集调用 on_start 需要的全部数据：
- runtime / prepared：WASM 引擎与已编译组件的 Arc。
- capabilities = inject_workspace_reader(...)：注入 workspace 读权限。
- config_json：通道配置（从外部传入的 JSON）。
- timeout：回调超时（来自 runtime.config().callback_timeout）。
- credentials：通过 get_credentials() 拿到的占位符 → 明文映射（用于 {TELEGRAM_BOT_TOKEN} 替换）。
- host_credentials：根据 capabilities 自动解密、按 host 注入的凭证（如 Bearer）。
- pairing_store、workspace_store、websocket_outbound_tx：通道回调可访问的运行时依赖。

3. 阻塞线程执行回调（2053–2096 行）

tokio::time::timeout(timeout, async move {
tokio::task::spawn_blocking(move || {
// 1) create_store: 构造 wasmtime::Store，绑定 host functions
// 2) instantiate_component: 实例化 wasm 组件
// 3) call_on_start(&config_json)
// 4) convert_channel_config: WIT 配置 → Rust ChannelConfig
// 5) extract_host_state: 抽出 host 侧累积的状态（logs 等）
// 6) commit_callback_workspace_writes: 提交 callback 期间对 workspace 的写
}).await...
}).await...??
关键点：
- spawn_blocking：WASM 同步执行，不能占用 tokio worker。
- 三层 ??：外层 ? 处理 tokio::time::timeout 错误（→ WasmChannelError::Timeout），中层处理 spawn_blocking join 错误（→ ExecutionPanicked），最里层处理回调本身返回的 WasmChannelError。
- WIT → Rust 转换：on_start 返回的是 Result<wit_tool::ChannelConfig, String>，需用 convert_channel_config 转成宿主侧的 ChannelConfig，否则错误统一包成 CallbackFailed。

4. 持久化新快照（2098–2099 行）

回调若写入了 workspace，需要把改动落盘，下次启动 load_durable_workspace_snapshot 能恢复。

5. 返回值

Ok((config_result, host_state))
- config_result：回调产出的 ChannelConfig（HTTP 端点、轮询配置、display name）。
- host_state：回调期间累积的日志等状态，调用方（call_on_start）会用 log_on_start_host_state 把它刷到 tracing。

上游调用方

call_on_start（2112 行）调用它：
1. 拿到 (config_result, host_state)；
2. 把 host_state 里的日志刷出去；
3. 把 config_result 解包返回给 start()，用于注册 HTTP webhook、设置轮询等。

▎ 注意：第一次启动没有 component 时，call_on_start 会直接返回默认空配置（2114–2123 行），不走 execute_on_start_with_state。

✻ Churned for 39s

❯ 这为啥要写入workspace，这是啥东西

WASM 通道的 workspace 是啥？为啥 on_start 要写它？

概念

WASM 通道的 workspace 不是"用户的工作区"——它是 wasmtime Store 的 linear memory 里的私有 KV 存储。一个 HashMap<String, Vec<u8>>（或类似结构），由通道 WASM 通过 host function 读写。

可以把它想象成：给通道一块"白板"，通道自己挂上去就能记东西。

// 通道在 WASM 里这样调用（伪代码）：
let n = workspace_read("pending_user_id");  // 拿上一次记的值
if n is None {
workspace_write("pending_user_id", "u_123");  // 记下来
}

为啥需要它

on_start 只在 激活时 跑一次。但通道在生命周期里会发生很多次事件（on_http_request、on_message、on_pairing_response……），而有些状态需要 跨回调、跨进程重启 保留。

workspace 解决的就是 回调之间的本地持久化：

┌────────────────────────────────────────────────────┬─────────────────────────────────────────────────┐
│                        场景                        │                workspace 的角色                 │
├────────────────────────────────────────────────────┼─────────────────────────────────────────────────┤
│ OAuth 回调分两步（先 redirect、再 token exchange） │ 第一次把 state 写进 workspace，第二次读出来校验 │
├────────────────────────────────────────────────────┼─────────────────────────────────────────────────┤
│ 多步配对流程                                       │ 记录 pending_user_id、pending_device            │
├────────────────────────────────────────────────────┼─────────────────────────────────────────────────┤
│ 增量去重 / 已处理消息 ID                           │ 记 last_seen_id                                 │
├────────────────────────────────────────────────────┼─────────────────────────────────────────────────┤
│ 缓存上游 API 响应                                  │ 短期缓存                                        │
└────────────────────────────────────────────────────┴─────────────────────────────────────────────────┘

为啥要"持久化"到磁盘

WASM 通道的 linear memory 跟着 Store 走，Store 销毁，workspace 跟着没了。

如果通道进程崩溃重启、或者 worker 容器重启，内存里的 workspace 就清空了——配对流程会被打断，OAuth state 会失效。

所以两层存储：

1. 内存层：每次回调里直接读写（快）。
2. 快照层：每次回调结束（on_start / on_http_request / on_message 等），把 workspace 里声明要持久化的 path 序列化落盘。

下次通道激活时，先 load_durable_workspace_snapshot 把磁盘上的 KV 灌回内存，通道就像"没死过一样"继续工作。

对应回这个函数的代码

async fn execute_on_start_with_state(&self) ... {
// ① 回调开始前：把上次的快照恢复进内存
self.load_durable_workspace_snapshot().await;

    // ② 在 spawn_blocking 里跑 WASM
    //    - create_store 时把 workspace_store 绑进去
    //    - WASM 通过 host fn 读/写 workspace
    //    - 提取 host_state 时，host_state 携带了回调期间累计的写入
    let mut host_state = Self::extract_host_state(&mut store, ...);
    let committed_paths =
        Self::commit_callback_workspace_writes(&mut host_state, &workspace_store);
    //  → 关键：把回调里"声明要持久化"的写入提交到 workspace_store
    //    （不是所有写入都要落盘，通道要明确标记哪些 path 是 durable 的）

    // ③ 回调结束后：把新提交的 path 落盘
    self.persist_durable_workspace_snapshot_if_needed(&committed_paths).await;

    Ok((config_result, host_state))
}

安全意义

- WASM 沙箱：workspace 是 WASM 唯一可写的状态；落盘由宿主控制，通道自己写不进去。
- 租户隔离：workspace_store 按 owner scope key 隔离，不同用户的快照互不污染。
- 可清理：不需要的时候直接删快照文件，等于"重置"通道本地状态。

一句话总结

on_start 写 workspace = 通道在第一次激活时把它的本地状态（OAuth state、配对进度、缓存等）持久化下来，这样后续回调和重启都能接得上，而不会每次都从零开始。


响应
async fn respond(
&self,
msg: &IncomingMessage,
response: OutgoingResponse,
) -> Result<(), ChannelError> {
// Stop the typing indicator, we're about to send the actual response
self.cancel_typing_task().await;

        // Check if there's a pending synchronous response waiter
        if let Some(tx) = self.pending_responses.write().await.remove(&msg.id) {
            let _ = tx.send(response.content.clone());
        }

        // Call WASM on_respond
        // IMPORTANT: Use the ORIGINAL message's metadata, not the response's metadata.
        // The original metadata contains channel-specific routing info (e.g., Telegram chat_id)
        // that the WASM channel needs to send the reply to the correct destination.
        let metadata_json = serde_json::to_string(&msg.metadata).unwrap_or_default();
        // Store for owner-target routing (chat_id etc.) only when the configured
        // owner is the actor in this conversation.
        let owner_actor_id = self.owner_actor_id.read().await.clone();
        if should_update_owner_broadcast_metadata(
            &msg.user_id,
            &msg.sender_id,
            &self.owner_scope_id,
            owner_actor_id.as_deref(),
        ) {
            self.update_broadcast_metadata(&metadata_json).await;
        }
        let generated_image_attachments =
            self.take_generated_image_attachments(&msg.metadata).await;
        let mut attachments = response.attachments.clone();
        let response_already_has_generated_image = attachments
            .iter()
            .any(|path| is_staged_generated_image_path(path))
            || !response.inline_attachments.is_empty();
        if response_already_has_generated_image {
            tracing::debug!(
                channel = %self.name,
                skipped = generated_image_attachments.len(),
                "Skipping status-staged generated images because the final response already carries generated-image attachments"
            );
        } else {
            attachments.extend(generated_image_attachments.iter().cloned());
        }

        let result = self
            .call_on_respond(
                msg.id,
                &response.content,
                response.thread_id.as_ref().map(|t| t.as_str()),
                &metadata_json,
                &attachments,
                &response.inline_attachments,
            )
            .await;
        remove_staged_generated_image_attachments(&generated_image_attachments);

        result.map_err(|e| ChannelError::SendFailed {
            name: self.name.clone(),
            reason: e.to_string(),
        })?;

        Ok(())
    }
数据足够，下面是逐段拆解。

# `WasmChannel::respond()` 拆解

`src/channels/wasm/wrapper.rs:3808-3873`。这是"agent 给 WASM channel 回信"时 host 做的事。**它不是简单地把回复转发出去**——它要先做 5 件事：停 typing、回同步等待者、记下目的地、合入生成的图片、调 WASM 回调。

## 整体节奏

```
┌─ 1. 停 typing 指示器（4s 重发那个） ─┐
└─────────────────────────────────────┘
                    ↓
┌─ 2. 有同步等待者？给个回执 ─────────┐
│   pending_responses 里有这个 msg.id？ │
│   有 → oneshot.send(content)         │
│   没有 → 继续走正常路径              │
└─────────────────────────────────────┘
                    ↓
┌─ 3. 拿原始 metadata（不是 response 的）┐
│   metadata 含 chat_id、message_id 等  │
│   channel-specific 路由信息            │
└─────────────────────────────────────┘
                    ↓
┌─ 4. 缓存"广播目的地" ───────────────┐
│   只在这是 owner 自己说话时记          │
│   （应该_update_owner_broadcast_...） │
└─────────────────────────────────────┘
                    ↓
┌─ 5. 合并 status 阶段暂存的生成图片 ───┐
│   但 response 已带图就跳过             │
└─────────────────────────────────────┘
                    ↓
┌─ 6. 调 WASM 插件的 on_respond ───────┐
│   传 msg_id, content, thread_id,      │
│   metadata, attachments,              │
│   inline_attachments                  │
└─────────────────────────────────────┘
                    ↓
┌─ 7. 清理 staging 目录里的暂存图 ──────┐
└─────────────────────────────────────┘
```

## 逐段讲

### 1. `cancel_typing_task().await`

```rust
self.cancel_typing_task().await;
```

回信要发了，那个 4 秒一次的"typing..."重发任务**必须停**，否则会跟最终回复打架（用户先看到 typing、然后又看到内容 + typing 闪烁）。

回顾一下背景（`wrapper.rs:843-845, 2914-2922`）：

- `typing_task: RwLock<Option<JoinHandle>>` 保存后台任务句柄。
- `send_status(Thinking)` 启动它，每 4 秒调一次 `on_status` 发"typing"。
- `send_status(terminal)` 或 `send_status(approval_needed)` 时**取消**它。
- 现在 `respond()` 是另一条取消路径——**真正的回复马上要来了，typing 必须停**。

注释写得很硬：`Stop the typing indicator, we're about to send the actual response`。

### 2. 同步回执（`pending_responses`）

```rust
if let Some(tx) = self.pending_responses.write().await.remove(&msg.id) {
    let _ = tx.send(response.content.clone());
}
```

`pending_responses: RwLock<HashMap<Uuid, oneshot::Sender<String>>>` 是上篇提过的"**同步响应通道**"。

工作流：
- WASM 插件某次 `emit_message` 选了 `request_response` 模式，在 host 侧 `pending_responses` 里挂个 oneshot sender，等着 agent 立刻回。
- agent 跑完一轮，把回复传回 host 调 `respond()`。
- 这里 `remove(&msg.id)` 取出发送端，把 content 塞进 oneshot —— **调 emit 的那行同步代码立即拿到 agent 的回复**。

注意：取出发送端后**这个分支就 return 了**？不，**继续往下走**。注释说 respond() 的**完整流程**还是要跑（停 typing、调 on_respond），但同步等待者已经被满足了。两者并行不冲突。

如果 `msg.id` 不在 map 里（绝大多数情况——非同步 emit），就安静跳过。

### 3. **关键不变量**：用**原消息的** metadata，不是 response 的

```rust
// IMPORTANT: Use the ORIGINAL message's metadata, not the response's metadata.
// The original metadata contains channel-specific routing info (e.g., Telegram chat_id)
// that the WASM channel needs to send the reply to the correct destination.
let metadata_json = serde_json::to_string(&msg.metadata).unwrap_or_default();
```

这是整个 `respond()` 里**最值得讲的设计点**。

直觉里你可能觉得"response 的 metadata 是新写的，应该用新的"，但**错的**：

- 原始 `IncomingMessage.metadata` 里塞了 `chat_id`（Telegram）、`channel_id`（Slack）、`message_id` 等**怎么回信**的路由信息。
- 这些路由信息**只有入站那一刻才知道**（Telegram 用户在哪个 chat、Slack 事件来自哪个 channel）。
- `OutgoingResponse.metadata` 是 response 自己的元数据，跟"把回复发到哪"无关。

如果用错 metadata：调 `on_respond` 时 WASM 拿到 `{"foo": "bar"}`（response 的元数据），**根本不知道把消息发回哪个 chat**，IM 平台一收就 400。

### 4. 缓存广播目的地

```rust
let owner_actor_id = self.owner_actor_id.read().await.clone();
if should_update_owner_broadcast_metadata(
    &msg.user_id,
    &msg.sender_id,
    &self.owner_scope_id,
    owner_actor_id.as_deref(),
) {
    self.update_broadcast_metadata(&metadata_json).await;
}
```

**这段只看是"owner 自己在用 channel"才记**。`should_update_owner_broadcast_metadata` 规则（`wrapper.rs:1090-1099`）：

```rust
owner_actor_id.map_or(
    user_id == owner_scope_id,                // 没人 bound → 用 user_id
    |owner_actor_id| sender_id == owner_actor_id,  // 有人 bound → 用 sender_id
)
```

为什么要这么挑？这是 `installed ≠ active` 规则的延伸。

举例：
- 一个**没配对**的 Telegram channel：发消息的 `user_id` 就是 owner → 记下 `chat_id=xxx`，下次主动推给 owner。
- 一个**配对过**的 Telegram channel：可能有好几个 `sender_id` 都在用（owner 之外还有授权用户）→ **只**在 sender 恰好等于 `owner_actor_id` 时记，避免把别人的 chat_id 误当成 owner 的。

记下来后存到 `last_broadcast_metadata: Arc<RwLock<Option<String>>>`，**并**写进 `settings_store`（`update_broadcast_metadata` 内部），**跨重启保留**。这才是 `start()` 开头那行 `load_broadcast_metadata().await` 能恢复数据的原因。

### 5. 合入生成的图片（status 阶段暂存 → 终态发出）

```rust
let generated_image_attachments =
    self.take_generated_image_attachments(&msg.metadata).await;
let mut attachments = response.attachments.clone();
let response_already_has_generated_image = attachments
    .iter()
    .any(|path| is_staged_generated_image_path(path))
    || !response.inline_attachments.is_empty();
if response_already_has_generated_image {
    tracing::debug!(...);
} else {
    attachments.extend(generated_image_attachments.iter().cloned());
}
```

**问题**：agent 跑一轮可能产生 5 张图，每张在 `StatusUpdate::ImageGenerated` 阶段就 staged 到磁盘了。**但当时还不知道最终 reply 长什么样**——你不能每生成一张就发一条消息（用户会被刷屏）。**正确做法是**把图"挂"在当前这条消息上，等最终回复一起发。

实现：

- `take_generated_image_attachments(&msg.metadata)` 从 metadata 里取"这次消息暂存了哪些图"。
- 检查 `response` 本身**已经**带了 generated image（按 staging 路径前缀 / inline 附件判断）。
    - **是** → 跳过暂存图（避免重复）。
    - **否** → 把暂存图合进 `attachments` 一起发。
- 合并完传给 `call_on_respond`。

### 6. 调 WASM 插件的 `on_respond`

```rust
let result = self.call_on_respond(
    msg.id,
    &response.content,
    response.thread_id.as_ref().map(|t| t.as_str()),
    &metadata_json,
    &attachments,
    &response.inline_attachments,
).await;
```

跟其他 callback（`on_start` / `on_poll` / `on_http_request`）**完全同款**的"造 wasmtime Store + 加载 host functions + 实例化 + 调导出函数"流程。

参数关键点：

| 参数                           | 来源                          | 作用                           |
| ------------------------------ | ----------------------------- | ------------------------------ |
| `msg.id`                       | 原入站消息的 UUID             | 让插件回复时挂回同条消息       |
| `&response.content`            | agent 的回复文本              | 必发                           |
| `response.thread_id`           | 可选 `ExternalThreadId`       | 多回同一 thread（IM 平台概念） |
| `&metadata_json`               | **原消息**的 metadata         | 路由用（chat_id、channel_id…） |
| `&attachments`                 | response.attachments + 暂存图 | 走 host 注入凭据后出站         |
| `&response.inline_attachments` | response 自己的内嵌附件       | 不落盘直接出                   |

WASM 插件在 `on_respond` 里**真正的活**：
- 解析 metadata 取 `chat_id`。
- 调 `http_request` host function（host 注入 Bearer token 后发出站 HTTP）。
- 把消息发到 Telegram API / Slack API。

### 7. 清理 staging

```rust
remove_staged_generated_image_attachments(&generated_image_attachments);
```

不管 `call_on_respond` 成功失败，这步都跑。staging 目录里的暂存图生命周期跟"agent 这轮回复"绑定，回复发出后即可清。

### 错误处理

```rust
result.map_err(|e| ChannelError::SendFailed {
    name: self.name.clone(),
    reason: e.to_string(),
})?;
```

`call_on_respond` 失败 = 发不出去 = `ChannelError::SendFailed`。`ChannelManager::respond` 会把这个错误冒泡到 `agent_loop::respond_then_done`（`agent_loop.rs:686-714`）：

```rust
let respond_result = self.channels.respond(message, response).await;
// ... 删除 staging 图
// 然后继续发 Done（不论 respond 成败，turn 都要关闭）
```

也就是说：**WASM 发不出消息不会让 agent 死循环**，Done 信号照发、UI 知道这一轮收尾了。

## 写博客可以拎出来的金句

1. **"用原消息的 metadata 不用 response 的"**——这条注释是整个函数里**唯一**带 IMPORTANT 的，凡是 channel 抽象必踩的坑：路由信息只属于入站那一刻。
2. **"owner 在说话才记广播目的地"**——`should_update_owner_broadcast_metadata` 的两段式判定，规避多用户 channel 误把客人的 chat_id 标成 owner 的。
3. **"生成的图先暂存，最终回复时合包发"**——为什么 `pending_generated_image_attachments` 是个 `HashMap<msg_id, Vec<path>>`，而不是每张图独立 emit。
4. **"respond 失败不影响 Done"**——`respond_then_done` 故意解耦"消息到达"和"轮次结束"两件事，跟 `agent_loop.rs:681-685` 注释（#2079 SSE 早关闭 turn 的修复）一脉相承。
5. **"同步回执是少数派，绝大多数走 on_respond"**——`pending_responses.remove(&msg.id)` 是个 NoOp 的概率 99%，但留着是 1% 同步调用的关键通道。

## 速记表

```
┌─ respond() 5 件事 ─────────────────────────┐
│ 1. 停 typing            ← cancel_typing_task │
│ 2. 同步回执 (1% 概率)   ← pending_responses  │
│ 3. 路由用原 metadata     ← chat_id 来源       │
│ 4. 缓存 broadcast 目的地 ← owner only        │
│ 5. 合入暂存生成图       ← 避免刷屏            │
│ 6. 调 on_respond        ← 真正发信           │
│ 7. 清理 staging         ← 生命周期结束        │
└────────────────────────────────────────────┘
```

完整链路
# WasmChannel 完整调用链路（第一次连接、发消息、收回复）

> 假设场景：用户首次在 Telegram 给 bot 发 `/start`。
> 假设 WASM Telegram channel 插件用 webhook 模式（path = `/webhook/telegram`）。
> 所有引用均来自实际读到的代码。

## 总览（鸟瞰图）

```
Telegram 服务器 ──POST──> WasmChannelRouter ──> WasmChannel::on_http_request
                                                        │
                                                        ▼
                                            call_on_http_request (新 WASM 实例)
                                                        │
                                                        ▼
                                          WASM 插件: parse + emit_message
                                                        │
                                                        ▼
                                            Agent 主循环 (channel merge)
                                                        │
                                                        ▼
                                              agent 处理 + 调工具
                                                        │
                                                        ▼
                                          Agent::respond → respond_then_done
                                                        │
                                                        ▼
                                            ChannelManager::respond
                                                        │
                                                        ▼
                                            WasmChannel::respond
                                                        │
                                                        ▼
                                            call_on_respond (新 WASM 实例)
                                                        │
                                                        ▼
                                          WASM 插件: 解析 chat_id + http_request
                                                        │
                                                        ▼
                                      Telegram API ◄──Host 注入 Bearer token
                                                        │
                                                        ▼
                                              用户在 Telegram 看到回复
```

## 阶段 0：启动 wiring（程序启动时一次性）

```
src/main.rs
  └─ let mut gw = GatewayChannel::new(...).with_workspace_pool(...);
  └─ ... 链式 with_* 注入所有 subsystem
```

但 `WasmChannel` **不在** main.rs 链上，它是在 `WasmChannelLoader` 启动时按 `bundled.rs` / 磁盘扫描出来的（`src/channels/wasm/mod.rs:106`）：

```rust
WasmChannelLoader::new(runtime, pairing_store, settings_store, owner_scope_id)
  └─ load_installed()  // 扫描 ~/.ironclaw/channels/*.wasm
       ├─ 编译（首次）/ 取缓存（后续）
       └─ 构造 WasmChannel 实例
  └─ 每个 WasmChannel::start()
       └─ （就是上一篇讲的 5 步）
```

`start()` 完成后，channel 的：

- `message_tx: Arc<RwLock<Option<mpsc::Sender<IncomingMessage>>>>` 已就绪
- `endpoints: RwLock<Vec<RegisteredEndpoint>>` 已注册
- `WasmChannelRouter.path_to_channel["/webhook/telegram"] = "telegram"` 已建立

**WasmChannel **本身**不**在 `ChannelManager` 里（除了 `GatewayChannel`）——WAS M channel 是**单独走 web channel 路径**：

```
GatewayChannel 是 Channel trait 的实现（"gateway" channel 名）
  └─ ChannelManager::hot_add(GatewayChannel)   ← 一开始就注册

WasmChannel 也是 Channel trait 的实现
  └─ 但它自己直接被 start() 调起，message_tx 直接注入自己的 mpsc
  └─ 或者用 WasmChannelLoader 的回调路径：把 WasmChannel 的 mpsc 串到一个内部 forwarding task
       forwarding task: msg_rx → ??? → agent 主循环
```

> 具体怎么把 WasmChannel 的 mpsc 串进 `agent.channels.start_all()` 的合并流，要看 `WasmChannelLoader::load_installed` 的实现，但论文里你已经看到了核心循环，本文聚焦**单条消息的一次完整往返**。

## 阶段 1：Telegram 推 webhook

### 1.1 Telegram 服务器

用户给 bot 发消息。Telegram 用**主动回调**模式：调用我们暴露的 URL，把 update JSON POST 过来。

```
POST https://<your-domain>/webhook/telegram
X-Telegram-Bot-Api-Secret-Token: <shared secret>
Content-Type: application/json
{ "update_id": 12345, "message": { "chat": { "id": 999 }, "from": {...}, "text": "/start" } }
```

### 1.2 路由命中

在 `WasmChannelRouter` 内部，预先注册过的表：

```rust
path_to_channel["/webhook/telegram"] = "telegram"
path_methods["/webhook/telegram"]   = ["POST"]
secrets["telegram"]                 = "telegram-shared-secret"
secret_headers["telegram"]          = "X-Telegram-Bot-Api-Secret-Token"
```

`router.rs:62-67` 注释明确说明 Telegram 用专用 header（其他 channel 默认 `X-Webhook-Secret`）。

### 1.3 验签

`router.rs:445-447` 检查 `X-Telegram-Bot-Api-Secret-Token` 是否等于配置的 secret，**不等直接 403**——host 验签，**完全不让 WASM 看到 webhook 原始 body** 之前就拒掉伪造请求。

### 1.4 找到 channel 实例

```rust
let channel: Arc<WasmChannel> = channels.get("telegram").unwrap();
```

调 `channel.call_on_http_request(body, headers)`。

## 阶段 2：WasmChannel 跑 `on_http_request`

`wrapper.rs:2142` 起的函数。**和上一篇 `respond()` 同样的"造新实例"流程**：

```rust
let _callback_guard = self.callback_lock.lock().await;   // 1. 串行化所有回调

// 2. tokio::time::timeout(callback_timeout=30s)
// 3. tokio::task::spawn_blocking(|| {
//      a. prepare_response_attachments(...)            // 几乎没活
//      b. create_store(...)                             // 装 host functions
//      c. instantiate_component(...)                    // 装 prepared 模块
//      d. let channel_iface = instance.near_agent_channel();
//      e. channel_iface.call_on_http_request(&mut store, &wit_request)
//   })
```

### 2.1 `create_store`（`wrapper.rs:1874`）

新建一个 wasmtime `Store<ChannelStoreData>`，注入：

- `limiter: ResourceLimiter`（fuel + 内存）
- `host_state: ChannelHostState { emitted_messages, pending_writes, base HostState { logger } }`
- `http_client: reqwest::Client`（预解析 host credentials 时用）
- `credentials: HashMap<placeholder, value>`（占位符级）
- `host_credentials: Vec<ResolvedHostCredential>`（按 host 自动注入的）
- `pairing_store`（guest 访问控制）
- `workspace_store`（`Arc<ChannelWorkspaceStore>`，跨回调 KV）
- `runtime: Option<Runtime>`（仅 ws 模式有）

**host functions 注册**（`add_to_linker` 生成代码）：

```
logging::log(level, target, message)
http::http_request(method, url, headers_json, body, timeout_ms) -> response_json
http::http_request_with_secrets(method, url, headers_json, body, timeout_ms, ...) -> response_json
pairing::is_paired(actor_id) -> bool
pairing::request_pairing(actor_id, channel, metadata_json) -> bool
workspace::workspace_read(path) -> option<string>
workspace::workspace_write(path, value) -> result
websocket::websocket_send(message) -> result
```

### 2.2 `instantiate_component`

从 `prepared.component: Arc<Option<Component>>` 拿到预编译产物，实例化。**线性内存全空**——保证"fresh instance per callback"不变量。

### 2.3 调 `on_http_request`

WASM 侧走它自己的实现，**典型 Telegram 插件逻辑**：

```rust
// 伪代码：WASM 插件 on_http_request
fn on_http_request(req: HttpRequest) -> Result<HttpResponse, String> {
    let update: Update = serde_json::from_str(&req.body)?;
    let text = update.message.text;
    let chat_id = update.message.chat.id;

    // 关键：把 Telegram 私有信息塞进 metadata
    let metadata = json!({
        "chat_id": chat_id,
        "message_id": update.message.message_id,
        "user_id": update.message.from.id,  // 整数，保留做"channel-private 路由"
        "telegram_update_id": update.update_id,
    });

    // emit 给 agent
    emit_message(EmitMessage {
        content: text,
        user_id: "alice".to_string(),  // ← 这个是 owner/已配对用户的 user_id
        sender_id: update.message.from.id.to_string(),  // ← 这个是 Telegram 侧 actor
        thread_id: None,
        metadata_json: metadata.to_string(),
        is_internal: false,
        // 关键：request_response: false（绝大多数情况）
    });
    Ok(HttpResponse { status: 200 })
}
```

`emit_message` 是 host function，**把消息写进 `host_state.emitted_messages`**——这一步 WASM 不会立刻把消息发出去，**等回调结束统一 dispatch**。

### 2.4 `extract_host_state`

回调返回后，从 `Store` 取出 `emitted_messages` + `pending_writes`（workspace 写）+ 累积的 guest 日志。

### 2.5 `commit_callback_workspace_writes`

把"这次回调写过的 workspace 路径"提交到 `ChannelWorkspaceStore`（`Arc<ChannelWorkspaceStore>`），跨 tick 保留。`durable_workspace_paths` 之外的会进 settings_store 跨重启。

### 2.6 `dispatch_emitted_messages`（关键！）

把 `emitted_messages` 转成 `IncomingMessage` 投到 `message_tx`：

```rust
// wrapper.rs:3117
self.dispatch_emitted_messages(
    EmitDispatchContext {
        channel_name: &self.name,
        capabilities: &self.capabilities,
        owner_scope_id: &self.owner_scope_id,
        owner_actor_id: current_owner.as_deref(),
        ...
    },
    emitted_messages,
).await?;
```

`dispatch_emitted_messages` 内部做：

1. 速率限制（`rate_limiter.check`）。
2. 检查 `max_message_size`（默认 64KB）。
3. 构造 `IncomingMessage::new(channel, user_id, content).with_metadata(metadata)`。
4. 关键——`with_metadata` 会**强制**把字符串 `metadata.user_id` 覆写成 `self.user_id`（`channel.rs:231-251` 的安全不变量，防止 WASM 伪造 owner）。**非字符串** user_id（如 Telegram 整数）保留不动（SSE 路由层 `as_str()` 视为 missing 失败关闭）。
5. `message_tx.send(msg).await`——入 mpsc。

## 阶段 3：消息进 agent 主循环

`WasmChannel::start()` 返回 `Box::pin(ReceiverStream::new(rx))` 给 `WasmChannelLoader`，loader 用 spawn 起来的 forwarding task 把消息喂到 host 的统一入站流。

> 具体怎么 merge 进 `ChannelManager.start_all()` 的流：看 `WasmChannelLoader::load_installed` 末段。

最终，`agent_loop.rs:1080` 拿到的 `message_stream` 能 `next()` 拿到这条消息：

```rust
let mut message_stream = self.channels.start_all().await?;
loop {
    select! {
        msg = message_stream.next() => {
            let message = msg.unwrap();
            // ...转写、文档提取
            match self.handle_message(&message).await {
                Ok(HandleOutcome::Respond(response)) => {
                    self.respond_then_done(&message, response).await;
                }
                ...
            }
        }
    }
}
```

`handle_message`（`agent_loop.rs:1713`）分发到：

- v1: `ChatDelegate::run_agentic_loop()`（共享 `agentic_loop.rs`）
- v2: `bridge::router::handle_with_engine()`（engine v2）

假设 v1。`ChatDelegate` 跑 agentic 循环：LLM 调用 → 工具执行 → 重复 → 出 final response。

中间 agent 还会触发一堆 `StatusUpdate`（Thinking / ToolStarted / ToolResult / ReasoningUpdate / …），这些走 `ChannelManager::send_status("telegram", status, &metadata)` → `WasmChannel::send_status` → 调 `on_status` 回调（`wrapper.rs:2688-2773`），WASM 插件可以选择发"Telegram typing..."（**4 秒重发机制**就在 send_status 路径上）。

## 阶段 4：agent 回信

`agent_loop.rs:686-714` `respond_then_done`：

```rust
async fn respond_then_done(&self, message, response) {
    // 1. 先发正文
    self.channels.respond(message, response).await;     // ★
    // 2. 不论成功失败都发 Done
    self.channels.send_status(
        &message.channel,                              // "telegram"
        StatusUpdate::Status("Done".into()),
        &message.metadata,
    ).await;
}
```

`★` 走到 `ChannelManager::respond`（`manager.rs:136-150`）：

```rust
pub async fn respond(&self, msg, response) {
    if let Some(channel) = channels.get(&msg.channel) {   // "telegram" 找 WasmChannel
        channel.respond(msg, response).await
    }
}
```

## 阶段 5：`WasmChannel::respond`（上一篇详解的那个）

`wrapper.rs:3808-3873`。**5 件事**：

1. `cancel_typing_task`（停 4 秒 typing 重发）
2. 检查 `pending_responses`（同步回执，1% 路径）
3. **用原消息的 metadata**（不是 response 的）—— 路由信息 `chat_id` 来源
4. 缓存 `last_broadcast_metadata`（仅 owner 在说话时记）
5. 合入 `pending_generated_image_attachments`（status 阶段暂存的图）
6. `call_on_respond(msg_id, content, thread_id, metadata_json, attachments, inline_attachments)`

## 阶段 6：`call_on_respond`（`wrapper.rs:2386-2554`）

跟 `call_on_http_request` 同款"新实例"流程：

```
1. callback_lock.lock() 串行化
2. tokio::time::timeout(callback_timeout=30s)
3. spawn_blocking:
   a. create_store  (同前)
   b. instantiate_component
   c. wit_response = AgentResponse { message_id, content, thread_id, metadata_json, attachments }
   d. channel_iface.call_on_respond(&mut store, &wit_response)
   e. extract_host_state
   f. commit_callback_workspace_writes
4. drain_guest_logs  ← 不论成功失败都保留 guest 日志
5. persist_durable_workspace_snapshot_if_needed  ← durable paths 写 settings_store
```

WASM 插件 `on_respond` 实现：

```rust
// 伪代码
fn on_respond(response: AgentResponse) -> Result<(), String> {
    let metadata: serde_json::Value = serde_json::from_str(&response.metadata_json)?;
    let chat_id = metadata["chat_id"].as_i64().ok_or("no chat_id")?;
    let reply_url = "https://api.telegram.org/bot{BOT_TOKEN}/sendMessage";
    // 占位符 {BOT_TOKEN} 在 host 注入凭据时已被替换
    
    let body = json!({
        "chat_id": chat_id,
        "text": response.content,
        "reply_to_message_id": metadata["message_id"],
    });
    
    let resp = http_request("POST", reply_url, "{}", &body.to_string(), 10000)?;
    Ok(())
}
```

**关键**：WASM 拿不到 `{BOT_TOKEN}` 的明文——要么是 `credentials: HashMap<placeholder, value>` 里的占位符（`wrapper.rs:154-172` `inject_credentials`），要么是 host 在出站 HTTP 时按 `host_patterns` 自动注入 `Authorization: Bearer ...`（`wrapper.rs:250-280`）。

host 注入完发出去 → Telegram 收到 → 用户在 Telegram 看到回复。

## 阶段 7：响应回流

WASM 回调 `Ok(())` → `call_on_respond` 返回 `Ok(())` → `WasmChannel::respond` 返回 `Ok(())` → `ChannelManager::respond` 返回 `Ok(())` → `respond_then_done` 继续发 `Status("Done")` → `WasmChannel::send_status` → `on_status`（WASM 看到 terminal 不重发 typing）→ agent 主循环下一轮。

## 完整时序

```
Telegram        WasmChannelRouter   WasmChannel   spawn_blocking   Agent    ChannelManager
   │                   │                │               │            │            │
   │───POST /webhook──>│                │               │            │            │
   │                   │──验签─────────>│               │            │            │
   │                   │                │               │            │            │
   │                   │                │──callback_lock.lock()────>│            │
   │                   │                │──spawn_blocking──────────>│            │
   │                   │                │  create_store             │            │
   │                   │                │  instantiate              │            │
   │                   │                │  call_on_http_request────>│            │
   │                   │                │  ← 写入 host_state.emitted│            │
   │                   │                │  extract_host_state       │            │
   │                   │                │  commit_workspace_writes  │            │
   │                   │                │<─Ok()───                  │            │
   │                   │                │               │            │            │
   │                   │                │──dispatch_emitted────────>│            │
   │                   │                │               │  message_tx.send       │
   │                   │                │               │            │            │
   │                   │                │               │       message_stream.next()
   │                   │                │               │            │──handle_message
   │                   │                │               │            │   (LLM + tools)
   │                   │                │               │            │   respond(response)
   │                   │                │               │            │            │
   │                   │                │               │            │──channels.respond──>│
   │                   │                │               │            │            │   channel.respond
   │                   │                │               │            │            │
   │                   │                │──cancel_typing_task        │            │
   │                   │                │──callback_lock.lock()      │            │
   │                   │                │──spawn_blocking            │            │
   │                   │                │  create_store              │            │
   │                   │                │  instantiate               │            │
   │                   │                │  call_on_respond──>WASM    │            │
   │                   │                │  WASM 解析 chat_id         │            │
   │                   │                │  http_request──────────────┼──>Telegram API
   │                   │                │  ← 200 OK                 │            │
   │                   │                │<─Ok()──                   │            │
   │                   │                │  drain_guest_logs         │            │
   │                   │                │  persist_durable_workspace│            │
   │                   │                │<─Ok()──                   │            │
   │                   │                │               │            │            │
   │                   │                │               │            │<─Ok()──────│
   │                   │                │               │            │            │
   │                   │                │               │            │──send_status("Done")
   │                   │                │               │            │            │
   │                   │                │  call_on_status(Terminal)─│            │
   │                   │                │  WASM 不再发 typing        │            │
   │                   │                │               │            │            │
   │<─用户看到 bot 回复──────────────────────────────────────────────────────────│
```

## 关键不变量

| 不变量                               | 在哪                                          | 为什么                                           |
| ------------------------------------ | --------------------------------------------- | ------------------------------------------------ |
| **fresh instance per callback**      | `create_store` + `instantiate_component` 每次 | 线性内存/句柄不残留（防 token 滞留）             |
| **callback_lock**                    | `call_on_*` 开头                              | 同一 channel 实例的回调串行，防 workspace 写竞争 |
| **callback_timeout=30s**             | `tokio::time::timeout`                        | 沙箱失控兜底（防死循环/CPU 占用）                |
| **spawn_blocking**                   | 包裹 wasmtime                                 | wasmtime 是 CPU-bound，不该污染 tokio 调度       |
| **host 验签早于 WASM**               | `WasmChannelRouter` 验签                      | 假 webhook 根本不进 WASM                         |
| **host 注入凭据早于出站**            | `http::http_request` host fn                  | WASM 永远拿不到明文                              |
| **emit_message 后处理**              | `dispatch_emitted_messages`                   | WASM 不能在回调内直接 await 网络                 |
| **用原 metadata 不用 response 的**   | `WasmChannel::respond` 注释                   | 路由信息只属于入站那一刻                         |
| **owner-only 缓存 broadcast 目的地** | `should_update_owner_broadcast_metadata`      | 防客人的 chat_id 误标成 owner                    |
| **Done 不论 respond 成败都发**       | `respond_then_done`                           | 防止 #2079 SSE 早关闭 turn                       |

## 给博客的开头

> "一次 Telegram 消息在我们系统里的旅程：消息从 Telegram 服务器到你的屏幕，要穿过 4 个 host 屏障（Telegram API 验签、WASM 沙箱、agent 循环、host 出站凭据注入），每道屏障都把插件可能捅出的问题压窄。**关键不是 WASM 跑得多快，而是 host 在哪些地方强制介入。**"

③wit

WASM Channel 安全模型

// Security Model:                                                                                                                                                                                     
// - WASM channels are untrusted and run in a sandbox                                                                                                                                                  
//   （WASM channel 是不可信的，跑在沙箱里）                                                                                                                                                           
// - Fresh instance per callback (no shared mutable state)                                                                                                                                             
//   （每次回调都用全新的实例——不共享可变状态）                                                                                                                                                        
// - All capabilities are opt-in (default: no access)                                                                                                                                                  
//   （所有能力都是显式开启的——默认什么都不能做）                                                                                                                                                      
// - Secrets are NEVER exposed to WASM; credentials are injected at host boundary                                                                                                                      
//   （WASM 永远拿不到原始凭据；凭据由 host 在边界处注入）                                                                                                                                             
// - Workspace writes are prefixed with channels/<name>/ to prevent escape                                                                                                                             
//   （workspace 写入自动加 `channels/<name>/` 前缀，防止越界）                                                                                                                                        
// - Message emission is rate-limited                                                                                                                                                                  
//   （消息发送受速率限制）

我数了一下，`wit/channel.wit` 中 `channel-host` 接口**共包含 12 个函数（func）**，按职责可分为三大类，加上 5 个类型定义。下文按"基础能力 → 渠道特定能力 → 配对能力"依次讲解。

---

## 一、类型定义（5 个，非函数）

虽然你问的是"接口"，但 `channel-host` 还内嵌了一些数据契约，先列出来便于理解函数签名：

| 类型                             | 类别 | 作用                                                                             |
| -------------------------------- | ---- | -------------------------------------------------------------------------------- |
| `log-level` (enum)               | 基础 | 日志等级：`trace`/`debug`/`info`/`warn`/`error`                                  |
| `http-response` (record)         | 基础 | HTTP 响应：状态码 + 头（JSON）+ body                                             |
| `inbound-attachment` (record)    | 渠道 | 入站附件：文件 id、MIME、文件名、大小、源 URL、存储 key、抽取文本、`extras-json` |
| `emitted-message` (record)       | 渠道 | 发往 agent 的消息：user-id/name、content、thread-id、metadata、attachments       |
| `pairing-upsert-result` (record) | 配对 | 配对请求结果：code + 是否新建                                                    |

---

## 二、基础能力（Base Capabilities, 5 个）

源自通用 `tool host`，但被 `channel-host` 重新声明以扩展渠道场景。

### 1. `log(level, message)`
结构化日志。**约束**：单个 callback 最多 1000 条、每条 ≤ 4KB；callback 结束后统一输出。

### 2. `now-millis() -> u64`
当前 Unix 毫秒时间戳。给 WASM 提供统一时间基准，避免依赖宿主时区。

### 3. `workspace-read(path) -> option<string>`
读取 `~/.ironclaw/workspace/channels/<name>/` 下的文件。
- 路径**自动加前缀**，不能以 `/` 开头，不能含 `..`
- 文件不存在返回 `none`

### 4. `http-request(method, url, headers-json, body, timeout-ms) -> result<http-response, string>`
允许列表内的出站 HTTP（**这是渠道访问外网的主要出口**）。
- **凭据由宿主注入**，WASM 永不接触明文 secret
- 响应会做 secret 泄露扫描再返回
- `timeout-ms` 缺省 30000 ms，上限为 `callback_timeout`，适配 Telegram long-polling 等场景

### 5. `secret-exists(name) -> bool`
只能**判定是否存在**，永远拿不到值。真正的凭据在 `http-request` 调用时由宿主按允许列表注入。

---

## 三、渠道特定能力（Channel-Specific, 4 个）

### 6. `store-attachment-data(attachment-id, data) -> result`
把下载好的二进制（语音、图片、PDF 原文等）先存到宿主侧，再通过 `emit-message` 把 `attachment-id` 关联回去。
- 单个附件 ≤ 20 MB，整个 callback ≤ 50 MB
- callback 结束后**自动清除**

### 7. `emit-message(msg)`
**核心能力**：把用户消息推入 `MessageStream` 等待 agent 处理。
- 速率：单 callback ≤ 100 条，渠道全局可在配置中限流
- 内容 ≤ 64 KB
- callback 成功结束才真正投递，失败则丢弃（不会发出半成品消息）

### 8. `workspace-write(path, content) -> result`
写入 `channels/<name>/` 命名空间下的工作区文件。`emit-message` 可借助 `metadata-json` 引用，方便 agent 看到渠道侧的上下文。

### 9. `websocket-send-text(payload) -> result`
通过宿主维护的 WebSocket runtime 主动推送文本帧。**传输无关**：渠道自己负责构造协议 payload（如 Slack Events API、Discord gateway）。渠道无活跃 WS 或正在关闭时返回错误。

---

## 四、DM 配对能力（DM Pairing, 3 个）

用于"陌生发件人"白名单流程。

### 10. `pairing-upsert-request(channel, id, meta-json) -> result<(code, created)>`
陌生用户首次发消息时，宿主**幂等创建**配对请求，返回短码。
- `created=true`：渠道应发一条配对提示给用户
- `created=false`：已存在，沿用旧码（避免重复骚扰）

### 11. `pairing-resolve-identity(channel, external-id) -> result<option<string>>`
把 `(channel, external-id)` 解析成已配对的 `owner-id`，agent 后续按 owner 做权限与会话归属判定。**新渠道应当优先使用它**而非旧的 `allowFrom` 机制。

### 12. `pairing-read-allow-from(channel) -> result<list<string>>`
读取某渠道历史已配对的外部 ID 列表，仅为**兼容旧的 allowFrom 准入检查**。新接入的渠道不应再依赖它。

---

## 五、汇总

| #   | 函数                       | 类别 | 一句话用途                        |
| --- | -------------------------- | ---- | --------------------------------- |
| 1   | `log`                      | 基础 | 结构化日志                        |
| 2   | `now-millis`               | 基础 | 统一时间戳                        |
| 3   | `workspace-read`           | 基础 | 读渠道工作区                      |
| 4   | `http-request`             | 基础 | 出站 HTTP（含凭据注入与泄露扫描） |
| 5   | `secret-exists`            | 基础 | 仅探测 secret 是否存在            |
| 6   | `store-attachment-data`    | 渠道 | 上传附件二进制到宿主              |
| 7   | `emit-message`             | 渠道 | 把消息投递到 agent 队列           |
| 8   | `workspace-write`          | 渠道 | 写渠道工作区                      |
| 9   | `websocket-send-text`      | 渠道 | 通过宿主 WS 主动推帧              |
| 10  | `pairing-upsert-request`   | 配对 | 创建/复用配对短码                 |
| 11  | `pairing-resolve-identity` | 配对 | 解析 paired owner-id（推荐）      |
| 12  | `pairing-read-allow-from`  | 配对 | 读取旧 allowFrom（兼容）          |

**安全模型总结**（与文件头注释一致）：WASM 是不可信沙箱、每个 callback 用全新实例、默认零能力、secret 永不外泄、`channels/<name>/` 前缀防逃逸、出入消息均有速率与大小限制。

# `channel` 接口（沙箱侧）统计

按 `wit/channel.wit` 中 `interface channel` 的定义，沙箱自身**实现 6 个生命周期回调（func）+ 6 个数据类型**。下表先给可调用方法，再讲细节。

| #   | 方法                                                      | 类别     | 触发方      | 一句话职责                               |
| --- | --------------------------------------------------------- | -------- | ----------- | ---------------------------------------- |
| 1   | `on-start(config-json) -> result<channel-config, string>` | 生命周期 | 宿主加载时  | 上报 HTTP 端点 / 轮询配置                |
| 2   | `on-http-request(req) -> outgoing-http-response`          | HTTP     | HTTP 路由器 | 处理入站 webhook                         |
| 3   | `on-poll()`                                               | 轮询     | 轮询调度器  | 周期性拉取（如 Telegram getUpdates）     |
| 4   | `on-respond(response) -> result<_, string>`               | 出站     | 响应调度    | 把 agent 回复投递到渠道                  |
| 5   | `on-status(update)`                                       | 状态     | 状态广播    | 展示"思考中/工具执行/需要审批"等 UI 状态 |
| 6   | `on-broadcast(user-id, response) -> result<_, string>`    | 出站     | 后台/告警   | 主动推送，无前置入站消息                 |
| —   | `on-shutdown()`                                           | 生命周期 | 宿主卸载    | 释放资源                                 |

> 备注：`on-shutdown` 在文件 `interface channel` 段尾出现（行 431），属于"清理回调"，未编号进上表但仍可被宿主调用。

---

## 一、数据类型（沙箱侧用得到，6 个）

| 类型                            | 形态   | 用途                                                                                                                                                    |
| ------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `http-endpoint-config`          | record | 注册到宿主 HTTP 路由器的端点描述                                                                                                                        |
| `poll-config`                   | record | 轮询间隔（≥ 30 000 ms）与开关                                                                                                                           |
| `channel-config`                | record | `on-start` 的返回值：display-name + endpoints + poll                                                                                                    |
| `incoming-http-request`         | record | 入站 webhook：method/path/headers/query/body/`secret-validated`                                                                                         |
| `outgoing-http-response`        | record | `on-http-request` 的同步响应                                                                                                                            |
| `attachment` / `agent-response` | record | 出站消息体，含原始字节附件                                                                                                                              |
| `status-type` (enum)            | enum   | `thinking`/`done`/`interrupted`/`tool-started`/`tool-completed`/`tool-result`/`approval-needed`/`status`/`job-started`/`auth-required`/`auth-completed` |
| `status-update`                 | record | `on-status` 收到的状态消息                                                                                                                              |

---

## 二、逐个讲解

### 1. `on-start(config-json) -> result<channel-config, string>`
- **何时被调**：宿主首次加载该 WASM 时调用一次
- **输入**：渠道能力文件中的原始 JSON 配置
- **输出**：`channel-config`——告诉宿主
    - 展示名（`display-name`）
    - 要注册哪些 HTTP 端点（路径、方法、是否要 `require-secret` 校验）
    - 是否开启轮询及 `interval-ms`（最低 30 秒，低于此值会被宿主拒绝）
- **失败语义**：返回 `Err(string)` 时宿主将拒绝加载该渠道

### 2. `on-http-request(req) -> outgoing-http-response`
- **何时被调**：HTTP 路由器把请求转过来
- **关键字段**：`secret-validated: bool` 表明宿主已先做 secret 校验，沙箱不应再做（也不可信）
- **必须同步返回**一个 `outgoing-http-response`，否则连接挂起
- **常配合 `emit-message`** 把入站消息排入 agent 队列

### 3. `on-poll()`
- **何时被调**：宿主轮询调度器按 `poll.interval-ms` 触发
- **无入参、无返回值**，内部应通过 `channel-host.emit-message` 上报发现的消息
- **适用场景**：无 webhook 的渠道（Telegram getUpdates、IMAP 轮询等）

### 4. `on-respond(response) -> result<_, string>`
- **何时被调**：agent 针对该渠道之前 `emit-message` 进来的消息产出了回复
- **入参**：`agent-response` 包含 `message-id`（用于关联）、`content`、`thread-id`、metadata、可选 `attachments`（**原始字节**）
- **职责**：把回复真正发到外部系统（Slack 调 `chat.postMessage`、Telegram 调 `sendMessage` 等）
- **凭据**：通过 `channel-host.http-request` 发起，宿主负责注入 secret

### 5. `on-status(update)`
- **何时被调**：agent 状态机变更（`status-type` 中 11 种事件）
- **典型用途**：
    - `thinking` → 在 UI 上显示"typing…"或 emoji
    - `tool-started`/`tool-completed`/`tool-result` → 展示工具进度
    - `approval-needed` → 渲染审批按钮
    - `auth-required`/`auth-completed` → 通知用户 OAuth 流程
    - `job-started` → 提示后台 sandbox 任务已启动
- **无返回值**，纯通知；失败由宿主记日志

### 6. `on-broadcast(user-id, response) -> result<_, string>`
- **何时被调**：agent **主动**向某用户发消息，无前置入站
- **用途**：告警、定时播报、带附件的主动通知
- **与 `on-respond` 区别**：没有要关联的 `message-id`（或说消息并非回复），接收方由 `user-id` 直接指定

### 7. `on-shutdown()`
- **何时被调**：宿主卸载/重启渠道实例
- **职责**：关闭 WS、释放文件句柄、flush 缓存
- **无入参无返回值**

---

## 三、调用方向总结图

```
宿主 (host)                       沙箱 (WASM channel)
  │                                       │
  │── on-start(config-json) ────────────▶│  (启动)
  │◀─ channel-config ─────────────────── │
  │                                       │
  │── on-http-request(req) ─────────────▶│  (webhook 触发)
  │◀─ outgoing-http-response ────────────│
  │                                       │
  │── on-poll() ────────────────────────▶│  (轮询触发)
  │                                       │
  │── on-respond(response) ─────────────▶│  (agent 回复)
  │◀─ result<_, err> ────────────────────│
  │                                       │
  │── on-status(update) ────────────────▶│  (状态广播)
  │                                       │
  │── on-broadcast(user-id, response) ──▶│  (主动推送)
  │◀─ result<_, err> ────────────────────│
  │                                       │
  │── on-shutdown() ────────────────────▶│  (卸载)
```

**沙箱不能主动调宿主**——所有"反向"能力（出站 HTTP、写文件、发消息、WS 发帧、配对）都通过 `channel-host` 接口的 12 个 import 函数（参见上一轮讲解）来调用。

---

## 四、关键约束速记

- **每个 callback 跑在全新 WASM 实例上**，沙箱不能依赖跨调用的内存状态；需要持久化请走 `workspace-read/write`
- **凭据永不进 WASM**：所有需要 secret 的 HTTP 一律走 `channel-host.http-request`，由宿主按允许列表注入
- **同步返回**：只有 `on-http-request` 必须同步返回响应；其他回调都是"先入队、再回调"的批处理模式
- **状态广播频次**：`on-status` 由宿主按事件触发，沙箱应保持幂等（同一状态可能多次到达）

④链路相关代码

关键区分

WASM channel 不是消息流的必经路径；它更像是"宿主里的一个沙箱"，通过宿主侧的入队/出队通道和 agent 通信。完整数据流是：

外部用户
│ webhook / poll
▼
[宿主 HTTP 路由器 / 轮询调度器]   ← 真实入口，永远在 Rust 侧
│ 反序列化
▼
[WASM channel 沙箱实例]           ← 调用 on-http-request / on-poll
│ 内部逻辑
▼
channel-host.emit-message()       ← 沙箱 *主动调用* 宿主 import
│
▼
[宿主 MessageStream / 队列]       ← 关键中转，不在 WASM 里
│
▼
[Agent 引擎]                      ← 读取消息、推理、调工具
│
▼
[宿主响应分发器]                   ← 拿到 agent-response
│
▼
[WASM channel 沙箱实例]           ← 调 on-respond / on-broadcast
│ 内部用 http-request 把消息发到 Slack/Telegram…
▼
外部用户

main.rs
// Start the unified webhook server if any routes were registered. wasm/tool启动一个webhook服务器
let webhook_server: Option<Arc<tokio::sync::Mutex<WebhookServer>>> = if !webhook_routes
.is_empty()
{

里面会添加路由：去监听wasmChannel发的请求。
Router::new()
// Catch-all for webhook paths
.route("/webhook/{*path}", get(webhook_handler))
.route("/webhook/{*path}", post(webhook_handler))
.with_state(state)

/// Generic webhook handler that routes to the appropriate WASM channel.
async fn webhook_handler(
State(state): State<RouterState>,
method: Method,
Path(path): Path<String>,
Query(query): Query<HashMap<String, String>>,
headers: HeaderMap,
body: Bytes,
) -> impl IntoResponse {

      // Find the channel for this path
    let channel = match state.router.get_channel_for_path(&full_path)//根据路径找到对应的channel

//检查方法是否在白名单
if !state
.router
.method_allowed_for_path(&full_path, &method)
.await

//签名校验
如果该 channel 注册了 webhook secret：
- 拿 secret_header 配置（请求头名，比如 X-Hub-Signature-256）
- 拿注册时存的 secrets: HashMap<String, String>
- 计算 HMAC 比对
- 校验失败 → 返回 401 "Invalid secret"
- 通过 → did_authenticate = true

//调该channel的wasm方法
match channel
.call_on_http_request(
method.as_str(),
&full_path,
&headers_map,
&query,
&body,
secret_validated,
)
.await

/// Execute the on_http_request callback.
///
/// Called when an HTTP request arrives at a registered endpoint.
pub async fn call_on_http_request(
&self,
method: &str,
path: &str,
headers: &HashMap<String, String>,
query: &HashMap<String, String>,
body: &[u8],
secret_validated: bool,
) -> Result<HttpResponse, WasmChannelError> {
let _callback_guard = self.callback_lock.lock().await;

        tracing::info!(
            channel = %self.name,
            method = method,
            path = path,
            body_len = body.len(),
            secret_validated = secret_validated,
            "call_on_http_request invoked (webhook received)"
        );

        // Log the body for debugging (truncated at char boundary)
        if let Ok(body_str) = std::str::from_utf8(body) {
            let truncated = if body_str.chars().count() > 1000 {
                format!("{}...", body_str.chars().take(1000).collect::<String>())
            } else {
                body_str.to_string()
            };
            tracing::debug!(body = %truncated, "Webhook request body");
        }

        // Log credentials state (without values)
        let creds = self.get_credentials().await;
        tracing::info!(
            credential_count = creds.len(),
            credential_names = ?creds.keys().collect::<Vec<_>>(),
            "Credentials available for on_http_request"
        );

        // If no WASM bytes, return 200 OK (for testing)
        if self.prepared.component().is_none() {
            tracing::debug!(
                channel = %self.name,
                method = method,
                path = path,
                "WASM channel on_http_request called (no WASM module)"
            );
            return Ok(HttpResponse::ok());
        }

        let runtime = Arc::clone(&self.runtime);
        let prepared = Arc::clone(&self.prepared);
        let capabilities = Self::inject_workspace_reader(&self.capabilities, &self.workspace_store);
        let timeout = self.runtime.config().callback_timeout;
        let credentials = self.get_credentials().await;
        let host_credentials = resolve_channel_host_credentials(
            &self.capabilities,
            self.secrets_store.as_deref(),
            &self.owner_scope_id,
        )
        .await;
        let pairing_store = self.pairing_store.clone();
        let workspace_store = self.workspace_store.clone();
        let websocket_outbound_tx = self.websocket_outbound_tx.read().await.clone();

        // Prepare request data
        let method = method.to_string();
        let path = path.to_string();
        let headers_json = serde_json::to_string(&headers).unwrap_or_default();
        let query_json = serde_json::to_string(&query).unwrap_or_default();
        let body = body.to_vec();

        let channel_name = self.name.clone();

        // Execute in blocking task with timeout
        let result = tokio::time::timeout(timeout, async move {
            tokio::task::spawn_blocking(move || {
                let mut store = Self::create_store(
                    &runtime,
                    &prepared,
                    &capabilities,
                    credentials,
                    host_credentials,
                    pairing_store,
                    websocket_outbound_tx,
                )?;
                let instance = Self::instantiate_component(&runtime, &prepared, &mut store)?;

                // Build the WIT request type
                let wit_request = wit_channel::IncomingHttpRequest {
                    method,
                    path,
                    headers_json,
                    query_json,
                    body,
                    secret_validated,
                };

                // Call on_http_request using the generated typed interface
                let channel_iface = instance.near_agent_channel();
                let wit_response = channel_iface
                    .call_on_http_request(&mut store, &wit_request)//调用wasm的实现的方法
                    .map_err(|e| Self::map_wasm_error(e, &prepared.name, prepared.limits.fuel))?;

                let response = convert_http_response(wit_response);
                let mut host_state =
                    Self::extract_host_state(&mut store, &prepared.name, &capabilities);

                // Commit pending workspace writes to the persistent store
                let committed_paths =
                    Self::commit_callback_workspace_writes(&mut host_state, &workspace_store);

                Ok((response, host_state, committed_paths))
            })
            .await
            .map_err(|e| WasmChannelError::ExecutionPanicked {
                name: channel_name.clone(),
                reason: e.to_string(),
            })?
        })
        .await;

        let channel_name = self.name.clone();
        match result {
            Ok(Ok((response, mut host_state, committed_paths))) => {
                self.persist_durable_workspace_snapshot_if_needed(&committed_paths)
                    .await;
                // Process emitted messages
                let emitted = host_state.take_emitted_messages();
                self.process_emitted_messages(emitted).await?;

                tracing::debug!(
                    channel = %channel_name,
                    status = response.status,
                    "WASM channel on_http_request completed"
                );
                Ok(response)
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(WasmChannelError::Timeout {
                name: channel_name,
                callback: "on_http_request".to_string(),
            }),
        }
    }ult<HttpResponse, WasmChannelError> {


飞书为例子
fn on_http_request(req: IncomingHttpRequest) -> OutgoingHttpResponse {
// Parse the request body as UTF-8.
let body_str = match std::str::from_utf8(&req.body) {
Ok(s) => s,
Err(_) => {
return json_response(400, serde_json::json!({"error": "Invalid UTF-8 body"}));
}
};

        // Parse as Feishu event envelope.
        let event: FeishuEvent = match serde_json::from_str(body_str) {
            Ok(e) => e,
            Err(e) => {
                channel_host::log(
                    channel_host::LogLevel::Error,
                    &format!("Failed to parse Feishu event: {}", e),
                );
                return json_response(200, serde_json::json!({}));
            }
        };

        let configured_token =
            channel_host::workspace_read(VERIFICATION_TOKEN_PATH).filter(|token| !token.is_empty());
        if !is_authenticated_webhook(
            req.secret_validated,
            configured_token.as_deref(),
            request_verification_token(&event),
        ) {
            channel_host::log(
                channel_host::LogLevel::Warn,
                "Rejecting unauthenticated Feishu webhook request",
            );
            return json_response(
                401,
                serde_json::json!({"error": "Webhook authentication failed"}),
            );
        }

        // Handle URL verification challenge (initial webhook setup).
        if event.event_type.as_deref() == Some("url_verification") {
            if let Some(challenge) = &event.challenge {
                channel_host::log(
                    channel_host::LogLevel::Info,
                    "Handling URL verification challenge",
                );
                return json_response(200, serde_json::json!({ "challenge": challenge }));
            }
        }

        // Handle v2.0 events.
        if let Some(header) = &event.header {
            match header.event_type.as_str() {
                "im.message.receive_v1" => {
                    if let Some(event_data) = &event.event {
                        handle_message_event(event_data);///处理数据
                    }
                }
                other => {
                    channel_host::log(
                        channel_host::LogLevel::Debug,
                        &format!("Ignoring event type: {}", other),
                    );
                }
            }
        }

        // Always respond 200 quickly (Feishu expects fast responses).
        json_response(200, serde_json::json!({}))
    }

******************
Feishu 沙箱内 on_http_request 做的事

位置：channels-src/feishu/src/lib.rs:371-439

完整 7 步处理

1. 解析 body 为 UTF-8 字符串（行 373-378）

let body_str = match std::str::from_utf8(&req.body) { ... };
// 失败 → 返回 400 "Invalid UTF-8 body"

2. 解析为飞书事件信封（行 381-390）

let event: FeishuEvent = match serde_json::from_str(body_str) { ... };
// 失败 → 返回 200 {}（不报错，飞书重试会很烦）

FeishuEvent 是飞书 Event Subscription v2.0 的标准 envelope，含 schema / header / event / challenge / token / type 字段。

3. 校验 webhook 来源（行 392-407）

双重认证：
- 首选：宿主已校验过 secret（req.secret_validated == true）
- 回退：对比请求里带的 verification_token 与本地配置的 token

fn is_authenticated_webhook(secret_validated, configured_token, request_token) -> bool {
if secret_validated { return true; }  // 宿主已认证
// 否则用 constant-time 比较 verification token
bool::from(expected.as_bytes().ct_eq(provided.as_bytes()))
}

任一通过 → 继续；都失败 → 返回 401。

4. 处理 URL 验证挑战（行 410-418）

飞书首次配置 webhook 时会发 url_verification 类型的事件：
{"type": "url_verification", "challenge": "xxx", "token": "yyy"}

沙箱必须回 {"challenge": "xxx"} 完成握手。

5. 处理 v2.0 事件（行 421-435）

按 header.event_type 分发：
- "im.message.receive_v1" → 调 handle_message_event(event_data)（核心逻辑）
- 其它 → 记 debug 日志忽略

6. 调 handle_message_event（行 471-623）

这是真正处理消息的地方，分 6 个子步骤：

6.1 解析消息事件结构

- sender_id.open_id → 发送者 ID
- message.message_id / chat_id / chat_type（p2p 或 group）
- message.content（JSON 字符串，text 类型为 {"text": "..."}）
- message.mentions / parent_id / root_id

6.2 Owner 限制（行 491-499）

如果 OWNER_ID_PATH 在 workspace 里被设了（on_start 写入），只接受 owner 的消息，其它静默忽略。

6.3 allow_from 白名单（行 502-515）

如果 ALLOW_FROM_PATH 配置了用户列表，只接受列表内的用户。

6.4 DM 配对流程（行 525-582）— p2p 私聊专用

按 dm_policy（"open" 或 "pairing"）分支：

┌────────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────┐
│                  情况                  │                                               行为                                                │
├────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤
│ dm_policy = "open"                     │ 跳过配对检查                                                                                      │
├────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤
│ dm_policy = "pairing" 且 sender 已配对 │ user_id = owner_id（消息归属到 owner）                                                            │
├────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤
│ dm_policy = "pairing" 且 sender 未配对 │ 调 pairing_upsert_request 创建配对码 → 主动发飞书消息告诉用户配对码 → return（不调 emit_message） │
├────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 配对查询失败                           │ 记错误日志，return                                                                                │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────┘

6.5 提取文本（行 585-595）

- 只处理 message_type == "text"
- image / post / file 等类型 → 记 debug 日志，忽略

提取逻辑（extract_text_content，行 629-649）：
1. 解析 content JSON 字符串里的 "text" 字段
2. 把 @_user_1 这种 mention 占位符替换为真实用户名
3. trim

6.6 构造 metadata + emit_message（行 598-622）

let metadata = FeishuMessageMetadata {
chat_id, message_id, chat_type,
};
let thread_id = root_id.or(parent_id);  // 飞书的 thread 链

channel_host::emit_message(&EmittedMessage {
user_id,        // owner_id（已配对）或 sender_id
user_name: None,
content: text,
thread_id,
metadata_json,  // 序列化的 FeishuMessageMetadata，回复时回读
attachments: vec![],  // 当前实现不支持附件
});


fn handle_message_event(event_data: &serde_json::Value) {
// 1. 解析消息结构          → MessageReceiveEvent
// 2. 提取发送者 open_id   → sender_id
// 3. Owner 限制过滤        → 陌生人发给 owner 机器人
// 4. allow_from 白名单     → 特定用户白名单
// 5. DM 配对（p2p 专属）   → 新用户配对流程
// 6. 提取文本内容           → 过滤掉图片/文件等
// 7. 构造 metadata + thread → 回复上下文
// 8. emit_message          → 推给 agent
}

/// Handle an im.message.receive_v1 event.
fn handle_message_event(event_data: &serde_json::Value) {
let msg_event: MessageReceiveEvent = match serde_json::from_value(event_data.clone()) {
Ok(e) => e,
Err(e) => {
channel_host::log(
channel_host::LogLevel::Error,
&format!("Failed to parse message event: {}", e),
);
return;
}
};

    let sender_id = msg_event
        .sender
        .sender_id
        .open_id
        .as_deref()
        .unwrap_or("unknown");

    // Owner restriction check.
    if let Some(owner_id) = channel_host::workspace_read(OWNER_ID_PATH) {
        if !owner_id.is_empty() && sender_id != owner_id {
            channel_host::log(
                channel_host::LogLevel::Debug,
                &format!("Ignoring message from non-owner: {}", sender_id),
            );
            return;
        }
    }

    // allow_from restriction: if configured, only listed user IDs may interact.
    if let Some(allow_from_json) = channel_host::workspace_read(ALLOW_FROM_PATH) {
        if let Ok(allow_list) = serde_json::from_str::<Vec<String>>(&allow_from_json) {
            if !allow_list.is_empty() && !allow_list.iter().any(|id| id == sender_id) {
                channel_host::log(
                    channel_host::LogLevel::Debug,
                    &format!(
                        "Ignoring message from user not in allow_from: {}",
                        sender_id
                    ),
                );
                return;
            }
        }
    }

    // DM pairing check for p2p chats.
    let chat_type = msg_event.message.chat_type.as_deref().unwrap_or("unknown");

    // Resolved user_id for the emitted message. Defaults to sender_id but
    // is overwritten with the owner_id when the sender is paired, ensuring
    // the message is scoped to the correct owner/tenant.
    let mut user_id = sender_id.to_string();

    if chat_type == "p2p" {
        let dm_policy =
            channel_host::workspace_read(DM_POLICY_PATH).unwrap_or_else(|| "pairing".to_string());

        if dm_policy == "pairing" {
            match channel_host::pairing_resolve_identity("feishu", sender_id) {
                Ok(Some(owner_id)) => {
                    // Sender is paired; scope message to owner.
                    user_id = owner_id;
                }
                Ok(None) => {
                    // Unknown sender — upsert a pairing request.
                    let meta = serde_json::json!({
                        "sender_id": sender_id,
                        "chat_id": msg_event.message.chat_id,
                        "chat_type": chat_type,
                    });
                    match channel_host::pairing_upsert_request(
                        "feishu",
                        sender_id,
                        &meta.to_string(),
                    ) {
                        Ok(result) => {
                            channel_host::log(
                                channel_host::LogLevel::Info,
                                &format!(
                                    "Pairing request created for {}: {}",
                                    sender_id, result.code
                                ),
                            );
                            let _ = send_message(
                                sender_id,
                                "open_id",
                                &format!(
                                    "Enter this code in IronClaw to pair your feishu account: `{}`. CLI fallback: `ironclaw pairing approve feishu {}`",
                                    result.code, result.code
                                ),
                            );
                        }
                        Err(e) => {
                            channel_host::log(
                                channel_host::LogLevel::Error,
                                &format!("Pairing upsert failed: {}", e),
                            );
                        }
                    }
                    return;
                }
                Err(e) => {
                    channel_host::log(
                        channel_host::LogLevel::Error,
                        &format!("Pairing check failed: {}", e),
                    );
                    return;
                }
            }
        }
    }

    // Extract text content.
    let text = extract_text_content(&msg_event.message);
    if text.is_empty() {
        channel_host::log(
            channel_host::LogLevel::Debug,
            &format!(
                "Ignoring non-text message type: {}",
                msg_event.message.message_type
            ),
        );
        return;
    }

    // Build metadata for responding.
    let metadata = FeishuMessageMetadata {
        chat_id: msg_event.message.chat_id.clone(),
        message_id: msg_event.message.message_id.clone(),
        chat_type: chat_type.to_string(),
    };

    let metadata_json = serde_json::to_string(&metadata).unwrap_or_else(|_| "{}".to_string());

    // Determine thread ID from reply chain.
    let thread_id = msg_event
        .message
        .root_id
        .as_deref()
        .or(msg_event.message.parent_id.as_deref())
        .map(|s| s.to_string());

    // Emit message to the agent.
    channel_host::emit_message(&EmittedMessage {//宿主能力
        user_id,
        user_name: None,
        content: text,
        thread_id,
        metadata_json,
        attachments: vec![],
    });
}

src/channels/wasm/wrapper.rs:634-698
/// Store data for WASM channel execution.
///
/// Contains the resource limiter, channel-specific host state, and WASI context.
struct ChannelStoreData {
limiter: WasmResourceLimiter,
host_state: ChannelHostState,//消息先emit进这里
wasi: WasiCtx,
table: ResourceTable,
/// Injected credentials for URL substitution (e.g., bot tokens).
/// Keys are placeholder names like "TELEGRAM_BOT_TOKEN".
credentials: HashMap<String, String>,
/// Pre-resolved credentials for automatic host-based injection.
/// Applied per-request by matching the URL host against host_patterns.
host_credentials: Vec<ResolvedHostCredential>,
/// Pairing store for DM pairing (guest access control).
pairing_store: Arc<PairingStore>,
/// Optional websocket outbound sender for channels with a managed runtime.
websocket_outbound_tx: Option<mpsc::Sender<String>>,
/// Dedicated tokio runtime for HTTP requests, lazily initialized.
/// Reused across multiple `http_request` calls within one execution.
http_runtime: Option<tokio::runtime::Runtime>,
}



// Implement the generated Host trait for channel-host interface
impl near::agent::channel_host::Host for ChannelStoreData {
fn emit_message(&mut self, msg: near::agent::channel_host::EmittedMessage) {
// 1. 速率限制 (MAX_EMITS_PER_EXECUTION, 默认 100)                                                                                                                                           
// 2. 校验 attachments (MIME / 大小 / 数量)                                                                                                                                                  
// 3. 截断超长 content (MAX_MESSAGE_CONTENT_SIZE = 64KB)                                                                                                                                     
// 4. push 到 self.emitted_messages: Vec<EmittedMessage>                                                                                                                                     
// 注: 此时还没投递！只是暂存在 ChannelStoreData
let attachments: Vec<crate::channels::wasm::host::Attachment> = msg
.attachments
.into_iter()
.map(|a| {
// Parse extras-json for well-known fields
let extras: serde_json::Value = if a.extras_json.is_empty() {
serde_json::Value::Null
} else {
serde_json::from_str(&a.extras_json).unwrap_or(serde_json::Value::Null)
};
let duration_secs = extras
.get("duration_secs")
.and_then(|v| v.as_u64())
.map(|v| v as u32);

                // Merge stored binary data (from store-attachment-data host call)
                let data = self
                    .host_state
                    .remove_attachment_data(&a.id)
                    .unwrap_or_default();

                crate::channels::wasm::host::Attachment {
                }
            })
            .collect();

        let mut emitted = EmittedMessage::new(msg.user_id.clone(), msg.content.clone());
        if let Some(name) = msg.user_name {
            emitted = emitted.with_user_name(name);
        }
        if let Some(tid) = msg.thread_id {
            emitted = emitted.with_thread_id(tid);
        }
        emitted = emitted.with_metadata(msg.metadata_json);
        emitted = emitted.with_attachments(attachments);

        match self.host_state.emit_message(emitted) {//添加到队列
        }
    }
飞书的 emit_message 命中飞书的 ChannelStoreData 不是靠"类型匹配"——是靠 WasmChannel 实例的 name 字段：WasmChannel 是飞书专属的（注册时 name = "feishu"），它的 call_on_http_request 在 create_store 时把
prepared.name = "feishu" 传给 ChannelStoreData::new，再传给 ChannelHostState::new，于是沙箱内 self.host_state 天然就是飞书专属的。沙箱代码不感知"自己在飞书里"——它只调                                 
channel_host::emit_message，import stub 自动转发到正确的 host state。


# WASM 沙箱怎么知道调 `ChannelStoreData` 的方法

这个问题分两层：

1. **WASM 怎么"知道"调哪个 Rust 方法** —— 由 `wit-bindgen` + wasmtime linker 决定
2. **怎么把飞书的 `ChannelStoreData` 实例传进去** —— 由 wasmtime `Linker` 的 state 机制决定

## 关键：沙箱根本不知道 `ChannelStoreData` 的存在

**沙箱只知道一个抽象的 import 函数签名**，对应 `wit/channel.wit` 里 `interface channel-host` 声明的 12 个 func。它不"知道"任何 Rust 类型。

## 完整机制：3 个组件协作

### 组件 1：`wit-bindgen` 生成的 import stub（沙箱侧）

**位置**：编译时由 `wit-bindgen` 生成

**`channels-src/feishu/src/lib.rs:30-33`**：
```rust
wit_bindgen::generate!({
    world: "sandboxed-channel",
    path: "../../wit/channel.wit",
});
```

`wit-bindgen` 读 `wit/channel.wit`，生成：
- 沙箱**导出**的 `on_start` / `on_http_request` 等 6 个 export 函数（`channel` interface）
- 沙箱**导入**的 `emit_message` / `log` / `workspace_read` 等 12 个 import 函数（`channel-host` interface）

生成的 `channel_host::emit_message` 看起来像普通函数调用，**实际**展开成：
```rust
pub fn emit_message(msg: &EmittedMessage) {
    unsafe {
        // 调 wasmtime 的 canonical ABI trampoline
        // 把参数按 WIT 编码规则写到线性内存
        // 触发 host import
    }
}
```

**沙箱源码**（`feishu/src/lib.rs:615`）：
```rust
channel_host::emit_message(&EmittedMessage { ... });
//                ↑
//        这个是 wit-bindgen 生成的本地函数
//        编译后变成 wasm import
```

**沙箱视角**：调一个普通函数。
**wasm 二进制视角**：这是一个 `import` 声明，import 名是 `near:agent/channel-host/emit-message`。
**Rust 视角**：这是一个由 `wit-bindgen` 展开的 trampoline。

### 组件 2：`Linker` 注册 host 实现（宿主侧）

**位置**：`src/channels/wasm/wrapper.rs:1486-1496`

```rust
// Add WASI support (required by the component adapter)
wasmtime_wasi::p2::add_to_linker_sync(linker).map_err(|e| {
    WasmChannelError::Config(format!("Failed to add WASI functions: {}", e))
})?;

// Use the generated add_to_linker function from bindgen for our custom interface
SandboxedChannel::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
    linker,
    |state: &mut ChannelStoreData| state,    // ← 关键！告诉 linker 怎么拿 state
)
.map_err(|e| WasmChannelError::Config(format!("Failed to add host functions: {}", e)))?;
```

`SandboxedChannel::add_to_linker` 也是 `wit-bindgen` 生成的（编译时），**它做了两件事**：

1. **注册 12 个 host import**：每个 WIT `channel-host` 函数都注册一个 host 实现
2. **指定 state 提取器**：`|state: &mut ChannelStoreData| state` —— 告诉 wasmtime"沙箱调 host 函数时，state 从哪儿拿"

**沙箱调 `emit_message` 时实际发生的事**：
```
沙箱代码: channel_host::emit_message(msg)
   ↓ (WIT canonical ABI)
wasm import call
   ↓ (wasmtime router)
Linker 找到 import 名 "near:agent/channel-host/emit-message"
   ↓
查表: 这个 import → ChannelStoreData::emit_message (impl Host for ChannelStoreData)
   ↓
拿 state = |state: &mut ChannelStoreData| state  (从 Store<ChannelStoreData> 里)
   ↓
调 ChannelStoreData::emit_message(state, msg)
```

### 组件 3：`Store<ChannelStoreData>` 提供 state 实例

**位置**：`src/channels/wasm/wrapper.rs:1897`

```rust
let store_data = ChannelStoreData::new(
    limits.memory_bytes,
    &prepared.name,           // ← "feishu"
    capabilities.clone(),     // ← 飞书的 capabilities
    credentials,              // ← 飞书的凭据
    host_credentials,
    pairing_store,
    websocket_outbound_tx,
);
let mut store = Store::new(engine, store_data);  // ← 装进 Store
```

`Store<ChannelStoreData>` 是 wasmtime 的执行环境：
- `engine` —— wasmtime engine（编译好的 wasm）
- `store_data` —— **本次 callback 的 state 实例**

**沙箱调 `emit_message` 时**，wasmtime：
1. 拿到当前 `Store<ChannelStoreData>`（飞书专属的那个）
2. 用 `|state: &mut ChannelStoreData| state` 提取器拿到 `&mut ChannelStoreData`
3. 调 `ChannelStoreData::emit_message(state, msg)`（即 `impl Host for ChannelStoreData`）

**这就是飞书的 `ChannelStoreData` 怎么"传进去"的** —— 不是显式传，是 wasmtime 在沙箱调 import 时**自动**从 `Store` 里取。

## 完整时间线

```
[编译期]
channels-src/feishu/Cargo.toml
  └─ wit-bindgen::generate!({ world: "sandboxed-channel", ... })
       ↓ 读 wit/channel.wit
       ↓ 生成:
       │   - feishu/src/bindings.rs 包含 channel_host::emit_message (本地函数)
       │   - 编译时这个本地函数展开成 wasm import call

src/channels/wasm/wrapper.rs
  └─ 引入 wit_bindgen 生成的 add_to_linker
       ↓ 编译时生成:
       │   - 12 个 host 函数实现注册到 Linker
       │   - state 提取器: |state| state

[运行期 - 飞书 callback 到来]
飞书 POST /webhook/feishu
   ↓
webhook_handler 命中 WasmChannel (name="feishu")
   ↓
WasmChannel::call_on_http_request(self = 飞书的 Arc<WasmChannel>)
   ↓
create_store(prepared.name = "feishu")
   ↓
ChannelStoreData::new(name="feishu", ...)  ← 飞书专属 state
   ↓
let mut store = Store::new(engine, store_data)
   ↓
instance = instantiate(prepared, &mut store)
   ↓
调沙箱的 on_http_request(wit_request)
   ↓ (wasmtime 执行沙箱)
   ↓ 沙箱内 channel_host::emit_message(msg)
   ↓
   ↓ wit-bindgen 生成的本地函数展开成:
   ↓   import "near:agent/channel-host/emit-message" (canonical ABI)
   ↓
   ↓ wasmtime 拦截 → 查 Linker 注册表
   ↓ 命中: ChannelStoreData::emit_message
   ↓
   ↓ 拿 state = |s: &mut ChannelStoreData| s
   ↓ 从 Store<ChannelStoreData> 提取 → 飞书的 store_data
   ↓
   ↓ 调 ChannelStoreData::emit_message(&mut 飞书 store_data, msg)
   ↓
   ↓ 在 impl Host for ChannelStoreData 里
   ↓ self.host_state.channel_name = "feishu"  ← 自然就是飞书的
```

## 沙箱视角的"无知"

**沙箱**（`channels-src/feishu/src/lib.rs`）**完全不知道**：
- 自己在哪个 `ChannelStoreData` 里
- `ChannelStoreData` 这个 Rust 类型存在
- `impl Host for ChannelStoreData` 的存在
- wasmtime 的存在

沙箱只知道：
- 有一个 `channel_host` 模块
- 可以调 `channel_host::emit_message(msg)`
- msg 是个结构体，有 `user_id` / `content` 等字段

**这是 WIT（WebAssembly Interface Type）设计的核心：语言无关的接口定义**。同一份 `wit/channel.wit` 可以生成：
- Rust import（飞书沙箱用）
- Go import（未来可能的 Go 沙箱用）
- Python import（未来可能的 Python 沙箱用）

每个语言的 import 调法都不同，但 host 侧只要 `impl Host for WhateverState` 就行。

## 为什么 `impl for ChannelStoreData` 而不是 `impl for WasmChannel`？

`ChannelStoreData` 是**为每次 callback 临时构造的**：
- 飞书 callback #1 → `ChannelStoreData` 实例 A（带"feishu"标识）
- 飞书 callback #2 → `ChannelStoreData` 实例 B（带"feishu"标识）
- 同时 telegram callback → `ChannelStoreData` 实例 C（带"telegram"标识）

`WasmChannel` 是**持久的**，跨 callback 共享。如果 `impl for WasmChannel`：
- state 在 callback 间共享
- `emitted_messages: Vec` 会**累积**，callback 结束后不会清空
- 速率限制状态会混乱

`ChannelStoreData` 是**一次性的**：
- 新 callback → 新 `ChannelStoreData` → 新空 `Vec`
- callback 结束 → drop → 清空一切
- 状态隔离干净

## 一句话

**WASM 沙箱"知道"调 `ChannelStoreData` 的方法**——是 `wit-bindgen` 在编译期把抽象 WIT 接口展开成 4 个具体协作机制：
1. **沙箱侧**：生成 `channel_host::emit_message` 本地函数，编译成 wasm import call
2. **宿主侧**：用 `wit_bindgen` 生成的 `add_to_linker` 把 12 个 WIT 函数注册成 `ChannelStoreData` 的方法
3. **state 注入**：`Store::new(engine, ChannelStoreData_for_feishu)` 把飞书专属的 state 装进 wasmtime
4. **运行时路由**：沙箱调 import → wasmtime 查 Linker → 提取 Store 里的 state → 调 `ChannelStoreData::emit_message`

**沙箱代码完全不知道 `ChannelStoreData` 的存在**——它只调 `channel_host::emit_message`，WIT + wasmtime 完成所有"飞书识别"的工作。

/// Host state for WASM channel callbacks.
///
/// Maintains all side effects during callback execution and enforces limits.
/// This is the channel-specific equivalent of HostState for tools.
pub struct ChannelHostState {
/// Base tool host state (logging, time, HTTP, etc.).
base: HostState,

    /// Channel name (for error messages).
    channel_name: String,

    /// Channel capabilities.
    capabilities: ChannelCapabilities,

    /// Emitted messages (queued for delivery).
    emitted_messages: Vec<EmittedMessage>,

    /// Pending workspace writes.
    pending_writes: Vec<PendingWorkspaceWrite>,

    /// Emit count for rate limiting within this execution.
    emit_count: u32,

    /// Whether emit is still allowed (false after rate limit hit).
    emit_enabled: bool,

    /// Count of emits dropped due to rate limiting.
    emits_dropped: usize,

    /// Binary data stored for attachments via `store-attachment-data`.
    /// Keyed by attachment ID, cleared after callback completes.
    attachment_data: HashMap<String, Vec<u8>>,

    /// Total bytes stored in attachment_data (for enforcing limits).
    attachment_data_total: u64,
}


/// Emit a message from the channel.
///
/// Messages are queued and delivered after callback execution completes.
/// Rate limiting is enforced per-execution and globally.
/// Attachments are validated for count, total size, and MIME type.
pub fn emit_message(&mut self, msg: EmittedMessage) -> Result<(), WasmChannelError> {
// Check per-execution limit
if !self.emit_enabled {
self.emits_dropped += 1;
return Ok(()); // Silently drop, don't fail execution
}
self.emitted_messages.push(msg);//关键入队
self.emit_count += 1;
Ok(())
}
WASM 沙箱在同步的 spawn_blocking 里跑，mpsc::Sender::send() 是 async，沙箱没法直接调——消息只能先暂存到 Vec，等 spawn_blocking 结束回到 tokio 世界再 tx.send().await。

    pub async fn call_on_http_request(
        &self,
        method: &str,
        path: &str,
        headers: &HashMap<String, String>,
        query: &HashMap<String, String>,
        body: &[u8],
        secret_validated: bool,
    ) -> Result<HttpResponse, WasmChannelError> {在请求内执行完WASM后，尾部会从上面emit的队列里取消息并tx

      ......同之前刚开始



              let channel_name = self.name.clone();
        match result {
            Ok(Ok((response, mut host_state, committed_paths))) => {
                self.persist_durable_workspace_snapshot_if_needed(&committed_paths)
                    .await;
                // Process emitted messages
                let emitted = host_state.take_emitted_messages();//取出并发送到agentLoop
                self.process_emitted_messages(emitted).await?;

                tracing::debug!(
                    channel = %channel_name,
                    status = response.status,
                    "WASM channel on_http_request completed"
                );

    /// Process emitted messages from a callback.
    async fn process_emitted_messages(
        &self,
        messages: Vec<EmittedMessage>,
    ) -> Result<(), WasmChannelError> {
                for emitted in messages {
                  if tx.send(msg).await.is_err() {//发送到agentLoop
                      tracing::error!(
                          channel = %self.name,
                          "Failed to send emitted message, channel closed"
                      );
                      break;
                  }

agent执行完后，想要回复：调对应channel的response

    async fn respond(
        &self,
        msg: &IncomingMessage,
        response: OutgoingResponse,
    ) -> Result<(), ChannelError> {
                let result = self
            .call_on_respond(//wasm能力
                msg.id,
                &response.content,
                response.thread_id.as_ref().map(|t| t.as_str()),
                &metadata_json,
                &attachments,
                &response.inline_attachments,
            )
            .await;
        result.map_err(|e| ChannelError::SendFailed {//执行报错上抛
            name: self.name.clone(),
            reason: e.to_string(),
        })?;

        Ok(())

channel-src
fn on_respond(response: AgentResponse) -> Result<(), String> {
let metadata: FeishuMessageMetadata = serde_json::from_str(&response.metadata_json)
.map_err(|e| format!("Failed to parse metadata: {}", e))?;

        send_reply(&metadata.message_id, &response.content)
    }

fn send_reply(message_id: &str, content: &str) -> Result<(), String> {
let api_base = channel_host::workspace_read(API_BASE_PATH)
.unwrap_or_else(|| "https://open.feishu.cn".to_string());

    let token = get_valid_token(&api_base)?;

    let url = format!("{}/open-apis/im/v1/messages/{}/reply", api_base, message_id);

    let body = ReplyMessageBody {
        msg_type: "text".to_string(),
        content: serde_json::json!({"text": content}).to_string(),
    };

    let body_json =
        serde_json::to_string(&body).map_err(|e| format!("Failed to serialize body: {}", e))?;

    let headers = serde_json::json!({
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": format!("Bearer {}", token),
    });

    let result = channel_host::http_request(//借用宿主的http能力发给飞书服务器
        "POST",
        &url,
        &headers.to_string(),
        Some(body_json.as_bytes()),
        Some(10_000),
    );

    match result {
        Ok(response) => {
            if response.status != 200 {
                let body_str = String::from_utf8_lossy(&response.body);
                return Err(format!(
                    "Feishu API returned {}: {}",
                    response.status, body_str
                ));
            }
            // Check API-level error code.
            if let Ok(api_resp) =
                serde_json::from_slice::<FeishuApiResponse<serde_json::Value>>(&response.body)
            {
                if api_resp.code != 0 {
                    return Err(format!(
                        "Feishu API error {}: {}",
                        api_resp.code, api_resp.msg
                    ));
                }
            }
            Ok(())
        }
        Err(e) => Err(format!("HTTP request failed: {}", e)),
    }
}

impl near::agent::channel_host::Host for ChannelStoreData {
fn http_request(
&mut self,
method: String,
url: String,
headers_json: String,
body: Option<Vec<u8>>,
timeout_ms: Option<u32>,
) -> Result<near::agent::channel_host::HttpResponse, String> {

        // Inject credentials into URL (e.g., replace {TELEGRAM_BOT_TOKEN} with actual token)
        let injected_url = self.inject_credentials(&url, "url");

        // Log whether injection happened (without revealing the token)
        let url_changed = injected_url != url;
        tracing::info!(url_changed = url_changed, "URL after credential injection");

        // Check if HTTP is allowed for this URL
        self.host_state
            .check_http_allowed(&injected_url, &method)
            .map_err(|e| {
                tracing::error!(error = %e, "HTTP not allowed");
                format!("HTTP not allowed: {}", e)
            })?;

        // Record the request for rate limiting
        self.host_state.record_http_request().map_err(|e| {
            tracing::error!(error = %e, "Rate limit exceeded");
            format!("Rate limit exceeded: {}", e)
        })?;

        // Parse headers from WASM and scan for leaks before credential injection.
        // Host-injected tokens (e.g., xoxb- Slack bot token) would otherwise
        // trigger the leak detector. The URL has template substitution applied
        // (`injected_url`) but not yet host credential injection.
        let raw_headers: std::collections::HashMap<String, String> = serde_json::from_str(
            &headers_json,
        )
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Malformed headers JSON from WASM; scanning empty headers");
            std::collections::HashMap::new()
        });

        let mut logical_url = injected_url;

        let leak_detector = LeakDetector::new();
        let raw_header_vec: Vec<(String, String)> = raw_headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        leak_detector//探测过滤
            .scan_http_request(&logical_url, &raw_header_vec, body.as_deref())
            .map_err(|e| format!("Potential secret leak blocked: {}", e))?;

        // Now inject credentials into header values
        // This allows patterns like "Authorization": "Bearer {WHATSAPP_ACCESS_TOKEN}"
        let mut headers: std::collections::HashMap<String, String> = raw_headers
            .into_iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    self.inject_credentials(&v, &format!("header:{}", k)),
                )
            })
            .collect();

        let headers_changed = headers
            .values()
            .any(|v| v.contains("Bearer ") && !v.contains('{'));

        // Inject pre-resolved host credentials (Bearer tokens, API keys, etc.)
        // after the leak scan so host-injected secrets don't trigger false positives.
        if let Some(host) = extract_host_from_url(&logical_url) {
            self.inject_host_credentials(&host, &mut headers, &mut logical_url);
        }


        // Get the max response size from capabilities (default 10MB).
        let max_response_bytes = self
            .host_state
            .capabilities()
            .tool_capabilities
            .http
            .as_ref()
            .map(|h| h.max_response_bytes)
            .unwrap_or(10 * 1024 * 1024);

        // Resolve hostname and reject private/internal IPs to prevent DNS rebinding.
        // Test/dev URL rewrites intentionally point at local fake servers.
        if !allow_private_test_target {
            reject_private_ip(&transport_url)?;
        }

        // Make the HTTP request using a dedicated single-threaded runtime.
        // We're inside spawn_blocking, so we can't rely on the main runtime's
        // I/O driver (it may be busy with WASM compilation or other startup work).
        // A dedicated runtime gives us our own I/O driver and avoids contention.
        // The runtime is lazily created and reused across calls within one execution.
        if self.http_runtime.is_none() {
            self.http_runtime = Some(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("Failed to create HTTP runtime: {e}"))?,
            );
        }
        let rt = self.http_runtime.as_ref().expect("just initialized");
        let result = rt.block_on(async {
            let client = ssrf_safe_client_builder()
                .connect_timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| format!("Failed to build HTTP client: {e}"))?;

            let mut request = match method.to_uppercase().as_str() {
                "GET" => client.get(&transport_url),
                "POST" => client.post(&transport_url),
                "PUT" => client.put(&transport_url),
                "DELETE" => client.delete(&transport_url),
                "PATCH" => client.patch(&transport_url),
                "HEAD" => client.head(&transport_url),
                _ => return Err(format!("Unsupported HTTP method: {}", method)),
            };

            // Add headers
            for (key, value) in headers {
                request = request.header(&key, &value);
            }

            // Add body if present
            if let Some(body_bytes) = body {
                request = request.body(body_bytes);
            }

            // Send request with caller-specified timeout (default 30s, max 5min).
            let timeout_ms = timeout_ms.unwrap_or(30_000).min(300_000) as u64;
            let timeout = std::time::Duration::from_millis(timeout_ms);
            let response = request.timeout(timeout).send().await.map_err(|e| {///发送请求
                // Walk the full error chain so we get the actual root cause
                // (DNS, TLS, connection refused, etc.) instead of just
                // "error sending request for url (...)".
                let mut chain = format!("HTTP request failed: {}", e);
                let mut source = std::error::Error::source(&e);
                while let Some(cause) = source {
                    chain.push_str(&format!(" -> {}", cause));
                    source = cause.source();
                }
                chain
            })?;

            let status = response.status().as_u16();
            let response_headers: std::collections::HashMap<String, String> = response
                .headers()
                .iter()
                .filter_map(|(k, v)| {
                    v.to_str()
                        .ok()
                        .map(|v| (k.as_str().to_string(), v.to_string()))
                })
                .collect();
            let headers_json = serde_json::to_string(&response_headers).unwrap_or_default();

            // Enforce max response body size to prevent memory exhaustion.
            let max_response = max_response_bytes;
            if let Some(cl) = response.content_length()
                && cl as usize > max_response
            {
                return Err(format!(
                    "Response body too large: {} bytes exceeds limit of {} bytes",
                    cl, max_response
                ));
            }
            let body = response
                .bytes()
                .await
                .map_err(|e| format!("Failed to read response body: {}", e))?;
            if body.len() > max_response {
                return Err(format!(
                    "Response body too large: {} bytes exceeds limit of {} bytes",
                    body.len(),
                    max_response
                ));
            }
            let body = body.to_vec();

            tracing::info!(
                status = status,
                body_len = body.len(),
                "HTTP response received"
            );

            // Log response body for debugging (truncated at char boundary)
            if let Ok(body_str) = std::str::from_utf8(&body) {
                let truncated = if body_str.chars().count() > 500 {
                    format!("{}...", body_str.chars().take(500).collect::<String>())
                } else {
                    body_str.to_string()
                };
                tracing::debug!(body = %truncated, "Response body");
            }

            // Leak detection on response body (best-effort).
            //
            // Telegram `getUpdates` is special: it is inbound polling data, so
            // user-pasted secrets can legitimately appear in the response body.
            // Those messages are still checked later by the inbound message
            // safety layer before they reach the LLM, so we allow the polling
            // response to continue here to avoid poisoning the offset state.
            if let Ok(body_str) = std::str::from_utf8(&body)
                && !should_skip_response_leak_scan(&logical_url)
            {
                leak_detector
                    .scan_and_clean(body_str)
                    .map_err(|e| format!("Potential secret leak in response: {}", e))?;
            }

            Ok(near::agent::channel_host::HttpResponse {
                status,
                headers_json,
                body,
            })
        });

        // Scrub credential values from error messages before logging or returning
        // to WASM. reqwest::Error includes the full URL (with injected credentials)
        // in its Display output.
        let result = result.map_err(|e| self.redact_credentials(&e));

        match &result {
            Ok(resp) => {
                tracing::info!(status = resp.status, "http_request completed successfully");
            }
            Err(e) => {
                tracing::error!(error = %e, "http_request failed");
            }
        }

        result
    }

⑤配对逻辑

现在有了完整链路。以下是飞书 DM 配对端到端流程：

---

## 飞书 DM 配对 — 完整链路

### 整体时序

```
外部用户 DM 飞书机器人
  → 飞书服务器 webhook
  → WebhookServer (axum)
  → WasmChannelRouter 路由
  → 签名验证
  → wasmtime 沙箱: on_http_request
  → handle_message_event
    ├── 已配对? → user_id=owner_id → emit_message → agent 处理
    └── 未配对? → pairing_upsert_request(生成配对码)
                → send_message(DM 发配对码给用户)
                → return (不 emit, 消息不进 agent)
  ← 用户拿到配对码
  → Web UI 或 CLI 提交配对码
  → PairingStore::approve(写入 channel_identities 表)
  → propagate_approval(更新运行中 channel 的 owner 绑定 + 重启 polling)
  ← 用户再次发 DM
  → resolve_identity 命中 → emit_message → agent 正常处理
```

---

### 阶段 1：webhook 进沙箱，判断是否 p2p

`channels-src/feishu/src/lib.rs:525-581`：

```rust
if chat_type == "p2p" {
    let dm_policy = channel_host::workspace_read(DM_POLICY_PATH)
        .unwrap_or_else(|| "pairing".to_string());

    if dm_policy == "pairing" {
        match channel_host::pairing_resolve_identity("feishu", sender_id) {
            Ok(Some(owner_id)) => {
                user_id = owner_id;  // 已配对，消息归属到 owner
            }
            Ok(None) => {
                // 未配对 → 创建配对请求 + 发 DM 告知配对码
                channel_host::pairing_upsert_request("feishu", sender_id, &meta);
                send_message(sender_id, "open_id",
                    "Enter this code in IronClaw to pair your feishu account: `{code}`");
                return;  // ← 不 emit_message，消息不进 agent
            }
        }
    }
}
```

关键判断在 `chat_type == "p2p"`（飞书的"私聊"即 DM）。群聊 (`chat_type == "group"`) 不受配对限制。

---

### 阶段 2：宿主侧 — `pairing_resolve_identity` 查 DB

飞书沙箱调的是 WIT host import → 落到 `src/channels/wasm/wrapper.rs:745`：

```rust
fn pairing_resolve_identity(&mut self, channel: String, external_id: String)
    -> Result<Option<String>, String>
{
    // block_in_place + block_on：在 spawn_blocking 的同步线程里跑异步 DB 查询
    let result = tokio::task::block_in_place(move || {
        handle.block_on(async move {
            store.resolve_identity(&channel, &external_id).await
        })
    });
    // 把 UserId 转回 String 给 WASM
}
```

然后 `src/pairing/store.rs:55`：

```rust
pub async fn resolve_identity(&self, channel: &str, external_id: &str)
    -> Result<Option<UserId>, DatabaseError>
{
    // 1. 先查内存缓存 (OwnershipCache)
    if let Some(identity) = self.cache.get(&channel, external_id) {
        return Ok(Some(identity));  // 热路径，零 DB 查询
    }
    // 2. 缓存未命中 → 查 DB (channel_identities JOIN users)
    let identity = db.resolve_channel_identity(&channel, external_id).await?;
    // 3. 查到就写入缓存
    if let Some(ref id) = identity {
        self.cache.insert(&channel, external_id, id.clone());
    }
    Ok(identity)
}
```

**返回 `Some(UserId)`** = 这个 `sender_id` 已被某个 IronClaw 用户 claim 过了，消息可以进 agent。

**返回 `None`** = 陌生人，需要走配对流程。

---

### 阶段 3：宿主侧 — `pairing_upsert_request` 生成配对码

`src/channels/wasm/wrapper.rs:715` → `PairingStore::upsert_request` → `src/pairing/store.rs:85`：

```rust
pub async fn upsert_request(&self, channel: &str, external_id: &str, meta: Option<Value>)
    -> Result<PairingRequestRecord, DatabaseError>
{
    db.upsert_pairing_request(&channel, external_id, meta).await
}
```

DB 层生成 8 位配对码（`src/pairing/code.rs` 字母表：`ABCDEFGHJKLMNPQRSTUVWXYZ23456789`，避免混淆字符 I/1、O/0），有效期 15 分钟。返回 `PairingRequestRecord { code, created, expires_at, ... }`。

飞书沙箱拿到 `code` 后，通过 `send_message`（飞书服务端 API）把配对码 DM 发给用户。

---

### 阶段 4：用户提交配对码 — 两条入口

**Web UI 路径**：`POST /api/pairing/{channel}/approve`

`src/channels/web/features/pairing/mod.rs:118` → `pairing_approve_handler`：
- 从 URL path 取 channel 名（如 `feishu`）
- 从 JSON body 取 `{ code, thread_id?, request_id? }`
- 调用 `PairingStore::approve`
- 成功后 `ext_mgr.complete_pairing_approval` 传播到运行中 channel
- 失败则 `store.revert_approval` 回滚

**CLI 路径**：`ironclaw pairing approve feishu XXXXXXX`

`src/cli/pairing.rs:132` → `run_approve`：
- 直接从 config 取 owner_id
- 调用 `PairingStore::approve`
- CLI 不做 channel 传播（无运行中 channel 引用）

---

### 阶段 5：`PairingStore::approve` — 写入配对关系

`src/pairing/store.rs:115`：

```rust
pub async fn approve(&self, channel: &str, code: &str, owner_id: &UserId)
    -> Result<PairingApprovalRecord, DatabaseError>
{
    // 规范化配对码（大小写、空格）
    let normalized = code.trim().to_ascii_uppercase();
    // DB 原子操作：验证 code 有效 + 创建 channel_identities 行
    db.approve_pairing(&channel, &normalized, owner_id.as_str()).await
}
```

DB 层的 `approve_pairing` 做的事：
1. 在 `pairing_requests` 表查找 `(channel, code)` → 拿到 `external_id`
2. 在 `channel_identities` 表插入 `(channel, external_id, owner_id)`
3. 删除已消费的配对请求行
4. 这在一个事务内完成

**后续 `resolve_identity` 就能命中**了：缓存热路径直接返回 `Some(owner_id)`，连 DB 都不用查。

---

### 阶段 6：`propagate_approval` — 通知运行中 channel

`src/pairing/approval.rs:34`（仅 Web UI 路径，CLI 跳过）：

```rust
pub async fn propagate_approval(channel, channel_name, external_id, deps) {
    // 1. 记录旧状态（用于失败回滚）
    let previous_owner = channel.owner_actor_id().await;
    let previous_config = channel.config_json_snapshot().await;

    // 2. 设置新 owner
    channel.set_owner_actor_id(Some(external_id)).await;

    // 3. 更新运行时配置（tunnel_url, owner_id 等）
    channel.update_config(config_updates).await;

    // 4. 调用 on_start() 重启 polling
    let config = channel.call_on_start().await?;

    // 5. 失败 → 回滚到旧状态
    //    成功 → ensure_polling 启动新轮询
    channel.ensure_polling(&config).await;
}
```

---

### 阶段 7：用户再次发 DM → 消息正常进入 agent

此次 `resolve_identity("feishu", "ou_xxx")` → `OwnershipCache` 命中 → `Some("ironclaw-user-1")`。

飞书沙箱设置 `user_id = "ironclaw-user-1"`，调用 `emit_message`。

---

### 阶段 8：`process_emitted_messages` 的 scope 二次校验

`src/channels/wasm/wrapper.rs:3185`：

```rust
let (resolved_user_id, is_owner_sender) = resolve_message_scope_with_pairing(
    &self.name,
    &self.owner_scope_id,
    owner_actor_id.as_deref(),
    channel_bound_user_id.as_deref(),
    &user_id,                    // WASM 沙箱传出来的
    self.pairing_store.as_ref(), // 再查一次 pairing store
).await;
```

这里的 `resolve_message_scope_with_pairing` 是**防御性二次校验**——即使 WASM 沙箱传了 `user_id`，宿主侧仍然用 `pairing_store` 再次确认归属关系，防止沙箱内的逻辑错误导致消息 scope 错乱。

---

### 一句话总结

**第一次 DM** → 飞书沙箱发现 `p2p` + 未配对 → 生成配对码 → DM 发给用户 → **消息不进入 agent**。

**用户在 Web UI/CLI 输入配对码** → `PairingStore::approve` 写入 `channel_identities(飞书, sender_id, owner_id)` → `propagate_approval` 重启 channel polling。

**第二次 DM** → `resolve_identity` 命中 → `user_id = owner_id` → `emit_message` → agent 正常处理。

好问题。**飞书不知道**——必须由人去飞书开放平台手动配置 webhook 地址。

## 完整初始化链路

```
[IronClaw 启动]
  → 加载 feishu.wasm
  → on_start() → 返回 http_endpoints: [{ path: "/webhook/feishu", methods: ["POST"] }]
  → WasmChannelRouter 注册路由 /webhook/feishu → POST
  → WebhookServer 启动，监听本地端口 (如 127.0.0.1:8742)
  → Tunnel (cloudflared/ngrok) 启动 → 公网 URL: https://xxx.tunnel.example.com

[人工操作] 开发者去飞书开放平台
  → 创建自建应用 → 开启"机器人"能力
  → 事件订阅 → 配置请求网址: https://xxx.tunnel.example.com/webhook/feishu
  → 飞书向该 URL 发送 URL 验证请求 (challenge)

[IronClaw 收到验证]
  → webhook_handler → 路由到 feishu.wasm
  → on_http_request → event_type == "url_verification"
  → 返回 { challenge: "收到的 challenge 值" }
  → 飞书确认验证通过 ✓

[此后] 用户给飞书机器人发 DM
  → 飞书服务器 POST https://xxx.tunnel.example.com/webhook/feishu
  → IronClaw WebhookServer 收到 → 沙箱处理 → 配对/emit
```

## 关键：`on_start()` 返回的 `http_endpoints` 就是注册的路由

`channels-src/feishu/src/lib.rs:360-368`：

```rust
fn on_start(config_json: String) -> Result<ChannelConfig, String> {
    // ... 存储 app_id, app_secret, verification_token 到 workspace ...
    // ... 获取 tenant_access_token ...

    Ok(ChannelConfig {
        display_name: "Feishu".to_string(),
        http_endpoints: vec![HttpEndpointConfig {
            path: "/webhook/feishu".to_string(),  // ← 这个路径被注册到 axum Router
            methods: vec!["POST".to_string()],
            require_secret: false,
        }],
        poll: None,  // 飞书用 webhook，不用轮询
    })
}
```

宿主侧在 `src/channels/wasm/setup.rs:281` 用这个信息注册路由：

```rust
let webhook_path = format!("/webhook/{}", channel_name);  // → "/webhook/feishu"
// 注册到 WasmChannelRouter → 最终合并到 WebhookServer 的 axum Router
```

同时把 tunnel 的公网 URL 通过运行时配置注入：

```rust
let mut config_updates = build_runtime_config_updates(
    config.tunnel.public_url.as_deref(),  // ← https://xxx.tunnel.example.com
    webhook_secret.as_deref(),
    owner_actor_id.as_deref(),
);
channel.update_config(config_updates).await;
```

这样飞书沙箱内部可以拿到 `tunnel_url` 来构造回链等，但 webhook 的注册本身是**宿主侧完成的**——沙箱只声明"我需要 `/webhook/feishu` 这个路径"，宿主负责把它挂到 axum Router 上。

## 一句话

**飞书不知道 agent 在哪——是 DevOps 在飞书开放平台"事件订阅"页，手动填入 IronClaw 的公网 tunnel URL (`https://<tunnel>/webhook/feishu`)，飞书发一个 challenge 验证通过后，才开始推送消息。** 没有任何自动发现或注册协议，纯手工配置。

