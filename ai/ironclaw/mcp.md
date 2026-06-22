一. 相关结构

1. MCPSessionManager（管理client和server之间的会话）

/// Manages MCP sessions across multiple `(user, server)` pairs.
///
/// Server names are typed via [`McpServerName`] so a free-form string can't
/// bypass allowlist validation at the boundary. Callers convert raw strings
/// via `McpServerName::new` (validating) or `McpServerName::from_trusted`
/// (for names the caller already validated). This makes identity-confusion
/// bugs — matching the shape described in `.claude/rules/types.md` — a
/// compile error rather than a runtime surprise.
pub struct McpSessionManager {
/// Active sessions keyed by `(user_id, server_name)`.
sessions: RwLock<HashMap<McpSessionKey, McpSession>>,

    /// Maximum idle time before a session is considered stale (in seconds).
    max_idle_secs: u64,30min
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct McpSessionKey {
user_id: String,
server_name: McpServerName,
}

/// Session state for a single `(user, server)` MCP connection.
#[derive(Debug, Clone)]
pub struct McpSession {
/// Session ID returned by the server (via Mcp-Session-Id header).
pub session_id: Option<String>,

    /// Last activity timestamp for this session.
    pub last_activity: Instant,

    /// Server URL this session is connected to.
    pub server_url: String,

    /// Whether initialization has completed.
    pub initialized: bool,
}

2. MCPProcessManager（管理本地server）

/// Manages stdio MCP server processes.
///
/// Handles spawning, tracking, and shutdown of child processes. Keyed
/// by `(user_id, server_name)` so that multiple tenants activating
/// the same server name end up with distinct, independently tracked
/// child processes — see `McpProcessKey` for the rationale.
pub struct McpProcessManager {
transports: RwLock<HashMap<McpProcessKey, Arc<StdioMcpTransport>>>,
configs: RwLock<HashMap<McpProcessKey, StdioSpawnConfig>>,
}

/// Composite key for a stdio MCP child process: the activating user
/// plus the server name. Both fields participate in `Hash` / `Eq` so
/// two users activating the same server name each get — and keep —
/// their own child process instead of one silently overwriting the
/// other's transport handle.
///
/// Stdio MCP servers receive credentials via their spawn `env` map, so
/// sharing a single child across users would leak one tenant's
/// credentials to the other's dispatches. Per-user children are
/// required; the process manager must track them independently.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct McpProcessKey {
pub user_id: String,
pub server_name: String,
}


1. "复合键"为啥是 (user, server_name) 而不是 server_name

直觉上 stdio MCP 是个全局的本地服务,似乎一个 server 跑一个进程就够了。但 stdio 模式下,每个用户激活时,凭据是通过 env 注入到子进程的环境变量里的(而不是像 HTTP 那样每次请求带 header)。

所以会出现这种场景:

┌───────┬─────────────────────────────────────────┐
│ 用户  │      同一个 MCP server github-mcp       │
├───────┼─────────────────────────────────────────┤
│ Alice │ spawn 时 env: { GH_TOKEN: alice_token } │
├───────┼─────────────────────────────────────────┤
│ Bob   │ spawn 时 env: { GH_TOKEN: bob_token }   │
└───────┴─────────────────────────────────────────┘

如果用 server_name 当 key,后到的 Bob 会覆盖 Alice 的子进程:
- Alice 的 child 被 kill
- Bob 的 child 占位
- 之后 Alice 的请求走到 Bob 的进程 → Alice 的请求用 Bob 的 token 调 GitHub

→ 凭据跨租户泄露,而且因为是静默覆盖,审计日志里也看不出来。

2. 为什么不通过请求时切换 env 复用同一个子进程

stdio 是单向字节流(stdin 写、stdout 读),不是按"租户"分流的。一个 child 进程内部,server 端的 process.env.GH_TOKEN 在启动那一刻就固定了——不是按请求切。

要支持多租户,只能多 child。

3. 这跟 HTTP / SSE 模式形成对比

┌───────────┬─────────────────────────────────┬─────────────────────────────────┐
│ Transport │           凭据怎么传            │    是否需要每用户一个 child     │
├───────────┼─────────────────────────────────┼─────────────────────────────────┤
│ stdio     │ spawn 时 env 注入,固定在进程里  │ ✅  需要                         │
├───────────┼─────────────────────────────────┼─────────────────────────────────┤
│ http      │ 每次请求带 Authorization header │ ❌  一个 server 进程服务所有用户 │
├───────────┼─────────────────────────────────┼─────────────────────────────────┤
│ sse       │ 同 http                         │ ❌                               │
└───────────┴─────────────────────────────────┴─────────────────────────────────┘

这是为啥前面讲 McpProcessManager 在 stdio 模式才用、http/sse 模式闲置——只有 stdio 有"凭据绑死进程"这个问题。

4. Hash / Eq 参与的意义

McpProcessManager 内部应该是 HashMap<ProcessKey, ChildHandle> 之类:

struct ProcessKey {
owner_id: String,
server_name: String,
}

只有两个字段都参与哈希,两个用户激活同名 server时才会被识别为不同的 key,各自拿到独立的 child,互不污染。

如果 Hash / Eq 只看 server_name(字段存在但没参与哈希),HashMap 行为就 undefined——这是 Rust 里经典的"字段没参与等价性"陷阱,注释把这点点出来是为了显式约束而不是默认行为。

5. 一句话

▎ stdio 模式下,凭据绑死子进程的 env 启动参数,所以必须按 (user, server_name) 复合键管理子进程——否则会跨用户泄露凭据,而且是静默的。HTTP/SSE 模式没这个问题,所以那个模式根本不需要这个 manager。

/// MCP transport that communicates with a child process over stdin/stdout.
///
/// The child process is spawned with piped stdin/stdout/stderr. Requests are
/// written as newline-delimited JSON to stdin, and responses are read from
/// stdout by a background reader task. Stderr is drained to tracing logs.
pub struct StdioMcpTransport {
server_name: String,
stdin: Arc<Mutex<tokio::process::ChildStdin>>,
pending: Arc<Mutex<HashMap<u64, oneshot::Sender<McpResponse>>>>,
reader_handle: Mutex<Option<JoinHandle<()>>>,
stderr_handle: Mutex<Option<JoinHandle<()>>>,
child: Arc<Mutex<Child>>,
}
这个 struct 是 IronClaw 跟本地 stdio MCP server 通信的桥梁。核心模式是:把 stdin/stdout 当成"无连接的 JSON-RPC 通道"用,自己实现请求-响应的匹配(pending map 起的作用)。

整体通信模型

┌─────────────────────────┐                    ┌────────────────────┐
│  IronClaw (parent)      │  stdin (write JSON) │  MCP server (child) │
│                         │ ───────────────────►│                    │
│  StdioMcpTransport      │                     │  npx mcp-foo       │
│                         │  stdout (read JSON) │                    │
│                         │ ◄─────────────────── │                    │
│                         │  stderr (drain log) │                    │
└─────────────────────────┘  ───────────────────►│                    │
└────────────────────┘

MCP 用的是 JSON-RPC over stdio:每条消息一行 JSON(以 \n 结尾),没有 HTTP 那种"请求-响应天然配对"的协议层——所有响应都是异步的,得自己按 id 匹配。

字段逐个拆

1. server_name: String

这台 server 的人话名字(比如 "github-mcp"),用于:

- 日志/排错时区分多个 server
- 报错信息里标识是哪个 server 出问题
- 可能参与 child process key(配合 user_id,见上条笔记)

2. stdin: Arc<Mutex<tokio::process::ChildStdin>>

写端——往子进程的 stdin 写 JSON-RPC 请求。

- Arc<Mutex<>> 是因为可能多任务并发写(比如工具调用 + 通知 + 心跳),需要互斥
- tokio::process::ChildStdin 兼容 async,等子进程慢的时候不会阻塞 runtime
- stdin 在 spawn 时设了 piped(),拿到 handle 才能写

3. pending: Arc<Mutex<HashMap<u64, oneshot::Sender<McpResponse>>>>

请求-响应的"信箱"——这是整个 transport 设计最巧妙也最关键的部分。

- key:u64 是 JSON-RPC 的 id(每发一个请求分配一个递增 id)
- value:oneshot::Sender<McpResponse> 是一次性 channel,谁发的请求,响应来了就交给谁

工作流程:

调用方:                          transport:                        后台 reader:
│
send(req)                                                              │
│                                                                     │
▼                                                                     │
id = next_id()                                                         │
│                                                                     │
▼                                                                     │
pending.insert(id, oneshot_tx) ◄── 在 map 里占个位                       │
│                                                                     │
▼                                                                     │
stdin.write(req.to_json() + "\n")  ─────►  子进程收到                   │
│                                                                     │
▼                                                                     │
oneshot_rx.await ──── 等响应 ────────────────────────────────────────  │
子进程从 stdout 吐回响应  │
│                  │
▼                  │
reader task 拿到响应           │
│                  │
▼                  │
pending.remove(&id).unwrap().send(resp) ─┘
│
▼
oneshot_rx 唤醒,返回给调用方

如果没这个 map,你就没法把"读到的响应"和"发出的请求"对上——JSON-RPC 的响应是异步回来的,响应顺序也不一定跟请求顺序一致。

4. reader_handle: Mutex<Option<JoinHandle<()>>>

后台读 stdout 的 task 的句柄。

- 启动 transport 时会 tokio::spawn 一个 task,死循环从 stdout 读一行 JSON,解析,按 id 派发到 pending 里对应的 sender
- JoinHandle<()> 是为了将来能 await 它(等它退出)或者 abort() 它(强制关)
- Option 是因为可能还没起,或者已经退了
- Mutex 因为 handle 可能被多个方法访问(正常调用、shutdown、debug)

5. stderr_handle: Mutex<Option<JoinHandle<()>>>

后台读 stderr 的 task 句柄。

- 子进程的 stderr 不参与协议,纯粹是日志输出
- 不能"不读"——pipe buffer 是有限的(通常 64KB),子进程 stderr 写满就阻塞
- 所以 IronClaw 启个 task 死循环读 stderr,转成 tracing::warn! / debug! 记下来
- 行为上类似"tail -f"那个进程

6. child: Arc<Mutex<Child>>

子进程本身的句柄。

- tokio::process::Child 包含 PID、stdin/stdout handle、wait 状态等
- Arc<Mutex> 是因为:
  - 写 stdin 时不需要锁 child,但要 kill 进程时得拿到 child
  - wait() 一次消耗状态,锁住避免重复 wait
  - 可能多处持有(transport 自己、shutdown 流程)
- 持有它是为了:child.kill().await / child.wait().await 干净地结束子进程

数据流时序图

spawn() 拿到 (Child, ChildStdin, ChildStdout, ChildStderr)
│
├─► tokio::spawn(read_stdout_task)  ◄── reader_handle
│      loop: line = read_line(); parse; dispatch by id to pending
│
├─► tokio::spawn(read_stderr_task)  ◄── stderr_handle
│      loop: line = read_line(); tracing::warn!(server=%server_name, "{}", line)
│
└─► 组装成 StdioMcpTransport { stdin, pending, child, ... }

send_request(req):
id = next_id()
pending.insert(id, tx)
stdin.write(req.to_json() + "\n").await
return rx.await   ◄── 等 reader 派发

shutdown:
stdin.close()    // 关写端,子进程读 EOF 会优雅退出
wait child with timeout
if still alive: child.kill()
abort reader_handle, stderr_handle

一句话

▎ StdioMcpTransport 把"子进程的 stdin/stdout"包装成"请求-响应匹配的 JSON-RPC 通道":stdin 写请求,pending map 按 id 把异步回来的响应回给对应调用方,reader_handle 持续从 stdout 解析响应,stderr_handle
▎ 持续把子进程日志倒进 tracing,child 是进程句柄用来 kill/wait,server_name 用于日志区分。最关键的设计是 pending map——JSON-RPC 的响应是异步无序的,没有它就没法把响应匹配回请求。


管道接口
/// Trait for sending JSON-RPC requests to an MCP server and receiving responses.
///
/// Implementations handle the underlying transport (HTTP, stdio, unix socket, etc.).
#[async_trait]
pub trait McpTransport: Send + Sync {
/// Send a request and wait for the corresponding response.
///
/// `headers` are used by HTTP-based transports (e.g., `Mcp-Session-Id`);
/// stream-based transports may ignore them.
async fn send(
&self,
request: &McpRequest,
headers: &HashMap<String, String>,
) -> Result<McpResponse, ToolError>;

    /// Shut down the transport, releasing any resources (child processes, connections).
    async fn shutdown(&self) -> Result<(), ToolError>;

    /// Whether this transport supports HTTP-specific features like session headers.
    fn supports_http_features(&self) -> bool {
        false
    }
}
***************

    pub async fn spawn(
        name: impl Into<String>,
        command: &str,
        args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>,
        env: impl IntoIterator<Item = (impl AsRef<std::ffi::OsStr>, impl AsRef<std::ffi::OsStr>)>,
    ) -> Result<Self, ToolError> {

......

let reader_handle = spawn_jsonrpc_reader(reader, pending.clone(), server_name.clone());创建一个后台任务，读返回值
去进行异步响应！！！
从pending里取出本次id,然后发送结构到tx内
.....

****************
impl McpTransport for StdioMcpTransport {
async fn send(
&self,
request: &McpRequest,
_headers: &HashMap<String, String>,
) -> Result<McpResponse, ToolError> {//发送消息
stream_transport_send(
&self.stdin,
&self.pending,
request,
&self.server_name,
Duration::from_secs(30),
)
.await
}
stream_transport_send(做的事情
let id = request.id.unwrap_or(0);
let (tx, rx) = oneshot::channel();//创建管道

    // Register the pending response handler before writing the request,
    // so we don't miss a fast response from the server.
    {
        let mut map = pending.lock().await;
        map.insert(id, tx);//插入pending
    }

    // Write the request.
    {
        let mut w = writer.lock().await;
        if let Err(e) = write_jsonrpc_line(&mut *w, request).await {//发送stdio请求
            // Remove the pending entry on write failure.
            let mut map = pending.lock().await;
            map.remove(&id);
            return Err(e);
        }
    }

    // Wait for the response with a timeout.
    match tokio::time::timeout(timeout_duration, rx).await {//tx发送，这边收到，然后返回mcpresponse, 后台任务是在
        Ok(Ok(response)) => Ok(response),//这个进程创建时启动的

**********

    async fn shutdown(&self) -> Result<(), ToolError> {//终止时清理
        // Kill the child process.
        {
            let mut child = self.child.lock().await;
            let _ = child.kill().await;
        }

        // Abort the reader tasks.
        if let Some(handle) = self.reader_handle.lock().await.take() {
            handle.abort();
        }
        if let Some(handle) = self.stderr_handle.lock().await.take() {
            handle.abort();
        }

        // Drain pending requests so waiters wake immediately instead of
        // hanging until their 30s timeout.
        {
            let mut pending = self.pending.lock().await;
            pending.clear(); // Dropping senders wakes receivers with Err
        }

        tracing::debug!("[{}] Stdio transport shut down", self.server_name);
        Ok(())
    }

    fn supports_http_features(&self) -> bool {
        false
    }
}


/// Request to an MCP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpRequest {
/// JSON-RPC version.
pub jsonrpc: String,
/// Request ID (None for notifications per JSON-RPC spec).
#[serde(skip_serializing_if = "Option::is_none")]
pub id: Option<u64>,
/// Method name.
pub method: String,
/// Request parameters.
#[serde(skip_serializing_if = "Option::is_none")]
pub params: Option<serde_json::Value>,
}

二. MCPServer加载

1. 多uid

● 这次问题问得对 —— 这段代码确实只创建了 owner 的 client，多租户确实没在这一步里体现。我先把上下文补齐让你看到全貌，然后解释清楚为什么这"看起来"是漏洞，但其实是有意分层的设计。

Read 2 files (ctrl+o to expand)

● 你说得对 —— 启动期只创建 owner 的 client，多用户 client 不是在这里建的

那段 app.rs:843 的代码确实是 owner 一次性预加载，只为 owner_id 创建 N 个 client（N = enabled_servers 数）。多用户的 client 不在这里建。

那"多用户多 client"到底在哪建？在每个用户运行时调 activate_mcp_server 时：

第二个创建点：extensions/manager.rs:5964

pub async fn activate_mcp_server(&self, name: &str, user_id: &str) -> Result<...> {
...
let client = crate::tools::mcp::create_client_from_config(
server.clone(),
&self.mcp_session_manager,
&self.mcp_process_manager,
Some(Arc::clone(&self.secrets)),
user_id,                       // ← 烧死调用方 user_id
).await?;
...
}

调用方是谁？任意用户在聊天/CLI 里触发"激活某个 MCP server"。流程大致是：

1. user B 在 web UI 里点 "Activate github-mcp"
2. 后端 handler 调 manager.activate_mcp_server("github-mcp", user_B)
3. 第 5964 行 create_client_from_config(server, ..., user_B) 给 user B 独立烧一个 client
4. 第 6046-6049 行 self.mcp_clients.insert(user_B, "github-mcp", client, ...) 插进 store
5. Tool wrappers 在这一步才被注册到全局 ToolRegistry（如果还没有 —— 因为 wrappers 是 per-server 不是 per-user）

关键架构：wrappers 是 per-server，不重复注册

这是为啥"启动期只为 owner 建"够用的原因。看 client.rs:888-895 那条注释我之前贴过：

▎ Holds a reference to the shared McpClientStore instead of a specific Arc<McpClient> so the same registered wrapper serves every user.

所以：

启动期：
owner 的 client → store[(owner, server)] ✓
wrappers 注册到 ToolRegistry（per-server，一份）✓

运行时 user B 激活：
user B 的 client → store[(user_B, server)] ✓   ← 新增这条 entry
wrappers 已经在 registry 里了 → 不再注册 ← 跳过这一步

dispatch 时：
McpToolWrapper.execute(args, ctx)
→ store.get(ctx.user_id, server_name)        ← 这里才按 ctx.user_id 路由
→ ctx.user_id == user_B → user_B 的 client（user_B 的 token、user_B 的 secrets handle）

没有重复创建 wrapper，也没有重复创建 client —— wrappers 是 server 级共享一份，client 是 user 级独立一份。

那 app.rs:843 的 owner 加载有什么意义？

不是"占住位置防别人插"，是启动时让 owner 提前可用（不用等到 owner 第一次激活请求）。其他用户仍然需要在各自第一次激活时走 manager.rs:5964 那条路径，没人能"白嫖" owner 那份 client —— 因为
McpClientStore.get(user_B, server_name) 查的是 (user_B, server_name)，不会返回 (owner, server_name) 那条 entry。

一个相关的设计护栏：check_surface_conflict

manager.rs:5995-6026 这个保护就是为了防御 ToolRegistry 是全局带来的跨租户隐患：

// Before registering any tool wrappers for this user, fingerprint
// the tool surface the server reported and reject activation if
// another user already has the same `name` active with a
// DIFFERENT surface. The `ToolRegistry` keys wrappers by tool
// name only, so without this check user B's incoming schemas
// would silently shadow user A's

意思是：如果 github-mcp 这个名字已经被 user A 激活了，user B 再去激活同名 server 时会比对其工具表面，如果发现 user B 的 server 报上来的工具集和 user A 的不一样（schema 不同），直接拒绝 —— 因为
ToolRegistry 里 wrapper 是按工具名存的，user B 的 schema 会顶掉 user A 的，导致 user A 看到的是 user B 的 schema。

如果 user B 的 server 工具表面和 user A 完全一致（同一个 MCP server 实现的同一组工具），就放行 —— wrappers 不重复注册，store 里多一条 (user_B, server_name) entry，dispatch 时各自拿到各自的 client。

修正后的完整多租户图景

                    ┌─────────────────────────────────┐
                    │      McpClientStore (单例)       │
                    │  key=(user_id, server_name)      │
                    │                                  │
                    │  (owner,    github-mcp) → C₁    │ ← app.rs:843 启动期
                    │  (alice,    github-mcp) → C₂    │ ← manager.rs:5964 运行时
                    │  (bob,      github-mcp) → C₃    │ ← manager.rs:5964 运行时
                    │  (alice,    notion-mcp) → C₄    │ ← manager.rs:5964 运行时
                    └─────────────────────────────────┘
                                    ↑
                    store.get(ctx.user_id, server_name)
                                    ↑
                    ┌─────────────────────────────────┐
                    │  McpToolWrapper (per-server)     │
                    │  github-mcp_search  ─┐           │
                    │  github-mcp_create  ─┼── 共享     │ ← ToolRegistry 按工具名 key
                    │  notion-mcp_query   ─┘           │   全局只一份 wrapper
                    └─────────────────────────────────┘
                                    ↑
                            Tool::execute(args, ctx)
                                    ↑
                                ctx.user_id

为什么是这样两层结构（不直接 store[(user, server)] → wrapper）

因为 ToolRegistry 是全局的、keyed by 工具名（不是 (user, tool_name)）。如果在 wrapper 里绑死 client，就会出现：

- wrapper github-mcp_search 内部存 Arc<McpClient> → 只能服务一个 user
- user B 激活时，要么 reuse 同一个 wrapper 但改 client（user A 的 wrapper 改了 → user A 后续调用拿 user B 的 client）→ 租户串
- 要么重新注册一个 wrapper github-mcp_search（user B 的）→ ToolRegistry 不允许同名

所以wrapper 必须和 client 解耦 —— wrapper 只持 store，运行时按 ctx 路由 client。这是被 ToolRegistry 的全局性逼出来的设计，不是随便选的。

一句话

▎ 启动期只给 owner 建 client 是设计如此（owner 预热 + wrappers 预注册），多用户的 client 在每个用户运行时调 activate_mcp_server 时建，插进共享 McpClientStore。wrappers 是 per-server
▎ 共享一份（按工具名注册到全局 ToolRegistry），client 是 per-user 独立一份（按 (user_id, server_name) 进 store），dispatch 时 wrapper 用 ctx.user_id 去 store 拿对应的 client —— 这就是"看起来 owner
▎ 一份就够、实际每个用户都自己一份"的原因。


2. 加载注册owner mcp

        let mcp_servers_future = {
            let secrets_store = self.secrets_store.clone();
            let db = self.db.clone();
            let mcp_sm = Arc::clone(&mcp_session_manager);
            let pm = Arc::clone(&mcp_process_manager);
            let owner_id = self.config.owner_id.clone();
            async move {
                let servers_result =
                    crate::tools::mcp::config::load_mcp_servers_ready(db.as_deref(), &owner_id)
                        .await;//得到要mcpserver
                match servers_result {
                    Ok(servers) => {
                        let enabled: Vec<_> = servers.enabled_servers().cloned().collect();
                        if !enabled.is_empty() {
                            tracing::debug!(
                                "Loading {} configured MCP server(s)...",
                                enabled.len()
                            );
                        }

                        let mut join_set = tokio::task::JoinSet::new();
                        for server in enabled {
                            let mcp_sm = Arc::clone(&mcp_sm);
                            let secrets = secrets_store.clone();
                            let pm = Arc::clone(&pm);
                            let owner_id = owner_id.clone();

                            join_set.spawn(async move {
                                let server_name = server.name.clone();
                                let has_custom_auth_header = server.has_custom_auth_header();
                                //客户端是每个用户一个
                                let client = match crate::tools::mcp::create_client_from_config(//根据不同uid、server创建对应的client
                                    server,
                                    &mcp_sm,
                                    &pm,
                                    secrets,
                                    &owner_id,
                                )

/// Create an `McpClient` from a server configuration, dispatching on the
/// effective transport type.
pub async fn create_client_from_config(
mut server: McpServerConfig,
session_manager: &Arc<McpSessionManager>,
process_manager: &Arc<McpProcessManager>,
secrets: Option<Arc<dyn SecretsStore + Send + Sync>>,
user_id: &str,
) -> Result<McpClient, McpFactoryError> {
match server.effective_transport() {
EffectiveTransport::Stdio { command, args, env } => {//根据不同类型创建不同transport, 然后给客户端
let transport = process_manager
.spawn_stdio(//通过管理器起本地子进程，然后过程在章节一
user_id,
validated_name.as_str(),
command,
args.to_vec(),
env.clone(),
)
.await
.map_err(|e| McpFactoryError::StdioSpawn {
name: server_name.clone(),
reason: e.to_string(),
})?;

            Ok(McpClient::new_with_transport(
                validated_name.as_str(),
                transport as Arc<dyn McpTransport>,
                None,
                secrets,
                user_id,
                Some(server),
            ))
        }


            EffectiveTransport::Http => {//远程mcpServer需要session_manager管理sessionId
            // Authenticated (OAuth) path: tokens exist or server requires auth.
            if let Some(ref secrets) = secrets {
                let has_tokens =
                    crate::tools::mcp::is_authenticated(&server, secrets, user_id).await;

                if has_tokens || server.requires_auth() {
                    return Ok(McpClient::new_authenticated(
                        server,
                        Arc::clone(session_manager),
                        Arc::clone(secrets),
                        user_id,
                    ));
                }
            }

            // Non-OAuth HTTP: wire the session manager into the *transport* so
            // it captures `Mcp-Session-Id` from responses. Passing it only to
            // the client (via `with_session_manager`) is not enough — the
            // transport must know about it to read/write the header.
            let transport = Arc::new(
                HttpMcpTransport::new(server.url.clone(), validated_name.as_str())
                    .with_session_manager(Arc::clone(session_manager), user_id),
            );
            Ok(McpClient::new_with_transport(
                validated_name.as_str(),
                transport,
                Some(Arc::clone(session_manager)),
                secrets,
                user_id,
                Some(server),
            ))
        }

/// MCP transport that communicates with a server over HTTP.
///
/// Sends JSON-RPC requests as HTTP POST with `Content-Type: application/json`
/// and accepts either JSON or SSE (`text/event-stream`) responses. Optionally
/// manages session IDs via [`McpSessionManager`] and supports custom headers.
pub struct HttpMcpTransport {
server_url: String,
/// Typed name so session-manager lookups cannot accidentally be keyed
/// by a free-form string. See `ironclaw_common::identity`.
server_name: McpServerName,
http_client: reqwest::Client,//通过客户端发送接受请求
session_manager: Option<Arc<McpSessionManager>>,
session_user_id: Option<String>,//烧录死，uid-serverName层次单独一个Client
custom_headers: HashMap<String, String>,
}


    async fn send(
        &self,
        request: &McpRequest,
        headers: &HashMap<String, String>,
    ) -> Result<McpResponse, ToolError> {//发送方法
        // Build the HTTP request.
        let mut req_builder = self
            .http_client
            .post(&self.server_url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .json(request);

        // Apply custom headers configured on the transport.
        for (key, value) in &self.custom_headers {
            req_builder = req_builder.header(key.as_str(), value.as_str());
        }

        // Apply per-request headers (e.g. Authorization, Mcp-Session-Id).
        for (key, value) in headers {
            req_builder = req_builder.header(key.as_str(), value.as_str());
        }

        // Send the request.
        let response = req_builder.send().await.map_err(|e| {
            let mut chain = format!("[{}] MCP HTTP request failed: {}", self.server_name, e);
            let mut source = std::error::Error::source(&e);
            while let Some(cause) = source {
                chain.push_str(&format!(" -> {}", cause));
                source = cause.source();
            }
            ToolError::ExternalService(chain)
        })?;

        // Handle error status codes before accepting any session state from the response.
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let sanitized = sanitize_error_body(&body);
            return Err(ToolError::ExternalService(format!(
                "[{}] MCP server returned status: {} - {}",
                self.server_name, status, sanitized
            )));
        }

        // Extract session ID from successful response headers before consuming the body.
        // Scope by `(session_user_id, server_name)` so a second user's
        // initialize handshake can't overwrite the first user's stored
        // session ID and silently redirect their subsequent requests to
        // the wrong server-side session.
        if let Some(ref session_manager) = self.session_manager
            && let Some(ref user_id) = self.session_user_id
            && let Some(session_id) = response
                .headers()
                .get("Mcp-Session-Id")
                .and_then(|v| v.to_str().ok())
        {
            let session_id = session_id.trim();
            if !is_safe_mcp_session_id(session_id) {
                return Err(ToolError::ExternalService(format!(
                    "[{}] MCP server returned invalid session id",
                    self.server_name
                )));
            }
            session_manager
                .update_session_id(user_id, &self.server_name, Some(session_id.to_string()))
                .await;
        }

        // MCP notifications commonly acknowledge with 202 Accepted and no body.
        if response.status() == reqwest::StatusCode::ACCEPTED {
            return Ok(McpResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: None,
                error: None,
            });
        }

        // Determine response format from Content-Type.
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        if content_type.contains("text/event-stream") {
            self.parse_sse_response(response, request.id).await
        } else {
            response.json().await.map_err(|e| {
                ToolError::ExternalService(format!(
                    "[{}] Failed to parse MCP response: {}",
                    self.server_name, e
                ))
            })
        }
    }

3. 初始化链接问题

❯ 本地和远程mcp是怎么初始化的？mcp什么时候和其建立链接，获取其工具列表？

● 仍是单轮源码定位，跳过 TaskCreate。

我把 MCP 客户端/连接/工具列表的完整生命周期一次性捋清 —— 包含初始化、连接时机、tools/list 拉取。

Searched for 2 patterns, read 2 files (ctrl+o to expand)

● 够了，开始回答。

三层时序：构造 → 初始化握手 → 取工具 → 注册 wrapper

阶段 1：transport 真正建立连接

这一步在 create_client_from_config 里就发生了，跟 McpClient 本身无关：

┌─────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  Transport  │                                                                    何时真正连                                                                     │
├─────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ stdio       │ process.rs:73 spawn_stdio 里 Command::new(command).args(args).envs(env).spawn() —— create_client_from_config 调用时立刻起子进程（同步阻塞 await） │
├─────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Unix socket │ UnixMcpTransport::connect 里 tokio::net::UnixStream::connect(path).await —— 创建 client 时立刻 dial                                               │
├─────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ HTTP/HTTPS  │ HttpMcpTransport::new(url, name) 只存 URL，不连。第一次 send() 才发 HTTP 请求                                                                     │
└─────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

所以本地（MCP server 跑在本机的进程）"创建 client" = "启动 server 子进程 / 连 socket" —— 连接发生在 client 构造时。

远程（HTTP server 在别的机器）"创建 client" = "只拿到一个 URL 句柄"，没真连。这是 lazy 的好处 —— 启动期 owner 预加载 5 个 HTTP server 不会卡 5 个 TCP。

阶段 2：MCP 协议握手 initialize

McpClient::initialize()（client.rs:655）的契约是 once-and-only-once 握手，不会重发：

pub async fn initialize(&self) -> Result<InitializeResult, ToolError> {
let result = self.initialized
.get_or_try_init(|| async {
if let Some(ref session_manager) = self.session_manager
&& session_manager.is_initialized(&self.user_id, &self.server_name).await
{
return Ok(InitializeResult::default());   // ← 已经握手过，跳过
}
self.reinitialize_session().await              // ← 真发 initialize JSON-RPC
}).await?;
Ok(result.clone())
}

三道防线：

1. OnceCell::get_or_try_init —— 同一 client 实例下，N 个并发调用只有一个真发握手，其他全 await 同一 future。
2. session_manager.is_initialized(user_id, server_name) —— 跨 client 实例共享状态。即使换了一个新 McpClient（比如 client 重建了），只要 manager 说"这个 user+server
   已经握过手了"，就直接跳过握手、复用之前的 Mcp-Session-Id。
3. reinitialize_session —— 真发 initialize JSON-RPC、收 InitializeResult、发 notifications/initialized 通知（client.rs:547-558）。

阶段 3：什么时候拉 tools/list

看 McpClient::list_tools（client.rs:674-700）的 lazy 模式：

pub async fn list_tools(&self) -> Result<Vec<McpTool>, ToolError> {
if let Some(tools) = self.tools_cache.read().await.as_ref() {
return Ok(tools.clone());     // ← 缓存命中，直接返
}
self.initialize().await?;          // ← 没握手就握手
let request = McpRequest::list_tools(self.next_request_id());
let response = self.send_request(request).await?;   // ← 发 tools/list
...
*self.tools_cache.write().await = Some(result.tools.clone());   // ← 缓存
Ok(result.tools)
}

三个事实：

1. 第一次 list_tools() 调用才会真正发 tools/list 请求。如果有缓存就不发。
2. tools_cache 是 client 实例内的 RwLock<Option<Vec<McpTool>>>（看 client.rs 里字段定义）—— client 死了缓存就丢，下一个 client 重建时得重新拉。
3. 握手 (initialize) 和取工具 (tools/list) 是分开的两步 —— 但 list_tools 会顺带确保握手已完成。

阶段 4：回到 factory / manager 的"何时触发"

把上面三个函数串到外层调用点看：

┌─────────────────────┬───────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│       调用点        │             何时发生              │                                                                  顺序                                                                  │
├─────────────────────┼───────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ app.rs:843 启动期   │ app 启动 init_extensions          │ 1. create_client_from_config 构造 client（本地：真 spawn/connect；HTTP：只拿 URL）<br>2. client.list_tools() —— 立刻触发握手 + 拉      │
│ owner 预加载        │ 时，对每个 enabled server         │ tools/list<br>3. 把 client 存进 McpClientStore[(owner, server)]<br>4. wrappers 在 manager.inject_mcp_client                            │
│                     │                                   │ 那一行才被注册（注册时不重新拉 tools，复用 list_tools 缓存 —— 看 manager.rs:1542 的 surface_signature 注释）                           │
├─────────────────────┼───────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ manager.rs:5964     │ 用户在 UI/CLI 触发                │ 1. create_client_from_config(server, ..., user_id) —— 同上<br>2. client.list_tools() —— 触发握手 + tools/list<br>3.                    │
│ 用户激活            │ activate_mcp_server(name,         │ check_surface_conflict（manager.rs:6014）—— 比对工具表面指纹<br>4. mcp_clients.insert(user_id, name, client, signature)<br>5.          │
│                     │ user_id)                          │ create_tools_with_store(...) —— 内部又调 list_tools（命中缓存，秒返）<br>6. 注册 wrappers 到全局 ToolRegistry                          │
├─────────────────────┼───────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 运行时 dispatch     │ agent 决策调 mcp_<server>_<tool>  │ 1. McpToolWrapper.execute(args, ctx)<br>2. store.get(ctx.user_id, server_name) 拿 client<br>3. client.call_tool(name, args) → 内部     │
│ 调工具              │                                   │ self.initialize().await?（OnceCell + session_manager 双保险，秒过）→ 发 tools/call JSON-RPC                                            │
└─────────────────────┴───────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

完整时序图（按"什么时候首次建立连接 + 拉 tools/list"）

─────────────────────────────────────────────────────────────────
本地 stdio MCP server（github-mcp 子进程）：

app 启动
└─ init_extensions
└─ for server in enabled:
├─ process_manager.spawn_stdio(...)        ← ★ 这一刻真起子进程
├─ create_client_from_config(...)          ← 装进 McpClient
├─ client.list_tools()
│    ├─ initialize()  ← 发 initialize JSON-RPC + notifications/initialized
│    └─ tools/list    ← 发 tools/list
└─ manager.inject_mcp_client(...)          ← 存 store + 注册 wrappers

─────────────────────────────────────────────────────────────────
远程 HTTP MCP server（mcp.notion.com）：

app 启动
└─ init_extensions
└─ for server in enabled:
├─ create_client_from_config(...)
│    └─ HttpMcpTransport::new(url, name)   ← 只存 URL，★ 没真连
├─ client.list_tools()
│    ├─ initialize()  ← 这一刻才发第一个 HTTP POST
│    └─ tools/list    ← 第二个 HTTP POST
└─ manager.inject_mcp_client(...)

─────────────────────────────────────────────────────────────────
dispatch 时（无论本地/远程）：

agent 调 mcp_github-mcp_list_issues
└─ McpToolWrapper::execute(args, ctx)
├─ store.get(ctx.user_id, "github-mcp")         ← 按 user_id 路由
├─ client.call_tool(...)
│    ├─ initialize()    ← OnceCell 命中，秒过
│    └─ tools/call      ← 发请求
└─ 返回 CallToolResult

         ├─ client.call_tool(...)
         │    ├─ initialize()    ← OnceCell 命中，秒过
         │    └─ tools/call      ← 发请求
         └─ 返回 CallToolResult

一句话总结

▎ - 本地（stdio/Unix）：create_client_from_config 一调就真起子进程 / 连 socket，transport 层连接立等可取；MCP 协议层握手 (initialize) 在第一次 list_tools / call_tool 时才发，由 OnceCell +
▎ session_manager 双保险保证"per-(user, server) 只发一次"。
▎ - 远程（HTTP）：create_client_from_config 只拿 URL，transport 层 lazy；MCP 协议握手 + tools/list 都在第一次 list_tools() 时一起发，握手完了 tools 也拉到了，wrapper 注册时复用 list_tools 缓存。
▎ - 何时拉 tools：启动期给 owner 预拉一次（保证 owner 可用），用户激活时再拉一次（保证 tools 在 store 里、surface 签名对得上），之后 tools_cache 命中不再发。dispatch 调工具时 initialize
▎ 永远秒过，不重发握手。

✻ Sautéed for 50s

❯ 注册 wrappers在哪代码？

Searched for 1 pattern, read 2 files (ctrl+o to expand)

● 仍是单轮源码定位，跳过 TaskCreate。

Wrappers 注册有三处，全在 ExtensionManager

核心三行：self.tool_registry.register(tool).await; —— 在 ToolRegistry 里按工具名注册 McpToolWrapper，让全局 ToolRegistry 在 dispatch 时按名找到它。

调用点 1：启动期 owner 预加载 —— manager.rs:1507 inject_mcp_client

pub(crate) async fn inject_mcp_client(
&self,
name: String,
user_id: &str,
client: Arc<crate::tools::mcp::McpClient>,
) -> Vec<String> {
...
let surface_signature = match client.list_tools().await { ... };   // 先拿工具列表做指纹
if let Some(other) = self.mcp_clients.check_surface_conflict(...).await { ... }   // 冲突检测

      // ★ 关键三步
      self.mcp_clients.insert(user_id, &name, client.clone(), surface_signature).await;   // 1) 存 store
      match client.create_tools_with_store(self.mcp_client_store()).await {                // 2) 创建 wrappers
          Ok(tool_impls) => {
              let tool_names: Vec<String> = tool_impls.iter().map(|t| t.name().to_string()).collect();
              for tool in tool_impls {
                  self.tool_registry.register(tool).await;                                  // 3) 注册到全局 registry
              }
              tool_names
          }
          ...
      }
}

app.rs:1067-1069 在启动期调它：

let registered = manager
.inject_mcp_client(normalized_name.clone(), &self.config.owner_id, client)
.await;

调用点 2：用户运行时激活 —— manager.rs:6069 activate_mcp_server

let client = crate::tools::mcp::create_client_from_config(server, ..., user_id).await?;
...
let surface_signature = crate::tools::mcp::surface_signature(&mcp_tools);
if let Some(other_user) = self.mcp_clients.check_surface_conflict(user_id, name, &surface_signature).await {
return Err(ExtensionError::ActivationFailed(...));    // 同 surface 冲突 → 拒绝
}

let mut updated_server = server.clone();
updated_server.cached_tools = mcp_tools.clone();
self.update_mcp_server(updated_server, user_id).await?;

let client = Arc::new(client);
self.mcp_clients.insert(user_id, name, client.clone(), surface_signature).await;   // 存 store

let tool_impls = match client.create_tools_with_store(self.mcp_client_store()).await {   // 创建 wrappers
Ok(tools) => tools,
Err(e) => { self.mcp_clients.remove(user_id, name).await; return Err(...); }          // 失败回滚
};

let tool_names: Vec<String> = tool_impls.iter().map(|t| t.name().to_string()).collect();

for tool in tool_impls {
self.tool_registry.register(tool).await;                                              // 注册到 registry
}

调用点 3：批量创建 wrapper 的工厂方法 —— client.rs:759 create_tools_with_store

这一处不直接调 register，只是把 McpTool 转成 Vec<Arc<dyn Tool>>（一组 McpToolWrapper），让上面两个调用点去 for-loop 注册：

pub async fn create_tools_with_store(
&self,
store: Arc<super::McpClientStore>,    // ← 关键：wrapper 持 store，不持 client
) -> Result<Vec<Arc<dyn Tool>>, ToolError> {
let mcp_tools = self.list_tools().await?;    // 触发握手 + 取工具（命中缓存秒返）
let server_name = self.server_name.as_str().to_string();

      // 后归一化碰撞检测（`search-all` vs `search_all` 归一化后撞名 → warn）
      ...

      let mut wrappers = Vec::with_capacity(mcp_tools.len());
      for tool in mcp_tools {
          let prefixed_name = mcp_tool_id(&server_name, &tool.name);
          wrappers.push(Arc::new(McpToolWrapper {
              tool,
              prefixed_name,
              provider_extension: server_name.clone(),
              server_name: server_name.clone(),
              client_store: store.clone(),    // ← wrapper 只持有 store，dispatch 时按 ctx.user_id 路由
          }) as Arc<dyn Tool>);
      }
      Ok(wrappers)
}

注意 McpToolWrapper.client_store —— wrapper 不绑 client，持的是 store，dispatch 时调 store.get(ctx.user_id, server_name) 拿对应用户的那份 client。

三处注册的关系

调用点 1: inject_mcp_client (启动期, owner)
│
└─→ client.create_tools_with_store(store)               ← 调用点 3 (生产 wrappers)
│
└─→ 返回 Vec<Arc<McpToolWrapper>>
│
└─→ for tool in wrappers { tool_registry.register(tool) }    ← 真正的注册

调用点 2: activate_mcp_server (运行时, 任意 user)
│
└─→ client.create_tools_with_store(store)               ← 调用点 3
│
└─→ 返回 Vec<Arc<McpToolWrapper>>
│
└─→ for tool in wrappers { tool_registry.register(tool) }    ← 真正的注册

两个调用点的差别

┌─────────────────────────────┬─────────────────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────────┐
│                             │                    调用点 1 (inject_mcp_client)                     │                调用点 2 (activate_mcp_server)                │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ 触发时机                    │ 启动期 init_extensions，仅 owner                                    │ 用户在 UI/CLI 主动激活                                       │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ 调用方                      │ app.rs:1067                                                         │ web/CLI handler → manager.activate_mcp_server(name, user_id) │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ client 来源                 │ app.rs:843 启动期预建的 owner client                                │ manager.rs:5964 现建（烧死调用方 user_id）                   │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ check_surface_conflict 行为 │ 同 owner 重注入时比对（自身冲突）                                   │ 严格比对其他 user 是否已激活同名 server 且 surface 不同      │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ 注册到 registry 的 wrappers │ ✅  同 server 名只第一次注册有意义，后续都被 registry 的同名规则接管 │ ✅  同上；wrappers 是 per-server 全局唯一                     │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ store entry                 │ (owner_id, name)                                                    │ (user_id, name)                                              │
└─────────────────────────────┴─────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────┘

为什么"两个调用点都注册，但 wrapper 只一份"

ToolRegistry 按工具名 key —— 第一次注册某个 mcp_github-mcp_search wrapper 时插入；后续任何人再注册同名 wrapper，按 registry 实现要么覆盖要么合并。wrapper 本身只是"指针 + 调度逻辑"，覆盖了不影响 store
里不同用户的 client（wrapper 永远持 store，dispatch 时按 ctx.user_id 路由），所以注册一次和注册 N 次效果一样，但代码上每次 activate 都走一遍 for-loop 是为了健壮性（同一个 user deactivate 再 activate
时 wrappers 不会被遗留）。

一句话

▎ Wrappers 在 ExtensionManager 里两处注册 —— 启动期 inject_mcp_client（manager.rs:1507-1589）为 owner 注册一次，运行时 activate_mcp_server（manager.rs:6069-6071）为新激活的 server 注册。两处都是
▎ client.create_tools_with_store(store) 生产 Vec<McpToolWrapper>（client.rs:759），再 for tool in wrappers { tool_registry.register(tool).await } 塞进全局 ToolRegistry。wrapper 内部只持
▎ Arc<McpClientStore>，dispatch 时按 ctx.user_id 路由到对应用户的 client —— 所以"重复注册"不会导致多用户串。

4. 注入详细代码

pub struct ExtensionManager {
registry: ExtensionRegistry,
discovery: OnlineDiscovery,

    // MCP infrastructure
    mcp_session_manager: Arc<McpSessionManager>,
    mcp_process_manager: Arc<crate::tools::mcp::process::McpProcessManager>,
    /// Active MCP clients keyed by `(user, server)`. Shared as `Arc` with
    /// every registered `McpToolWrapper` so tool dispatch can resolve the
    /// caller's per-user client at execute time instead of embedding a
    /// specific client in the globally-registered wrapper (which would
    /// let the second activating user's credentials shadow the first).
    mcp_clients: Arc<crate::tools::mcp::McpClientStore>, //缓存MCPCLient
    /// Per-server async mutex that serialises `activate_mcp` and the
    /// `McpServer` arm of `remove` on the same server name. Without this,
    /// user B's `remove` (which unregisters the server's global tool
    /// wrappers once it's the last user out) can interleave with user C's
    /// `activate` (which re-registers the wrappers and inserts C's
    /// client), leaving the store with C's client but the registry with
    /// C's wrappers already unregistered. Parallelism across *different*
    /// servers is preserved.
    mcp_lifecycle_locks: RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,

    // WASM tool infrastructure
    wasm_tool_runtime: Option<Arc<WasmToolRuntime>>,
    wasm_tools_dir: PathBuf,
    wasm_channels_dir: PathBuf,
    latent_wasm_provider_actions: RwLock<HashMap<String, Vec<LatentProviderAction>>>,
    /// Per-server URL cache for `mcp_supports_auth` metadata discovery.
    /// Avoids re-issuing a network probe on every `list()` call.
    mcp_auth_support_cache: RwLock<HashMap<String, bool>>,

    // WASM channel hot-activation infrastructure (set post-construction)
    channel_runtime: RwLock<Option<ChannelRuntimeState>>,
    /// Channel manager for hot-adding relay channels (set independently of WASM runtime).
    relay_channel_manager: RwLock<Option<Arc<ChannelManager>>>,

    // Shared
    secrets: Arc<dyn SecretsStore + Send + Sync>,
    tool_registry: Arc<ToolRegistry>,
    hooks: Option<Arc<HookRegistry>>,
    pending_auth: RwLock<HashMap<PendingAuthKey, PendingAuth>>,
    /// Tunnel URL for webhook configuration and remote OAuth callbacks.
    tunnel_url: Option<String>,
    user_id: String,
    /// Optional database store for DB-backed MCP config.
    store: Option<Arc<dyn crate::db::Database>>,
    /// When set, settings reads/writes go through this cache-backed store
    /// instead of the raw `Database`. Populated via `with_settings_store()`.
    settings_override: Option<Arc<dyn crate::db::SettingsStore + Send + Sync>>,
    /// Names of WASM/relay channels that are actively running in this process.
    ///
    /// This is runtime state, not install/discovery state. Installed WASM
    /// channels are still discovered from disk in `list()`, but only channels
    /// present in this set are reported as active.
    active_channel_names: RwLock<HashSet<String>>,
    /// Installed channel-relay extensions (no on-disk artifact, tracked in memory).
    installed_relay_extensions: RwLock<HashSet<String>>,
    /// Last activation error for each WASM channel (ephemeral, cleared on success).
    activation_errors: RwLock<HashMap<String, String>>,
    /// SSE broadcast manager (set post-construction via `set_sse_sender()`).
    sse_manager: RwLock<Option<Arc<crate::channels::web::sse::SseManager>>>,
    /// Shared registry of pending OAuth flows for gateway-routed callbacks.
    ///
    /// Keyed by CSRF `state` parameter. Populated in `start_wasm_oauth()`
    /// when running in gateway mode, consumed by the web gateway's
    /// `/oauth/callback` handler.
    pending_oauth_flows: crate::auth::oauth::PendingOAuthRegistry,
    /// OAuth proxy auth token for authenticating with the hosted token exchange proxy.
    /// Resolved once at construction from `IRONCLAW_OAUTH_PROXY_AUTH_TOKEN`,
    /// then `GATEWAY_AUTH_TOKEN` as a backward-compatible fallback.
    oauth_proxy_auth_token: Option<String>,
    /// Relay config captured at startup. Used by `auth_channel_relay` and
    /// `activate_channel_relay` instead of re-reading env vars.
    relay_config: Option<crate::config::RelayConfig>,
    /// Shared event sender for the relay webhook endpoint.
    /// Populated by `activate_channel_relay`, consumed by the web gateway's
    /// `/relay/events` handler.
    relay_event_tx: Arc<
        tokio::sync::Mutex<
            Option<tokio::sync::mpsc::Sender<crate::channels::relay::client::ChannelEvent>>,
        >,
    >,
    /// Per-instance callback signing secret fetched from channel-relay at activation.
    /// Stored here so the web gateway can verify incoming callbacks without
    /// any env var or shared secret.
    relay_signing_secret_cache: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
    /// PairingStore for multi-tenant relay identity resolution.
    pairing_store: Option<Arc<crate::pairing::PairingStore>>,
    /// When `true`, OAuth flows always return an auth URL to the caller
    /// instead of opening a browser on the server via `open::that()`.
    /// Set by the web gateway at startup via `enable_gateway_mode()`.
    gateway_mode: std::sync::atomic::AtomicBool,
    /// Reborn Telegram v2 ProductAdapter (issue #3285) feature flag.
    ///
    /// When `true`, [`Self::activate_wasm_channel`] fails closed on the
    /// legacy `telegram` WASM channel — both paths must not handle the
    /// same Telegram webhook installation. The runtime-tier startup
    /// guard in `main.rs` rejects the same conflict at boot; this
    /// post-startup flag closes the hot-activation bypass Henry flagged
    /// on PR #3356. Set by the host at startup via
    /// [`Self::set_reborn_telegram_v2_enabled`].
    reborn_telegram_v2_enabled: std::sync::atomic::AtomicBool,
    /// The gateway's own base URL for building OAuth redirect URIs.
    /// Set by the web gateway at startup via `enable_gateway_mode()`.
    gateway_base_url: RwLock<Option<String>>,
    pending_wechat_logins: RwLock<HashMap<String, PendingWechatLogin>>,
    channel_activation_locks: RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    #[cfg(test)]
    test_wasm_channel_loader: RwLock<Option<TestWasmChannelLoader>>,
    #[cfg(test)]
    test_wechat_login_starter: RwLock<Option<TestWechatLoginStarter>>,
    #[cfg(test)]
    test_wechat_login_poller: RwLock<Option<TestWechatLoginPoller>>,
}
***************************
/// Per-user MCP client registry. Typically held as `Arc<McpClientStore>`
/// by both `ExtensionManager` (for lifecycle) and every `McpToolWrapper`
/// (for dispatch-time lookup).
#[derive(Default)]
pub struct McpClientStore {uid-sreverName
clients: RwLock<HashMap<McpClientKey, McpClientEntry>>,
}

init_ext内部会调这个 ：主要是将工具统一注册(注册前会先初始化、获取工具列表)                 
let registered = manager
.inject_mcp_client(normalized_name.clone(), &self.config.owner_id, client)

    pub(crate) async fn inject_mcp_client(
        &self,
        name: String,
        user_id: &str,
        client: Arc<crate::tools::mcp::McpClient>,
    ) -> Vec<String> {


              self.mcp_clients
            .insert(user_id, &name, client.clone(), surface_signature)
            .await;//缓存
          
                    match client//初始化链接、获取列表，client维度的
            .create_tools_with_store(self.mcp_client_store())
            .await
        {
            Ok(tool_impls) => {//注册工具
                let tool_names: Vec<String> =
                    tool_impls.iter().map(|t| t.name().to_string()).collect();
                for tool in tool_impls {
                    self.tool_registry.register(tool).await;
                }
                tool_names
            }

McpClient

    pub async fn create_tools_with_store(
        &self,
        store: Arc<super::McpClientStore>,
    ) -> Result<Vec<Arc<dyn Tool>>, ToolError> {
        let mcp_tools = self.list_tools().await?;//去拉列表


**************
    /// List available tools from the MCP server.
    pub async fn list_tools(&self) -> Result<Vec<McpTool>, ToolError> {
        if let Some(tools) = self.tools_cache.read().await.as_ref() {//看看有没有缓存
            return Ok(tools.clone());
        }
        self.initialize().await?;//初始化链接

        let request = McpRequest::list_tools(self.next_request_id());//发送实际机请求，拉取tool_list
        let response = self.send_request(request).await?;

        if let Some(error) = response.error {
            return Err(ToolError::ExternalService(format!(
                "MCP error: {} (code {})",
                error.message, error.code
            )));
        }

        let result: ListToolsResult = response
            .result
            .ok_or_else(|| ToolError::ExternalService("No result in MCP response".to_string()))
            .and_then(|r| {
                serde_json::from_value(r)
                    .map_err(|e| ToolError::ExternalService(format!("Invalid tools list: {}", e)))
            })?;

        *self.tools_cache.write().await = Some(result.tools.clone());
        Ok(result.tools)
    }

**************
    /// Initialize the connection to the MCP server.
    ///
    /// Uses `OnceCell` to guarantee that exactly one caller performs the
    /// handshake, even under concurrent access. Subsequent calls return
    /// immediately.
    pub async fn initialize(&self) -> Result<InitializeResult, ToolError> {
        let result = self
            .initialized
            .get_or_try_init(|| async {//仅会初始化一次
                if let Some(ref session_manager) = self.session_manager
                    && session_manager
                        .is_initialized(&self.user_id, &self.server_name)
                        .await
                {
                    return Ok(InitializeResult::default());
                }
                self.reinitialize_session().await
            })
            .await?;

        Ok(result.clone())
    }
********************
重连
/// Re-run the MCP initialize handshake outside the OnceCell cache.
///
/// This is used for recoverable session-expiry failures when an MCP server
/// reports that the current session ID is no longer valid.
async fn reinitialize_session(&self) -> Result<InitializeResult, ToolError> {
if let Some(ref session_manager) = self.session_manager {
session_manager
.terminate(&self.user_id, &self.server_name)
.await;
session_manager
.get_or_create(&self.user_id, &self.server_name, &self.server_url)
.await;
}

        let request = McpRequest::initialize(self.next_request_id());
        let response = self
            .transport
            .send(&request, &self.build_request_headers().await?)//借助tranport去发连接信息
            .await?;

        if let Some(error) = response.error {
            return Err(ToolError::ExternalService(format!(
                "MCP initialization error: {} (code {})",
                error.message, error.code
            )));
        }

        let init_result: InitializeResult = response
            .result
            .ok_or_else(|| {
                ToolError::ExternalService("No result in initialize response".to_string())
            })
            .and_then(|r| {
                serde_json::from_value(r).map_err(|e| {
                    ToolError::ExternalService(format!("Invalid initialize result: {}", e))
                })
            })?;

        if let Some(ref session_manager) = self.session_manager {
            session_manager
                .mark_initialized(&self.user_id, &self.server_name)
                .await;
        }

        let notification = McpRequest::initialized_notification();
        if let Err(e) = self
            .transport
            .send(&notification, &self.build_request_headers().await?)
            .await
        {
            tracing::debug!(
                "Failed to send initialized notification to '{}': {}",
                self.server_name,
                e
            );
        }

        Ok(init_result)
    }

*****
    async fn build_request_headers(&self) -> Result<HashMap<String, String>, ToolError> {//每次client发送时会读sessionId
        let mut headers = self.custom_headers.clone();

        // Only inject OAuth token if the user hasn't set a custom Authorization header.
        let has_custom_auth = self
            .custom_headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("authorization"));
        if !has_custom_auth && let Some(token) = self.get_access_token().await? {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                headers.insert("Authorization".to_string(), format!("Bearer {}", trimmed));
            }
        }
        if let Some(ref session_manager) = self.session_manager
            && let Some(session_id) = session_manager
                .get_session_id(&self.user_id, &self.server_name)
                .await
        {
            headers.insert("Mcp-Session-Id".to_string(), session_id);
        }
        Ok(headers)
    }
****************
client.initialize()
└─ reinitialize_session()
├─ session_manager.terminate + get_or_create   ← 清旧建新（空）
├─ transport.send(initialize_request)          ← ① 发出
│       ↓ HTTP 响应回来
│    transport.send() 内部:
│       ├─ 检查 response.headers["Mcp-Session-Id"]   ← ② 抓 session ID
│       └─ session_manager.update_session_id(...)   ← ③ 存进 manager
├─ 解析 response.result (InitializeResult)     ← ④ 只读 JSON-RPC 响应体
├─ session_manager.mark_initialized(...)       ← ⑤ 标记握手完成
└─ transport.send(notifications/initialized)   ← ⑥ 同样会抓一遍 header
↓
response.headers["Mcp-Session-Id"]     ← ② 同上路径
session_manager.update_session_id(...)

*****************client本身有重试，发送，底层还是transport的send，被list_tool和call_tool调用
/// Send a request to the MCP server with auth and session headers.
/// Automatically attempts token refresh on 401 errors (HTTP transports only).
async fn send_request(&self, request: McpRequest) -> Result<McpResponse, ToolError> {
// For non-HTTP transports, just send directly without retry logic
if !self.transport.supports_http_features() {
let headers = self.build_request_headers().await?;
return self.transport.send(&request, &headers).await;
}

        // HTTP transport: try up to 2 times (first attempt, then retry after token refresh
        // or recoverable session reinitialization).
        for attempt in 0..2 {
            let headers = self.build_request_headers().await?;
            let result = self.transport.send(&request, &headers).await;
**************************too_call
/// Call a tool on the MCP server.
pub async fn call_tool(//在wraper的execute里调用这个，真正的执行方法
&self,
name: &str,
arguments: serde_json::Value,
) -> Result<CallToolResult, ToolError> {
self.initialize().await?;

        let request = McpRequest::call_tool(self.next_request_id(), name, arguments);
        let response = self.send_request(request).await?;

/// Session state for a single `(user, server)` MCP connection.
#[derive(Debug, Clone)]
pub struct McpSession {
/// Session ID returned by the server (via Mcp-Session-Id header).//注意sessionId
pub session_id: Option<String>,

    /// Last activity timestamp for this session.
    pub last_activity: Instant,

    /// Server URL this session is connected to.
    pub server_url: String,

    /// Whether initialization has completed.
    pub initialized: bool,
}

    /// Check if a session is initialized.
    pub async fn is_initialized(&self, user_id: &str, server_name: &McpServerName) -> bool {检查是否初始化
        let sessions = self.sessions.read().await;
        sessions
            .get(&Self::key(user_id, server_name))
            .map(|s| s.initialized)
            .unwrap_or(false)
    }

5. McpWrapper

struct McpToolWrapper {
tool: McpTool,//工具信息
prefixed_name: String,
provider_extension: String,
server_name: String,
client_store: Arc<super::McpClientStore>,实际借助Client去发起调用
}

    async fn execute(
        &self,
        params: serde_json::Value,
        ctx: &JobContext,
    ) -> Result<ToolOutput, ToolError> {
        let start = std::time::Instant::now();

        // Strip top-level null values before forwarding — LLMs often emit
        // `"field": null` for optional params, but many MCP servers reject
        // explicit nulls for fields that should simply be absent.
        let params = strip_top_level_nulls(params);

        let client = self
            .client_store
            .get(&ctx.user_id, &self.server_name)
            .await
            .ok_or_else(|| {
                ToolError::ExternalService(format!(
                    "MCP server '{}' is not active for this user",
                    self.server_name
                ))
            })?;
        let result = client.call_tool(&self.tool.name, params).await?;//这里发起调用
        let content: String = result
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<Vec<_>>()
            .join("\n");
        if result.is_error {
            return Err(ToolError::ExecutionFailed(content));
        }
        Ok(ToolOutput::text(content, start.elapsed()))
    }

