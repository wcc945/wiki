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

