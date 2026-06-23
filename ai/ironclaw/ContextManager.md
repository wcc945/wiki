一. 相关结构

1. ContextManager

负责管理多个并发 Job（任务）上下文的核心组件
/// Manages contexts for multiple concurrent jobs.
pub struct ContextManager {
/// Active job contexts.
contexts: RwLock<HashMap<Uuid, JobContext>>,
- HashMap<Uuid, JobContext>：以 Uuid（每个 job 唯一标识）为键，映射到 JobContext
- RwLock：读写锁。多个 reader 可并发读（如多 channel 同时查询 job 状态），writer 独占写（注册/清理 job 时）
- JobContext 通常包含：
    - 对话历史（messages 列表）
    - 当前工具调用栈
    - 中间状态变量
    - Job 元数据（owner、session、start time、status）
    - LLM 调用所需的 prompt 上下文


    /// Memory for each job.
    memories: RwLock<HashMap<Uuid, Memory>>,
作用：存储每个 job 的长期记忆访问句柄。
- Memory 是 workspace 模块提供的持久化记忆抽象（参见 src/workspace/README.md）
- 每个 job 有独立的 Memory 实例，保证 job 间记忆隔离
- 与 JobContext 的区别：
    - JobContext 是会话级短期上下文（一次执行的中间状态）
    - Memory 是跨会话长期记忆（通过 memory_search、memory_write 工具持久化）
      典型用法：
- 写入：manager.memories.write().insert(job_id, mem) — job 启动时绑定 memory
- 读取：manager.memories.read().get(&job_id) — 工具调用时访问该 job 的记忆系统


    /// Maximum concurrent jobs.
    max_jobs: usize,
}

ContextManager
│
├── contexts ──┐   短期：单次 job 执行的"工作台"
│              │
└── memories ──┘   长期：跨 job/会话的持久化记忆
│
└── 共用同一个 job_id (Uuid) 作为 key

2. JobContext

/// Context for a running job.
#[derive(Debug, Clone, Serialize)]
pub struct JobContext {
/// Unique job ID.
pub job_id: Uuid,
/// Current state.
pub state: JobState,
/// User ID that owns this job (for workspace scoping).
pub user_id: String,
/// Channel-specific requester/actor ID, when different from the owner scope.
#[serde(skip_serializing_if = "Option::is_none")]
pub requester_id: Option<String>,
/// Conversation ID if linked to a conversation.
pub conversation_id: Option<Uuid>,
/// Job title.
pub title: String,
/// Job description.
pub description: String,
/// Job category.
pub category: Option<String>,
/// Budget amount (if from marketplace).
pub budget: Option<Decimal>,
/// Budget token (e.g., "NEAR", "USD").
pub budget_token: Option<String>,
/// Our bid amount.
pub bid_amount: Option<Decimal>,
/// Estimated cost to complete.
pub estimated_cost: Option<Decimal>,
/// Estimated time to complete.
pub estimated_duration: Option<Duration>,
/// Actual cost so far.
pub actual_cost: Decimal,
/// Total tokens consumed by LLM calls in this job.
pub total_tokens_used: u64,
/// Maximum tokens allowed per job (0 = unlimited).
pub max_tokens: u64,
/// When the job was created.
pub created_at: DateTime<Utc>,
/// When the job was started.
pub started_at: Option<DateTime<Utc>>,
/// When the job was completed.
pub completed_at: Option<DateTime<Utc>>,
/// Number of repair attempts.
pub repair_attempts: u32,
/// State transition history.
pub transitions: Vec<StateTransition>,
/// Metadata.
pub metadata: serde_json::Value,
/// Extra environment variables to inject into spawned child processes.
///
/// Used by the worker runtime to pass fetched credentials to tools
/// (e.g., shell commands) without mutating the global process environment
/// via `std::env::set_var`, which is unsafe in multi-threaded programs.
///
/// Wrapped in `Arc` for cheap cloning on every tool invocation.
#[serde(skip)]
pub extra_env: Arc<HashMap<String, String>>,
/// Optional HTTP interceptor for trace recording/replay.
///
/// When set, tools that make outgoing HTTP requests should check this
/// interceptor before sending real requests. During recording, the
/// interceptor captures request/response pairs. During replay, it
/// returns pre-recorded responses.
#[serde(skip)]
pub http_interceptor: Option<Arc<dyn HttpInterceptor>>,
/// Stash of full tool outputs keyed by tool_call_id.
///
/// Tool outputs may be truncated before reaching the LLM context window,
/// but subsequent tools (e.g., `json`) may need the full output. This
/// stash stores the complete, unsanitized output so tools can reference
/// previous results by ID via `$tool_call_id` parameter syntax.
///
/// Also used for cross-tool implicit state (keys prefixed with `__`) such
/// as `__routine_last_name` for fallback recovery in routine tool chains.
#[serde(skip)]
pub tool_output_stash: Arc<tokio::sync::RwLock<HashMap<String, String>>>,
/// User's preferred timezone (IANA name, e.g. "America/New_York"). Defaults to "UTC".
pub user_timezone: String,
/// Approval context for tool execution in this job.
///
/// When set, tools check this context before executing to determine
/// if they're allowed to run in autonomous/non-interactive contexts.
#[serde(skip)]
pub approval_context: Option<ApprovalContext>,//工具执行待批准
}

3. Memory

// Combined memory for a job.
#[derive(Debug, Clone)]
pub struct Memory {
/// Job ID.
pub job_id: Uuid,
/// Conversation history.
pub conversation: ConversationMemory,//会话记录
/// Action history.
pub actions: Vec<ActionRecord>,//行为记录
/// Next action sequence number.
next_sequence: u32,
}

二. 短期长期记忆对比(Job)

● contexts vs memories 的区别

一句话区分

- contexts：短期、内存中、会话级——"这次对话的临时工作台"
- memories：长期、可持久化、跨会话——"这个用户/项目的永久记忆库"

详细对比

┌──────────┬──────────────────────────────────────────────┬────────────────────────────────────────────┐
│   维度   │            contexts (JobContext)             │             memories (Memory)              │
├──────────┼──────────────────────────────────────────────┼────────────────────────────────────────────┤
│ 生命周期 │ Job 运行期间存在，结束即清理                 │ 跨多次 job 持久保留                        │
├──────────┼──────────────────────────────────────────────┼────────────────────────────────────────────┤
│ 存储位置 │ 纯内存（RwLock<HashMap>）                    │ 内存缓存 + DB/向量库后端                   │
├──────────┼──────────────────────────────────────────────┼────────────────────────────────────────────┤
│ 数据形态 │ 对话消息列表、工具调用栈、中间变量           │ 已结构化的"记忆条目"（带向量、可搜索）     │
├──────────┼──────────────────────────────────────────────┼────────────────────────────────────────────┤
│ 大小     │ 受 LLM context window 限制，会被截断         │ 理论上可无限增长（分页/检索访问）          │
├──────────┼──────────────────────────────────────────────┼────────────────────────────────────────────┤
│ 访问方式 │ 直接顺序追加 messages                        │ 通过 memory_search / memory_write 工具检索 │
├──────────┼──────────────────────────────────────────────┼────────────────────────────────────────────┤
│ 隔离粒度 │ 每个 job 独立                                │ 每个 job 独立（避免串扰），但内容持久      │
├──────────┼──────────────────────────────────────────────┼────────────────────────────────────────────┤
│ 典型操作 │ append message、push tool call、update state │ semantic search、vector recall、FS write   │
└──────────┴──────────────────────────────────────────────┴────────────────────────────────────────────┘

举例说明

contexts 的工作场景

用户: "帮我看下昨天的日志"
↓
Job 启动 → 创建 JobContext {
messages: [
{role: user, content: "帮我看下昨天的日志"},
{role: assistant, content: "好的，我先读取..."},
{role: user, content: "重点看 error 部分"},
],
tool_stack: [tool_call("file_read", ...)],
...
}
↓
Job 结束 → JobContext 从 HashMap 移除（除非为了 resume）

关键：这些消息是为了喂给 LLM 的"上下文窗口"，job 结束就没用了。

memories 的工作场景

同一用户，过了 3 天后又来问："上次那个日志问题后来怎么解决的？"
↓
Agent 用 memory_search 检索 → 找到 3 天前写入的记忆条目：
"用户曾排查日志中 connection pool 耗尽问题，
根因是上游服务限流配置过低，建议调整至 100 QPS。"
↓
基于这条记忆回答用户

关键：这条记忆是结构化 + 带向量，长期保存在 DB 里，跨多次 job 复用。

设计意图

这种分离对应 AI Agent 系统的经典两层记忆模型：

┌─────────────────────────────────────────┐
│ contexts (JobContext)                   │  ← 工作记忆 / 短期
│ • 当前对话上下文                          │    (喂 LLM 用)
│ • 工具调用进行时状态                       │    (job 结束即丢)
│ • 类比：RAM                              │
├─────────────────────────────────────────┤
│ memories (Memory)                       │  ← 长期记忆
│ • 用户偏好、项目知识、过往经验              │    (跨 job 持久)
│ • 向量化后可语义检索                      │    (类比：硬盘)
│ • 关键信息会被压缩/摘要后写入               │
└─────────────────────────────────────────┘

为什么要分开存？

1. 性能差异大：contexts 频繁读写、需要锁；memories 主要走 DB / 向量检索
2. 保留策略不同：contexts 可以截断、压缩；memories 必须保留（CLAUDE.md 强调：LLM 数据永远不删）
3. 访问接口不同：contexts 是 Rust 结构体直接 .push()；memories 通过 memory_* 工具间接访问
4. 隔离边界清晰：contexts 失败可丢弃；memories 丢失需要专门的恢复流程

何时数据从 contexts 流向 memories？

通常在 job 关键节点（如任务完成、获得新认知）时，agent 会调用 memory_write 工具，把 context 里有价值的片段"沉淀"到 memories。这就是 记忆固化 过程。

三. Memory持久化时机

● memories 持久化时机

简短回答

Memory 实例本身只是个 Rust 句柄，它把"写什么"的操作代理到底层存储（PostgreSQL + libSQL）。持久化是同步发生的——每次 memory_write 调用都会立刻落库，不依赖 job 结束或 ContextManager 清理。

数据流向

Agent 工具调用
│
▼
memory_write("用户偏好深色模式")
│
▼
Memory::write() ──→ 立刻写入 DB（PostgreSQL/libSQL）
│
▼
RwLock<HashMap<Uuid, Memory>> 里只保存"句柄 + 缓存"，不阻塞写

详细解释

1. 持久化是 write 调用触发的

查看 Memory 抽象（src/workspace/），每次 memory_write 工具被调用时：

- 立刻把条目插入数据库 memory_entries 表
- 同时写入对应的 FTS 索引（全文搜索）
- 同时写入 embedding 向量表（语义搜索）
- 三者写入是事务性的（要么都成功，要么都回滚）

2. ContextManager 的 HashMap 里存的是什么？

memories: RwLock<HashMap<Uuid, Memory>>

存的是 Memory 结构体本身，不是数据副本。这个结构体内部：

- 持有 DB 连接 / 客户端句柄
- 持有最近访问条目的内存缓存（加速读）
- 持有当前 job 的命名空间 / scope 信息

HashMap 的作用是：让当前 job 能找到它专属的 Memory 访问句柄。句柄丢失 ≠ 数据丢失，数据一直在 DB 里。

3. 为什么"立刻写库"而不是"job 结束时写"？

几个关键原因：

1. CLAUDE.md 明确要求：LLM data is never deleted —— 所有 LLM 相关数据必须持久保留，不能因进程崩溃丢失
2. Job 可能中途崩溃：如果等到 job 结束才写，crash 后就丢了
3. 跨 job 立即可用：下一个 job 启动时就能搜到当前 job 刚写入的记忆
4. 支持 resume / recover：Job State Machine 里有 Stuck → InProgress (recovery)，恢复时需要历史记忆已落库

4. 内存 HashMap 中的 Memory 在何时被丢弃？

// job 结束时
contexts.remove(&job_id);
memories.remove(&job_id);   // 句柄被释放，缓存被丢弃

- 数据不会丢（已在 DB）
- 缓存被清空（节省内存）
- 下次相同 job_id 启动时，重新创建一个 Memory 句柄，从 DB 重新加载缓存

5. 写入路径示例

context/manager.rs:        Job 启动 → memory.insert(job_id, Memory::new(...))
│
workspace/memory.rs:       Memory::new() 打开 DB 连接，初始化缓存
│
tools/builtin/memory.rs:   memory_write 工具调用
│
workspace/repo.rs:         DB::insert_memory_entry(content, embedding, ...)
│
db/ (postgres/libsql):     事务写入 entries + fts + vectors 表

容易混淆的点

┌──────────────────────────────┬────────────────────────────────────┐
│             问题             │                答案                │
├──────────────────────────────┼────────────────────────────────────┤
│ HashMap 里存的是数据吗？     │ ❌  是句柄 + 缓存                   │
├──────────────────────────────┼────────────────────────────────────┤
│ 数据什么时候落库？           │ ✅  每次 memory_write 立即落库      │
├──────────────────────────────┼────────────────────────────────────┤
│ job 结束数据会丢吗？         │ ❌  不会，已持久化                  │
├──────────────────────────────┼────────────────────────────────────┤
│ HashMap 清空影响持久化吗？   │ ❌  不影响，下次重新加载            │
├──────────────────────────────┼────────────────────────────────────┤
│ 进程崩溃会丢未写入的记忆吗？ │ ❌  不会，所有已调用的 write 已落库 │
└──────────────────────────────┴────────────────────────────────────┘

性能权衡

立刻写库的开销被几个手段缓解：

- 批量 embedding：相邻写入可能合并 embedding 计算
- 内存缓存：高频读命中缓存，不查 DB
- 异步写入（部分路径）：write 调用 fire-and-forget，立即返回，DB 写入在后台 task 完成
- WAL 模式：libSQL 用 WAL，PostgreSQL 默认有 WAL，写入只追加日志不立即刷盘

总结

▎ Memory 不是"暂存区等批量刷盘"，而是每次操作立即落库的持久化抽象。ContextManager 里的 HashMap 只是句柄注册表，不参与持久化时机的决策。

