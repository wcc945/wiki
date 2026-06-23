一. 相关结构

/// Tracks costs and action rates, enforcing configurable limits.
///
/// Thread-safe; designed to be shared via `Arc<CostGuard>`.
pub struct CostGuard {
config: CostGuardConfig,

    /// Running cost total for the current day (in USD, not cents).
    daily_cost: Mutex<DailyCost>,前全局每日累计花费（USD，不以美分计）。

    /// Sliding window of action timestamps for rate limiting.
    action_window: Mutex<VecDeque<Instant>>,
作用：滑动窗口，存储最近 N 次动作的时间戳，用于速率限制。

- VecDeque<Instant>：
    - 队尾追加新动作的 Instant
    - 队头弹出超过窗口时长的旧时间戳
    - 适合"两端操作"的滑动窗口
- 检查逻辑：每次动作前，先清理过期项，再判断 len() >= limit

示例（假设限制每分钟 60 次）：
[10:00:01, 10:00:05, 10:00:30, ..., 10:01:05]  // 100 个时间戳
↓ 清理超过 60s 的
[10:00:05, ..., 10:01:05]  // 仅剩窗口内项
↓ 长度判断
len() < 60 → 允许


    /// Flag set when daily budget is exceeded to short-circuit checks.
    budget_exceeded: AtomicBool,
作用：每日预算超支的"短路开关"。

- AtomicBool 而非 Mutex<bool>：
    - 检查操作极频繁（每个动作都要查），atomic 无锁开销最小
    - 只涉及简单的 load/store，不需要复杂同步
- 短路优化：一旦置 true，后续检查直接返回 false，不再查 HashMap / 遍历 VecDeque，节省锁竞争



    /// Per-model token usage since startup.
    model_tokens: Mutex<HashMap<String, ModelTokens>>,按模型名称分组的 token 用量统计。输入输出花销

    /// Per-user daily cost tracking. Each entry resets independently at midnight UTC.
    per_user_daily_cost: Mutex<HashMap<String, DailyCost>>,按用户分组的每日花费追踪。
}

#[derive(Debug, Clone, Default)]
pub struct CostGuardConfig {
/// Maximum spend per day in cents (e.g. 10000 = $100). None = unlimited.
pub max_cost_per_day_cents: Option<u64>,
/// Maximum LLM calls per hour. None = unlimited.
pub max_actions_per_hour: Option<u64>,
/// Maximum spend per user per day in cents. None = unlimited.
/// Applied independently per user alongside the global budget.
pub max_cost_per_user_per_day_cents: Option<u64>,
}
通常包含：
- 每日总预算上限（USD）
- 每用户每日预算上限
- 动作速率限制（如每分钟最多 N 次）
- 时间窗口大小（滑动窗口的时长）
- 告警阈值（如 80% 时发通知）

二. 滑动窗口限流

三层限制，不同拒绝方式

调用 LLM 前
│
▼
1. budget_exceeded (原子短路)
   │  true → 直接 Err，零开销
   ▼
2. action_window 速率限制
   │  满 → Err(TooManyRequests)
   ▼
3. daily_cost / per_user 预算
   │  超 → Err(BudgetExceeded)
   ▼
4. LLM 调用真正发出
***************************
错误如何传递给用户

Agent loop
│
▼
cost_guard.check(user_id, model) → Err(BudgetExceeded)
│
▼
agent loop 捕获错误，决定如何响应
│
├─→ 转换为用户友好消息：
│   "今日预算已用完，请明天再试或联系管理员调整限额"
│
├─→ 写入 JobContext 的 messages（作为 tool_result 错误）
│   让 LLM 看到错误并决定下一步
│
└─→ 终止当前 job（或标记为 Failed）

● action_window 滑动窗口详解

一句话

滑动窗口是最近 N 次动作的时间戳队列，用来判断"在最近一段时间内动作是否太频繁"。

  ---
为什么需要滑动窗口？

问题：怎么限制"每分钟最多 60 次 LLM 调用"？

朴素方案：计数器，每分钟清零。
- 缺点：用户可以在 0:59 打 60 次，下一秒 1:00 又打 60 次 → 实际 1 秒内 120 次

滑动窗口方案：保留每次动作的精确时间戳，只看窗口内的数量。
- 优点：任意时刻回溯过去 60 秒，精确判断

  ---
举个例子

配置：限制每 60 秒最多 3 次动作。

动作序列：

时刻      动作    窗口内容（VecDeque）         长度  是否允许
─────────────────────────────────────────────────────────────
10:00:00  A1     [10:00:00]                    1    ✅  允许
10:00:10  A2     [10:00:00, 10:00:10]          2    ✅  允许
10:00:20  A3     [10:00:00, 10:00:10, 10:00:20] 3  ✅  允许（刚到上限）
10:00:25  A4     [10:00:00, 10:00:10, 10:00:20] 3  ❌  拒绝（已满）
10:00:35  A5     [10:00:00, 10:00:10, 10:00:20] 3  ❌  拒绝（已满）
10:00:45  A6     清理 10:00:00（>60s前）
→     [10:00:10, 10:00:20]           2    ✅  允许
10:00:50  A7     [10:00:10, 10:00:20, 10:00:50] 3 ✅  允许

  ---
代码逻辑

fn check_rate_limit(&self) -> bool {
let mut window = self.action_window.lock().unwrap();
let now = Instant::now();
let window_duration = Duration::from_secs(60);

      // 1. 清理过期项（队头时间戳早于 now - window_duration）
      while let Some(&front) = window.front() {
          if now.duration_since(front) > window_duration {
              window.pop_front();
          } else {
              break;  // 剩下的都还在窗口内
          }
      }

      // 2. 判断窗口内项数
      if window.len() >= self.config.max_actions_per_window {
          return false;  // 拒绝
      }

      // 3. 记录本次动作
      window.push_back(now);
      true  // 允许
}

  ---
实际场景

场景 1：防止 LLM API 被刷爆

配置：max_actions_per_window = 60, window = 60s

如果 agent 每秒调一次 LLM：
第 60 次时窗口内有 60 个 → 第 61 次被拒绝
等 1 秒后最早那个过期 → 又允许 1 次

→ 严格保证"任意 60 秒内不超过 60 次"

场景 2：防止工具滥用

用户疯狂点击"发送消息"按钮：
每按一次记一个时间戳
10 秒内按了 20 次 → 第 11 次起被拒绝
→ 提示"操作过于频繁"

场景 3：防止 routine 心跳风暴

HEARTBEAT 配置 30 分钟一次，但配置错误成 1 秒一次：
滑动窗口检测到 → 自动阻断 → 防止烧钱

  ---
与其他限流方案对比

┌─────────────────────────────────────┬──────┬───────────────────────┬──────────────────────┐
│                方案                 │ 精度 │       边界突刺        │       内存开销       │
├─────────────────────────────────────┼──────┼───────────────────────┼──────────────────────┤
│ 固定窗口（每分钟清零计数器）        │ 低   │ ❌  窗口切换瞬间可双倍 │ O(1)                 │
├─────────────────────────────────────┼──────┼───────────────────────┼──────────────────────┤
│ 滑动日志（本方案：VecDeque 时间戳） │ 高   │ ✅  无突刺             │ O(N)，N = 窗口内次数 │
├─────────────────────────────────────┼──────┼───────────────────────┼──────────────────────┤
│ 滑动窗口计数（哈希+求和近似）       │ 中   │ ⚠️ 近似               │ O(窗口数)            │
├─────────────────────────────────────┼──────┼───────────────────────┼──────────────────────┤
│ 令牌桶（按速率补充 token）          │ 高   │ ✅  平滑突发           │ O(1)                 │
└─────────────────────────────────────┴──────┴───────────────────────┴──────────────────────┘

本项目选滑动日志原因：
- 实现简单（VecDeque 即可）
- 精确（不会突发）
- 每次动作才记录，开销与动作量成正比，不是定时任务

  ---
为什么用 VecDeque 而不是 Vec？

┌──────────┬────────────────────┬───────────┐
│   操作   │        Vec         │ VecDeque  │
├──────────┼────────────────────┼───────────┤
│ 尾部追加 │ O(1) 均摊          │ O(1) 均摊 │
├──────────┼────────────────────┼───────────┤
│ 头部弹出 │ O(n)（需整体移动） │ O(1)      │
├──────────┼────────────────────┼───────────┤
│ 头部查看 │ O(1)               │ O(1)      │
└──────────┴────────────────────┴───────────┘

滑动窗口频繁 pop_front（每次清理过期项），所以 VecDeque 是必然选择。

  ---
总结

▎ action_window 是一个"最近动作时间戳的滑动队列"，通过清理过期项 + 长度判断，实现精确的"任意时间窗口内不超过 N 次"的速率限制。它牺牲一点内存（O(N)），换来无突刺的精确限流。

三. token统计

● Token 统计机制详解

一句话

Token 统计发生在 LLM provider 返回响应时，从 provider 的 usage 字段读取，写入 model_tokens 和 daily_cost。

  ---
数据流

LLM Provider 调用
│
▼
Response { content, usage: Usage { input_tokens, output_tokens } }
│
▼
CostGuard.record_usage(model, input_tokens, output_tokens)
│
├──→ model_tokens[model].add(input, output)
│
└──→ daily_cost.add(calculate_cost(model, input, output))

  ---
关键点：Token 从哪来？

1. Provider 返回的 usage 字段

不同 LLM provider 都在响应中返回 token 用量：

Anthropic：
{
"usage": {
"input_tokens": 1234,
"output_tokens": 567
}
}

OpenAI：
{
"usage": {
"prompt_tokens": 1234,
"completion_tokens": 567,
"total_tokens": 1801
}
}

OpenAI 兼容 / Ollama / NEAR AI / Bedrock：
- 都遵循类似结构（部分支持 total_tokens）

铁律：不要本地估算 token（不准确），永远信任 provider 返回值。

  ---
成本计算

价格配置

不同模型不同价格（每 1K tokens）：

struct ModelPricing {
input_per_1k: f64,   // 输入价格（USD）
output_per_1k: f64,  // 输出价格（通常更贵）
}

// 示例
claude-opus-4-6:  { input: 0.015,  output: 0.075  }
gpt-5:            { input: 0.005,  output: 0.015  }
llama-3.1-70b:    { input: 0.0007, output: 0.0007 }

计算公式

fn calculate_cost(model: &str, input: u64, output: u64) -> f64 {
let pricing = self.pricing.get(model).unwrap_or_default();
let input_cost = (input as f64 / 1000.0) * pricing.input_per_1k;
let output_cost = (output as f64 / 1000.0) * pricing.output_per_1k;
input_cost + output_cost
}

注意：输出 token 通常比输入贵 3-5 倍（生成成本更高），所以计费时要分别算。

  ---
写入流程（在 record_usage 中）

fn record_usage(&self, model: &str, user_id: &str, input: u64, output: u64) {
let cost = self.calculate_cost(model, input, output);

      // 1. 更新 model_tokens（用于仪表盘/分析）
      {
          let mut tokens = self.model_tokens.lock().unwrap();
          let entry = tokens.entry(model.to_string()).or_default();
          entry.input_tokens += input;
          entry.output_tokens += output;
          entry.request_count += 1;
          entry.last_used = Instant::now();
      }

      // 2. 更新全局 daily_cost
      {
          let mut daily = self.daily_cost.lock().unwrap();
          daily.maybe_reset_if_new_day();
          daily.total += cost;
          if daily.total > self.config.daily_limit {
              self.budget_exceeded.store(true, Ordering::Relaxed);
          }
      }

      // 3. 更新 per_user_daily_cost
      {
          let mut users = self.per_user_daily_cost.lock().unwrap();
          let user = users.entry(user_id.to_string()).or_default();
          user.maybe_reset_if_new_day();
          user.total += cost;
      }
}

  ---
调用时机（在 LLM 调用栈中）

agent loop
│
▼
LlmProvider::complete(messages)   ← 调用 provider
│
▼
Response { content, usage }
│
▼
agent loop 收到响应后：
1. 把 content 加到 messages
2. 调用 cost_guard.record_usage(model, user, usage.input, usage.output)
3. 检查 budget_exceeded

关键：record_usage 紧跟在 LLM 调用成功后，原子地完成统计和预算检查。

  ---
Cache token 的处理（高级）

Anthropic 等 provider 支持 prompt caching，有额外字段：

{
"usage": {
"input_tokens": 1234,
"output_tokens": 567,
"cache_creation_input_tokens": 5000,   // 写入缓存
"cache_read_input_tokens": 8000       // 命中缓存
}
}

计费规则：
- cache_creation_input_tokens：按略高于正常 input 价格（如 1.25x）
- cache_read_input_tokens：按大幅折扣价格（如 0.1x）
- 仍计入"输入"，但 cost 不同

CostGuard 处理时需要识别这些字段，分别计费。

  ---
特殊场景

1. 流式响应（streaming）

流式响应中，usage 通常在最后一个 chunk 返回：

let mut stream = provider.stream(messages).await?;
while let Some(chunk) = stream.next().await {
match chunk {
StreamChunk::Delta(text) => emit(text),
StreamChunk::Usage(u) => cost_guard.record_usage(model, user, u.input, u.output),
}
}

2. 错误响应

如果 provider 返回 4xx/5xx，没有 usage，不计入成本。
但可能有限流错误（429）——此时不会扣费，但需要 backoff。

3. 嵌入（embedding）

嵌入也有 token 用量：
- 走 EmbeddingProvider 自己的统计
- 价格通常比 LLM 低很多
- 可能不计入 CostGuard（独立预算）

4. 工具调用 token

工具调用的 tool_use 块的 token 也算 output_tokens，由 provider 计入 usage.output_tokens。

  ---
数据展示

model_tokens 字段最终会被展示在：

- CLI 的 /status 命令
- Web UI 的"用量"页面
- 监控 /metrics 端点（Prometheus 格式）

// 示例输出
{
"claude-opus-4-6": {
"input_tokens": 1_234_567,
"output_tokens": 234_567,
"request_count": 1234,
"estimated_cost_usd": 23.45
},
"gpt-5": {
"input_tokens": 567_890,
"output_tokens": 89_012,
"request_count": 567,
"estimated_cost_usd": 4.12
}
}

  ---
总结

┌──────────────────┬───────────────────────────┬──────────────────────────────────────────────────────┐
│       步骤       │          谁负责           │                       数据来源                       │
├──────────────────┼───────────────────────────┼──────────────────────────────────────────────────────┤
│ 1. LLM 调用      │ LlmProvider               │ 构造请求                                             │
├──────────────────┼───────────────────────────┼──────────────────────────────────────────────────────┤
│ 2. 获取 token 数 │ Provider 响应             │ usage.input_tokens / output_tokens                   │
├──────────────────┼───────────────────────────┼──────────────────────────────────────────────────────┤
│ 3. 计算成本      │ CostGuard::calculate_cost │ 模型定价表                                           │
├──────────────────┼───────────────────────────┼──────────────────────────────────────────────────────┤
│ 4. 累加统计      │ CostGuard::record_usage   │ 写入 model_tokens / daily_cost / per_user_daily_cost │
├──────────────────┼───────────────────────────┼──────────────────────────────────────────────────────┤
│ 5. 检查预算      │ CostGuard::check          │ 与 config.daily_limit / per_user_limit 比较          │
└──────────────────┴───────────────────────────┴──────────────────────────────────────────────────────┘

核心原则：永远用 provider 返回的 token 数，不要用本地 tokenizer 估算（容易出错且性能差）。

