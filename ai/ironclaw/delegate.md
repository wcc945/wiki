一. 相关类型

1. trait

pub trait LoopDelegate: Send + Sync {
/// Called at the start of each iteration. Check for external signals
/// (cancellation, user messages, stop requests).
async fn check_signals(&self) -> LoopSignal;

    /// Called before the LLM call. Allows the delegate to refresh tool
    /// definitions, enforce cost guards, or inject messages.
    /// Return `Some(outcome)` to break the loop early.
    async fn before_llm_call(
        &self,
        reason_ctx: &mut ReasoningContext,
        iteration: usize,
    ) -> Option<LoopOutcome>;

    /// Call the LLM and return the result. Delegates own the LLM call
    /// to handle consumer-specific concerns (rate limiting, auto-compaction,
    /// cost tracking, force_text mode).
    async fn call_llm(
        &self,
        reasoning: &Reasoning,
        reason_ctx: &mut ReasoningContext,
        iteration: usize,
    ) -> Result<ironclaw_llm::RespondOutput, Error>;

    /// Handle a text-only response from the LLM.
    /// Return `TextAction::Return` to exit the loop, `TextAction::Continue` to proceed.
    async fn handle_text_response(
        &self,
        text: &str,
        metadata: ResponseMetadata,
        reason_ctx: &mut ReasoningContext,
    ) -> TextAction;

    /// Execute tool calls and add results to context.
    /// Return `Some(outcome)` to break the loop (e.g. approval needed).
    ///
    /// Implementations should call `reason_ctx.set_last_tool_batch_all_failed(true/false)`
    /// to report whether every tool in the batch failed. This enables the
    /// duplicate tool call detector to escalate repeated identical failures.
    async fn execute_tool_calls(
        &self,
        tool_calls: Vec<ironclaw_llm::ToolCall>,
        content: Option<String>,
        reason_ctx: &mut ReasoningContext,
        reasoning: Option<String>,
    ) -> Result<Option<LoopOutcome>, Error>;

    /// Called when the LLM expresses tool intent without actually calling a tool.
    /// Delegates can use this to emit events or log the nudge for observability.
    async fn on_tool_intent_nudge(&self, _text: &str, _reason_ctx: &mut ReasoningContext) {}

    /// Called after each successful iteration (no error, no early return).
    async fn after_iteration(&self, _iteration: usize) {}
}


2. JobDelegate

3. ChatDelegate

二. 核心逻辑

/// Run the unified agentic loop.
///
/// This is the single implementation used by all three consumers (chat, job, container).
/// The `delegate` provides consumer-specific behavior via the `LoopDelegate` trait.
pub async fn run_agentic_loop(
delegate: &dyn LoopDelegate,
reasoning: &Reasoning,
reason_ctx: &mut ReasoningContext,
config: &AgenticLoopConfig,
) -> Result<LoopOutcome, Error> {
let mut consecutive_tool_intent_nudges: u32 = 0;
// Accumulates across all iterations (not reset by text responses) so
// non-consecutive truncations still escalate to force_text.
let mut truncation_count: u32 = 0;
let mut dup_tracker = DuplicateToolCallTracker::new();

    for iteration in 1..=config.max_iterations {//最多迭代多少轮次 llm
        // Check for external signals (stop, cancellation, user messages)
        match delegate.check_signals().await {//检查是否收到外部信号(创建job时的那个tx，控制板开始的)
            LoopSignal::Continue => {}
            LoopSignal::Stop => return Ok(LoopOutcome::Stopped),
            LoopSignal::InjectMessage(msg) => {
                reason_ctx.messages.push(ChatMessage::user(&msg));
            }
        }

        // Pre-LLM call hook (cost guard, tool refresh, iteration limit nudge)
        if let Some(outcome) = delegate.before_llm_call(reason_ctx, iteration).await {//调llm之前的拦截
            return Ok(outcome);
        }

        // Call LLM
        let output = delegate.call_llm(reasoning, reason_ctx, iteration).await?;//调用llm

        match output.result {
            RespondResult::Text(text) => {
                // Tool intent nudge: if the LLM says "let me search..." without
                // actually calling a tool, inject a nudge message.
                if config.enable_tool_intent_nudge
                    && !reason_ctx.available_tools.is_empty()
                    && !reason_ctx.force_text
                    && consecutive_tool_intent_nudges < config.max_tool_intent_nudges
                    && ironclaw_llm::llm_signals_tool_intent(&text)
                {
                    consecutive_tool_intent_nudges += 1;
                    tracing::info!(
                        iteration,
                        "LLM expressed tool intent without calling a tool, nudging"
                    );
                    delegate.on_tool_intent_nudge(&text, reason_ctx).await;
                    reason_ctx.messages.push(ChatMessage::assistant(&text));
                    reason_ctx
                        .messages
                        .push(ChatMessage::user(ironclaw_llm::TOOL_INTENT_NUDGE));
                    delegate.after_iteration(iteration).await;
                    continue;
                }

                // Reset nudge counter since we got a non-intent text response
                if !ironclaw_llm::llm_signals_tool_intent(&text) {
                    consecutive_tool_intent_nudges = 0;
                }

                // Text response breaks any duplicate tool call streak.
                dup_tracker.reset();

                match delegate
                    .handle_text_response(&text, output.metadata, reason_ctx)
                    .await
                {
                    TextAction::Return(outcome) => return Ok(outcome),
                    TextAction::Continue => {}
                }
            }
            RespondResult::ToolCalls {
                tool_calls,
                content,
                reasoning,
            } => {
                // If the response was truncated, tool call parameters are likely
                // incomplete. Discard them and tell the LLM to try a different
                // approach rather than executing malformed tool calls.
▎ 这是 "claim without evidence" 的强制刹车——LLM 嘴上说要做事但没真的发 tool_calls 时，agentic loop 会不退出而是注入一条 TOOL_INTENT_NUDGE 提示，让 LLM 下一轮必须走结构化 tool_calls                   
▎ 路径真调工具；如果连续 max_tool_intent_nudges 次还嘴硬，就放弃干预让 loop 退出。触发条件由 llm_signals_tool_intent 纯文本检测（排除对话体前缀后匹配"动作意图"）把关。
if output.finish_reason == FinishReason::Length {

                    truncation_count += 1;
                    let names: Vec<&str> = tool_calls.iter().map(|tc| tc.name.as_str()).collect();
                    tracing::warn!(
                        iteration,
                        tools = ?names,
                        truncation_count,
                        "Discarding truncated tool calls (finish_reason=Length)"
                    );
                    if let Some(ref text) = content {
                        reason_ctx.messages.push(ChatMessage::assistant(text));
                    }
                    reason_ctx
                        .messages
                        .push(ChatMessage::user(ironclaw_llm::TRUNCATED_TOOL_CALL_NOTICE));
                    // After repeated truncations, force text-only mode so the LLM
                    // stops attempting tool calls it can't fit in the output budget.
                    if truncation_count >= 3 {
                        reason_ctx.force_text = true;
                    }
                    delegate.after_iteration(iteration).await;
                    continue;
                }

                consecutive_tool_intent_nudges = 0;
                truncation_count = 0;

                // Fingerprint before execution (avoids cloning the full Vec).
                let batch_fingerprint = DuplicateToolCallTracker::fingerprint(&tool_calls);

                // Reset the flag before execution; delegates set it in execute_tool_calls.
                reason_ctx.last_tool_batch_all_failed = false;

                if let Some(outcome) = delegate
                    .execute_tool_calls(tool_calls, content, reason_ctx, reasoning)//执行工具调用
                    .await?
                {
                    return Ok(outcome);
                }

                // Track duplicate failing tool calls and escalate.
                let dup_count = dup_tracker.record_with_fingerprint(
                    batch_fingerprint,
                    reason_ctx.last_tool_batch_all_failed,
                );
                if dup_count >= DUPLICATE_FORCE_TEXT_THRESHOLD {
                    tracing::debug!(
                        iteration,
                        dup_count,
                        "Repeated duplicate failing tool calls — forcing text mode"
                    );
                    reason_ctx.force_text = true;
                    reason_ctx
                        .messages
                        .push(ChatMessage::user(DUPLICATE_TOOL_CALL_WARNING));
                } else if dup_count >= DUPLICATE_WARNING_THRESHOLD {
                    tracing::debug!(
                        iteration,
                        dup_count,
                        "Repeated duplicate failing tool calls — injecting warning"
                    );
                    reason_ctx
                        .messages
                        .push(ChatMessage::user(DUPLICATE_TOOL_CALL_WARNING));
                }
            }
        }

        delegate.after_iteration(iteration).await;//在当前轮次执行完后的处理
    }

    Ok(LoopOutcome::MaxIterations)//agent_loop执行完成
}

## 答：处理 LLM 输出被 `max_tokens` 截断、导致 `tool_calls` 参数不完整的兜底

当 LLM 一次输出**太长**、被 `max_tokens=4096`（见 `respond_with_tools`）一刀切了，结果是 `tool_calls` 数组里的 `arguments` JSON **被从中间腰斩**——例如应该 `{"path":"/home/...","content":"abcdef"}` 被切成 `{"path":"/hom`。如果照常 `execute_tool_calls`，dispatcher 会拿这半截 JSON 去 `serde_json::from_value`，要么解析失败，要么解析出**错的值**（缺字段、走 default）然后**真**去执行——比方说把整个磁盘当 path。

所以这段是**截断检测 + 放弃 + 引导换思路**。

## 5 步处理

```rust
if output.finish_reason == FinishReason::Length {       // ① 检测截断
    truncation_count += 1;                              // ② 累计次数

    let names: Vec<&str> = tool_calls.iter().map(|tc| tc.name.as_str()).collect();
    tracing::warn!(iteration, tools = ?names, truncation_count,
        "Discarding truncated tool calls (finish_reason=Length)");

    if let Some(ref text) = content {                  // ③ 把 LLM 那段已说的人话留下
        reason_ctx.messages.push(ChatMessage::assistant(text));
    }
    reason_ctx.messages.push(ChatMessage::user(        // ④ 塞"换思路"的提醒
        ironclaw_llm::TRUNCATED_TOOL_CALL_NOTICE,
    ));

    if truncation_count >= 3 {                         // ⑤ 3 次还截 → 强制纯文本
        reason_ctx.force_text = true;
    }
    delegate.after_iteration(iteration).await;
    continue;  // 跳回 loop 顶部，**不**调任何工具
}
```

`TRUNCATED_TOOL_CALL_NOTICE`（`reasoning.rs:29-33`）：

```rust
pub const TRUNCATED_TOOL_CALL_NOTICE: &str = "\
Your previous response was truncated while generating tool call parameters. \
The tool calls were discarded. Please try a different approach — \
summarize or transform the data instead of echoing it verbatim in a tool call.";
```

## 4 个关键决策

| 决策                          | 做法                                                                   | 为什么                                                                                                                                                                                           |
| ----------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **不调工具**                  | 直接 `continue`，跳过 `execute_tool_calls`                             | 截断的 `arguments` 是垃圾，**绝对不能**进 dispatcher                                                                                                                                             |
| **LLM 已经说的人话留下**      | `reason_ctx.messages.push(ChatMessage::assistant(text))`               | 保留思考连续性，不让 LLM 觉得自己的输出凭空消失                                                                                                                                                  |
| **不再 push tool_calls 消息** | 注意代码里**没有** `push(ChatMessage::assistant_with_tool_calls(...))` | 这是和"成功路径"的关键差别——`execute_tool_calls` 写的那条 `assistant_with_tool_calls` 消息在 `job.rs:1768-1771`；这段 truncate 路径**故意不写**，否则下轮 LLM 会以为这些 tool_calls 已经发出去了 |
| **不报错、不让 loop 退出**    | `continue`                                                             | 给 LLM 一个补救机会（换个更短的策略重试）                                                                                                                                                        |

## 第 5 步 `truncation_count >= 3 → force_text` 是关键

LLM 不知道自己的 `max_tokens` 上限。**前两次**截断时，loop 期待它能自己调整策略（分步调用、改成汇总式、缩小 payload），所以**只 nudge**。**第 3 次还截**就说明 LLM **学不会**——这时：

```rust
reason_ctx.force_text = true;
```

把这个开关打开（`respond_with_tools` 里 `effective_tools = if context.force_text { Vec::new() } else { ... }`），下一轮 LLM 收到的 `available_tools` 是空的，**根本没法发 tool_call**——只能产出纯文本。它会被迫描述"为什么做不了 / 下一步该用户怎么帮我"。

## 为什么是 3 次而不是 2 或 5

注释没明说，但 3 是经验值：
- 1 次：可能偶发（用户输入意外长）→ 让它再试；
- 2 次：模式开始形成 → 还愿意相信 LLM 能自我修正；
- 3 次：定型 → 强制放弃工具路径，避免无限循环浪费 token。

## 和 `TOOL_INTENT_NUDGE` 的对照

两条都是 **"LLM 没做对事 → 注入提醒"** 模式，但触发条件相反：

|      | `TOOL_INTENT_NUDGE`                  | `TRUNCATED_TOOL_CALL_NOTICE`                |
| ---- | ------------------------------------ | ------------------------------------------- |
| 触发 | LLM **说**要做事但**没**发 tool_call | LLM **发了** tool_call 但**被截断**         |
| 处理 | `continue` 下一轮 LLM 重发           | 丢掉 tool_call，下一轮 LLM 换思路           |
| 升级 | `max_tool_intent_nudges` 达到 → 放弃 | `truncation_count >= 3` → 强制 `force_text` |
| 目标 | "嘴硬"                               | "嘴破"                                      |

## 一句话

> **这是 `FinishReason::Length` 截断的兜底**——LLM 输出超过 `max_tokens=4096` 导致 `tool_calls` 参数被腰斩，**直接丢弃不调工具**（否则 dispatcher 会拿半截 JSON 跑出灾难性副作用），塞一条"换思路"提示让 LLM 重新规划；累计 3 次还截就把 `force_text` 打开强制纯文本，**彻底断掉** tool_call 路径，避免 token 浪费在不可能完成的工具调用上。
***********************************************
## 答：检测"卡死循环"——LLM 反复调同一个失败的工具组合时分两级干预

这是 `run_agentic_loop` 里的"无限失败循环"刹车：当 LLM 连续多轮**重复同样的失败工具组合**（同样的工具名 + 参数指纹）时，给 LLM 一句警告；**继续**重复就强制切到纯文本模式，让它**别再调工具了**。

## 5 个关键点

### 1. 去重指纹 `batch_fingerprint`

同一轮里 LLM 可能调多个工具（`tool_calls` 是 `Vec`）。`batch_fingerprint` 是这批工具调用的**归一化指纹**——`(tool_name, 参数哈希)` 的有序组合。同样的工具组合按同样顺序失败 → 同一指纹。

### 2. `dup_tracker.record_with_fingerprint(fingerprint, last_tool_batch_all_failed)`

这是去重计数器的两步操作（`src/agent/agentic_loop.rs`）：
- 如果指纹**已存在** → 计数 +1；
- 如果**新** → 计数 = 1；
- `last_tool_batch_all_failed` 是上一轮的工具执行结果（你之前看过 `execute_tool_calls` 里设的 `reason_ctx.last_tool_batch_all_failed`）——**只有"全失败"的那批才计入重复**，部分失败不计。

→ 所以"卡死"的定义是：**同样的工具组合，**连续**全部失败**。

### 3. 两级阈值（按调用频率递减干预）

```rust
const DUPLICATE_WARNING_THRESHOLD: u32 = 2;     // 重复 2 次 → 警告
const DUPLICATE_FORCE_TEXT_THRESHOLD: u32 = 4;  // 重复 4 次 → 强制纯文本
```

**两级反应**（按 dup_count 决定）：

| 计数                | 行为                                            | 用意                                                       |
| ------------------- | ----------------------------------------------- | ---------------------------------------------------------- |
| `2 ≤ dup_count < 4` | 推一条 `DUPLICATE_TOOL_CALL_WARNING` user 消息  | "提示一下"，LLM 有机会自己换思路                           |
| `dup_count ≥ 4`     | 推警告消息 **+** `reason_ctx.force_text = true` | "强制放弃"，下一轮 LLM 拿不到 `available_tools` 只能纯文本 |

`force_text = true` 之后（`respond_with_tools` 里 `effective_tools = if context.force_text { Vec::new() } else { ... }`），**LLM 拿到的工具清单是空的**——它只能描述"为什么做不了 / 请用户介入"。

### 4. `DUPLICATE_TOOL_CALL_WARNING` 的内容

```rust
// 来自 src/agent/agentic_loop.rs 顶部常量
const DUPLICATE_TOOL_CALL_WARNING: &str = "\
You have called the same tool(s) multiple times with similar arguments and they have all failed. \
Stop repeating the same failing call. Either try a fundamentally different tool, \
or explain to the user what is blocking progress and ask for guidance.";
```

明确告诉 LLM：**别再重复**同样的失败，**要么换工具，要么坦白跟用户说**。

### 5. **不**调 `execute_tool_calls`、**不** `return` LoopOutcome

和 `TOOL_INTENT_NUDGE` / `TRUNCATED_TOOL_CALL_NOTICE` 一样走 `continue`——下轮 LLM 在看到警告 + 历史失败记录后**自己**决定下一步。这是个**引导**机制，**不是惩罚**。

## 为什么需要这个

循环失败的真实成本：

| 没有 dup_tracker                                             | 有 dup_tracker                                                    |
| ------------------------------------------------------------ | ----------------------------------------------------------------- |
| LLM 不断 `curl https://broken-url`（同一个 4xx）             | 第 2 次 → 推警告，LLM 可能改用 `web_fetch`                        |
| token 持续消耗，账单往上跑                                   | 第 4 次 → 强制纯文本，LLM 必须说"我搞不定"                        |
| 用户的 job 卡在 `InProgress` 永远不退                        | job 自救，loop 退出，回到 `JobState` 由 `SelfRepair` 检测 `Stuck` |
| 可能触发"幻觉循环"（LLM 自圆其说地把同一次失败解释成"成功"） | 物理上断掉工具调用，幻觉循环不可能发生                            |

## 和你之前看过的三道防线的对照

| 防线                                      | 触发条件                              | 干预                        |
| ----------------------------------------- | ------------------------------------- | --------------------------- |
| `TOOL_INTENT_NUDGE`                       | LLM **说**要做但**没**发 `tool_calls` | 注入提醒，让它真去做        |
| `TRUNCATED_TOOL_CALL_NOTICE`              | LLM **发了**但被 `Length` 截断        | 丢掉 tool_calls，让它换思路 |
| **`DUPLICATE_TOOL_CALL_WARNING`**（这条） | LLM **发了**但**反复失败**            | 警告 → 强制纯文本           |
| `AutonomousUnavailable`                   | 工具**根本不在白名单**                | 错误回灌 LLM，让它换工具    |

四条防线覆盖了 agentic loop 里的四类典型 LLM "行为偏差"，全部用 `continue + 注入消息 + 必要时 force_text` 的统一模式处理。

## 一句话

> **这是"反复失败循环"检测器**——LLM 连续多轮调同一组工具且**全部失败**时，先推一条警告让 LLM 主动换思路（`dup_count >= 2`），继续重试就强制 `force_text` 切到纯文本模式（`dup_count >= 4`），**物理上**断掉工具调用能力，避免 token 浪费在不可能完成的重复失败上，也避免 job 一直卡在 `InProgress` 等 `SelfRepair` 来救。

