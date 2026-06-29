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

/// Spawn the cron ticker background task.
pub fn spawn_cron_ticker(
engine: Arc<RoutineEngine>,
interval: Duration,
) -> tokio::task::JoinHandle<()> {
tokio::spawn(async move {
// Recover orphaned runs from a previous process crash before
// dispatching any new work, so we don't confuse fresh dispatches
// with crash orphans.
engine.sync_dispatched_runs().await;

        // Run one cron check immediately so routines due at startup don't
        // wait an extra full polling interval.
        engine.check_cron_triggers().await;

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Periodic event cache refresh so web/CLI mutations are picked up
        // without requiring tool-path code to call refresh_event_cache().
        // Uses wall-clock elapsed time so the refresh cadence is stable
        // regardless of the cron tick interval configuration.
        let refresh_interval = Duration::from_secs(60);
        let mut last_refresh = tokio::time::Instant::now();

        loop {//循环检查
            ticker.tick().await;
            // Sync first: only processes runs from before boot_time, so it
            // never races with FullJobWatcher instances from this process.
            engine.sync_dispatched_runs().await;
            engine.check_cron_triggers().await;

            if last_refresh.elapsed() >= refresh_interval {
                engine.refresh_event_cache().await;
                last_refresh = tokio::time::Instant::now();
            }
        }
    })
}

这段注释的意思是：如果进程之前崩溃了，可能有些 routine run 还停留在 running，但它实际关联的 full job 已经结束了。这个函数会定期检查这些“遗留的运行记录”，根据 job 的真实状态把 routine run 补成完成/失
败等最终状态。它只碰当前进程启动前创建的 run，所以不会和当前进程正在正常监听 job 状态的 watcher 抢着更新同一条记录。
pub async fn sync_dispatched_runs(&self) {
let runs = match self.store.list_dispatched_routine_runs().await {//从db查正在运行的
Ok(r) => r,
Err(e) => {
tracing::error!("Failed to list dispatched routine runs: {}", e);
return;
}
};

        // Only process runs from a previous process instance. Runs started
        // after boot_time are actively watched by a FullJobWatcher in this
        // process and should not be finalized here.
        let orphaned: Vec<_> = runs
            .into_iter()
            .filter(|r| r.started_at < self.boot_time)
            .collect();

        if orphaned.is_empty() {
            return;
        }



        for run in orphaned {
            let job_id = match run.job_id {
                Some(id) => id,
                None => continue, // Should not happen (query filters), but guard anyway
            };

            // Fetch the linked job
            let job = match self.store.get_job(job_id).await {
                Ok(Some(j)) => j,
                Ok(None) => {
                    // Orphaned: job record was deleted or never persisted
                    self.complete_dispatched_run(
                        &run,
                        RunStatus::Failed,
                        &format!("Linked job {job_id} not found (orphaned)"),
                    )
                    .await;
                    continue;
                }
                Err(e) => {
                    continue;
                }
            };

            // Map job state to final run status
            let final_status = match job.state {
                JobState::Completed | JobState::Submitted | JobState::Accepted => {
                    Some(RunStatus::Ok)
                }
                JobState::Failed | JobState::Cancelled => Some(RunStatus::Failed),
                // Pending, InProgress, Stuck — still running
                _ => None,
            };

            let status = match final_status {
                Some(s) => s,
                None => continue, // Job still active, check again next tick
            };

            // Build summary
            let summary = if status == RunStatus::Failed {
                match self.store.get_agent_job_failure_reason(job_id).await {//得到失败的原因
                    Ok(Some(reason)) => format!("Job {job_id} failed: {reason}"),
                    _ => format!("Job {job_id} {}", job.state),
                }
            } else {
                format!("Job {job_id} completed successfully")
            };

            self.complete_dispatched_run(&run, status, &summary).await;
        }
    }

这个函数是在 **收尾一个已经派发出去的 routine run**，尤其服务于前面说的 crash recovery 场景。

函数名：

```rust
complete_dispatched_run(&self, run: &RoutineRun, status: RunStatus, summary: &str)
```

意思是：某个 routine run 已经派发成 full job 了，现在知道这个 job 的最终结果了，于是把 routine run 补齐为完成/失败状态，并做后续清理和通知。

它按顺序做了这些事。

1. **把 routine run 标记为最终状态**

```rust
self.store
    .complete_routine_run(run.id, status, Some(summary), None)
    .await
```

更新 DB 里的 `routine_run` 记录：

```text
running -> completed / failed / canceled / ...
```

并保存 `summary`。

如果这一步失败，函数直接 `return`。因为 run 本身都没法完成，后续更新 routine 统计、发通知都不可靠。

2. **加载对应的 routine**

```rust
let mut routine = match self.store.get_routine(run.routine_id).await
```

`RoutineRun` 只是一次运行记录，它关联一个 `routine_id`。

这里重新查 routine，是为了更新：

- `run_count`
- `consecutive_failures`
- `state`
- `next_fire_at`
- 通知配置 `routine.notify`
- 所属用户 `routine.user_id`
- 会话线程信息

如果 routine 不存在或查询失败，就记录日志然后退出。

3. **更新失败次数**

```rust
let new_failures = if status == RunStatus::Failed {
    routine.consecutive_failures + 1
} else {
    0
};
```

如果这次失败，连续失败次数加 1。

如果这次不是失败，连续失败次数清零。

4. **更新 routine 的验证/运行状态**

```rust
routine.state = apply_routine_verification_result(
    &routine.state,
    routine_verification_fingerprint(&routine),
    status,
    now,
);
```

这里是在根据本次运行结果更新 routine 的内部状态。

从函数名看，它会把当前 routine 的 fingerprint 和运行结果写进 `routine.state`，用于后续判断 routine 配置是否变化、验证结果是否过期、运行是否健康等。

5. **如果是 cron routine，计算下一次触发时间**

```rust
let next_fire = if let Trigger::Cron { schedule, timezone } = routine.trigger {
    next_cron_fire(schedule, timezone.as_deref()).unwrap_or(None)
} else {
    None
};
```

只有 cron 类型需要 `next_fire_at`。

比如这次定时任务跑完了，要算下一次什么时候再跑。

如果不是 cron trigger，比如 message_event 或 system_event，就没有下一次固定时间，所以是 `None`。

6. **更新 routine 的 runtime 字段**

```rust
self.store.update_routine_runtime(
    routine.id,
    now,
    next_fire,
    routine.run_count + 1,
    new_failures,
    &routine.state,
)
```

这里更新的是 routine 本身的运行统计/运行态：

```text
last_run_at = now
next_fire_at = next_fire
run_count = old + 1
consecutive_failures = new_failures
state = routine.state
```

注意：前面第 1 步更新的是 **run 记录**；这里更新的是 **routine 总体状态**。

7. **如果这个 trigger 使用事件缓存，同步更新内存缓存**

```rust
if runtime_updated && trigger_uses_event_cache(&routine.trigger) {
    update_cached_event_runtime(...).await;
}
```

event/message/system event 类型的 routine 可能存在内存缓存里，用来快速匹配事件。

如果 DB 更新成功，也要把缓存里的运行统计同步一下，否则内存状态可能滞后。

8. **把结果写进 routine 的 conversation thread**

```rust
get_or_create_routine_conversation(...)
add_conversation_message(conv_id, "assistant", &msg)
```

它会拿到或创建这个 routine 对应的会话线程，然后写一条消息：

```text
[dispatched] completed: xxx
```

或者：

```text
[dispatched] failed: xxx
```

这样 routine 的执行历史可以在 conversation/thread 里看到。

9. **发送通知**

```rust
send_notification(...)
```

把执行结果通知给用户。

通知内容会带上：

- routine 的通知配置 `routine.notify`
- routine 所属用户 `routine.user_id`
- routine 名称
- 本次状态 `status`
- 总结 `summary`
- conversation thread id

前面 `agent_loop.rs` 里那个 `notify_rx` 后台任务会接住这个通知，再转发到具体 channel。

10. **不递减 `running_count`**

最后注释很关键：

```rust
// Note: we do NOT decrement running_count here.
```

正常流程中，`execute_routine()` 启动 full job 后，会有 `FullJobWatcher` 等 job 结束，然后由正常路径维护 `running_count`。

但这个函数主要用于 **崩溃恢复**：进程重启后，内存里的 `running_count` 已经是 0 了。
如果这里再 decrement，就会把计数减错。

所以这个函数只补 DB、routine runtime、thread、notification，不碰当前进程内的运行计数。

简化成一句话：

```text
complete_dispatched_run() 用来在发现派发出去的 routine job 已经结束时，
把 routine run 从 running 补成最终状态，更新 routine 统计，刷新缓存，
记录到会话线程，并通知用户；它不改 running_count，因为这是崩溃恢复路径。
```

    /// Check all due cron routines and fire them. Called by the cron ticker.
    pub async fn check_cron_triggers(&self) {
        let routines = match self.store.list_due_cron_routines().await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to load due cron routines: {}", e);
                return;
            }
        };

        for routine in routines {
            if self.running_count.load(Ordering::Relaxed) >= self.config.max_concurrent_routines {
                tracing::warn!("Global max concurrent routines reached, skipping remaining");
                break;
            }

            if !self.check_cooldown(&routine) {
                continue;
            }

            if !self.check_concurrent(&routine).await {
                continue;
            }

            let detail = if let Trigger::Cron { ref schedule, .. } = routine.trigger {
                Some(schedule.clone())
            } else {
                None
            };

            self.spawn_fire(routine, "cron", detail);//z真正启动
        }

    /// Spawn a fire in a background task.
    fn spawn_fire(
        &self,
        routine: Routine,
        trigger_type: &str,
        trigger_detail: Option<String>,
    ) -> JoinHandle<()> {
        let run = RoutineRun {
            id: Uuid::new_v4(),
            routine_id: routine.id,
            trigger_type: trigger_type.to_string(),
            trigger_detail,
            started_at: Utc::now(),
            completed_at: None,
            status: RunStatus::Running,
            result_summary: None,
            tokens_used: None,
            job_id: None,
            created_at: Utc::now(),
        };

        // Use per-user workspace so each routine executes in the correct
        // user's context. Fall back to the engine-wide workspace when the
        // routine belongs to the same user (avoids unnecessary allocation).
        let routine_workspace = if routine.user_id == self.workspace.user_id() {
            self.workspace.clone()
        } else {
            Arc::new(self.store.workspace_for_user(&routine.user_id))
        };

        let engine = EngineContext {//基本同引擎
            config: self.config.clone(),
            store: self.store.clone(),
            llm: self.llm.clone(),
            workspace: routine_workspace,
            notify_tx: self.notify_tx.clone(),
            running_count: self.running_count.clone(),
            scheduler: self.scheduler.clone(),
            extension_manager: self.extension_manager.clone(),
            tools: self.tools.clone(),
            safety: self.safety.clone(),
            sandbox_readiness: self.sandbox_readiness,
            event_cache: Arc::clone(&self.event_cache),
            http_interceptor: self.http_interceptor.clone(),
            runtime_policy: self.runtime_policy.clone(),
        };

        // Record the run in DB, then spawn execution
        let store = self.store.clone();
        tokio::spawn(async move {
            if let Err(e) = store.create_routine_run(&run).await {//创建一个运行记录
                tracing::error!(routine = %routine.name, "Failed to record run: {}", e);
                return;
            }
            execute_routine(engine, routine, run).await;//执行routine
        })
    }

/// Execute a routine run. Handles both lightweight and full_job modes.
async fn execute_routine(ctx: EngineContext, mut routine: Routine, run: RoutineRun) {
// Increment running count (atomic: survives panics in the execution below)
ctx.running_count.fetch_add(1, Ordering::Relaxed);

    // Retry constants for transient lightweight execution failures.
    //
    // NOTE: Multiplicative retry budgets — `ctx.llm` is wrapped in `RetryProvider`
    // which has its own retry budget (default 3). Although `LlmFailed` errors are
    // excluded from outer retry (only `EmptyResponse`/`TruncatedResponse` retry
    // here), be aware that each outer attempt triggers a full inner retry budget
    // for the LLM call itself. With MAX_RETRIES=2, worst case is 2 outer x 3 inner
    // = 6 LLM calls per routine run.
    const MAX_RETRIES: u32 = 2;
    const BASE_DELAY_MS: u64 = 1000;

    let is_lightweight = matches!(routine.action, RoutineAction::Lightweight { .. });

    // The retry block returns both the execution result and any accumulated
    // token count so that usage is preserved even on final failure.
    let (result, accumulated_tokens) = {
        let mut attempt = 0u32;
        // Track accumulated tokens as Option to preserve None semantics:
        // None = no attempt reported tokens; Some(n) = at least one attempt did.
        let mut accumulated_tokens: Option<i32> = None;
        let uses_tools = matches!(
            routine.action,
            RoutineAction::Lightweight {
                use_tools: true,
                ..
            }
        ) && ctx.config.lightweight_tools_enabled;

        /// Extract partial_tokens from any RoutineError variant that carries them.
        fn extract_partial_tokens(e: &RoutineError) -> Option<i32> {
            match e {
                RoutineError::LlmFailed {
                    partial_tokens: Some(t),
                    ..
                }
                | RoutineError::EmptyResponse {
                    partial_tokens: Some(t),
                }
                | RoutineError::TruncatedResponse {
                    partial_tokens: Some(t),
                } => Some(*t),
                _ => None,
            }
        }

        /// Merge an optional partial token count into the accumulator,
        /// only materializing Some when at least one source had Some.
        fn accumulate(acc: Option<i32>, partial: Option<i32>) -> Option<i32> {
            match (acc, partial) {
                (Some(a), Some(p)) => Some(a.saturating_add(p)),
                (Some(a), None) => Some(a),
                (None, p) => p,
            }
        }

        loop {
            let execution_result = match &routine.action {
                RoutineAction::Lightweight {
                    prompt,
                    context_paths,
                    max_tokens,
                    use_tools,
                    max_tool_rounds,
                } => {
                    execute_lightweight(///最关键选择执行类型，来自创建routine持久化的字段
                        &ctx,
                        &routine,
                        prompt,
                        context_paths,
                        *max_tokens,
                        *use_tools,
                        *max_tool_rounds,
                    )
                    .await
                }
                RoutineAction::FullJob {
                    title,
                    description,
                    max_iterations,
                } => {
                    let execution = FullJobExecutionConfig {
                        title,
                        description,
                        max_iterations: *max_iterations,
                    };
                    execute_full_job(&ctx, &routine, &run, &execution).await
                }
            };

            match execution_result {
                Ok((status, summary, tokens)) => {
                    // Merge tokens: only produce Some when at least one source had Some.
                    let total = accumulate(accumulated_tokens, tokens);
                    break (Ok((status, summary, total)), accumulated_tokens);
                }
                Err(ref e)
                    if is_lightweight
                        && !uses_tools
                        && e.is_retryable()
                        // Skip outer retry for LlmFailed — RetryProvider already
                        // retries transient LLM errors with its own budget. Retrying
                        // here would create a multiplicative retry count.
                        && !matches!(e, RoutineError::LlmFailed { .. })
                        && attempt < MAX_RETRIES =>
                {
                    // Accumulate partial tokens from the failed attempt.
                    accumulated_tokens = accumulate(accumulated_tokens, extract_partial_tokens(e));

                    attempt += 1;

                    let delay = Duration::from_millis(
                        BASE_DELAY_MS.saturating_mul(2u64.saturating_pow(attempt - 1)),
                    );
                    tracing::event!(target: "transient_routine_errors", tracing::Level::WARN, routine = %routine.name, attempt = attempt, max_retries = MAX_RETRIES, delay_ms = delay.as_millis() as u64, "Transient routine error, retrying: {}", e);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    // Accumulate tokens from the final failed attempt.
                    accumulated_tokens = accumulate(accumulated_tokens, extract_partial_tokens(&e));
                    break (Err(e), accumulated_tokens);
                }
            }
        }
    };

    // Decrement running count
    ctx.running_count.fetch_sub(1, Ordering::Relaxed);

    // Process result — on failure, preserve accumulated token total from
    // earlier retry attempts so usage reporting stays accurate.
    let (status, summary, tokens) = match result {
        Ok(execution) => execution,
        Err(e) => {
            tracing::error!(routine = %routine.name, "Execution failed: {}", e);
            (RunStatus::Failed, Some(e.to_string()), accumulated_tokens)
        }
    };

    // Complete the run record
    if let Err(e) = ctx
        .store
        .complete_routine_run(run.id, status, summary.as_deref(), tokens)
        .await
    {
        tracing::error!(routine = %routine.name, "Failed to complete run record: {}", e);
    }

    let now = Utc::now();
    routine.state = apply_routine_verification_result(
        &routine.state,
        routine_verification_fingerprint(&routine),
        status,
        now,
    );

    // Update routine runtime state
    let next_fire = if let Trigger::Cron {
        ref schedule,
        ref timezone,
    } = routine.trigger
    {
        next_cron_fire(schedule, timezone.as_deref()).unwrap_or(None)
    } else {
        None
    };

    let new_failures = if status == RunStatus::Failed {
        routine.consecutive_failures + 1
    } else {
        0
    };

    let runtime_updated = match ctx
        .store
        .update_routine_runtime(
            routine.id,
            now,
            next_fire,
            routine.run_count + 1,
            new_failures,
            &routine.state,
        )
        .await
    {
        Ok(()) => true,
        Err(e) => {
            tracing::error!(routine = %routine.name, "Failed to update runtime state: {}", e);
            false
        }
    };

    if runtime_updated && trigger_uses_event_cache(&routine.trigger) {
        update_cached_event_runtime(
            ctx.event_cache.as_ref(),
            routine.id,
            now,
            routine.run_count + 1,
            new_failures,
        )
        .await;
    }

    // Persist routine result to its dedicated conversation thread
    let thread_id = match ctx
        .store
        .get_or_create_routine_conversation(routine.id, &routine.name, &routine.user_id)
        .await
    {
        Ok(conv_id) => {
            tracing::debug!(
                routine = %routine.name,
                routine_id = %routine.id,
                conversation_id = %conv_id,
                "Resolved routine conversation thread"
            );
            // Record the run result as a conversation message
            let msg = match (&summary, status) {
                (Some(s), _) => format!("[{}] {}: {}", run.trigger_type, status, s),
                (None, _) => format!("[{}] {}", run.trigger_type, status),
            };
            if let Err(e) = ctx
                .store
                .add_conversation_message(conv_id, "assistant", &msg)
                .await
            {
                tracing::error!(routine = %routine.name, "Failed to persist routine message: {}", e);
            }
            Some(conv_id.to_string())
        }
        Err(e) => {
            tracing::error!(routine = %routine.name, "Failed to get routine conversation: {}", e);
            None
        }
    };

    // Send notifications based on config
    send_notification(
        &ctx.notify_tx,
        &routine.notify,
        &routine.user_id,
        &routine.name,
        status,
        summary.as_deref(),
        thread_id.as_deref(),
    )
    .await;
}

/// Execute a lightweight routine with optional tool support.
///
/// If tools are enabled, this runs a simplified agentic loop (max 3-5 iterations).
/// If tools are disabled, this does a single LLM call (original behavior).
async fn execute_lightweight(
ctx: &EngineContext,
routine: &Routine,
prompt: &str,
context_paths: &[String],
max_tokens: u32,
use_tools: bool,
max_tool_rounds: u32,
) -> Result<(RunStatus, Option<String>, Option<i32>), RoutineError> {
// Load context from workspace
let mut context_parts = Vec::new();
for path in context_paths {
match ctx.workspace.read(path).await {
Ok(doc) => {
context_parts.push(format!("## {}\n\n{}", path, doc.content));
}
Err(e) => {
tracing::debug!(
routine = %routine.name,
"Failed to read context path {}: {}", path, e
);
}
}
}

    // Load routine state from workspace (name sanitized to prevent path traversal)
    let safe_name = sanitize_routine_name(&routine.name);
    let state_path = format!("routines/{safe_name}/state.md");
    let state_content = match ctx.workspace.read(&state_path).await {
        Ok(doc) => Some(doc.content),
        Err(_) => None,
    };

    let full_prompt = build_lightweight_prompt(
        prompt,
        &context_parts,
        state_content.as_deref(),
        &routine.notify,
        use_tools,
    );

    // Get system prompt
    let system_prompt = match ctx.workspace.system_prompt().await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(routine = %routine.name, "Failed to get system prompt: {}", e);
            String::new()
        }
    };

    // Determine max_tokens from model metadata with fallback
    let effective_max_tokens = match ctx.llm.model_metadata().await {
        Ok(meta) => {
            let from_api = meta.context_length.map(|ctx| ctx / 2).unwrap_or(max_tokens);
            from_api.max(max_tokens)
        }
        Err(_) => max_tokens,
    };

    // If tools are enabled (both globally and per-routine), use the tool execution loop
    if use_tools && ctx.config.lightweight_tools_enabled {//关键
        execute_lightweight_with_tools(
            ctx,
            routine,
            &system_prompt,
            &full_prompt,
            effective_max_tokens,
            max_tool_rounds,
        )
        .await
    } else {
        execute_lightweight_no_tools(
            ctx,
            routine,
            &system_prompt,
            &full_prompt,
            effective_max_tokens,
        )
        .await
    }
}
**************************
/// Execute a lightweight routine with tool execution support (agentic loop).
///
/// This is a simplified version of the full dispatcher loop:
/// - Max 3-5 iterations (configurable)
/// - Sequential tool execution (not parallel)
/// - Uses the owner's live autonomous tool scope when lightweight tools are enabled
/// - Auto-approval of non-Always tools
/// - No hooks or approval dialogs
async fn execute_lightweight_with_tools(//本质自己实现了agenttic，没走委托
ctx: &EngineContext,
routine: &Routine,
system_prompt: &str,
full_prompt: &str,
effective_max_tokens: u32,
max_tool_rounds: u32,
) -> Result<(RunStatus, Option<String>, Option<i32>), RoutineError> {
let mut messages = if system_prompt.is_empty() {
vec![ChatMessage::user(full_prompt)]
} else {
vec![
ChatMessage::system(system_prompt),
ChatMessage::user(full_prompt),
]
};

    let max_iterations = max_tool_rounds
        .min(ctx.config.lightweight_max_iterations)
        .min(5);
    let mut iteration = 0;
    let mut total_input_tokens = 0;
    let mut total_output_tokens = 0;

    // Create a minimal job context for tool execution with unique run ID.
    // Carry the routine's notify config in metadata so the message tool can
    // resolve channel/target — mirrors the full-job path in execute_full_job().
    let run_id = Uuid::new_v4();
    let mut lw_metadata = serde_json::json!({
        "owner_id": routine.user_id
    });
    if let Some(channel) = &routine.notify.channel {
        lw_metadata["notify_channel"] = serde_json::json!(channel);
    }
    lw_metadata["notify_user"] = serde_json::json!(&routine.notify.user);
    let job_ctx = JobContext {
        job_id: run_id,
        user_id: routine.user_id.clone(),
        title: "Lightweight Routine".to_string(),
        description: routine.name.clone(),
        metadata: lw_metadata,
        // Inherit the global HTTP interceptor so routine-fired tool
        // dispatches honor the same `IRONCLAW_TEST_HTTP_REMAP` /
        // recording / replay layer as chat-fired tools. Without
        // this, http tool calls from a Lightweight action reach the
        // real network even when the rest of the system is wired
        // through mocks. See `EngineContext::http_interceptor` for
        // the upstream plumbing.
        http_interceptor: ctx.http_interceptor.clone(),
        ..Default::default()
    };
    let allowed_tools =
        autonomous_allowed_tool_names(&ctx.tools, ctx.extension_manager.as_ref(), &routine.user_id)
            .await;

    loop {
        iteration += 1;

        // Force text-only response at iteration limit
        let force_text = iteration >= max_iterations;

        if force_text {
            // Final iteration: no tools, just get text response.
            // Claude 4.6 rejects assistant prefill; NEAR AI rejects any non-user-ending
            // conversation. Ensure the last message is user-role.
            crate::util::ensure_ends_with_user_message(&mut messages);
            let request = CompletionRequest::new(messages)
                .with_max_tokens(effective_max_tokens)
                .with_temperature(0.3);

            let response = ctx.llm.complete(request).await.map_err(|e| {
                let retryable = ironclaw_llm::retry::is_retryable(&e);
                RoutineError::LlmFailed {
                    reason: e.to_string(),
                    partial_tokens: tokens_to_option(total_input_tokens, total_output_tokens),
                    retryable,
                }
            })?;

            total_input_tokens += response.input_tokens;
            total_output_tokens += response.output_tokens;

            return handle_text_response(
                &response.content,
                response.finish_reason,
                total_input_tokens,
                total_output_tokens,
            );
        } else {//执行
            // Tool-enabled iteration. Use the policy-filtered variant
            // when configured so routine-driven LLM iterations see the
            // same model-facing tool surface as the dispatcher
            // (#3243 HIGH iteration-2 gap).
            let tool_defs = match &ctx.runtime_policy {
                Some(policy) => ctx.tools.tool_definitions_visible_under(policy).await,
                None => ctx.tools.tool_definitions().await,
            }
            .into_iter()
            .filter(|tool| allowed_tools.contains(&tool.name))
            .collect();

            let request_messages = snapshot_messages_for_tool_iteration(&messages);
            let request = ToolCompletionRequest::new(request_messages, tool_defs)
                .with_max_tokens(effective_max_tokens)
                .with_temperature(0.3);

            let response = ctx.llm.complete_with_tools(request).await.map_err(|e| {
                let retryable = ironclaw_llm::retry::is_retryable(&e);
                RoutineError::LlmFailed {
                    reason: e.to_string(),
                    partial_tokens: tokens_to_option(total_input_tokens, total_output_tokens),
                    retryable,
                }
            })?;

            total_input_tokens += response.input_tokens;
            total_output_tokens += response.output_tokens;

            // Check if LLM returned text (no tool calls)
            if response.tool_calls.is_empty() {
                let content = response.content.unwrap_or_default();
                return handle_text_response(
                    &content,
                    response.finish_reason,
                    total_input_tokens,
                    total_output_tokens,
                );
            }

            // LLM returned tool calls: add assistant message and execute tools.
            // Carry reasoning so the next request can echo it — required for
            // DeepSeek thinking-mode and Gemini 2.5+ to validate the chain
            // (#3201, #3225).
            messages.push(
                ChatMessage::assistant_with_tool_calls(
                    response.content.clone(),
                    response.tool_calls.clone(),
                )
                .with_reasoning(response.reasoning.clone()),
            );

            // Execute tools sequentially
            for tc in response.tool_calls {
                let result = execute_routine_tool(ctx, &job_ctx, &allowed_tools, &tc).await;

                // Sanitize and wrap result (including errors)
                let result_content = match result {
                    Ok(output) => {
                        let sanitized = ctx.safety.sanitize_tool_output(&tc.name, &output);
                        ctx.safety.wrap_for_llm(&tc.name, &sanitized.content)
                    }
                    Err(e) => {
                        let error_msg = format!("Tool '{}' failed: {}", tc.name, e);
                        let sanitized = ctx.safety.sanitize_tool_output(&tc.name, &error_msg);
                        ctx.safety.wrap_for_llm(&tc.name, &sanitized.content)
                    }
                };

                // Truncate oversized tool output to prevent unbounded context growth.
                // Routine tool loops are lightweight and should not accumulate
                // large payloads across iterations.
                const MAX_TOOL_OUTPUT_CHARS: usize = 8192;
                let result_content = if result_content.len() > MAX_TOOL_OUTPUT_CHARS {
                    let truncated = &result_content
                        [..result_content.floor_char_boundary(MAX_TOOL_OUTPUT_CHARS)];
                    format!("{truncated}\n... [output truncated to {MAX_TOOL_OUTPUT_CHARS} chars]")
                } else {
                    result_content
                };

                // Add tool result to context
                messages.push(ChatMessage::tool_result(&tc.id, &tc.name, &result_content));
            }

            // Continue loop to next LLM call
        }
    }
}

async fn execute_full_job(
ctx: &EngineContext,
routine: &Routine,
run: &RoutineRun,
execution: &FullJobExecutionConfig<'_>,
) -> Result<(RunStatus, Option<String>, Option<i32>), RoutineError> {
// Full-job routines dispatch through the scheduler (same as /job
// commands) — no Docker sandbox required when sandbox is disabled.
// However, if sandbox is *enabled* but Docker is unavailable, that's
// a misconfiguration we should surface.
if matches!(ctx.sandbox_readiness, SandboxReadiness::DockerUnavailable) {
return Err(RoutineError::JobDispatchFailed {
reason: "Sandbox is enabled but Docker is not available. \
Install Docker or set SANDBOX_ENABLED=false."
.to_string(),
});
}

    let scheduler = ctx
        .scheduler
        .as_ref()
        .ok_or_else(|| RoutineError::JobDispatchFailed {
            reason: "scheduler not available".to_string(),
        })?;

    let mut metadata = serde_json::json!({
        "max_iterations": execution.max_iterations,
        "owner_id": routine.user_id
    });
    // Carry the routine's notify config in job metadata so the message tool
    // can resolve channel/target per-job without global state mutation.
    if let Some(channel) = &routine.notify.channel {
        metadata["notify_channel"] = serde_json::json!(channel);
    }
    metadata["notify_user"] = serde_json::json!(&routine.notify.user);

    // Prepend execution context so the LLM knows it's already inside a
    // routine and should execute the task directly — not set up infrastructure.
    let contextualized_description = format!(
        "IMPORTANT: You are executing inside routine \"{routine_name}\". \
         The routine and its schedule are already configured. \
         Tools and credentials are already set up. \
         Do NOT create routines, jobs, or try to discover/install/authenticate tools. \
         Execute the task directly.\n\n{desc}",
        routine_name = routine.name,
        desc = execution.description,
    );

    let job_id = scheduler//最终是走的这个创建子任务去执行
        .dispatch_job(
            &routine.user_id,
            execution.title,
            &contextualized_description,
            Some(metadata),
        )
        .await
        .map_err(|e| RoutineError::JobDispatchFailed {
            reason: format!("failed to dispatch job: {e}"),
        })?;

    // Link the routine run to the dispatched job.
    // This MUST succeed — if it fails, sync_dispatched_runs() will never find
    // this run (it filters on job_id IS NOT NULL), leaving it stuck as 'running'
    // with running_count permanently elevated.
    ctx.store
        .link_routine_run_to_job(run.id, job_id)
        .await
        .map_err(|e| RoutineError::Database {
            reason: format!("failed to link run to job: {e}"),
        })?;

    tracing::info!(
        routine = %routine.name,
        job_id = %job_id,
        max_iterations = execution.max_iterations,
        "Dispatched full job for routine, watching for completion"
    );

    // Watch the job until it finishes — keeps the routine run active
    // so concurrency guardrails (running_count, routine_runs status)
    // remain enforced for the full job lifetime.
    let watcher = FullJobWatcher::new(ctx.store.clone(), job_id, routine.name.clone());
    let (status, summary) = watcher.wait_for_completion().await;
    Ok((status, summary, None))
}

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

3. RoutineRun

/// A single execution of a routine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineRun {
pub id: Uuid,
pub routine_id: Uuid,
pub trigger_type: String,
pub trigger_detail: Option<String>,
pub started_at: DateTime<Utc>,
pub completed_at: Option<DateTime<Utc>>,
pub status: RunStatus,
pub result_summary: Option<String>,
pub tokens_used: Option<i32>,
pub job_id: Option<Uuid>,
pub created_at: DateTime<Utc>,
}
一次单个运行的记录

4. FullJobWatcher

`FullJobWatcher` 是一个 **routine 派发 full_job 后，用来盯着这个 job 直到结束的小型 watcher**。

注释里说得比较准确：

```rust
/// Watches a dispatched full_job until the linked scheduler job completes.
```

也就是：

```text
routine 触发后
  -> 创建/派发一个 scheduler job
  -> job 在后台跑
  -> FullJobWatcher 轮询这个 job 的状态
  -> job 结束后，把 job 结果映射成 routine run 的结果
```

它有三个字段：

```rust
struct FullJobWatcher {
    store: SystemScope,
    job_id: Uuid,
    routine_name: String,
}
```

分别是：

- `store`：用来查 DB 里的 job 状态，比如 `store.get_job(job_id)`。
- `job_id`：它要盯的那个 scheduler job。
- `routine_name`：主要用于日志，方便知道是哪个 routine 派发的 job。

它的职责不是执行 job，而是 **等待 job 结束并判断最终状态**。

大概逻辑可以理解成：

```rust
loop {
    let job = store.get_job(job_id).await?;

    match job.state {
        Pending | InProgress | Stuck => {
            sleep(interval).await;
            continue;
        }
        Completed => return RunStatus::Completed,
        Failed => return RunStatus::Failed,
        Canceled => return RunStatus::Canceled,
        ...
    }
}
```

为什么需要它？

因为 routine 的一次执行记录是 `RoutineRun`，而真正干活的是另一个系统里的 `Job`。

所以中间需要一个桥：

```text
scheduler job 结束
        ↓
FullJobWatcher 发现 job 已结束
        ↓
routine engine 把 RoutineRun 标记为 completed/failed
        ↓
更新 routine 统计、写会话、发通知
```

它和前面那个 `complete_dispatched_run()` 的关系是：

- **正常流程**：`FullJobWatcher` 在当前进程里一直等 job 结束，然后走 routine 的正常收尾逻辑。
- **崩溃恢复流程**：如果进程中途挂了，`FullJobWatcher` 没了；重启后由 `sync_dispatched_runs()` 扫描遗留的 `running` run，再调用类似 `complete_dispatched_run()` 的逻辑补账。

所以一句话：

```text
FullJobWatcher 是 routine full_job 执行路径里的“job 状态观察器”，负责轮询 scheduler job，等它完成后把 job 的最终状态转成 routine run 的最终状态。
```

因为 `Lightweight` 没有“外部 job 生命周期”要盯。

两种 routine action 的差别在 [src/agent/routine.rs](E:/codes/Rust/projects/ironclaw/src/agent/routine.rs:291) 里：

```rust
RoutineAction::Lightweight { ... }
RoutineAction::FullJob { ... }
```

核心区别是：

```text
Lightweight:
  routine task 自己直接执行 LLM 调用/简单工具循环
  执行完就直接返回结果
  execute_routine() 立刻完成 run、更新 runtime、发通知

FullJob:
  routine task 只是把任务派发给 Scheduler
  真正执行发生在另一个 scheduler job 里
  routine run 必须等那个 job 结束后才能收尾
  所以需要 FullJobWatcher 轮询 job 状态
```

`Lightweight` 的路径大概是：

```text
spawn_fire()
  -> tokio::spawn
    -> execute_routine()
      -> execute_lightweight()
        -> execute_lightweight_no_tools()
           或 execute_lightweight_with_tools()
      -> 得到结果
      -> complete_routine_run()
      -> update_routine_runtime()
      -> add_conversation_message()
      -> send_notification()
```

这里所有事情都在 `execute_routine()` 这条异步任务里完成。它调用 LLM 或轻量工具循环，`await` 返回后就知道结果是 `Ok` 还是 `Failed`，所以不需要额外 watcher。

`FullJob` 的路径是：

```text
spawn_fire()
  -> tokio::spawn
    -> execute_routine()
      -> execute_full_job()
        -> scheduler.dispatch_job()
        -> link_routine_run_to_job()
        -> FullJobWatcher.wait_for_completion()
          -> 反复 store.get_job(job_id)
      -> job 结束后得到结果
      -> complete_routine_run()
      -> update_routine_runtime()
      -> add_conversation_message()
      -> send_notification()
```

这里 `scheduler.dispatch_job()` 返回后，并不代表任务完成，只代表 job 被创建/派发了。真正的 job 可能跑很久，状态在 DB 里的 `jobs` 表里变化：

```text
Pending -> InProgress/Stuck -> Completed/Failed/Cancelled/...
```

所以 `FullJobWatcher` 的作用就是把这个“外部 scheduler job 的生命周期”接回 routine：

```text
scheduler job 完成
  -> watcher 发现
  -> 映射成 RunStatus
  -> execute_routine() 才能继续收尾 routine run
```

为什么 `Lightweight` 不需要？

因为它不是这种结构：

```text
routine run -> linked job_id -> scheduler job
```

而是：

```text
routine run -> 当前 routine task 直接执行 -> 直接得到结果
```

所以它没有 `job_id`，也没有需要轮询的 `JobState`。

另外还有一个重要影响：`FullJobWatcher` 会让 `RoutineRun` 在 full job 整个生命周期里保持 `running`，这样并发限制才准确。比如一个 full job 跑 30 分钟，这 30 分钟里这个 routine run 仍然算正在运行，防止重复触发太多次。

`Lightweight` 通常是单次 LLM 调用，或者最多几轮轻量工具调用，执行窗口就在当前 task 内，`running_count` 和 DB run 状态由 `execute_routine()` 自己维护就够了。

