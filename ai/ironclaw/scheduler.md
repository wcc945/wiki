一. 相关结构


//真正是不是走容器是由creat_job工具决定的。
CreateJobTool 命中容器 + Worker 模式时完全不走 Scheduler——execute_sandbox 直接调 ContainerJobManager::create_job（job.rs:496），后者走 docker.create_container + start_container 起容器，容器内 worker
binary 跑 WorkerRuntime::run → ContainerDelegate，全程在独立的容器 job 生命周期里。host Scheduler（Scheduler.dispatch_job → Worker::run → JobDelegate）和容器 ContainerJobManager 是两个并列体系，只在
ContextManager.register_sandbox_job 这一处把容器 job 登记到统一查询层（list_jobs / job_status / job_events / cancel_job 工具跨系统查）。

Scheduler（host）和 ContainerJobManager（容器）确实是完全独立的两条 job 管理体系——Scheduler.jobs: HashMap<Uuid, ScheduledJob>（JoinHandle + mpsc::Sender）和 ContainerJobManager.containers:           
HashMap<Uuid, ContainerHandle>（container_id + ContainerState）互不可见，各有自己的限流/取消/事件流/DB 持久化。唯一汇合点是 ContextManager.register_sandbox_job——CreateJobTool::execute_sandbox 把容器
job 也登记到 ContextManager，让 list_jobs / job_status / job_events / cancel_job 这套查询工具能跨两套体系读，CancelJobTool 还会两边都停。
**************************
触发路径（已对照代码确认）

全部 5 个真正调用 Scheduler.dispatch_job 的位置

┌─────┬───────────────────────────────────┬─────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────┐
│  #  │              触发方               │               入口文件:行               │                                        最终调用的 API                                        │
├─────┼───────────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ 1   │ 用户 /job ... 命令                │ commands.rs:99-102 → handle_create_job  │ scheduler.dispatch_job(user_id, title, description, None)                                    │
├─────┼───────────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ 2   │ RoutineAction::FullJob 触发       │ routine_engine.rs:1505-1515             │ scheduler.dispatch_job(&routine.user_id, title, &contextualized_description, Some(metadata)) │
├─────┼───────────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ 3   │ Chat 对话中 agent 调起            │ tools/builtin/job.rs:365                │ dispatch_job(&ctx.user_id, title, description, None)    creat_job  非容器     │
├─────┼───────────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ 4   │ 容器路径 job 派发                 │ scheduler.rs:142 同一入口被容器委托复用 │ （不是单独调用，是 Worker/JobDelegate 入口被复用）                                           │
├─────┼───────────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ 5   │ RoutineEngine + tools/builtin/job │ 同 #2 / #3                              │ —                                                                                            │
└─────┴───────────────────────────────────┴─────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘

▎ 我之前说的"claude_bridge 容器自己 dispatch"在代码里不存在——claude_bridge.rs 全文件无 scheduler/dispatch 引用，容器是被 dispatch 进来跑，不是反过来。

5 个路径的完整执行流

路径 1: 用户 /job
用户消息 → SubmissionParser 解析 /job <rest>
→ Router::route_command → MessageIntent::CreateJob { title: rest, description: rest, category: None }
→ Router::route → commands.rs::handle_create_job
→ scheduler.dispatch_job(user_id, title, description, None)
→ 写 category 字段
→ 返回 "Created job: <title>\nID: <job_id>"

路径 2: RoutineEngine FullJob
RoutineEngine 主循环 (routine_engine.rs:2211-2213)
→ check_cron_triggers() / event matcher
→ execute_routine() → 走 FullJob 分支
→ 沙箱检查 (DockerUnavailable 时 fail-closed)
→ ctx.scheduler.dispatch_job(&routine.user_id, title, &contextualized_description, Some(metadata))
→ link_routine_run_to_job(run.id, job_id)  // 关键：必须 link，否则 sync_dispatched_runs 找不到
→ FullJobWatcher::wait_for_completion()  // 阻塞 routine run 直到 job 终态

路径 3: Chat 对话中发起 job（最关键的新发现）
ChatDelegate 处理用户消息 → run_agentic_loop
→ LLM 决定调 "job" 内置工具
→ tools/builtin/job.rs::execute()  (line 365 附近)
→ ctx.scheduler.dispatch_job(&ctx.user_id, title, description, None)
→ 返回 job_id 给 LLM
→ LLM 继续生成回复
(这是 ChatDelegate 不直接调 Scheduler 的"特例路径"——通过 builtin tool 间接调)

路径 4 & 5: 派发后的事
dispatch_job_inner (scheduler.rs:183-252)
→ context_manager.create_job_for_user (建 job + ctx)
→ 原子更新 metadata / max_tokens / approval_context (Issue #807/#813)
→ store.save_job (DB 持久化)
→ schedule_with_context → 选 JobDelegate / ContainerDelegate → spawn Worker

1. Scheduler

/// Schedules and manages parallel job execution.
pub struct Scheduler {
config: AgentConfig,
context_manager: Arc<ContextManager>,
llm: Arc<dyn LlmProvider>,
safety: Arc<SafetyLayer>,
tools: Arc<ToolRegistry>,
extension_manager: Option<Arc<ExtensionManager>>,
store: Option<SystemScope>,
hooks: Arc<HookRegistry>,
/// SSE manager for live job event streaming.
sse_tx: Option<Arc<crate::channels::web::sse::SseManager>>,
/// HTTP interceptor for trace recording/replay (propagated to workers).
http_interceptor: Option<Arc<dyn ironclaw_llm::recording::HttpInterceptor>>,
/// Resolved runtime policy propagated to per-job workers so the
/// model-facing tool list filter applies to background jobs too.
/// `None` in tests / before `Config::with_runtime_overrides` runs.
runtime_policy: Option<ironclaw_host_api::runtime_policy::EffectiveRuntimePolicy>,
/// Running jobs (main LLM-driven jobs).
jobs: Arc<RwLock<HashMap<Uuid, ScheduledJob>>>,
/// Running sub-tasks (tool executions, background tasks).
subtasks: Arc<RwLock<HashMap<Uuid, ScheduledSubtask>>>,
}

    /// Create, persist, and schedule a job in one shot.
    ///
    /// This is the preferred entry point for dispatching new jobs. It:
    /// 1. Creates the job context via `ContextManager`
    /// 2. Optionally applies metadata (e.g. `max_iterations`)
    /// 3. Persists the job to the database (so FK references from
    ///    `job_actions` / `llm_calls` work immediately)
    /// 4. Schedules the job for worker execution
    ///
    /// Returns the new job ID.
    pub async fn dispatch_job(
        &self,
        user_id: &str,
        title: &str,
        description: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<Uuid, JobError> {
        let approval_context = self.autonomous_approval_context(user_id).await;
        self.dispatch_job_inner(//调用内部
            user_id,
            title,
            description,
            metadata,
            Some(approval_context),
        )
        .await
    }

    /// Shared implementation for `dispatch_job` and `dispatch_job_with_context`.
    async fn dispatch_job_inner(
        &self,
        user_id: &str,
        title: &str,
        description: &str,
        metadata: Option<serde_json::Value>,
        approval_context: Option<ApprovalContext>,
    ) -> Result<Uuid, JobError> {
        let job_id = self
            .context_manager
            .create_job_for_user(user_id, title, description)
            .await?;

......

        // Persist to DB before scheduling so the worker's FK references are valid.
        // The context was read under the same lock as the update (atomic), preventing
        // concurrent worker interference (Issue #807: non-transactional context updates).
        if let Some(ref store) = self.store {
            store.save_job(&ctx).await.map_err(|e| JobError::Failed {
                id: job_id,
                reason: format!("failed to persist job: {e}"),
            })?;
        }

        self.schedule_with_context(job_id, approval_context).await?;
        Ok(job_id)
    }

    /// Schedule a job with an optional approval context.
    async fn schedule_with_context(
        &self,
        job_id: Uuid,
        approval_context: Option<ApprovalContext>,
    ) -> Result<(), JobError> {
                  // Transition job to in_progress
            self.context_manager
                .update_context(job_id, |ctx| {
                    ctx.transition_to(
                        JobState::InProgress,
                        Some("Scheduled for execution".to_string()),
                    )
                })
            // Create worker channel
            let (tx, rx) = mpsc::channel(16);
            // Create worker with shared dependencies
            let deps = WorkerDeps {
                context_manager: self.context_manager.clone(),
                llm: self.llm.clone(),
                safety: self.safety.clone(),
                tools: self.tools.clone(),
                store: self.store.clone(),
                hooks: self.hooks.clone(),
                timeout: self.config.job_timeout,
                use_planning: self.config.use_planning,
                sse_tx: self.sse_tx.clone(),
                approval_context,
                http_interceptor: self.http_interceptor.clone(),
                multi_tenant: self.config.multi_tenant,
                runtime_policy: self.runtime_policy.clone(),
            };
            let worker = Worker::new(job_id, deps);//启一个worker

            // Spawn worker task
            let handle = tokio::spawn(async move {
                if let Err(e) = worker.run(rx).await {//注意启动通知rx收到
                    tracing::error!("Worker for job {} failed: {}", job_id, e);
                }
            });

            // Start the worker
            if tx.send(WorkerMessage::Start).await.is_err() {
                tracing::error!(job_id = %job_id, "Worker died before receiving Start message");
            }

            // Insert while still holding the write lock
            jobs.insert(job_id, ScheduledJob { handle, tx });//记录作业状态


*********************
                // Cleanup task for this job to avoid capacity leaks
        let jobs = Arc::clone(&self.jobs);
        tokio::spawn(async move {//完成了则清理
            loop {
                let finished = {
                    let jobs_read = jobs.read().await;
                    match jobs_read.get(&job_id) {
                        Some(scheduled) => scheduled.handle.is_finished(),
                        None => true,
                    }
                };

                if finished {
                    jobs.write().await.remove(&job_id);
                    break;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });


2. ScheduledJob

/// Status of a scheduled job.
#[derive(Debug)]
pub struct ScheduledJob {
pub handle: JoinHandle<()>,
pub tx: mpsc::Sender<WorkerMessage>,
}

```
                ┌──────────┐
                │ Pending  │
                └────┬─────┘
       启动执行  ┌────┼────┐  无法启动 / 取消
       ┌────────┘    │    └─────────┐
       ▼             │              ▼
  ┌──────────┐   用户取消      ┌─────────┐
  │InProgress│ ───────────────▶│Failed   │ (终)
  └────┬─────┘                  └─────────┘
       │ 异常失败                  ▲
       ├──────────────────────────┘
       │
       │ 工作完成
       ▼
  ┌──────────┐  提交审核  ┌──────────┐  审核通过  ┌──────────┐
  │Completed │ ─────────▶ │Submitted │ ─────────▶│Accepted  │ (终)
  └────┬─────┘            └────┬─────┘            └──────────┘
       │ 提交前失败            │ 审核驳回
       └──────────────────────▶│
                              ▼
                         ┌─────────┐
                         │ Failed  │ (终)
                         └─────────┘

  ┌──────────┐
  │  Stuck   │ ◀── 修复器判定卡死  (InProgress → Stuck)
  └────┬─────┘
       │ self_repair 恢复 (repair_attempts++)
       ▼
  ┌──────────┐
  │InProgress│  (循环)
  └──────────┘

  InProgress / Stuck / Pending ──用户取消──▶ Cancelled (终)
```
▎ 仓库里没有"审核人"代码。Submitted → Accepted 只是状态机里给 NEAR AI marketplace 外部撮合方预留的一条边；当前 worker 自己走到 Completed 就停了，由本地 RuleBasedEvaluator 决定是否投递到              
▎ Submitted，Accepted 等外部回包——而那段回包逻辑目前并未实现。

3. worker

/// Worker that executes a single job.
pub struct Worker {
job_id: Uuid,
deps: WorkerDeps,
}

    /// Run the worker until the job is complete or stopped.
    pub async fn run(self, mut rx: mpsc::Receiver<WorkerMessage>) -> Result<(), Error> {

        // Wait for start signal
        match rx.recv().await {
            Some(WorkerMessage::Start) => {}//收到开始信号后就开始
            Some(WorkerMessage::Stop) | None => {
                tracing::debug!("Worker for job {} stopped before starting", self.job_id);
                return Ok(());
            }
            Some(WorkerMessage::Ping) | Some(WorkerMessage::UserMessage(_)) => {}
        }

        // Get job context
        let job_ctx = self.context_manager().get_context(self.job_id).await?;

        // Create reasoning engine
        let reasoning =
            Reasoning::new(self.llm().clone()).with_model_name(self.llm().active_model_name());//推理引擎

        // Build initial reasoning context (tool definitions refreshed each iteration in execution_loop)
        let mut reason_ctx = ReasoningContext::new().with_job(&job_ctx.description);

        // Add system message
        reason_ctx.messages.push(ChatMessage::system(format!(
            r#"You are an autonomous agent working on a job.

Job: {}
Description: {}

You have access to tools to complete this job. Plan your approach and execute tools as needed.
You may request multiple tools at once if they can be executed in parallel.
Report when the job is complete or if you encounter issues you cannot resolve."#,
job_ctx.title, job_ctx.description
)));

        // Main execution loop with timeout
        let result = tokio::time::timeout(self.timeout(), async {
            self.execution_loop(&mut rx, &reasoning, &mut reason_ctx)//真正执行
                .await
        })
        .await;

        match result {
            Ok(Ok(())) => {
                tracing::info!("Worker for job {} completed successfully", self.job_id);
                // Only mark completed if still in an active, non-stuck state.
                let current_state = self
                    .context_manager()
                    .get_context(self.job_id)
                    .await
                    .map(|ctx| ctx.state);
                match current_state {
                    Ok(state) if state.is_terminal() => {}
                    Ok(JobState::Completed) => {}
                    Ok(JobState::Stuck) => {
                        tracing::info!(
                            "Job {} returned Ok but is Stuck — leaving for self-repair",
                            self.job_id
                        );
                    }
                    Ok(_) => {
                        self.mark_completed().await?;
                    }
                    Err(e) => {
                        tracing::warn!(
                            job_id = %self.job_id,
                            "Failed to get job context, cannot mark as completed: {}", e
                        );
                    }
                }
            }
            Ok(Err(e)) => {
                tracing::error!("Worker for job {} failed: {}", self.job_id, e);
                self.mark_failed(&e.to_string()).await?;
            }
            Err(_) => {
                tracing::warn!("Worker for job {} timed out", self.job_id);
                self.mark_stuck("Execution timeout").await?;//这些里面都会通过sse，通知回前端
            }
        }

        Ok(())
    }

    async fn execution_loop(
        &self,
        rx: &mut mpsc::Receiver<WorkerMessage>,
        reasoning: &Reasoning,
        reason_ctx: &mut ReasoningContext,
    ) -> Result<(), Error> {
        let max_iterations = self
            .context_manager()
            .get_context(self.job_id)
            .await
            .ok()
            .and_then(|ctx| ctx.metadata.get("max_iterations").and_then(|v| v.as_u64()))
            .unwrap_or(50) as usize;
        let max_iterations = max_iterations.min(ironclaw_common::MAX_WORKER_ITERATIONS as usize);

        // Initial tool definitions for planning (will be refreshed in loop).
        // Use the policy-filtered variant when a runtime policy is configured
        // so background-job workers see the same model-facing tool surface
        // the dispatcher does — closing the iteration-2 gap (#3243 HIGH).
        reason_ctx.available_tools = match &self.deps.runtime_policy {
            Some(policy) => self.tools().tool_definitions_visible_under(policy).await,
            None => self.tools().tool_definitions().await,
        };

        // Generate plan if planning is enabled
        let plan = if self.use_planning() {
            match reasoning.plan(reason_ctx).await {
                Ok(p) => {
                    tracing::info!(
                        "Created plan for job {}: {} actions, {:.0}% confidence",
                        self.job_id,
                        p.actions.len(),
                        p.confidence * 100.0
                    );

                    // Add plan to context as assistant message
                    reason_ctx.messages.push(ChatMessage::assistant(format!(
                        "I've created a plan to accomplish this goal: {}\n\nSteps:\n{}",
                        p.goal,
                        p.actions
                            .iter()
                            .enumerate()
                            .map(|(i, a)| format!("{}. {} - {}", i + 1, a.tool_name, a.reasoning))
                            .collect::<Vec<_>>()
                            .join("\n")
                    )));

                    self.log_event("message", serde_json::json!({//将生成好的计划sse->ui
                        "role": "assistant",
                        "content": format!("Plan: {}\n\n{}", p.goal,
                            p.actions.iter().enumerate()
                                .map(|(i, a)| format!("{}. {} - {}", i + 1, a.tool_name, a.reasoning))
                                .collect::<Vec<_>>().join("\n"))
                    }));

                    Some(p)
                }
                Err(e) => {
                    tracing::warn!(
                        "Planning failed for job {}, falling back to direct selection: {}",
                        self.job_id,
                        e
                    );
                    None
                }
            }
        } else {
            None
        };

        // If we have a plan, execute it.
        if let Some(ref plan) = plan {
            self.execute_plan(rx, reasoning, reason_ctx, plan).await?;//真正执行计划

            if let Ok(ctx) = self.context_manager().get_context(self.job_id).await
                && (ctx.state.is_terminal()
                    || ctx.state == JobState::Stuck
                    || ctx.state == JobState::Completed)
            {
                return Ok(());//结束
            }
        }//执行完计划后，不一定job达到完成状态，继续走下面

        // Build the delegate and run the shared agentic loop 没有计划走agentLoop ReAct
        let delegate = JobDelegate {
            worker: self,
            rx: tokio::sync::Mutex::new(rx),
            consecutive_rate_limits: std::sync::atomic::AtomicUsize::new(0),
            recovery_state: tokio::sync::Mutex::new(AutonomousRecoveryState::default()),
            has_text_response: std::sync::atomic::AtomicBool::new(false),
            cached_user_info: tokio::sync::OnceCell::new(),
            cached_admin_tool_policy: tokio::sync::OnceCell::new(),
        };

        let config = AgenticLoopConfig {
            max_iterations,
            enable_tool_intent_nudge: true,
            max_tool_intent_nudges: 2,
        };

        let outcome = run_agentic_loop(&delegate, reasoning, reason_ctx, &config).await?;//执行计划完还没执行，委托给agentic_loop执行

        match outcome {
            LoopOutcome::Response(_) => {
                // Completion was already handled in handle_text_response via mark_completed
            }
            LoopOutcome::MaxIterations => {
                self.mark_failed("Maximum iterations exceeded: job hit the iteration cap")
                    .await?;
            }
            LoopOutcome::Failure(reason) => {
                self.mark_failed(&reason).await?;
            }
            LoopOutcome::Stopped => {
                // Stop signal handled — nothing more to do
            }
            LoopOutcome::NeedApproval(_) | LoopOutcome::AuthPending(_) => {}
        }

        Ok(())
    }

▎ execute_plan 的循环体只跑工具、不调 LLM；LLM 调用集中在两端——入参的 ActionPlan（已生成好的）和出参的"完成了吗"判定（reasoning.respond()                                                              
▎ 一次）。计划内每一步的决策权都来自上游一次性规划，本函数只负责执行。

/// Execute a pre-generated plan.
async fn execute_plan(
&self,
rx: &mut mpsc::Receiver<WorkerMessage>,
reasoning: &Reasoning,
reason_ctx: &mut ReasoningContext,
plan: &ActionPlan,
) -> Result<(), Error> {
for (i, action) in plan.actions.iter().enumerate() {//一共多轮
// Check for stop signal and injected user messages
while let Ok(msg) = rx.try_recv() {//每轮都会尝试接受用户新指令
match msg {
WorkerMessage::Stop => {
tracing::debug!(
"Worker for job {} received stop signal during plan execution",
self.job_id
);
return Ok(());
}
WorkerMessage::Ping => {
tracing::trace!("Worker for job {} received ping", self.job_id);
}
WorkerMessage::Start => {}
WorkerMessage::UserMessage(content) => {
tracing::info!(
job_id = %self.job_id,
"User message received during plan execution, abandoning plan"
);
reason_ctx.messages.push(ChatMessage::user(&content));//收到消息后终止计划执行
self.log_event(//通知ui
"message",
serde_json::json!({
"role": "user",
"content": content,
}),
);
self.log_event(
"status",
serde_json::json!({
"message": "Plan interrupted by user message, re-evaluating...",
}),
);
return Ok(());
}
}
}

            tracing::debug!(
                "Job {} executing planned action {}/{}: {} - {}",
                self.job_id,
                i + 1,
                plan.actions.len(),
                action.tool_name,
                action.reasoning
            );

            let selection = ToolSelection {//计划中选好的tool
                tool_name: action.tool_name.clone(),
                parameters: action.parameters.clone(),
                reasoning: action.reasoning.clone(),
                alternatives: vec![],
                tool_call_id: format!("plan_{}_{}", self.job_id, i),
            };

            reason_ctx
                .messages
                .push(ChatMessage::assistant_with_tool_calls(//附加工具信息到本次推理上下文
                    None,
                    vec![ToolCall {
                        id: selection.tool_call_id.clone(),
                        name: selection.tool_name.clone(),
                        arguments: selection.parameters.clone(),
                        reasoning: if action.reasoning.is_empty() {
                            None
                        } else {
                            Some(action.reasoning.clone())
                        },
                        signature: None,
                        arguments_parse_error: None,
                    }],
                ));

            let result = self
                .execute_tool(&action.tool_name, &action.parameters)//仅执行工具，无llm调用
                .await;

            self.process_tool_result_job(reason_ctx, &selection, result)//处理tool执行结果，通知前端，塞到推理上下文
                .await?;

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Plan completed — ask the LLM whether the job is done.
        let msg_count_before = reason_ctx.messages.len();
        reason_ctx.messages.push(ChatMessage::user(
            "All planned actions have been executed. Assess the results: \
             if the job is fully complete, state that the job is complete. \
             Otherwise, briefly list what remains.",
        ));

        let response = reasoning.respond(reason_ctx).await?;
        let response = crate::agent::strip_suggestions(&response);

        if crate::util::llm_signals_completion(&response) {//若完成，则标记完成。且通知回前端
            reason_ctx.messages.push(ChatMessage::assistant(&response));
            self.mark_completed().await?;
        } else {//没完成
            // Replace the completion-check exchange with an action-oriented
            // continuation prompt. Leaving the "Is the job complete?" / "No"
            // dialogue in context causes the agentic loop to repeat the same
            // analysis instead of calling tools (self-dialogue loop).
            reason_ctx.messages.truncate(msg_count_before);
            reason_ctx.messages.push(ChatMessage::user(format!(//塞入信息给后面的agent_loop参考
                "The planned actions are done but the job is not yet complete. \
                 Remaining work:\n\n{response}\n\n\
                 Continue executing now — use tools to finish the job."
            )));
            tracing::info!(
                "Job {} plan completed but work remains, falling back to direct selection",
                self.job_id
            );
            self.log_event(
                "status",
                serde_json::json!({
                    "message": "Plan completed but job needs more work, continuing...",
                }),
            );
        }

        Ok(())
    }

    async fn execute_tool(
        &self,
        tool_name: &str,
        params: &serde_json::Value,
    ) -> Result<String, Error> {
        Self::execute_tool_inner(&self.deps, self.job_id, tool_name, params).await
    }


    /// Inner tool execution logic that can be called from both single and parallel paths.
    async fn execute_tool_inner(
        deps: &WorkerDeps,
        job_id: Uuid,
        tool_name: &str,
        params: &serde_json::Value,
    ) -> Result<String, Error> {
        let tool =
            deps.tools
                .get(tool_name)
                .await
                .ok_or_else(|| crate::error::ToolError::NotFound {
                    name: tool_name.to_string(),
                })?;

        let normalized_params = prepare_tool_params(tool.as_ref(), params);

        // Fetch job context early for approval checking and other needs
        let mut job_ctx = deps.context_manager.get_context(job_id).await?;

        // Check approval: additive semantics - BOTH job-level AND worker-level must approve
        let requirement = tool.requires_approval(&normalized_params);//判断是否需要批准

        // Check job-level approval context (if set by tools like the builder)
        let job_level_blocked = job_ctx
            .approval_context
            .as_ref()
            .map(|ctx| ctx.is_blocked(tool_name, requirement))
            .unwrap_or(false);

        // Check worker-level approval context (set by scheduler for autonomous jobs)
        let worker_level_blocked =
            ApprovalContext::is_blocked_or_default(&deps.approval_context, tool_name, requirement);

        // Tool is blocked if EITHER level blocks it (additive/intersection semantics)
        // This maintains defense in depth: job-level cannot bypass worker-level restrictions
        if job_level_blocked || worker_level_blocked {//触发锁定
            let reason = if job_level_blocked && worker_level_blocked {
                format!(
                    "Tool '{}' is blocked by both job-level and worker-level approval context",
                    tool_name
                )
            } else if job_level_blocked {
                format!(
                    "Tool '{}' is not in the job-level allowed tools list",
                    tool_name
                )
            } else {
                format!(
                    "Tool '{}' is not available for autonomous execution",
                    tool_name
                )
            };
            return Err(crate::error::ToolError::AutonomousUnavailable {//直接结束不执行工具
                name: tool_name.to_string(),
                reason,
            }
            .into());
        }

        // Propagate http_interceptor for trace recording/replay
        if job_ctx.http_interceptor.is_none() {
            job_ctx.http_interceptor = deps.http_interceptor.clone();
        }

        // Check per-tool rate limit before running hooks or executing (cheaper check first)
        if let Some(config) = tool.rate_limit_config()
            && let RateLimitResult::Limited { retry_after, .. } = deps
                .tools
                .rate_limiter()
                .check_and_record(&job_ctx.user_id, tool_name, &config)
                .await
        {
            return Err(crate::error::ToolError::RateLimited {
                name: tool_name.to_string(),
                retry_after: Some(retry_after),
            }
            .into());
        }

        // Run BeforeToolCall hook
        let effective_params = {//运行工具执行的hook
            use crate::hooks::{HookError, HookEvent, HookOutcome};
            let hook_params = redact_params(&normalized_params, tool.sensitive_params());
            let event = HookEvent::ToolCall {
                tool_name: tool_name.to_string(),
                parameters: hook_params,
                user_id: job_ctx.user_id.clone(),
                context: format!("job:{}", job_id),
            };
            match deps.hooks.run(&event).await {
                Err(HookError::Rejected { reason }) => {
                    return Err(crate::error::ToolError::ExecutionFailed {
                        name: tool_name.to_string(),
                        reason: format!("Blocked by hook: {}", reason),
                    }
                    .into());
                }
                Err(err) => {
                    return Err(crate::error::ToolError::ExecutionFailed {
                        name: tool_name.to_string(),
                        reason: format!("Blocked by hook failure mode: {}", err),
                    }
                    .into());
                }
                Ok(HookOutcome::Continue {
                    modified: Some(new_params),
                }) => match serde_json::from_str(&new_params) {
                    // Hook output is fresh JSON text and may reintroduce stringified scalars or
                    // containers, so we normalize it again. The fallback path reuses the already
                    // normalized input because no hook mutation was applied.
                    Ok(parsed) => prepare_tool_params(tool.as_ref(), &parsed),
                    Err(e) => {
                        tracing::warn!(
                            tool = %tool_name,
                            "Hook returned non-JSON modification for ToolCall, ignoring: {}",
                            e
                        );
                        normalized_params
                    }
                },
                _ => normalized_params,
            }
        };
        if job_ctx.state == JobState::Cancelled {
            return Err(crate::error::ToolError::ExecutionFailed {
                name: tool_name.to_string(),
                reason: "Job is cancelled".to_string(),
            }
            .into());
        }

        // Validate tool parameters
        let validation = deps
            .safety
            .validator()
            .validate_tool_params(&effective_params);
        if !validation.is_valid {
            let details = validation
                .errors
                .iter()
                .map(|e| format!("{}: {}", e.field, e.message))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(crate::error::ToolError::InvalidParameters {
                name: tool_name.to_string(),
                reason: format!("Invalid tool parameters: {}", details),
            }
            .into());
        }

        // Redact sensitive parameter values before they touch any observability or audit path.
        let safe_params = redact_params(&effective_params, tool.sensitive_params());
        let risk = tool.risk_level_for(&effective_params);
        tracing::debug!(
            tool = %tool_name,
            params = %safe_params,
            job = %job_id,
            risk = %risk,
            "Tool call started"
        );

        // Execute with per-tool timeout and timing
        let tool_timeout = tool.execution_timeout();
        let start = std::time::Instant::now();
        let result = tokio::time::timeout(tool_timeout, async {/真正执行
            tool.execute(effective_params.clone(), &job_ctx).await
        })
        .await;
        let elapsed = start.elapsed();

        match &result {//打印结果日志
            Ok(Ok(output)) => {
                let result_size = serde_json::to_string(&output.result)
                    .map(|s| s.len())
                    .unwrap_or(0);
                tracing::debug!(
                    tool = %tool_name,
                    elapsed_ms = elapsed.as_millis() as u64,
                    result_size_bytes = result_size,
                    "Tool call succeeded"
                );
            }
            Ok(Err(e)) => {
                tracing::debug!(
                    tool = %tool_name,
                    elapsed_ms = elapsed.as_millis() as u64,
                    error = %e,
                    "Tool call failed"
                );
            }
            Err(_) => {
                tracing::debug!(
                    tool = %tool_name,
                    elapsed_ms = elapsed.as_millis() as u64,
                    timeout_secs = tool_timeout.as_secs(),
                    "Tool call timed out"
                );
            }
        }

        // Record action in memory and get the ActionRecord for persistence
        let action = match &result {//记录到长期Memory
            Ok(Ok(output)) => {
                let output_str = serde_json::to_string_pretty(&output.result)
                    .ok()
                    .map(|s| deps.safety.sanitize_tool_output(tool_name, &s).content);
                match deps
                    .context_manager
                    .update_memory(job_id, |mem| {
                        let rec = mem.create_action(tool_name, safe_params.clone()).succeed(
                            output_str.clone(),
                            output.result.clone(),
                            elapsed,
                        );
                        mem.record_action(rec.clone());
                        rec
                    })
                    .await
                {
                    Ok(rec) => Some(rec),
                    Err(e) => {
                        tracing::warn!(job_id = %job_id, tool = tool_name, "Failed to record action in memory: {e}");
                        None
                    }
                }
            }
            Ok(Err(e)) => {
                match deps
                    .context_manager
                    .update_memory(job_id, |mem| {
                        let rec = mem
                            .create_action(tool_name, safe_params.clone())
                            .fail(e.to_string(), elapsed);
                        mem.record_action(rec.clone());
                        rec
                    })
                    .await
                {
                    Ok(rec) => Some(rec),
                    Err(e) => {
                        tracing::warn!(job_id = %job_id, tool = tool_name, "Failed to record action in memory: {e}");
                        None
                    }
                }
            }
            Err(_) => {
                match deps
                    .context_manager
                    .update_memory(job_id, |mem| {
                        let rec = mem
                            .create_action(tool_name, safe_params.clone())
                            .fail("Execution timeout", elapsed);
                        mem.record_action(rec.clone());
                        rec
                    })
                    .await
                {
                    Ok(rec) => Some(rec),
                    Err(e) => {
                        tracing::warn!(job_id = %job_id, tool = tool_name, "Failed to record action in memory: {e}");
                        None
                    }
                }
            }
        };

        // Persist action to database (fire-and-forget)
        if let (Some(action), Some(store)) = (action, deps.store.clone()) {//持久化到db
            tokio::spawn(async move {
                if let Err(e) = store.save_action(job_id, &action).await {
                    tracing::warn!("Failed to persist action for job {}: {}", job_id, e);
                }
            });
        }

        // Handle the result
        let output = result
            .map_err(|_| crate::error::ToolError::Timeout {//返回String结果
                name: tool_name.to_string(),
                timeout: tool_timeout,
            })?
            .map_err(|e| crate::error::ToolError::ExecutionFailed {
                name: tool_name.to_string(),
                reason: e.to_string(),
            })?;

        // Return result as string
        serde_json::to_string_pretty(&output.result).map_err(|e| {
            crate::error::ToolError::ExecutionFailed {
                name: tool_name.to_string(),
                reason: format!("Failed to serialize result: {}", e),
            }
            .into()
        })
    }
## 1. 什么是"自治 job"

**自治 job = 跑在后台、用户不在场的 agent job**。和"人在回路的对话 turn"是 IronClaw 里两个完全独立的工作模式，对应到代码上由 `ApprovalContext` 这个 enum 显式区分（`src/tools/tool.rs:37-75`）：

```rust
pub enum ApprovalContext {
    /// Autonomous job with no interactive user. Only tools in `allowed_tools`
    /// may run; interactive approval requirements are ignored.
    Autonomous {
        allowed_tools: HashSet<String>,
    },
}
```

判断规则在 `is_blocked`：

| `ApprovalRequirement`                        | 自治 job                              | 人在回路（无 context） |
| -------------------------------------------- | ------------------------------------- | ---------------------- |
| `Never`（默认，工具声明不需要批准）          | ✅ 允许                               | ✅ 允许                |
| `UnlessAutoApproved`（"需要批准但能自动批"） | ✅ 允许（自治=隐式 auto-approve）     | ❌ 阻塞（要人批）      |
| `Always`（必须人批，auto-approve 都不行）    | ❌ 阻塞，除非显式列在 `allowed_tools` | ❌ 阻塞                |

`AUTONOMOUS_TOOL_DENYLIST`（`src/tools/autonomy.rs:8-25`）是另一道闸——`routine_create` / `create_job` / `secret_list` / `tool_install` 等元工具**一律禁止在自治模式下出现**，连"在白名单里"都不行。

`JobContext` 字段 `approval_context: Option<ApprovalContext>` 是开关：
- `Some(Autonomous{…})` → 自治（后台 job / routine / 容器 worker / system init）
- `None` → 默认采用"人在回路"行为：所有非 `Never` 工具都阻塞（`is_blocked_or_default` 的 None 分支，`tool.rs:78-87`）

> 注：`JobContext::default()` 的注释（`state.rs:425-431`）明确写：**没有 `approval_context` 是更安全的默认**，要自治必须显式调 `with_approval_context()`，避免"忘了设就放开了"。

## 2. 人在回路（HITL）的两条并行链路

**A. 工具级"每次调用都要批" → 暂存等回复（你说的"暂存"就是这条）**
**B. 工具级"先一次性锁死权限" → 不再问（auto-allow）**

这两条对自治 job **都关不上**——自治 job 不走 HITL 链路。

---

### 链路 A：`requires_approval()` → `NeedApproval` → 用户 yes/no → 恢复 loop

**触发**：`Tool::requires_approval(params)` 返回 `UnlessAutoApproved` 或 `Always`（`tool.rs:415-417`），且当前不是自治上下文。

**注册位**：`ChatDelegate`（`src/agent/dispatcher.rs`）在 agentic loop 里检测到 `requires_approval` 工具时返回 `LoopOutcome::NeedApproval(Box<PendingApproval>)`（`agentic_loop.rs:49`）。

**完整路径**：

```
LLM 选工具 + 解析参数
  ↓
agentic_loop.execute_tool_calls() 检测到该工具 requires_approval
  ↓
ChatDelegate → LoopOutcome::NeedApproval(PendingApproval)
  ↓
Web 网关把 PendingApproval 存到 SessionManager 里（thread.pending_approval）
  ↓
SSE 推 approval_needed 事件到前端（含 tool 名、参数、reasoning、request_id）
  ↓
前端渲染 approval 卡片，调用 /api/chat/exec-approval 提交 yes/no/always
  ↓
SubmissionParser 解析为 ApprovalResponse / ExecApproval
  ↓
命中 thread 的 pending_approval → 消费、清除
  ↓
loop 恢复，执行该工具调用，把结果塞回 context 继续 LLM
```

关键源码位（已核对）：
- `src/agent/agentic_loop.rs:48-49` — `LoopOutcome::NeedApproval`
- `src/agent/session.rs` — `pub struct PendingApproval`（在 mod.rs:69-70 导出）
- `src/agent/dispatcher.rs` — `ChatDelegate` 把需要批准的 tool 编译成 `PendingApproval` 返回
- 提交 `yes` / `no` / `always` 在 `src/agent/CLAUDE.md:155-158` 的命令表里（"approval response" / `ExecApproval` 来自 web 网关的 `/api/chat/exec-approval`）
- `always` 选项触发 `PermissionState::AlwaysAllow`（`src/tools/permissions.rs:22-29`），让那条 session 内的同一工具不再弹

### 链路 B：常驻的 `PermissionState` 三态

持久化在 `permissions` 表（`src/tools/permissions.rs`）：
- `AlwaysAllow` — 永不问
- `AskEachTime` — 每次走 A 链路
- `Disabled` — 直接拒

`seeded_default_permission`（`permissions.rs:37-76`）给内置工具设了基线：`shell` / `write_file` / `tool_install` 等高危工具默认 `AskEachTime`；`echo` / `time` / `memory_search` 等只读工具默认 `AlwaysAllow`。

管理员还有第二道闸：`AdminToolPolicy`（`permissions.rs:129-...`），存 `settings` 表 `(ADMIN_SETTINGS_USER_ID, ADMIN_TOOL_POLICY_KEY)`，可以全局禁用某些工具，**且从 LLM 上下文里把工具定义剥掉**——用户连"知道有这个工具"都做不到，无法重新打开。

### 关键边界：自治 job 与 HITL 是互斥的

回到你之前贴的 `AutonomousUnavailable` 分支——它的存在就是为了**早死**，**不让自治 job 误入 HITL 链路**：

- `ApprovalContext::Autonomous` 一旦存在，`UnlessAutoApproved` 工具被视作 auto-approve（"自治" = 隐式 `always`），根本不会走 `NeedApproval`。
- `Always` 工具在 `Autonomous` 上下文里**只能通过白名单豁免**——白名单本身就是启动时由调度器/routine 引擎根据 `JobContext.with_approval_context()` 注入的静态集合，**不支持运行时追加**。
- 所以 `AutonomousUnavailable` 的语义是"这个工具在这个 job 里**配置阶段**就没被授权"——不存在"运行时去要授权"这条路径。

## 简答

> **自治 job** = `JobContext.approval_context = Some(Autonomous{ allowed_tools })`，后台跑、用户不在场；走 `AUTONOMOUS_TOOL_DENYLIST` + `allowed_tools` 双重白名单，`UnlessAutoApproved` 隐式放行。
>
> **HITL 链路**有两个独立通道：**(A) `NeedApproval` 暂存**——LLM 选完带 `requires_approval` 的工具，agentic loop 在 `ChatDelegate` 处返回 `LoopOutcome::NeedApproval(PendingApproval)`，web 网关把 `PendingApproval` 存到 `Session`，SSE 推 `approval_needed`，等用户 `/api/chat/exec-approval` 提交 yes/no/always 再恢复 loop；**(B) 常驻 `PermissionState`**——`AlwaysAllow` / `AskEachTime` / `Disabled` 三态持久化，附带 `AdminToolPolicy` 强制覆盖。
>
> 这两条链**都对自治 job 关不上**——自治 job 要么走白名单，要么 `AutonomousUnavailable` 早死，不会"暂存等批准"。

    /// Process a tool execution result and add it to the reasoning context.
    async fn process_tool_result_job(
        &self,
        reason_ctx: &mut ReasoningContext,
        selection: &ToolSelection,
        result: Result<String, Error>,
    ) -> Result<(), Error> {
        self.log_event(//上报
            "tool_use",
            serde_json::json!({
                "tool_name": selection.tool_name,
                "input": truncate_for_preview(
                    &selection.parameters.to_string(), 500),
            }),
        );

        // Use shared result processing for sanitize → wrap → ChatMessage.
        // The wrapped content (XML tags) goes into reason_ctx for the LLM.
        // The raw sanitized content goes into events/SSE for human-readable UI.
        let (_wrapped, message) = process_tool_result(
            &self.deps.safety,
            &selection.tool_name,
            &selection.tool_call_id,
            &result,
        );
        reason_ctx.messages.push(message);//塞入上下文

        match result {
            Ok(raw_output) => {
                let sanitized = self
                    .deps
                    .safety
                    .sanitize_tool_output(&selection.tool_name, &raw_output);
                self.log_event(//结果上报
                    "tool_result",
                    serde_json::json!({
                        "tool_name": selection.tool_name,
                        "success": true,
                        "output": truncate_for_preview(&sanitized.content, 500),
                    }),
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    "Tool {} failed for job {}: {}",
                    selection.tool_name,
                    self.job_id,
                    e
                );

                // Record failure for self-repair tracking
                if let Some(store) = self.store() {
                    let store = store.clone();
                    let tool_name = selection.tool_name.clone();
                    let error_msg = e.to_string();
                    tokio::spawn(async move {
                        if let Err(db_err) = store.record_tool_failure(&tool_name, &error_msg).await
                        {
                            tracing::warn!("Failed to record tool failure: {}", db_err);
                        }
                    });
                }

                let error_preview = {
                    let msg = format!("Error: {}", e);
                    truncate_for_preview(&msg, 500).into_owned()
                };
                self.log_event(//上报
                    "tool_result",
                    serde_json::json!({
                        "tool_name": selection.tool_name,
                        "success": false,
                        "output": error_preview,
                    }),
                );

                // All tool errors (including AutonomousUnavailable) are
                // recoverable — the error message is already recorded in
                // reason_ctx so the LLM can see it and try a different
                // approach. Returning Err here would kill the entire job.
                Ok(())
            }
        }
    }

4. Reasoning

/// Reasoning engine for the agent.
pub struct Reasoning {
llm: Arc<dyn LlmProvider>,
/// Optional workspace for loading identity/system prompts.
workspace_system_prompt: Option<String>,
/// Optional skill context block to inject into system prompt.
skill_context: Option<String>,
/// Names of active skills (used to suppress extension search for covered domains).
active_skill_names: Vec<String>,
/// Channel name (e.g. "discord", "telegram") for formatting hints.
channel: Option<String>,
/// Model name for runtime context.
model_name: Option<String>,
/// Whether this is a group chat context.
is_group_chat: bool,
/// Channel-specific conversation context (e.g., sender number, UUID, group ID).
/// This is passed to the LLM to provide clarity about who/group it's talking to.
conversation_context: std::collections::HashMap<String, String>,
/// Platform identity and runtime metadata for self-awareness.
platform_info: Option<ironclaw_common::PlatformInfo>,
}
Reasoning = LLM provider + 上下文（身份/技能/频道/平台） + 思考标签清洗 + 几个高层动作（complete / plan / select_tool）。它是"调一次 LLM 该带什么、回来要清什么"的封装，不包含循环。

/// Context for reasoning operations.
pub struct ReasoningContext {
/// Conversation history.
pub messages: Vec<ChatMessage>,
/// Available tools.
pub available_tools: Vec<ToolDefinition>,
/// Job description if working on a job.
pub job_description: Option<String>,
/// Current state description.
pub current_state: Option<String>,
/// Opaque metadata forwarded to the LLM provider (e.g. thread_id for chaining).
pub metadata: std::collections::HashMap<String, String>,
/// When true, force a text-only response (ignore available tools).
/// Used by the agentic loop to guarantee termination near the iteration limit.
/// Sticky: once set, never cleared within a loop invocation. Callers must
/// create a fresh `ReasoningContext` per `run_agentic_loop()` call.
pub force_text: bool,
/// Pre-built system prompt. When set, `respond_with_tools` uses this directly
/// instead of calling `build_system_prompt_with_tools`. Allows callers to build
/// the prompt once and reuse it across iterations.
pub system_prompt: Option<String>,
/// Per-user model override. When set, completion requests use this model
/// instead of the provider's default. Only effective with providers that
/// support per-request model overrides (e.g. NearAI).
pub model_override: Option<String>,
/// User-configured default temperature. When set, overrides the hardcoded
/// 0.7 default in `respond_with_tools`. Per-request temperature from API
/// callers takes precedence over this.
pub temperature: Option<f32>,
/// Set by `execute_tool_calls` to indicate whether every tool in the last
/// batch failed. Used by the duplicate tool call tracker in the agentic loop.
/// Reset to `false` at the start of each iteration.
pub last_tool_batch_all_failed: bool,
}

    /// Generate a plan for completing a goal.
    pub async fn plan(&self, context: &ReasoningContext) -> Result<ActionPlan, LlmError> {
        let system_prompt = self.build_planning_prompt(context);

        let system_prompt = merge_system_messages(system_prompt, &context.messages);
        let mut messages = vec![ChatMessage::system(system_prompt)];
        messages.extend(
            context
                .messages
                .iter()
                .filter(|m| m.role != Role::System)
                .cloned(),
        );

        if let Some(ref job) = context.job_description {
            messages.push(ChatMessage::user(format!(
                "Please create a plan to complete this job:\n\n{}",
                job
            )));
        }

        let request = CompletionRequest::new(messages)
            .with_max_tokens(2048)
            .with_temperature(0.3);

        let response = self.llm.complete(request).await?;

        // Clean reasoning model artifacts before parsing JSON.
        // Pre-truncate at tool tags to avoid strip_xml_tag discarding
        // content after unclosed tags (issue #789).
        let pre_truncated = truncate_at_tool_tags(&response.content);
        let cleaned = clean_response(&pre_truncated);
        self.parse_plan(&cleaned)//返回生成好的计划
    }


▎ 这是一次专门用于"完成判定"的 LLM 调用——把"所有 plan 步骤已执行完，评估结果：做完了就说做完了，没做完就列出还差什么"作为 user 消息喂回去，让 LLM 看完整段 tool 结果后下个判断。                       
▎ - 说"做完了" → 状态机走到 Completed，execute_plan 收工；                                                                                                                                             
▎ - 说"还差 X" → 把 LLM 答的"差什么"塞回 context 继续 loop。                                                                                                                                           
▎                                                                                                                                                                                                      
▎ 本质是给"机械执行"加一个语义收口——plan 只能描述"做什么"，"是不是真做完了"必须由 LLM 看完结果后给一句话。

/// Generate a response that may include tool calls, with token usage tracking.
///
/// Returns `RespondOutput` containing the result and token usage from the LLM call.
/// The caller should use `usage` to track cost/budget against the job.
pub async fn respond_with_tools(
&self,
context: &ReasoningContext,
) -> Result<RespondOutput, LlmError> {
let system_prompt = match context.system_prompt {
Some(ref prompt) => prompt.clone(),
None => self.build_system_prompt_with_tools(&context.available_tools),
};

        let system_prompt = merge_system_messages(system_prompt, &context.messages);
        let mut messages = vec![ChatMessage::system(system_prompt)];
        messages.extend(
            context
                .messages
                .iter()
                .filter(|m| m.role != Role::System)
                .cloned(),
        );

        let effective_tools = if context.force_text {
            Vec::new()
        } else {
            context.available_tools.clone()
        };

        // Clamp to the provider-supported range. The frontend enforces this
        // too, but a bad DB value or per-request override must not reach the
        // provider — some backends reject out-of-range temperatures outright.
        let temperature = context.temperature.unwrap_or(0.7).clamp(0.0, 2.0);

        // If we have tools, use tool completion mode
        if !effective_tools.is_empty() {
            let mut request = ToolCompletionRequest::new(messages, effective_tools)
                .with_max_tokens(4096)
                .with_temperature(temperature)
                .with_tool_choice("auto");
            request.metadata = context.metadata.clone();
            if let Some(ref model) = context.model_override {
                request.model = Some(model.clone());
            }

            let response = self.llm.complete_with_tools(request).await?;
            let usage = TokenUsage {
                input_tokens: response.input_tokens,
                output_tokens: response.output_tokens,
                cache_read_input_tokens: response.cache_read_input_tokens,
                cache_creation_input_tokens: response.cache_creation_input_tokens,
            };

            // If there were tool calls, return them for execution
            if !response.tool_calls.is_empty() {
                let narrative = response.content.map(|c| {
                    let pre_truncated = truncate_at_tool_tags(&c);
                    clean_response(&pre_truncated)
                });
                let provider_reasoning = response.reasoning;
                // Populate per-tool reasoning from the shared narrative when the
                // provider did not supply per-tool rationale.
                let tool_calls: Vec<ToolCall> = response
                    .tool_calls
                    .into_iter()
                    .map(|mut tc| {
                        if tc.reasoning.as_ref().is_none_or(|r| r.trim().is_empty()) {
                            tc.reasoning = narrative.as_ref().filter(|n| !n.is_empty()).cloned();
                        } else {
                            // Clean provider-supplied per-tool reasoning the same way
                            // we clean the shared narrative (strip thinking/tool tags).
                            tc.reasoning = tc
                                .reasoning
                                .map(|r| {
                                    let pre_truncated = truncate_at_tool_tags(&r);
                                    clean_response(&pre_truncated)
                                })
                                .filter(|r| !r.trim().is_empty());
                        }
                        tc
                    })
                    .collect();
                return Ok(RespondOutput {
                    result: RespondResult::ToolCalls {
                        tool_calls,
                        content: narrative,
                        reasoning: provider_reasoning,
                    },
                    usage,
                    finish_reason: response.finish_reason,
                    metadata: ResponseMetadata::default(),
                });
            }

            let content = response.content.unwrap_or_default();

            // Some models (e.g. GLM-4.7) emit tool calls as XML tags in content
            // instead of using the structured tool_calls field. Try to recover
            // them before giving up and returning plain text.
            // NOTE: Recovery runs on the raw content (before truncation) so it can
            // parse tool-call JSON from the XML tags. Truncation only applies to the
            // remaining *text* content returned alongside the recovered tool calls.
            let recovered = recover_tool_calls_from_content(&content, &context.available_tools);
            if !recovered.is_empty() {
                let pre_truncated = truncate_at_tool_tags(&content);
                let cleaned = clean_response(&pre_truncated);
                return Ok(RespondOutput {
                    result: RespondResult::ToolCalls {
                        tool_calls: recovered,
                        content: if cleaned.is_empty() {
                            None
                        } else {
                            Some(cleaned)
                        },
                        // XML-tag-recovered tool calls don't come with native
                        // reasoning artifacts — those would have been on the
                        // structured tool_calls path instead.
                        reasoning: response.reasoning,
                    },
                    usage,
                    finish_reason: response.finish_reason,
                    metadata: ResponseMetadata::default(),
                });
            }

            // Guard against empty text after cleaning. This can happen when:
            // 1. Reasoning models (e.g. GLM-5) return chain-of-thought in
            //    reasoning_content wrapped in <think> tags — clean_response
            //    strips the think tags leaving an empty string.
            // 2. Local models (Qwen3, DeepSeek) emit <tool_call> XML in text
            //    responses even in force_text mode — strip_xml_tag discards
            //    from unclosed opening tag onward (issue #789).
            // Pre-truncate at tool tags to preserve text before the tag.
            let pre_truncated = truncate_at_tool_tags(&content);
            let cleaned = clean_response(&pre_truncated);
            let metadata = if cleaned.trim().is_empty() {
                tracing::warn!(
                    "LLM response was empty after cleaning (original len={}), using fallback",
                    content.len()
                );
                ResponseMetadata {
                    anomaly: Some(ResponseAnomaly::EmptyToolCompletion),
                }
            } else {
                ResponseMetadata::default()
            };
            let final_text = if metadata.anomaly.is_some() {
                "I'm not sure how to respond to that.".to_string()
            } else {
                cleaned
            };
            Ok(RespondOutput {
                result: RespondResult::Text(final_text),
                usage,
                finish_reason: response.finish_reason,
                metadata,
            })
        } else {
            // No tools, use simple completion
            let mut request = CompletionRequest::new(messages)
                .with_max_tokens(4096)
                .with_temperature(temperature);
            request.metadata = context.metadata.clone();
            if let Some(ref model) = context.model_override {
                request.model = Some(model.clone());
            }

            let response = self.llm.complete(request).await?;
            let pre_truncated = truncate_at_tool_tags(&response.content);
            let cleaned = clean_response(&pre_truncated);
            let metadata = if cleaned.trim().is_empty() {
                tracing::warn!(
                    "LLM response was empty after cleaning (original len={}), using fallback",
                    response.content.len()
                );
                ResponseMetadata {
                    anomaly: Some(ResponseAnomaly::EmptyTextResponse),
                }
            } else {
                ResponseMetadata::default()
            };
            let final_text = if metadata.anomaly.is_some() {
                "I'm not sure how to respond to that.".to_string()
            } else {
                cleaned
            };
            Ok(RespondOutput {
                result: RespondResult::Text(final_text),
                usage: TokenUsage {
                    input_tokens: response.input_tokens,
                    output_tokens: response.output_tokens,
                    cache_read_input_tokens: response.cache_read_input_tokens,
                    cache_creation_input_tokens: response.cache_creation_input_tokens,
                },
                finish_reason: response.finish_reason,
                metadata,
            })
        }
    }

二. 自治Job和human in loop

## 答：是的，`dispatch_job` 全是自治；人在回路走的是**对话 turn 的 `NeedApproval` 链路**，与 Scheduler 平行

## 1. `dispatch_job` 走的就是自治 job

`scheduler.rs:142-158` 的实现细节：

```rust
pub async fn dispatch_job(&self, user_id, title, description, metadata) -> Result<Uuid, JobError> {
    let approval_context = self.autonomous_approval_context(user_id).await;  // ← 关键
    self.dispatch_job_inner(..., Some(approval_context)).await
}
```

`autonomous_approval_context`（`scheduler.rs:254-259`）的产物：

```rust
ApprovalContext::autonomous_with_tools(autonomous_allowed_tool_names(...).await)
```

`autonomous_allowed_tool_names`（`src/tools/autonomy.rs:46-67`）干两件事：
1. `retain(|n| !is_autonomous_tool_denylisted(n))` — 去掉 `routine_create` / `secret_list` / `tool_install` 等元工具；
2. 加上当前 owner 的扩展工具（同样过滤 denylist）。

→ **出来的 `ApprovalContext` 是 `Autonomous { allowed_tools }`，`worker.rs:547-583` 那段你之前看过的"白名单 + denylist 双重过滤"就是它**。注释 `scheduler.rs:235-236` 明确写："Currently unreachable via dispatch_job() which always provides Some(approval_context)"。

所有 `dispatch_job` 的调用方（已 grep 实证）：
- `src/agent/commands.rs:101` — `/job` 显式创建后台 job
- `src/agent/routine_engine.rs:1506` — `Routine::FullJob` action
- `src/channels/web/features/jobs/mod.rs:639` — web `/api/jobs/{id}/restart`
- `src/tools/builtin/job.rs:365` — `create_job` 工具

**没有一条走"人在回路"。**

## 2. 人在回路（tool approval）走的是 `NeedApproval` 链路，与 Scheduler **平行**

`NeedApproval` 是 `LoopOutcome`（`src/agent/agentic_loop.rs:49`）的变体，由 **`ChatDelegate`** 触发——这是处理**对话 turn**的 delegate，**不是** `JobDelegate`。两者用不同的 `JobContext`、`ApprovalContext`、不同的 transport。

### 完整链路（实证过的 5 个源码点）

```
用户通过 web 网关发消息: POST /api/chat/send
  ↓
SubmissionParser::parse() 解析成 Submission::UserInput
  ↓
ChatDelegate.run_agentic_loop() 启动
  ↓
每轮 LLM respond → 选了带 requires_approval=UnlessAutoApproved|Always 的工具
  ↓
ChatDelegate.execute_tool_calls() 检测 → LoopOutcome::NeedApproval(Box<PendingApproval>)
  ↓
agent_loop.rs:2558 SubmissionResult::NeedApproval { .. } 分支
  ↓
PendingApproval 存到 thread.pending_approval（session.rs）
  ↓
SSE 推 approval_needed 事件到前端（含 tool 名、参数、request_id）
  ↓
前端渲染审批卡片，调用 /api/chat/gate/resolve 提交 approve/deny
  ↓
SubmissionParser 把 yes/no/always 解析成 ApprovalResponse{ approved }
  ↓
命中 thread.pending_approval → 消费、清空
  ↓
loop 恢复执行被批准的工具调用
```

关键位（已 grep 实证）：
- `agentic_loop.rs:48-49` — `LoopOutcome::NeedApproval`
- `src/agent/dispatcher.rs` — `ChatDelegate` 把需要批准的工具编译成 `PendingApproval`
- `agent_loop.rs:2558` — `SubmissionResult::NeedApproval` 处理分支
- `src/agent/CLAUDE.md:155-158` — yes/no/always 提交命令清单
- `src/channels/web/features/chat/mod.rs` — `/api/chat/approval` + `/api/chat/gate/resolve` 路由
- web/CLAUDE.md 第 73-77 行表 — `approval_needed` SSE 事件 + `/api/chat/approval` 路由
- `web/CLAUDE.md:255` — pending gates 持久化在 `~/.ironclaw/pending-gates.json`，**文件备份**以跨重启/重连存活

### 关键边界：两条链路**互不交叉**

| 维度                          | `dispatch_job`（自治）                                           | `NeedApproval`（HITL）                                                                  |
| ----------------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| 入口                          | `/job` 命令、routine FullJob、create_job 工具、jobs/{id}/restart | `/api/chat/send`、对话消息                                                              |
| Delegate                      | `JobDelegate`（`src/worker/job.rs`）                             | `ChatDelegate`（`src/agent/dispatcher.rs`）                                             |
| `JobContext.approval_context` | `Some(Autonomous{ allowed_tools })`                              | **`None`**（默认"人在回路"行为：非 `Never` 工具阻塞）                                   |
| 工具调用阻塞时                | `AutonomousUnavailable` 错误（**不**暂存，回灌 LLM）             | `PendingApproval` 存 session，**真的暂存**                                              |
| LLM 决策循环                  | worker.run() 里的 `execution_loop`                               | `run_agentic_loop` + `ChatDelegate`                                                     |
| 状态机                        | `JobState::Pending → InProgress → …`                             | 走 `ThreadState`（不是 `JobState`）：`Idle → Processing → AwaitingApproval → Completed` |
| 持久化                        | `job_actions` + `agent_jobs` 表                                  | `~/.ironclaw/pending-gates.json`（文件备份，跨进程存活）                                |
| 通道                          | SSE `job_*` 事件                                                 | SSE `approval_needed` / `gate_required`                                                 |

## 3. 第三个常被混淆的：sandbox job（`/api/jobs/*`）

web 网关上的 `/api/jobs`（`web/CLAUDE.md` "Jobs (sandbox)" 节）又是**第三个独立域**——沙盒容器跑的 Claude Code bridge，也是自治的（参考 `src/worker/container.rs:ContainerDelegate`）。它有自己的 `JobEvent` → SSE 事件投影（`gateway-events.md` 提到 `sandbox JobEvent` 源日志），**也**不经 `NeedApproval`。批准 / 中断沙盒 job 走 `/api/jobs/{id}/cancel` / `/api/jobs/{id}/prompt`——是**控制平面**（注入消息、终止），不是"工具级 approval"。

## 简答

> **是的，`Scheduler::dispatch_job` 全是自治 job**——它内部固定构造 `ApprovalContext::Autonomous { allowed_tools }`，调用方没法选"人在回路"模式。所有触发点（`/job` 命令、routine FullJob、`create_job` 工具、`/api/jobs/{id}/restart`）都是后台跑。
>
> **人在回路的工具批准走 `NeedApproval` 链路**，与 Scheduler **平行、不交叉**：
> - 入口是 `/api/chat/send`（对话 turn），不是 `/job`；
> - 由 `ChatDelegate`（不是 `JobDelegate`）触发；
> - `JobContext.approval_context` 是 `None`（默认"人在回路"行为），不是 `Autonomous`；
> - `PendingApproval` 存 session（文件备份 `~/.ironclaw/pending-gates.json` 跨进程存活），SSE 推 `approval_needed` / `gate_required`；
> - 用户通过 `/api/chat/gate/resolve` 或 `/api/chat/approval`（legacy shim）提交 yes/no/always，loop 恢复；
> - 状态机走 `ThreadState`（不是 `JobState`）。
>
> 第三个域（`/api/jobs/*` 沙盒 job）也是自治的，批准通过 `/cancel` / `/prompt` 这类**控制平面**端点，不是工具级 approval。

