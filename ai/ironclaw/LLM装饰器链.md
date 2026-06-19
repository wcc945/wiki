一. 实现的接口

/// Trait for LLM providers.
#[async_trait]
pub trait LlmProvider: Send + Sync {
/// Get the model name.
fn model_name(&self) -> &str;

    /// Get cost per token (input, output).
    fn cost_per_token(&self) -> (Decimal, Decimal);

    /// Complete a chat conversation.
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError>;

    /// Complete with tool use support.
    async fn complete_with_tools(
        &self,
        request: ToolCompletionRequest,
    ) -> Result<ToolCompletionResponse, LlmError>;

    /// List available models from the provider.
    /// Default implementation returns empty list.
    async fn list_models(&self) -> Result<Vec<String>, LlmError> {
        Ok(Vec::new())
    }

    /// Fetch metadata for the current model (context length, etc.).
    /// Default returns the model name with no size info.
    async fn model_metadata(&self) -> Result<ModelMetadata, LlmError> {
        Ok(ModelMetadata {
            id: self.model_name().to_string(),
            context_length: None,
        })
    }

    /// Resolve which model should be reported for a given request.
    ///
    /// Providers that ignore per-request model overrides should override this
    /// and return `active_model_name()`.
    fn effective_model_name(&self, requested_model: Option<&str>) -> String {
        normalized_model_override(requested_model)
            .map(std::borrow::ToOwned::to_owned)
            .unwrap_or_else(|| self.active_model_name())
    }

    /// Get the currently active model name.
    ///
    /// May differ from `model_name()` if the model was switched at runtime
    /// via `set_model()`. Default returns `model_name()`.
    fn active_model_name(&self) -> String {
        self.model_name().to_string()
    }

    /// Switch the active model at runtime. Not all providers support this.
    fn set_model(&self, _model: &str) -> Result<(), LlmError> {
        Err(LlmError::RequestFailed {
            provider: "unknown".to_string(),
            reason: "Runtime model switching not supported by this provider".to_string(),
        })
    }

    /// Calculate cost for a completion.
    fn calculate_cost(&self, input_tokens: u32, output_tokens: u32) -> Decimal {
        let (input_cost, output_cost) = self.cost_per_token();
        input_cost * Decimal::from(input_tokens) + output_cost * Decimal::from(output_tokens)
    }

    /// Cost multiplier for cache-creation tokens (Anthropic prompt caching).
    ///
    /// Returns `1.0` by default (no surcharge). Anthropic providers return
    /// `1.25` for 5-minute TTL or `2.0` for 1-hour TTL.
    fn cache_write_multiplier(&self) -> Decimal {
        Decimal::ONE
    }

    /// Discount divisor for cache-read tokens.
    ///
    /// Cached-read cost = `input_rate / cache_read_discount()`.
    /// Returns `1` by default (no discount). Anthropic returns `10` (90% off),
    /// OpenAI would return `2` (50% off).
    fn cache_read_discount(&self) -> Decimal {
        Decimal::ONE
    }
}

二. Rig装饰器，将第三方接入自己的trait

pub struct RigAdapter<M: CompletionModel> {
model: M,
model_name: String,
input_cost: Decimal,
output_cost: Decimal,
/// Prompt cache retention policy (Anthropic only).
/// When not `CacheRetention::None`, injects top-level `cache_control`
/// via `additional_params` for Anthropic automatic caching. Also controls
/// the cost multiplier for cache-creation tokens.
cache_retention: CacheRetention,
/// Parameter names that this provider does not support (e.g., `"temperature"`).
/// These are stripped from requests before sending to avoid 400 errors.
unsupported_params: HashSet<String>,
/// Default additional parameters merged into every request.
/// Used by providers that need extra top-level fields (e.g., Ollama `think: true`).
default_additional_params: Option<serde_json::Value>,
/// Optional model-discovery endpoint. When set, [`LlmProvider::list_models`]
/// issues a `GET` instead of returning the empty default. rig-core's
/// `CompletionModel` does not expose model discovery, so this is wired
/// explicitly per protocol (OpenAI-compatible, Anthropic, Ollama).
models_endpoint: Option<ModelsEndpoint>,
}

三. 主链和cheap链

RecordingLlm →
SwappableLlmProvider →
CachedProvider →
CircuitBreakerProvider →
FailoverProvider →  兜底层
SmartRoutingProvider → 智能路由层
RetryProvider → 重试层
裸 provider(Rig),实际发起请求的一层 （主和廉价）

1. RetryProvider

pub struct RetryProvider {
inner: Arc<dyn LlmProvider>,内部包裹
config: RetryConfig,//重试次数配置，来自config
}

//重试逻辑  
async fn retry_loop<T, F, Fut>(&self, mut op: F, label: &str) -> Result<T, LlmError>
where
F: FnMut() -> Fut,
Fut: Future<Output = Result<T, LlmError>>,
{
let mut last_error: Option<LlmError> = None;

        for attempt in 0..=self.config.max_retries {
            match op().await {//op是函数参数，调用层的裸provider请求llm
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    //不匹配这些 请求失败、速率限制、无效响应等等直接失败
                    if !is_retryable(&err) || attempt == self.config.max_retries {
                        return Err(err);
                    }


                    /// Calculate exponential backoff delay with random jitter.
///
/// Base delay is 1 second, doubled each attempt, with +/-25% jitter.
/// - attempt 0: ~1s (0.75s - 1.25s)
/// - attempt 1: ~2s (1.5s - 2.5s)
/// - attempt 2: ~4s (3.0s - 5.0s)
let delay = match &err {//重试延迟
LlmError::RateLimited {
retry_after: Some(duration),
..
}
| LlmError::BadGateway {
retry_after: Some(duration),
..
} => *duration,
_ => retry_backoff_delay(attempt),
};

                    tracing::warn!(
                        provider = %self.inner.model_name(),
                        attempt = attempt + 1,
                        max_retries = self.config.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        error = %err,
                        "Retrying after transient error{label}"
                    );

                    last_error = Some(err);
                    tokio::time::sleep(delay).await;//睡眠
                }
            }
        }

        Err(last_error.unwrap_or_else(|| LlmError::RequestFailed {//重试到达最大次数，返回报错
            provider: self.inner.model_name().to_string(),
            reason: "retry loop exited unexpectedly".to_string(),
        }))
    }
}

2. SmartRouter

        let cheap: Arc<dyn LlmProvider> = if retry_config.max_retries > 0 {
            Arc::new(RetryProvider::new(cheap, retry_config.clone()))
        } else {
            cheap
        };廉价模型也先包重试

**********
pub struct SmartRoutingConfig {
/// Enable cascade mode: retry with primary if cheap model response seems uncertain.
pub cascade_enabled: bool,
/// Custom domain keywords for the scorer (None uses defaults).
pub domain_keywords: Option<Vec<String>>,domain_keywords 是给打分器自定义领域关键词用的，命中关键词会被认为「这事儿挺专业」，倾向走 primary；不传就用内置默认关键词列表。    
}
**********
/// Configuration for the complexity scorer.
#[derive(Debug, Clone, Default)]
pub struct ScorerConfig {
/// Weights for each scoring dimension.
pub weights: ScorerWeights,
/// Custom domain-specific keywords (overrides defaults if provided).
/// Each entry is a word or regex pattern fragment.
pub domain_keywords: Option<Vec<String>>,
}
*****
/// Weights for each of the 13 scoring dimensions.
#[derive(Debug, Clone)]
pub struct ScorerWeights {
pub reasoning_words: f32,
pub token_estimate: f32,
pub code_indicators: f32,
pub multi_step: f32,
pub domain_specific: f32,
pub ambiguity: f32,
pub creativity: f32,
pub precision: f32,
pub context_dependency: f32,
pub tool_likelihood: f32,
pub safety_sensitivity: f32,
pub question_complexity: f32,
pub sentence_complexity: f32,
}
这 13 个维度是给 prompt 打分的 13 个角度：内容上判"是不是推理 / 代码 / 多步 / 创意 / 精确 / 上下文相关 / 像要调工具 / 涉及安全敏感 / 命中领域词"；形式上判"长度 / 句子复杂度 / 问句复杂度 / 含糊程
> 度"。每一维按规则打成 0–100，乘以默认权重（来自 ScorerWeights::default()）加总，得到的总分决定走 cheap 还是 primary。


// smart_routing.rs:600 附近
let total: f32 = [
("reasoning_words", weights.reasoning_words),
("token_estimate", weights.token_estimate),
("code_indicators", weights.code_indicators),
("multi_step", weights.multi_step),
("domain_specific", weights.domain_specific),
("ambiguity", weights.ambiguity),
("creativity", weights.creativity),
("precision", weights.precision),
("context_dependency", weights.context_dependency),
("tool_likelihood", weights.tool_likelihood),
("safety_sensitivity", weights.safety_sensitivity),
("question_complexity", weights.question_complexity),
("sentence_complexity", weights.sentence_complexity),
]
.iter()
.map(|(name, w)| components[*name] as f32 * w)
.sum();

每个维度先按上面的规则算成 0–100 的分，再乘以自己的权重，加权求和 → 总分（也是 0–100 量级）。最后总分映射到 TaskComplexity：

0~20  → Tier::Flash        （闲聊级，几乎一定走 cheap）
20~40 → Tier::Standard     （标准任务）
40~70 → Tier::Pro          （专业任务）
70+   → Tier::Frontier     （前沿/高难度任务，几乎一定走 primary）




• /// 智能路由 provider：根据任务复杂度打分并路由到合适的模型。
///
/// - `complete()` — 按 13 个维度评估复杂度，先检查 pattern 覆写，
///   再决定走 cheap 还是 primary。中等复杂度的任务走 cascade
///   （先试 cheap，结果不确定时升级到 primary）。
/// - `complete_with_tools()` — 永远走 primary（工具调用需要可靠的结构化输出）。
pub struct SmartRoutingProvider {
primary: Arc<dyn LlmProvider>,
cheap: Arc<dyn LlmProvider>,
config: SmartRoutingConfig,
scorer_config: ScorerConfig,//打分配置
/// Pre-compiled domain regex (built once at construction time).
domain_regex: Regex,正则匹配
stats: SmartRoutingStats,//总次数、主模型次数、便宜模型次数、级联次升主模型次数最终叠加到主模型。
}

    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);

        let complexity = self.classify(&request);//复杂度判断

        match complexity {
            TaskComplexity::Simple => {
                tracing::trace!(
                    model = %self.cheap.model_name(),
                    "Smart routing: Simple task -> cheap model"
                );
                self.stats.cheap_requests.fetch_add(1, Ordering::Relaxed);
                self.cheap.complete(request).await
            }
            TaskComplexity::Complex => {
                tracing::trace!(
                    model = %self.primary.model_name(),
                    "Smart routing: Complex task -> primary model"
                );
                self.stats.primary_requests.fetch_add(1, Ordering::Relaxed);
                self.primary.complete(request).await
            }
            TaskComplexity::Moderate => {
                if self.config.cascade_enabled {
                    tracing::trace!(
                        model = %self.cheap.model_name(),
                        "Smart routing: Moderate task -> cheap model (cascade enabled)"
                    );
                    self.stats.cheap_requests.fetch_add(1, Ordering::Relaxed);

                    let response = self.cheap.complete(request.clone()).await?;

                    if Self::response_is_uncertain(&response) {//是否包含 im not sure等，包含重新请求主模型
                        tracing::info!(
                            cheap_model = %self.cheap.model_name(),
                            primary_model = %self.primary.model_name(),
                            "Smart routing: Escalating to primary (cheap model response uncertain)"
                        );
                        self.stats
                            .cascade_escalations
                            .fetch_add(1, Ordering::Relaxed);
                        self.stats.primary_requests.fetch_add(1, Ordering::Relaxed);
                        self.primary.complete(request).await
                    } else {
                        Ok(response)
                    }
                } else {
                    // Without cascade, moderate tasks go to cheap model
                    tracing::trace!(
                        model = %self.cheap.model_name(),
                        "Smart routing: Moderate task -> cheap model (cascade disabled)"
                    );
                    self.stats.cheap_requests.fetch_add(1, Ordering::Relaxed);
                    self.cheap.complete(request).await
                }
            }
        }
    }

    fn classify(&self, request: &CompletionRequest) -> TaskComplexity {
        let last_user_msg = request
            .messages
            .iter()
            .rev()
            .find(|m| m.role == Role::User)
            .map(|m| m.content.as_str())
            .unwrap_or("");

        // Normalize: trim whitespace so anchored regexes and token scoring are consistent.
        let last_user_msg = last_user_msg.trim();

        // Highest priority: explicit tier hints (e.g. "[tier:flash]")
        if let Some(caps) = RE_TIER_HINT.captures(last_user_msg) {用户或上级显示提醒按用户的来
            // SAFETY: RE_TIER_HINT has exactly one capture group; get(1) is guaranteed Some after match.
            let tier_str = caps.get(1).expect("capture group 1 exists").as_str(); // safety: RE_TIER_HINT has group 1
            let tier = match tier_str.to_lowercase().as_str() {
                "flash" => Tier::Flash,
                "standard" => Tier::Standard,
                "pro" => Tier::Pro,
                "frontier" => Tier::Frontier,
                other => {
                    tracing::error!(tier = %other, "Unexpected tier in hint despite regex constraint");
                    Tier::Standard
                }
            };
            let complexity = TaskComplexity::from(tier);
            tracing::trace!(
                %tier,
                ?complexity,
                "Smart routing: explicit tier hint"
            );
            return complexity;
        }

        // Fast-path: check pattern overrides
        for po in DEFAULT_OVERRIDES.iter() {//寒暄、日期、审计、漏洞审查，次优先级
            if po.regex.is_match(last_user_msg) {
                let complexity = TaskComplexity::from(po.tier);
                tracing::trace!(
                    tier = %po.tier,
                    ?complexity,
                    "Smart routing: pattern override matched"
                );
                return complexity;
            }
        }

        // Full 13-dimension scoring (uses pre-compiled domain regex)
        let breakdown = score_complexity_with_regex(//13维度打分
            last_user_msg,
            &self.scorer_config.weights,
            &self.domain_regex,
        );
        let complexity = TaskComplexity::from(breakdown.tier);
        tracing::trace!(
            score = breakdown.total,
            tier = %breakdown.tier,
            ?complexity,
            hints = ?breakdown.hints,
            "Smart routing: scored complexity"
        );
        complexity
    }

13维度打分
## 组 A：纯正则命中（命中数 × 50，封顶 100）

这一组完全靠正则 RE_* 在 prompt 里数命中次数：

score = min(count × 50, 100)

也就是命中 0 次 = 0 分、1 次 = 50 分、2 次 = 100 分（封顶），3+ 次还是 100。


*************
## 组 B：基于 prompt 长度（结构特征）

### token_estimate

// smart_routing.rs:521
let char_count = prompt.len();
let token_score = ((char_count as i32 - 20).max(0) as f32 / 5.0).min(100.0) as u32;

公式：

score = clamp((char_count - 20) / 5, 0, 100)

也就是：

- < 20 chars → 0 分
- 20 chars → 0 分
- 25 chars → 1 分（几乎 0）
- 100 chars → 16 分
- 200 chars → 36 分
- 520 chars → 100 分（封顶）
- > 520 chars → 100 分（封顶）

逻辑简单粗暴：字符数越长，越怀疑复杂。20 chars 以下不扣分（短消息不一定是简单任务，但权重低），超过 520 chars 满分（一定复杂）。

> 注意这是字符数，不是 token 数。prompt.len() 是 byte length（如果是 ASCII 字符等价于字符数；中文等多字节字符会被高估）。


*******************

clauses = 逗号数 + 分号数 × 2 + 从句连词数
score = min(clauses × 12, 100)

RE_CONJUNCTIONS：and / but / or / however / therefore / because / although / while / whereas / moreover / furthermore。

分号权重是逗号的 2 倍，因为分号通常意味着真正的语义切分。and / or / but 也算一句之内。

举例：

Prompt                                     逗号    分号    连词    clauses     分                                                                                                                     
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━  ━━━━━━  ━━━━━━  ━━━━━━━━━  ━━━━━
hi                                            0       0       0          0      0
─────────────────────────────────────────  ──────  ──────  ──────  ─────────  ─────
first, then, finally                          2       0       0          2     24
─────────────────────────────────────────  ──────  ──────  ──────  ─────────  ─────
a; b; c; d; e                                 0       4       0          8     96
─────────────────────────────────────────  ──────  ──────  ──────  ─────────  ─────
a, and b, because c, while d, however e       4       0       4          8     96

***********************


## 一个细节：为什么大部分系数是 × 50

因为 2 次命中就满分，对短 prompt + 多个信号词这种典型场景比较友好——只要消息里命中 2 个相关词，就打到 100 分。考虑：

- 太敏感（× 10）：5 次命中才满分，可能漏判
- 太迟钝（× 100）：1 次命中就满分，可能过激
- × 50 是经验中间值

ambiguity 用 × 25 是因为 RE_VAGUE 的关键词（it/this/that/something）太常出现，几乎所有 prompt 都会命中几次，需要更宽容。

## 一句话总结

> 13 个维度里有 9 个用「正则命中数 × 系数」算分（系数大多 50，ambiguity 25），2 个走长度（token_estimate 按字符数线性），2 个走复合结构特征（question_complexity 看问号 + 开放式词，sentence_complexity
> 看逗号/分号/连词）。每个 0-100，乘权重求和得总分。

3. Failover

failure_threshold 是「连续失败多少次就进冷却」（默认 3），cooldown_duration 是「冷却期持续多久」（默认 300s）。冷却期内 provider 直接跳过、成功一次立刻解封；全部 provider 都冷却时会挑最早进入的那个
> 硬试一次作为安全网。

        let fallback: Arc<dyn LlmProvider> = if retry_config.max_retries > 0 {
            Arc::new(RetryProvider::new(fallback, retry_config.clone()))
        } else {//也包重试了

        let cooldown_config = CooldownConfig {
            cooldown_duration: std::time::Duration::from_secs(config.nearai.failover_cooldown_secs),
            failure_threshold: config.nearai.failover_cooldown_threshold,
        };
        Arc::new(FailoverProvider::with_cooldown(
            vec![llm, fallback],//((smart,cheap), fallback)
            cooldown_config,
        )?)

pub struct FailoverProvider {
providers: Vec<Arc<dyn LlmProvider>>,候选 provider 列表（primary 在前）
/// Index of the provider that last handled a request successfully.
/// Used by `model_name()` and `cost_per_token()` so downstream cost
/// tracking reflects the provider that actually served the request.
last_used: AtomicUsize,「最近一次成功」= 跨请求全局视角
/// Per-provider cooldown tracking (same length as `providers`).
cooldowns: Vec<ProviderCooldown>,每个 provider 各自的失败计数 + 冷却时间戳
/// Reference instant for computing elapsed nanos. Shared across all
/// cooldown timestamps so they are comparable.
epoch: Instant,构造时间锚，所有冷却时间戳共用
/// Cooldown configuration.
cooldown_config: CooldownConfig,冷却策略（threshold + duration）
/// Request-scoped provider index keyed by Tokio task ID.
///
/// This allows `effective_model_name()` to report the provider that handled
/// the *current* request, even when other concurrent requests update
/// `last_used`.
provider_for_task: Mutex<HashMap<tokio::task::Id, usize>>, 「当前请求」实际用的 provider
}

    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let (provider_idx, response) = self
            .try_providers(|provider| {
                let req = request.clone();
                async move { provider.complete(req).await }
            })
            .await?;
        self.bind_provider_to_current_task(provider_idx);
        Ok(response)
    }

            .try_providers(|provider| {
// Step 1: 划分可用的 / 冷却中的
/ Step 2: 日志
// Step 3: 安全网——所有 provider 都在冷却时挑最早进入的那个硬试
// Step 4: 依次尝试 available 里的 provider
/ Step 6: 所有 available 都失败了，返回最后一次错误

4. CircuitBreakerProvider

        let cb_config = CircuitBreakerConfig {
            failure_threshold: threshold,//熔断上限
            recovery_timeout: std::time::Duration::from_secs(config.circuit_breaker_recovery_secs),//恢复秒数
            ..CircuitBreakerConfig::default()
        };

对内则维护一个三态状态机 Closed → Open → HalfOpen → Closed，用 tokio::Mutex 串行化状态变更。
- Closed（默认）：正常放行调用，并累计连续失败次数。
    - Open（熔断）：任何调用在 check_allowed 阶段就被拒掉，直接返回 LlmError::RequestFailed，错误里带"连续失败 N 次、还需 X 秒恢复"。这段时间内不再碰后端。
    - HalfOpen（探测）：过了 recovery_timeout（默认 30 秒）之后，放过一个探测调用进来：
        - 探测成功并累计到 half_open_successes_needed（默认 2 次）→ 回到 Closed。
        - 探测失败 → 立刻重新进入 Open，重新计时。

pub struct CircuitBreakerConfig {
/// Consecutive transient failures before the circuit opens.
pub failure_threshold: u32,
/// How long the circuit stays open before allowing a probe.
pub recovery_timeout: Duration,
/// Successful probes needed in half-open to close the circuit.
pub half_open_successes_needed: u32,
}

pub struct CircuitBreakerProvider {
inner: Arc<dyn LlmProvider>,
state: Mutex<BreakerState>,
config: CircuitBreakerConfig,
}

5. CachedProvider

pub struct CachedProvider {
inner: Arc<dyn LlmProvider>,
/// `std::sync::Mutex` (not tokio) — never held across an `.await` point,
/// so blocking acquisition is safe and keeps `set_model()` synchronous.
cache: Mutex<HashMap<String, CacheEntry>>,
config: ResponseCacheConfig, 1h和1k条
/// Total `complete()` calls (hits + misses) for periodic stats logging.
request_count: AtomicU64,
/// Running total of cache hits, independent of entry lifecycle.
/// Never decremented on eviction, so `hit_rate_pct` in stats doesn't
/// drift down as entries expire or are LRU-evicted.
total_hit_count: AtomicU64,
}

key需要的字段
• cache_key 实际用到的字段就 5 个，我按它们在哈希流里的顺序列一下，并标注是否可选。

#      字段              类型                   是否可选    来源
━━━━━  ━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1      模型名            &str                   否          inner.effective_model_name(request.model.as_deref())
─────  ────────────────  ─────────────────────  ──────────  ──────────────────────────────────────────────────────
2      messages          Vec<ChatMessage>       否          request.messages → serde_json::to_string
─────  ────────────────  ─────────────────────  ──────────  ──────────────────────────────────────────────────────
3      max_tokens        Option<u32>            是          request.max_tokens（小端 4 字节）
─────  ────────────────  ─────────────────────  ──────────  ──────────────────────────────────────────────────────
4      temperature       Option<f32>            是          request.temperature（小端 4 字节）
─────  ────────────────  ─────────────────────  ──────────  ──────────────────────────────────────────────────────
5      stop_sequences    Option<Vec<String>>    是          request.stop_sequences（逐条字符串 + \x00 分隔）

6. SwappableLlmProvider重建链(runtime)

pub struct SwappableLlmProvider {
state: RwLock<ProviderSnapshot>,//读写锁守护
}

struct ProviderSnapshot {
inner: Arc<dyn LlmProvider>,
model_name: &'static str,
active_model_name: Arc<str>,
cost_per_token: (Decimal, Decimal),
cache_write_multiplier: Decimal,
cache_read_discount: Decimal,
}

    /// Replace the inner provider chain with a freshly rebuilt provider.
    /// Metadata is refreshed atomically in the same critical section.
    pub fn swap(&self, inner: Arc<dyn LlmProvider>) {
        let fresh = ProviderSnapshot::capture(inner);
        *write(&self.state) = fresh;
    }

    fn current(&self) -> Arc<dyn LlmProvider> {
        read(&self.state).inner.clone()
    }

    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.current().complete(request).await 所有请求内部的都先调current()获取锁
    }

    fn set_model(&self, model: &str) -> Result<(), LlmError> {
        // Hold the write lock across both the delegate call and the snapshot
        // refresh so a concurrent `swap()` cannot overwrite the just-updated
        // inner provider with a snapshot captured from an older one. Inner
        // `set_model` impls are synchronous (no `.await`), so holding a
        // std::sync lock across the call is safe.
        let mut guard = write(&self.state);
        guard.inner.set_model(model)?;
        let refreshed = ProviderSnapshot::capture(Arc::clone(&guard.inner));
        *guard = refreshed;
        Ok(())
    }

7. RecordingLlm

/// LLM provider decorator that records interactions into a trace file.
把所有流经的 LLM 请求/响应（含工具调用、HTTP 流量、内存快照）序列化进一个 JSON
trace 文件，专门用于后续回放测试。
pub struct RecordingLlm {
inner: Arc<dyn LlmProvider>,
steps: Mutex<Vec<TraceStep>>,// 每一次“循环步骤”的轨迹累积器。每条 TraceStep 通常包含当时的 messages、模型响应、tool calls 等。RecordingLlm 在每次循环结束后 steps.push(TraceStep { ... }),回放时按顺序还原一整轮 agent 行为。
prev_message_count: Mutex<usize>,// 上一次记录时 messages 的长度,用来在每一步里只输出 增量(新追加的 message),而不是每步都拷贝整个对话历史。这样 trace 文件更紧凑,也便于人眼看 diff。
output_path: PathBuf,// trace JSON 的落盘路径。可在 RecordingLlm::new 显式传入,或由环境变量 IRONCLAW_TRACE_OUTPUT 控制,默认形如 trace_20260619T103000.json。
model_name: String,// 写入 trace 顶部的模型名。它是 记录用的标签,并不真正改变 inner 的模型;默认是 "recorded-" + inner.model_name(),也可用 IRONCLAW_TRACE_MODEL_NAME 覆盖,方便区分不同回放用例。
memory_snapshot: Mutex<Vec<MemorySnapshotEntry>>,工作区 / 长期记忆 (workspace memory) 的快照列表。运行时会调用 take_memory_snapshot() 把当前相关 memory 文档抓进来,写进 trace。回放时用它复现“同一时刻 agent 看到了哪些 memory”,避免重放时检索结果不一
致。
http_interceptor: Arc<RecordingHttpInterceptor>,HTTP 出站请求的拦截器。LLM provider 在调用 OpenAI / Anthropic 等外部 API 时,会通过 http_interceptor() 拿到它,把请求 / 响应 含凭证脱敏后的版本记下来,这样 trace 能完整回放外部依赖,又不会泄漏 API
key。
}

pub struct TraceStep {
#[serde(skip_serializing_if = "Option::is_none")]
pub request_hint: Option<RequestHint>,
pub response: TraceResponse,
/// Tool results that appeared in the message context since the previous step.
/// During replay, the test harness can compare actual tool results against
/// these to verify tool output hasn't changed (regression detection).
#[serde(default, skip_serializing_if = "Vec::is_empty")]
pub expected_tool_results: Vec<ExpectedToolResult>,
}
******
pub struct RequestHint {
#[serde(skip_serializing_if = "Option::is_none")]
pub last_user_message_contains: Option<String>,
当前步骤录制时,最近一条 user 消息里的一段稳定子串。
- 录制时 (recording.rs 中 capture_new_messages) 会从最近一条 Role::User 消息里截取一个稳定前缀(会避开 UUID、时间戳、上一步的 mission id 等会变的部分),把它作为指纹。
- 回放时 (tests/support/trace_llm.rs 的 TraceLlm) 的匹配策略是:若 trace 队头的步骤有该 hint,且当前请求的 user 消息里包含这个子串,就选它;否则向前扫描,找到第一条 hint 能匹配的步骤。这样在多线程交错
执行(如 engine v2 mission 子线程和主线程并发调 LLM)时,各自能取到属于自己那一支的步骤。

      - 之所以是 “软校验” 而不是硬相等,是因为提示信息可能在两段运行之间略有变化;匹配失败只 hint_mismatches += 1 并打印警告,不会让测试直接挂掉。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_message_count: Option<usize>,
录制该步骤时,消息历史的长度下限。
- 回放时被当作 完整性检查:如果当前 messages 数量比 min_message_count 还少,说明前置上下文丢失了,记一次 hint_mismatches 并打印 WARN,提醒这条响应可能与预期不匹配。
- 它不参与挑选步骤,只做“事后软校验”,用于发现上下文被压缩/截断/丢失的情况。
}
### 录制时:从 user 消息里抽取稳定前缀

录制器 (recording.rs) 在 stable_hint_prefix 里只截稳定部分,扔掉会变的部分。两条规则:

规则 1:如果是工具结果 message,以第一个 : 结尾

完整 user 消息(实际发给 LLM 时的样子,这里 [Tool ... returned: ...] 是经过 sanitize 的 user 消息形式):

[Tool `mission_create` returned: {"mission_id": "8f3c-2026-06-19-aaaa", "status": "queued", "created_at": "2026-06-19T10:30:00Z"}]

稳定前缀(只保留到第一个冒号):

[Tool `mission_create` returned:

这个值会被写进 last_user_message_contains。注意:

- mission_id 这种 UUID、时间戳全被扔掉了 → 即使重新跑 mission_create 拿到新 ID,hint 仍然能匹配上,不会因为 ID 不同而失配。
- : 故意保留 → 防止它和别的 [Tool xxx ... 前缀撞车,保持唯一性。

规则 2:普通 user 消息 → 截到 80 字节(UTF-8 字符边界)

完整 user 消息:

请帮我把 README 里第 3 段的"安装步骤"翻译成英文,保持 markdown 格式,翻译完直接覆盖回原文件。

稳定前缀(前 80 字节,在字符边界截断,大致就是):

请帮我把 README 里第 3 段的"安装步骤"翻译成英文,保持 markdown 格式,翻译完直接覆盖回原文件。

(中文一句话常常不到 80 字节就结束了,这里就是原样保留。)

### 回放时:TraceLlm 怎么用它匹配

假设 trace 里有这样一步:

{
"request_hint": {
"last_user_message_contains": "[Tool `mission_create` returned:",
"min_message_count": 4
},
"response": { "type": "text", "content": "好的,任务已创建。" }
}

回放时,当前请求的最近一条 user 消息是:

[Tool `mission_create` returned: {"mission_id": "NEW-UUID-HERE", "status": "queued"}]

→ 在这条消息里找子串 "[Tool \mission_create` returned:"→ **能找到** → 把队头这步响应"好的,任务已创建。"` 返回给上层。

如果回放时 user 消息是别的工具结果,比如:

[Tool `mission_fire` returned: {"ok": true}]

→ 找不到 "mission_create" 子串 → 走 hint scan 向前找,实在找不到就退回到队头那步,并 hint_mismatches += 1 打 WARN。

### 测试里的最小例子

/tests/support_unit_tests.rs:2638 直接构造了一个 hint "hello",验证只要 user 消息里包含 "hello" 就能匹配上:

RequestHint {
last_user_message_contains: Some("hello".to_string()),
min_message_count: Some(1),
},

请求是 "say hello please" → 包含 "hello" → 命中 → 返回 "matched",hint_mismatches() 为 0。

把 "hello" 换成 "banana"、user 消息保持 "apple" → 匹配不上 → 但仍然返回响应(只是 hint_mismatches == 2,一条是 last_user_message_contains,一条是 min_message_count)。这就是“软校验”的含义 —— 匹配错也不阻
断,只警告。

## 一句话总结

last_user_message_contains 就是一段从 user 消息里挑出来的 “稳定指纹”(通常是 [Tool \xxx` returned:` 这种前半句,或者前 80 字节),用来在并发回放里把 trace 步骤正确地分发给对应的那一轮请求;它避开 UUID/时
间戳这些运行间会变的部分,所以“同一逻辑点”的步骤,在两次运行里仍然能匹配上。





****
pub enum TraceResponse {
Text {
content: String,
input_tokens: u32,
output_tokens: u32,
},
ToolCalls {
tool_calls: Vec<TraceToolCall>,
input_tokens: u32,
output_tokens: u32,
},
/// Marker for a user message that triggered subsequent LLM calls.
/// Not an LLM response — replay providers must skip these.
UserInput { content: String },
}
****
pub struct ExpectedToolResult {
pub tool_call_id: String,
pub name: String,
/// The full tool result content as it appeared in the message context.
pub content: String,
}
****
pub struct RecordingHttpInterceptor {
exchanges: Mutex<Vec<HttpExchange>>,
}
*****
pub struct HttpExchange {
pub request: HttpExchangeRequest,
pub response: HttpExchangeResponse,
}

pub struct TraceFile {
pub model_name: String,
/// Workspace memory documents captured before the recording session.
/// Replay should restore these before running the trace.
#[serde(default, skip_serializing_if = "Vec::is_empty")]
pub memory_snapshot: Vec<MemorySnapshotEntry>,
/// HTTP exchanges recorded during the session, in order.
/// Replay should return these instead of making real HTTP requests.
#[serde(default, skip_serializing_if = "Vec::is_empty")]
pub http_exchanges: Vec<HttpExchange>,
pub steps: Vec<TraceStep>,
}

    pub async fn flush(&self) -> Result<(), std::io::Error> {写入文件
        let steps = self.steps.lock().await;
        let memory_snapshot = self.memory_snapshot.lock().await;
        let http_exchanges = self.http_interceptor.take_exchanges().await;

        let trace = TraceFile {
            model_name: self.model_name.clone(),
            memory_snapshot: memory_snapshot.clone(),
            http_exchanges,
            steps: steps.clone(),
        };
        let json = serde_json::to_string_pretty(&trace).map_err(std::io::Error::other)?;
        tokio::fs::write(&self.output_path, json).await?;
        tracing::info!(
            steps = steps.len(),
            memory_docs = memory_snapshot.len(),
            path = %self.output_path.display(),
            "Flushed LLM trace recording"
        );
        Ok(())
    }




    /// Extract new user messages, tool results, and build request hint.
    ///
    /// Returns `(hint, tool_results)` where tool_results are new `Role::Tool`
    /// messages since the last call — these become `expected_tool_results` on
    /// the next step for replay verification.
    async fn capture_new_messages(//录制
        &self,
        messages: &[ChatMessage],
    ) -> (Option<RequestHint>, Vec<ExpectedToolResult>) {
        let mut prev_count = self.prev_message_count.lock().await;
        let current_count = messages.len();
        // After context compaction, the message list may shrink below
        // prev_count.  Clamp to avoid an out-of-bounds slice.
        let start = (*prev_count).min(current_count);

        let new_messages = &messages[start..];

        // Emit UserInput steps for new user messages
        let new_user_messages: Vec<&ChatMessage> = new_messages
            .iter()
            .filter(|m| m.role == Role::User)
            .collect();

        if !new_user_messages.is_empty() {
            let mut steps = self.steps.lock().await;
            for msg in &new_user_messages {
                steps.push(TraceStep {
                    request_hint: None,
                    response: TraceResponse::UserInput {
                        content: msg.content.clone(),
                    },
                    expected_tool_results: Vec::new(),
                });
            }
        }

        // Capture new tool result messages for expected_tool_results
        let tool_results: Vec<ExpectedToolResult> = new_messages
            .iter()
            .filter(|m| m.role == Role::Tool)
            .map(|m| ExpectedToolResult {
                tool_call_id: m.tool_call_id.clone().unwrap_or_default(),
                name: m.name.clone().unwrap_or_default(),
                content: m.content.clone(),
            })
            .collect();

        *prev_count = current_count;

        // Build request hint from last user message.
        //
        // The hint is used by `TraceLlm` for replay matching: a step is a
        // candidate when the *current* request's last user message contains
        // the hint as a substring. So the hint must be (a) distinctive enough
        // to identify the step and (b) free of values that change between
        // runs (UUIDs, timestamps, mission IDs from prior tool calls).
        //
        // Strategy: truncate at the first JSON-content boundary so we keep
        // the stable prefix (e.g. "[Tool `mission_create` returned:") and
        // drop the volatile payload (mission UUIDs, etc.). The fallback is
        // a hard cap at 80 bytes on a UTF-8 char boundary.
        let hint = messages
            .iter()
            .rev()
            .find(|m| m.role == Role::User)
            .map(|msg| {
                let hint_text = stable_hint_prefix(&msg.content);
                RequestHint {
                    last_user_message_contains: Some(hint_text),
                    min_message_count: Some(current_count),
                }
            });

        (hint, tool_results)
    }
********
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {调用llm
        let (hint, tool_results) = self.capture_new_messages(&request.messages).await;
        let response = self.inner.complete(request).await?;

        self.steps.lock().await.push(TraceStep {
            request_hint: hint,
            response: TraceResponse::Text {
                content: response.content.clone(),
                input_tokens: response.input_tokens,
                output_tokens: response.output_tokens,
            },
            expected_tool_results: tool_results,
        });

        Ok(response)
    }

信息够了。下面是一个**完整的、可对照代码**的运行实例。我把录制和回放两边都拆开,跟着 test 里 `validates_request_hints` / `hint_mismatch_warns_but_continues` 的真实调用流程走。

---

# 完整运行示例

## 场景

agent 处理一条 user 输入 `"say hello please"`,调一次 LLM,期望回复 `"matched"`。我们把这次交互**录制下来**,然后**用 trace 回放**,观察:

- `last_user_message_contains` 是怎么填的、怎么匹配的;
- `min_message_count` 是怎么填的、怎么软校验的;
- 不匹配时为什么不挂掉、只 `hint_mismatches += 1`。

测试代码在 [tests/support_unit_tests.rs:2633](/tests/support_unit_tests.rs:2633) 和 [tests/support_unit_tests.rs:2662](/tests/support_unit_tests.rs:2662)。这里把测试展开成一步步走读。

---

## 一、录制阶段(实际跑 agent 时发生的事)

假设 `RecordingLlm` 包着一个真的 provider。某次 agent 循环里,`messages` 长这样(在调用 `complete_with_tools` **之前**):

```text
messages = [
    ChatMessage { role: User, content: "say hello please" },
]
```

`RecordingLlm.complete_with_tools` 内部会先调 `capture_new_messages(messages)`。

### 1. `capture_new_messages` 做什么

逐行跟着 [recording.rs:947](/crates/ironclaw_llm/src/recording.rs:947) 看:

```rust
let mut prev_count = self.prev_message_count.lock().await;
let current_count = messages.len();        // = 1
let start = (*prev_count).min(current_count);  // 起步是 0,min(0,1) = 0
let new_messages = &messages[start..];     // 整段都是新追加的
```

`new_user_messages` 过滤出 user 角色:

```rust
let new_user_messages: Vec<&ChatMessage> = new_messages
    .iter()
    .filter(|m| m.role == Role::User)
    .collect();
// = 1 条 user
```

`steps.push(TraceStep { response: UserInput { ... }, .. })` —— **塞一个标记**:

```rust
steps.push(TraceStep {
    request_hint: None,
    response: TraceResponse::UserInput { content: "say hello please".into() },
    expected_tool_results: Vec::new(),
});
```

`tool_results` 过滤出 tool 角色(本场景没有):

```rust
let tool_results: Vec<ExpectedToolResult> = new_messages
    .iter()
    .filter(|m| m.role == Role::Tool)
    .map(|m| ExpectedToolResult { ... })
    .collect();
// = []
```

更新游标:

```rust
*prev_count = current_count;  // 0 → 1
```

构造 hint(`rev()` + `find`):

```rust
let hint = messages
    .iter()
    .rev()                              // 反向:[user "say hello please"]
    .find(|m| m.role == Role::User)     // 命中第一条
    .map(|msg| {
        let hint_text = stable_hint_prefix(&msg.content);
        // stable_hint_prefix 对非 "[Tool " 开头的 user 消息,就是 80 字节上限
        // "say hello please" 本身很短,原样保留
        RequestHint {
            last_user_message_contains: Some("say hello please".into()),
            min_message_count: Some(current_count),  // = 1
        }
    });
// Some(RequestHint { last_user_message_contains: "say hello please", min_message_count: 1 })
```

返回 `(Some(hint), [])`。

### 2. 调真实 LLM + 记录 step

`RecordingLlm` 继续走(伪代码):

```rust
let response = self.inner.complete_with_tools(messages, tools).await?;
let (hint, tool_results) = capture_new_messages(messages).await;

let mut steps = self.steps.lock().await;
steps.push(TraceStep {
    request_hint: hint,                                 // Some(hint)
    response: TraceResponse::Text {
        content: response.content.clone(),              // "matched"
        input_tokens: response.input_tokens,            // 10
        output_tokens: response.output_tokens,          // 5
    },
    expected_tool_results: tool_results,                // []
});
```

### 3. 最终内存里的 `steps`

```text
steps = [
    // 标记,回放时跳过
    TraceStep {
        request_hint: None,
        response: UserInput { content: "say hello please" },
        expected_tool_results: [],
    },
    // 真正的 LLM 调用
    TraceStep {
        request_hint: Some(RequestHint {
            last_user_message_contains: "say hello please",
            min_message_count: 1,
        }),
        response: Text { content: "matched", input_tokens: 10, output_tokens: 5 },
        expected_tool_results: [],
    },
]
```

### 4. 落盘

调 `flush()`(在 [recording.rs:919](/crates/ironclaw_llm/src/recording.rs:919)):

```rust
let steps = self.steps.lock().await;                // 上面这 2 条
let memory_snapshot = self.memory_snapshot.lock().await;
let http_exchanges = self.http_interceptor.take_exchanges().await;  // []

let trace = TraceFile {
    model_name: self.model_name.clone(),
    memory_snapshot: memory_snapshot.clone(),
    http_exchanges,
    steps: steps.clone(),
};
let json = serde_json::to_string_pretty(&trace).map_err(...)?;
tokio::fs::write(&self.output_path, json).await?;
```

落盘后,**内存里的 `steps` 仍然有 2 条**(没有 `clear`)。`memory_snapshot` 同理。只有 `http_interceptor.take_exchanges()` 会清空 HTTP 缓冲。

trace JSON 大致长这样(简化):

```json
{
  "model_name": "test-model",
  "memory_snapshot": [],
  "http_exchanges": [],
  "steps": [
    {
      "response": { "type": "user_input", "content": "say hello please" }
    },
    {
      "request_hint": {
        "last_user_message_contains": "say hello please",
        "min_message_count": 1
      },
      "response": { "type": "text", "content": "matched", "input_tokens": 10, "output_tokens": 5 }
    }
  ]
}
```

---

## 二、回放阶段(用同一个 trace 跑测试)

测试里直接构造 trace(不经过录制),代码是 [tests/support_unit_tests.rs:2633](/tests/support_unit_tests.rs:2633):

```rust
let trace = LlmTrace::single_turn(
    "test-model",
    "say hello please",
    vec![TraceStep {
        request_hint: Some(RequestHint {
            last_user_message_contains: Some("hello".to_string()),
            min_message_count: Some(1),
        }),
        response: TraceResponse::Text {
            content: "matched".to_string(),
            input_tokens: 10,
            output_tokens: 5,
        },
        expected_tool_results: Vec::new(),
    }],
);
let llm = TraceLlm::from_trace(trace);
```

`single_turn` 把这步包成 1 个 turn;`from_trace` 再把所有 turn 的 step 摊平进一个 `VecDeque`。本例只有 1 个 step(没有 `UserInput` 标记,因为是手写的)。

### 调用 `complete_with_tools`

```rust
let resp = llm
    .complete_with_tools(make_request("say hello please"))
    .await
    .unwrap();
```

`make_request` 构造:

```rust
ToolCompletionRequest::new(
    vec![ChatMessage::user("say hello please")],
    vec![],   // 无 tools
)
```

进入 [tests/support/trace_llm.rs:475](/tests/support/trace_llm.rs:475) 的 `next_step`:

```rust
let last_user_content: Option<String> = messages
    .iter().rev()
    .find(|m| matches!(m.role, Role::User))
    .map(|m| m.content.clone());
// = Some("say hello please")
```

走匹配策略(在 [tests/support/trace_llm.rs:506](/tests/support/trace_llm.rs:506)):

```text
steps = [TraceStep { hint.last_user_message_contains = "hello", min=1, response=Text("matched") }]

(1) Head fast path:
    step_matches(head, "say hello please")
        step_hint = "hello"
        last_user_content = "say hello please"
        "hello" 是不是 "say hello please" 的子串? YES
    → pop_front,选 head

(2)(3) 不需要走。
```

然后做 `min_message_count` 软校验([tests/support/trace_llm.rs:542](/tests/support/trace_llm.rs:542)):

```text
hint.min_message_count = 1
messages.len() = 1
1 < 1? NO
→ 不增加 hint_mismatches
```

模板替换:本例是 `Text` 响应,跳过 `ToolCalls` 分支。

返回 step,`TraceLlm` 把 `Text { content: "matched", ... }` 包装成 `LlmResponse` 返回。

### 测试断言

```rust
assert_eq!(resp.content.as_deref(), Some("matched"));   // ✅ 通过
assert_eq!(llm.hint_mismatches(), 0);                   // ✅ 通过
```

---

## 三、不匹配时:依然返回响应,只 WARN

测试 [tests/support_unit_tests.rs:2662](/tests/support_unit_tests.rs:2662):

```rust
let trace = LlmTrace::single_turn(
    "test-model",
    "apple",
    vec![TraceStep {
        request_hint: Some(RequestHint {
            last_user_message_contains: Some("banana".to_string()),
            min_message_count: Some(5),
        }),
        response: TraceResponse::Text {
            content: "still works".to_string(),
            input_tokens: 10,
            output_tokens: 5,
        },
        expected_tool_results: Vec::new(),
    }],
);
let llm = TraceLlm::from_trace(trace);

let resp = llm
    .complete_with_tools(make_request("apple"))   // 当前 user="apple"
    .await
    .unwrap();
```

走匹配:

```text
steps = [TraceStep { hint.last_user_message_contains = "banana", min=5, response=Text("still works") }]

(1) Head fast path:
    step_matches(head, "apple")
        step_hint = "banana"
        "banana" 是不是 "apple" 的子串? NO
    → 不选 head

(2) Hint scan:
    steps.len() = 1,扫描范围 (1..1) 为空
    → 找不到

(3) Legacy fallback:
    hint_mismatches += 1   ← ① "banana" 不在 "apple" 里
    eprintln!("[TraceLlm WARN] Request hint mismatch: expected last user message to contain \"banana\", got \"apple\"")
    pop_front → 选 head 这步

然后做 min_message_count 软校验:
    messages.len() = 1,min_message_count = 5
    1 < 5? YES
    hint_mismatches += 1   ← ② "expected >= 5 messages, got 1"
    eprintln!("[TraceLlm WARN] Request hint mismatch: expected >= 5 messages, got 1")

返回 step → 返回响应 "still works"
```

测试断言:

```rust
assert_eq!(resp.content.as_deref(), Some("still works"));   // ✅ 依然有响应
assert_eq!(llm.hint_mismatches(), 2);                       // ✅ 两次不匹配都记上
```

**这就是“软校验”的完整含义:**即使两个 hint 都不匹配,trace 仍然继续往下走、上层代码不会因为回放失败而崩溃,只是计数 + WARN。这种设计让 e2e 测试在 trace 略有漂移时仍然能跑,而不是“一个 hint 错就全挂”。

---

## 四、对应到 `RecordingLlm` 里

`RecordingLlm` 的录制端并不调用 `TraceLlm::next_step` —— 那个匹配逻辑只在**回放端**用。录制端只负责往 `steps` 里塞 step 并附上 hint;回放端 (`TraceLlm`) 才用 hint 决定“当前请求该用 trace 队列里的哪一步响应”。两端分工是:

| 阶段 | 谁 | 干什么 |
|---|---|---|
| 录制 | `RecordingLlm::complete_with_tools` | 调真实 LLM,把响应 + 这次调用前夕的 `RequestHint` 一起 push 进 `steps` |
| 录制 | `RecordingLlm::flush` | 把 `steps` / `memory_snapshot` / `http_exchanges` 克隆后写盘,**不清内存** |
| 回放 | `TraceLlm::from_trace` | 把 trace 摊平成 step 队列 |
| 回放 | `TraceLlm::next_step` | 按 head-match / hint-scan / legacy-fallback 挑步,跑完软校验,返回响应 |

---

## 五、把录制端和回放端拼起来的完整一帧

把上面所有片段拼起来,一次 agent 循环里的“录制 → 落盘 → 另一进程回放”长这样:

```text
[进程 A:真实运行]
  messages = [User("say hello please")]
  → capture_new_messages
    · push UserInput 标记
    · 构造 hint = { last_user_message_contains: "say hello please", min_message_count: 1 }
  → inner.complete_with_tools → "matched"
  → push 真实 step: Text("matched") + hint + expected_tool_results=[]
  → flush → 写 trace_20260619T103000.json
    (内存里 steps 仍然是 2 条,没清)

[进程 B:测试回放]
  TraceLlm::from_file("trace_...json")
    → step 队列 = [UserInput, Text("matched")]
  测试调用 complete_with_tools([User("say hello please")])
    → 跳过 UserInput
    → head-match: "say hello please" 含 "say hello please" ✅
    → 软校验: 1 ≥ 1 ✅
    → 返回 Text("matched")
  assert_eq!(resp.content.as_deref(), Some("matched"))   ✅
```

这就是 `RecordingLlm` + `TraceLlm` 完整的一轮运行。后面所有 agent 步骤都按这个套路循环往复 —— 每条 user 消息打一个 `UserInput` 标记、每次 LLM 调用打一个 `Text` 或 `ToolCalls` step、每个 tool 结果合并到下一次调用的 `expected_tool_results` 里,直到 `flush()` 把整张时间线落盘,测试下次再 `from_file` 拿出来重放。

四. 重载句柄reload_handle(不在链上)

pub struct LlmReloadHandle {
primary: Arc<SwappableLlmProvider>,
cheap: Option<Arc<SwappableLlmProvider>>,
/// Serializes concurrent `reload()` calls so rapid setting toggles
/// don't fire overlapping chain rebuilds (each rebuild can touch OAuth
/// refresh and HTTP probes; letting them pile up wastes upstream quota
/// and leaves the wrapper briefly pointing at a half-built chain).
reload_lock: tokio::sync::Mutex<()>,
}

/// Rebuild the provider chain from `config` and atomically replace the
/// inner providers of the primary (and cheap, if present) wrappers.
///
/// Reloads are serialized so two concurrent callers cannot race.
pub async fn reload(
&self,
config: &crate::LlmConfig,
session: Arc<crate::SessionManager>,
) -> Result<(), LlmError> {
let _guard = self.reload_lock.lock().await;

        let components = crate::build_provider_chain_components(config, session).await?;

        self.primary.swap(components.primary);

        if let Some(ref cheap_handle) = self.cheap {
            let new_cheap = components
                .cheap
                .unwrap_or_else(|| self.primary.clone() as Arc<dyn LlmProvider>);
            cheap_handle.swap(new_cheap);
        } else if components.cheap.is_some() {
            // Asymmetry: no cheap wrapper was allocated at startup, so a
            // newly-configured cheap model cannot be activated via hot-reload.
            // Surfacing this through tracing so ops don't think the swap
            // silently took effect.
            tracing::warn!(
                "llm hot-reload: cheap provider is now configured but was not at startup; \
                 it will only take effect after a full restart",
            );
        }

        Ok(())
    }原子化重建主和cheap

五. TraceLLM（回放）

1. 结构

pub struct LlmTrace {
pub model_name: String,
pub turns: Vec<TraceTurn>,
按 user 消息切分的轮次。每个 TraceTurn = { user_input, steps, expects }。

- 这是回放的主驱动数据。TraceLlm::from_trace 把所有 turn 的 steps 摊平成一个 VecDeque 顺序消费(在 /tests/support/trace_llm.rs:430)。
- 用 turn 而不是单一扁平 step 数组,是因为 user 输入天然是时间线的边界;一层 turn 让 “这一步是回应哪条 user 消息” 在 JSON 里一目了然。
- JSON 向后兼容:老的扁平 "steps" 数组(没有 "turns")在反序列化时会被 LlmTrace 按 UserInput 边界自动切分成 turn,见 doc-comment。
  /// Workspace memory documents captured before the recording session.
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub memory_snapshot: Vec<MemorySnapshotEntry>,
  /// HTTP exchanges recorded during the session, in order.
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub http_exchanges: Vec<HttpExchange>,
  /// Declarative expectations for the whole trace (optional).
  #[serde(default, skip_serializing_if = "TraceExpects::is_empty")]
  pub expects: TraceExpects,
  /// Raw steps before turn conversion (populated only for recorded traces).
  /// Used by `playable_steps()` for recorded-format inspection.
  #[serde(skip)]
  #[allow(dead_code)]
  pub steps: Vec<TraceStep>,
  }

  /// Load a trace from a JSON file.
  pub fn from_file(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
  let contents = std::fs::read_to_string(path)?;
  let trace: Self = serde_json::from_str(&contents)?;
  Ok(trace)
  }

  async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
  // complete() is called when Reasoning has force_text=true (no tools
  // available). Skip any remaining ToolCalls steps in the trace and
  // return the next Text step, since in real usage the LLM would
  // produce text when no tools are offered.
  loop {
  let step = self.next_step(&request.messages, None)?;//从录制找到匹配的结果
  match step.response {
  TraceResponse::Text {
  content,
  input_tokens,
  output_tokens,
  } => {
  return Ok(CompletionResponse {
  content,
  input_tokens,
  output_tokens,
  finish_reason: FinishReason::Stop,
  reasoning: None,
  cache_read_input_tokens: 0,
  cache_creation_input_tokens: 0,
  });
  }
  TraceResponse::ToolCalls { .. } => {
  // Skip tool_calls steps — complete() is called in
  // force_text mode so the LLM can't use tools anyway.
  continue;
  }
  TraceResponse::UserInput { .. } => {
  return Err(LlmError::RequestFailed {
  provider: self.model_name.clone(),
  reason: "TraceLlm::complete() encountered a user_input step; \
  these should have been filtered out during construction"
  .to_string(),
  });
  }
  }
  }
  }


2. 理论





Trace 回放验证的不是 LLM,而是 agent 系统在改动后是否还按原有路径工作。

这是一个**架构层面**的问题,值得展开讲。IronClaw 这套 `RecordingLlm` + `TraceLlm` 不是普通的 “录个 log”,而是为了 **离线、确定、可回归测试**而设计的一整套 LLM 替身系统。

## 一、为什么需要这套机制

真实 LLM 调用有三个根本性难题,这套机制就是用来解决它们的:

### 1. 不确定性
- 真实模型每次返回的文本、tool call 参数、甚至**是否调用某个 tool**,都可能因为温度、版本、prompt 微小改动而不同。
- 断言 “agent 在这个 user 输入下返回 ‘Hello world’” 几乎注定不稳定 —— 哪怕只是升级模型版本,文本就变了。

### 2. 成本
- 一个 e2e 测试可能跑几十轮 LLM 调用(agent 循环 + tool 反馈循环),用真实模型 = 每次 `cargo test` 都消耗 token、花钱、慢。

### 3. 外部依赖
- 真实模型要联网、有 rate limit、有可用性问题,CI 网络抖动就会让测试 flaky。

`RecordingLlm` + `TraceLlm` 把 **真实模型从测试路径里彻底剔除**,换成 **录制好的固定响应**。

## 二、录制阶段解决什么

**目的**:把 “agent 在真实环境下跑一次” 的全过程 **冻结成一份 trace 文件**,作为后续回放的 “剧本”。

具体能力:

### a) 把 LLM 输出当成 fixture 固化
录制时调真实模型,把返回的 `content` / `tool_calls` / `input_tokens` / `output_tokens` **原样抄进 JSON**。之后所有回放都用这份 JSON,完全不依赖真实模型。

### b) 捕获非确定变量 → 模板化
录制时检测 tool call 参数里的 **跨步骤引用**(比如第二步 `mission_fire` 引用第一步 `mission_create` 产生的 `mission_id`),把字面值替换成模板 `{{call_id.field}}`。这样回放时即使 `mission_create` 产生新 ID,`mission_fire` 也能拿到对的 ID,跨运行对齐。

### c) 捕获上下文快照
- `memory_snapshot`:会话开始时 workspace memory 的样子 → 回放时哪怕真实 memory 被改了,也能复现 agent 当初看到的视野。
- `http_exchanges`:工具发的 HTTP 请求/响应(已脱敏)→ 回放时不再打外网,直接拿录制响应。

### d) 打指纹(用于并发回放)
每一步记 `request_hint.last_user_message_contains`(user 消息稳定前缀)和 `min_message_count`(消息总数下限),回放时用来挑步骤 + 软校验。

## 三、回放阶段解决什么

**目的**:让测试在 **完全离线、完全确定性** 的情况下重放 agent 行为,并且能精确断言。

### a) 离线、不耗 token、不依赖外网
CI 上跑测试不需要 OpenAI / Anthropic key,也不消耗配额。

### b) 确定性
trace 写定 → 响应就定 → agent 行为就定。多次运行字节级一致。

### c) 跨运行 ID 对齐
模板 `{{call_id.field}}` 在回放时被当前请求里 tool 结果的实际值替换,解决 “录制 ID ≠ 回放 ID” 的经典问题。

### d) 并发回放
mission 子线程 + 主线程交错执行时,hint-based matching 让各自挑到属于自己的步骤,不靠 “必须按录制顺序”,这是 engine v2 并发架构的必要支撑(在 [tests/support/trace_llm.rs:259](/tests/support/trace_llm.rs:259) doc-comment 里讲得很明确)。

### e) 软校验 + 异常退路
- `min_message_count` 不达标 → `hint_mismatches += 1` + WARN,但不阻断 —— 上下文丢失能被看到,但不会让整条测试链断掉。
- 完全没有 hint 匹配(规则 3 fallback) → 仍然返回队头响应,WARN,响应正常 —— 兼容老 trace,允许 trace 漂移。

### f) 多档断言能力
- **值断言**:`assert_eq!(resp.content.as_deref(), Some("matched"))` —— 直接比对回放值。
- **请求快照**:`captured_requests[i]` 看第 i 次调用发出去的完整 messages,验证 dispatcher 给模型发了什么。
- **Tool 表面快照**:`captured_tool_definitions[i]` 看第 i 次调用暴露了哪些 tool,验证 runtime policy filtering。
- **声明式期望**:`expects.response_contains` / `tools_used` / `tools_order` 等,跨多步骤聚合断言。

## 四、这套机制典型用在哪些场景

### 1. e2e / Tier B 测试
agent 跑真实场景(几十轮 LLM + tool 调用),期望最终结果符合 `expects`。录制一次,反复回放验证不回归。

### 2. Tool 覆盖测试
[tests/e2e_builtin_tool_coverage.rs](/tests/e2e_builtin_tool_coverage.rs) 用 trace 验证每个 builtin tool 在典型场景下被正确调用 —— 不靠真实模型,纯靠回放验证 agent 决策路径。

### 3. Runtime 行为回归
dispatcher 给模型发了哪些 tool 定义、worker 暴露了哪些 tool(`captured_tool_definitions`)—— 这种 “agent 内部行为” 不能靠最终文本验证,只能靠录制时记下的请求快照 + 回放时比对。

### 4. 并发子线程路径
engine v2 mission 线程和主线程交错 —— 没有 hint-based matching 的话,光靠线性回放根本对不齐。trace 让两支线程各自的 LLM 调用都能复现。

### 5. PR 评审 / Debug
trace JSON 是 **可读的、有结构的**(有 `UserInput` 标记、有 step 序列、有 hint),看 trace 就能知道 agent 当初为什么做这个决策,不用跑实际环境。

## 五、和传统 “mock LLM” 的本质区别

| | 传统 mock | TraceLlm |
|---|---|---|
| 响应来源 | 测试代码手写每个响应 | 录制时从真实模型抄,或手写 fixture |
| 状态 | 无状态 | 有完整 step 队列、hint、expectations |
| 并发 | 难做(谁拿哪个 mock?) | hint-based matching 自动分配 |
| 跨步骤对齐 | 手写脚本把 ID 接起来 | 模板 `{{key.field}}` 自动接 |
| 软校验 | 无 | `min_message_count` + `hint_mismatches` |
| 上下文快照 | 无 | memory_snapshot + http_exchanges |
| 录制成本 | 0(本来就手写) | 跑一次录制,但回放时无限复用 |

简单说:**传统 mock 是 “每个测试写一次假响应”,trace 回放是 “跑一次真场景,后续无限次离线复用”**。前者适合单元测试,后者适合 e2e / Tier B / 并发路径。

## 六、一句话总结

**录制** 把 “agent + 真实 LLM” 的一次运行变成 **冻结的、可读的 JSON**,**回放** 让测试在不依赖真实模型、不耗 token、不联网、完全确定的前提下,验证 agent 行为、tool 表面、并发路径、跨步骤 ID 对齐这些 “真实 LLM 测试根本做不稳” 的事。配合 hint 匹配 + 模板替换 + 软校验 + 声明式 `expects`,它把 LLM 调用从测试的 “不确定来源” 变成了 “可断言的 fixture”,这才是它的核心价值。

