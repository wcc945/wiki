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
steps: Mutex<Vec<TraceStep>>,// 主录制：每一步的请求/响应
prev_message_count: Mutex<usize>,// 跟踪消息数变化，避免重复记录 user_input
output_path: PathBuf,// trace 文件输出路径
model_name: String,// trace 文件里标记用的模型名
memory_snapshot: Mutex<Vec<MemorySnapshotEntry>>,
http_interceptor: Arc<RecordingHttpInterceptor>,
}



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

