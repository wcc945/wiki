一. 介绍

1. 介绍

//CLI命令主要是通过clap 的 #[derive(Subcommand)]按子命令解析去实现

//首先解析然后去匹配command的枚举
let cli = Cli::parse();
let enable_non_cli = non_cli_channels_enabled(cli.cli_only);

    // Handle non-agent commands first (they don't need full setup)
    match &cli.command {
        Some(Command::Tool(tool_cmd)) => {
            init_cli_tracing();
            return run_tool_command(tool_cmd.clone()).await;//当匹配到时，直接执行对应逻辑，然后结束本次运行。
        }

2. 涉及的类

#[command(version)]
#[command(color = ColorChoice::Auto)] // Enable auto-color for help (if the terminal supports it)
pub struct Cli {//Cli类，主要是下面的命令参数
#[command(subcommand)]
pub command: Option<Command>, ****//命令

Command枚举，对应不同的子命令，最主要的是下面两个子命令：运行和首次配置
pub enum Command {
/// Run the agent (default if no subcommand given)
Run,

    Onboard {
        /// Skip authentication (use existing session)
        #[arg(long)]
        skip_auth: bool,

        /// Reconfigure channels only
        #[arg(long, conflicts_with_all = ["provider_only", "quick", "step"], help = "Deprecated: use --step channels")]
        channels_only: bool,

        /// Reconfigure LLM provider and model only
        #[arg(long, conflicts_with_all = ["channels_only", "quick", "step"], help = "Deprecated: use --step provider")]
        provider_only: bool,

        /// Quick setup: auto-defaults everything except LLM provider and model
        #[arg(long, conflicts_with_all = ["channels_only", "provider_only", "step"])]
        quick: bool,

        /// Run only specific setup steps (comma-separated: provider, channels, model, database, security)
        #[arg(long, value_delimiter = ',', conflicts_with_all = ["channels_only", "provider_only", "quick"])]
        step: Vec<String>,
    },


}

二. Onboard做了什么

1. 创建引导

{
let config = SetupConfig {//入参
skip_auth: *skip_auth,
channels_only: *channels_only,
provider_only: *provider_only,
quick: *quick,
steps: step.clone(),
};
let mut wizard =
SetupWizard::try_with_config_and_toml(config, cli.config.as_deref())?;//加载配置config和setting toml文件得到引导配置
wizard.run().await?;//执行初始化
}

2. ownerId





本地开发不设 IRONCLAW_OWNER_ID 也不写 TOML → 就是 "default"；多租户部署在环境变量或 TOML 配置文件里指定 IRONCLAW_OWNER_ID / owner_id。

主要是通过这个方法解决身份：不从db查：解决先有鸡还是先有蛋的问题(去db查都需要ownerId)
pub(crate) fn resolve_owner_id(settings: &Settings) -> Result<String, ConfigError> {
let env_owner_id = self::helpers::optional_env("IRONCLAW_OWNER_ID")?;//通过完整的 env 查找链（真实 env > runtime overlay > injected secrets），优先级最高。
let settings_owner_id = settings.owner_id.clone();//TOML 配置文件里的 owner_id 字段。
let configured_owner_id = env_owner_id.clone().or(settings_owner_id.clone());

    let owner_id = configured_owner_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "default".to_string());//兜底 "default" — 什么都不配就是它。

    if owner_id == "default"
        && (env_owner_id.is_some()
            || settings_owner_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()))
    {
        WARNED_EXPLICIT_DEFAULT_OWNER_ID.call_once(|| {
            tracing::warn!(
                "IRONCLAW_OWNER_ID resolved to the legacy 'default' scope explicitly; durable state will keep legacy owner behavior"
            );
        });
    }

    Ok(owner_id)
}

三. wizard.run()





设置完会持久化到db，若已有设置，则会加载。

1. step_database()





选择并连接数据库后端——交互式让用户选 PostgreSQL 还是 libSQL（除非环境变量 DATABASE_BACKEND 已指定），收集连接信息（URL/路径），然后把 DATABASE_URL / DATABASE_BACKEND 写入 ~/.ironclaw/.env 持久化。

            // Step 1: Database
            print_step(1, total_steps, "Database Connection");
            self.step_database().await?;

            let step1_settings = self.settings.clone();
            self.try_load_existing_settings().await;
            self.settings.merge_from(&step1_settings);

            self.persist_after_step().await;//每一步后都写回数据库

2. Security





加密密钥存储

step_security 是初始化主密钥——用来加解密 DB secrets 表里存的 API key/token。提供三种存储方式：

1. OS Keychain（macOS/Linux 系统级凭据管理，推荐本地用）
2. 环境变量 SECRETS_MASTER_KEY（CI/Docker 用，密钥存 ~/.ironclaw/.env）
3. 跳过（不用 secrets 功能）

密钥查找优先级：已有的 env var > OS keychain 里已有的 > 交互生成新的。选哪种方式就把 secrets_master_key_source 设为对应值、并立即初始化 SecretsCrypto 实例，后续 step 就能用这个 key 加密凭据了。

3. step_inference_provider





设置llm provider



从provider.json里加载各个产商的需要的配置，然后从环境变量或用户输入。得到llm，然后加密存储到db中(secretStore)

4. Model selection





选择模型

从 provider API 拉取可用模型列表，让用户选一个作为默认模型。流程：

1. 已有模型 → 问"保留还是改"
2. 根据刚选的 llm_backend 走不同分支：
    - OpenAI / Anthropic / OpenRouter 等 → 调 provider API 实时拉模型列表（顺便起到验证 API key 有效的作用——拉不下来说明 key 错了）
    - Ollama → 调本地 ollama list
    - Bedrock / NEAR AI / Gemini → 用硬编码的模型清单
    - 其他 → 让用户手动输入模型 ID

3. 用户上/下键选一个，写入 self.settings.selected_model

总结：setup 收集 API key（step 3），选模型时实际调 API 拉列表（step 4）——如果 key 无效，这里就会报错，所以不需要单独的"验证连接"步骤。

5. Embeddings





是否启用语义搜索

6. Channel





setup_tunnel_tailscale 仅收集用户选择的隧道信息
先初始化隧道，为webhook等提供ip。

隧道（tunnel）是这样绕过这个限制的：

你的机器                           隧道服务器（ngrok/Cloudflare）              外部服务
|                                     |                                     |
|--- 1. 主动建立长连接 (outbound) --->|                                     |
|                                     |                                     |
|                                     |<--- 2. webhook POST 过来 -----------|
|<-- 3. 通过现有连接转发给你 ---------|                                     |

关键点是第 1 步是你主动发起的出站连接 — 这不受 NAT/防火墙限制。隧道服务器的公网 IP 是真实可路由的，外部服务打到它，它再沿着你已经建好的那条连接把数据转发回你的本地进程。

        let mut discovered_channels = discover_wasm_channels(&channels_dir).await; 发现 ~/.ironclaw/channels/cap.json能力文件，已经安装的
        // Build channel list from registry (if available) + bundled + discovered, 
        //Bundled（available_channel_names() — 随二进制分发的预编译 channel）
        //Registry（load_registry_catalog() — 远端 registry 清单里的 channel）
        let wasm_channel_names = build_channel_options(&discovered_channels); 
得到最终channel列表让用户去选

registry：
mcp、channel、tools，存在一个清单json
"source": {
"dir": "channels-src/feishu",
"capabilities": "feishu.capabilities.json",
"crate_name": "feishu-channel"
},
"artifacts": {
"wasm32-wasip2": {
"sha256": "edddd28d478a73add695315b99485cfb00656e1682b1f45e6f3a0a90c56e5378",
"url": "https://github.com/nearai/ironclaw/releases/download/ironclaw-v0.26.0/channel-feishu-0.2.2-wasm32-wasip2.tar.gz"
}
},
对于channel两种url路径去下载安装。

        // Build options list dynamically 可选的channel
        let mut options: Vec<(String, bool)> = vec![
            ("CLI/TUI (always enabled)".to_string(), true),
            (
                "HTTP webhook".to_string(),
                self.settings.channels.http_enabled,
            ),
            ("Signal".to_string(), self.settings.channels.signal_enabled),
        ];

        // HTTP channel
        if selected.contains(&CHANNEL_INDEX_HTTP) {
            println!();
            if let Some(ref ctx) = secrets {
                let result = setup_http(ctx).await?;//存储一个密钥验证，防止滥用
                self.settings.channels.http_enabled = result.enabled;
                self.settings.channels.http_port = Some(result.port);
            } else {
                self.settings.channels.http_enabled = true;
                self.settings.channels.http_port = Some(8080);
                print_info("HTTP webhook enabled on port 8080 (set HTTP_WEBHOOK_SECRET in env)");
            }
        } else {

//最后，根据配置的channel能力文件中的setup的值，交互式收集然后，存到db

遍历 selected_wasm_channels
│
▼
┌─────────────────────────────────────────────┐
│  discovered_by_name 有 cap_file？           │
├─────────────────────────────────────────────┤
│  ├─ 无 → print "channel 不可用"，跳过        │
│  │                                             │
│  └─ 有                                         │
│       │                                        │
│       ▼                                        │
│  required_secrets 非空？                      │
│       ├─ 空 → enabled=true, overrides=空       │
│       └─ 非空                                 │
│            │                                  │
│            ▼                                  │
│  setup_wasm_channel（交互式收集）             │
│       │                                        │
│       ├─ secret_input / auto_generate         │
│       ├─ save_secret 存 DB                   │
│       └─ 返回 WasmChannelSetupResult         │
└─────────────────────────────────────────────┘
│
▼
result.enabled？
├─ 否 → 下一 channel
└─ 是
│
├─ channel_name 加入 enabled_wasm_channels
│
▼
┌─────────────────────────────────────────┐
│  merge_wasm_channel_runtime_overrides_  │
│  for_channel(已有 overrides, config)    │
│       │                                  │
│       └─ 合并已有值 + 本次新值            │
│            │                              │
│            ▼                              │
│  遍历 channel_overrides                  │
│       │                                   │
│       ▼                                   │
│  key = wasm_channel_runtime_override_key │
│       (channel, config_key)              │
│       │                                   │
│       ▼                                   │
│  enabled_runtime_overrides[key] = value  │
└─────────────────────────────────────────┘
│
▼
所有 channel 遍历完毕
│
├─ settings.wasm_channels = enabled_wasm_channels
└─ settings.wasm_channel_runtime_overrides = enabled_runtime_overrides

setup 阶段：
wizard → setup_wasm_channel 收集 token → save_secret 加密存 DB
↓
enabled_wasm_channels → settings.wasm_channels（启动快照）
enabled_runtime_overrides → settings.wasm_channel_runtime_overrides
key 形如 "telegram:bot_username"

agent 启动阶段：
setup_wasm_channels()
→ 扫描 ~/.ironclaw/channels/，按 activated_channels 过滤
→ 对每个 channel 调用 register_startup_loaded_channels
→ 读 secrets（DB 解密）
→ load_wasm_channel_runtime_overrides 按 "channel:" 前缀拆出配置
→ build_runtime_config_updates 拼装 {tunnel.public_url, webhook_secret, owner_id, ...}
→ channel.update_config(config_updates) 注入到 WASM 进程
→ WASM channel 进程启动 → 调用 Telegram/Slack API 注册 webhook
（webhook URL = tunnel.public_url + "/webhook/{channel}"）

7. Extensions (tools)





同外部channel，基本一样。

8. Docker沙箱





第三方不可信扩展（channels/tools）  →  WASM 沙箱



LLM 触发的代码执行（shell/python/build）  →  Docker 沙箱



内置 Rust tool + 主机进程                  →  无隔离（受信代码）

```
        step_docker_sandbox 启动
              |
              v
       "Enable Docker sandbox?" (confirm)
         /              \
       否                 是
       |                  |
       v                  v
  sandbox.enabled   check_docker()
       = false           |
                  ┌──────┴──────┬──────────────┐
                  |             |              |
              Available    NotInstalled    NotRunning
                  |         (相同分支)         (相同分支)
                  v             |              |
          enabled = true        v              v
          print_success     区分 not_installed：
          ensure_worker_       - true  → "Docker is not installed."
          image()                 + install_hint()
                                 - false → "Docker is installed but not running."
                                   + start_hint()
              |                |
              |                v
              |           "Retry after install/starting?" (confirm)
              |                /              \
              |              否                是
              |              |                |
              |              v                v
              |      enabled = false   retry check_docker()
              |      提示之后           /         \
              |      SANDBOX_ENABLED     OK          not OK
              |      =true             /               \
              |                       v                 v
              |                enabled = true    enabled = false
              |                ensure_worker_   "Still not available"
              |                image()            提示
              |
              v
         Disabled 状态
              |
              v
        enabled = false (静默)
              |
              v
        sandbox.enabled 为 true?
         /          \
       否            是
       |             v
       |     step_claude_code_sandbox()
       |             |
       |             v
       |     "Enable Claude Code sandbox mode?" (confirm)
       |      /              \
       |    否               是
       |    |                v
       |    |        检查 ANTHROPIC_API_KEY 或 OAuth token
       |    |          /            \
       |    |       有              无
       |    |       |                v
       |    |       v          "Retry after setting up credentials?"
       |    |  enabled = true   /              \
       |    |  print_success  否                是
       |    |                   |                v
       |    |                   v          再次检查 has_credentials
       |    |             claude_code           /         \
       |    |             enabled = false     有           无
       |    |             提示                   v          enabled=false
       |    |                                 enabled=true
       |    v                                   print_success
       |  return Ok(())
       |
       v
   return Ok(())

9. Heartbeat





是否开



间隔



通知channel



最后收集到setting

10. .env





最后将需要存到.env的存下。

