一. 相关类型

1. SkillRegistry

/// Registry of available skills.
pub struct SkillRegistry {
/// All loaded skills.  skills: Vec<LoadedSkill>

已加载的全部技能列表。
- 类型 LoadedSkill = 加载完毕的技能对象(包含名字、内容、信任级别、元数据等)
- 这是注册表对外的"成品库",工具调用 / 提示词拼装都从这里取
- 加载流程:discover_all() 扫描下方四个目录 → 解析 SKILL.md → 放进这个 Vec
  skills: Vec<LoadedSkill>,

  /// User skills directory (~/.ironclaw/skills/). Skills here are Trusted.
  user_dir: PathBuf, 用户技能目录,对应 ~/.ironclaw/skills/。
- 信任级别:Trusted(完全信任)
- 含义:你自己放进去的技能 → 拥有全部工具权限,不受限制
- 使用场景:用户自己写技能、调试、定制提示词

  /// Registry-installed skills directory (~/.ironclaw/installed_skills/). Skills here are Installed.
  installed_dir: Option<PathBuf>, installed_dir: Option<PathBuf>

从注册表安装来的技能目录,对应 ~/.ironclaw/installed_skills/。
- 信任级别:Installed(已安装,受限)
- Option 的原因:可能没安装任何第三方技能
- 含义:从注册表(类似应用商店)下载的技能 → 只读工具权限,不能写文件 / 调危险 API

  /// Optional workspace skills directory.
  workspace_dir: Option<PathBuf>,  项目工作区里的技能目录。
- Option 的原因:不是所有部署都有 workspace 概念(没 DB 时就没 workspace)
- 含义:跟随某个项目/工作区的技能,跟用户和全局技能分开
- 信任级别通常也是 Trusted(用户/项目自己的)

  /// Bundled skill content compiled into the binary (name, raw SKILL.md content).
  /// Loaded as Trusted at lowest discovery priority.
  bundled_content: &'static [(String, String)],  bundled_content: &'static [(String, String)]

编译进二进制里的内置技能——一组 (技能名, SKILL.md 原始内容) 的静态切片。

- &'static:程序整个生命周期都有效,生命周期跟二进制同寿
- 通过 include_str!() 在编译期把 .md 文件塞进二进制,运行时无需读盘
- 信任级别:Trusted
- 优先级最低:最后被发现、最后被选用
- 使用场景:IronClaw 自带的开箱即用技能,比如"如何用工具"、"如何回复格式"等

  /// Maximum recursion depth for bundle directory scanning (default: 3).
  max_scan_depth: usize, max_scan_depth: usize
  扫描目录时的最大递归深度,默认 3。
- 防止无限递归(技能目录嵌套技能目录时不会爆栈)
  }

2. LoadedSkill

/// A fully loaded skill ready for activation.
#[derive(Debug, Clone)]
pub struct LoadedSkill {
/// Parsed manifest from YAML frontmatter.
pub manifest: SkillManifest,
/// Raw prompt content (markdown body after frontmatter).
pub prompt_content: String,
/// Trust state (determined by source location).
pub trust: SkillTrust,
/// Where this skill was loaded from.
pub source: SkillSource,
/// SHA-256 hash of the prompt content (computed at load time).
pub content_hash: String,
/// Pre-compiled regex patterns from activation criteria (compiled at load time).
pub compiled_patterns: Vec<Regex>,
/// Pre-computed lowercased keywords for scoring (avoids per-message allocation).
/// Derived from `manifest.activation.keywords` at load time — do not mutate independently.
pub lowercased_keywords: Vec<String>,
/// Pre-computed lowercased exclude keywords for veto scoring.
/// Derived from `manifest.activation.exclude_keywords` at load time.
pub lowercased_exclude_keywords: Vec<String>,
/// Pre-computed lowercased tags for scoring (avoids per-message allocation).
/// Derived from `manifest.activation.tags` at load time — do not mutate independently.
pub lowercased_tags: Vec<String>,
}

pub enum SkillTrust {
/// Registry/external skill. Read-only tools only.
Installed = 0,
/// User-placed skill (local or workspace). Full trust, all tools available.
Trusted = 1,
}

pub enum SkillSource {
/// Workspace skills directory (<workspace>/skills/).
Workspace(PathBuf),
/// User skills directory (~/.ironclaw/skills/).
User(PathBuf),
/// Registry-installed skills directory (~/.ironclaw/installed_skills/).
Installed(PathBuf),
/// Bundled with the application.
Bundled(PathBuf),
}

二. 原理

1. tool

   /// Register skill management tools (list, search, install, remove).
   ///
   /// These allow the LLM to manage prompt-level skills through conversation.
   pub fn register_skill_tools(
   &self,
   registry: Arc<std::sync::RwLock<SkillRegistry>>,
   catalog: Arc<SkillCatalog>,
   ) {
   self.register_sync(Arc::new(SkillListTool::new(Arc::clone(&registry))));
   self.register_sync(Arc::new(SkillSearchTool::new(
   Arc::clone(&registry),
   Arc::clone(&catalog),
   )));
   self.register_sync(Arc::new(SkillInstallTool::new(
   Arc::clone(&registry),
   Arc::clone(&catalog),
   )));
   self.register_sync(Arc::new(SkillRemoveTool::new(registry)));
   tracing::debug!("Registered 4 skill management tools");
   }

2. 调用

  ---
阶段 0:启动期一次性扫盘(app.rs:1301)

目标:把所有"装好的" skill 从磁盘全读进内存,不筛选。

SkillRegistry::new(local_dir)                       // ~/.ironclaw/skills/
.with_installed_dir(installed_dir)              // ~/.ironclaw/installed_skills/
.with_bundled_content(load_bundled_skills())    // 编译进二进制
let loaded = registry.discover_all().await

discover_all() 按 4 个目录全扫:
- workspace 目录(项目级)
- user 目录(~/.ironclaw/skills/,Trusted)
- installed 目录(~/.ironclaw/installed_skills/,Installed)
- bundled(编译期塞进二进制的内置)

每个文件:
1. 读文件 → 解析 frontmatter(元数据) + 正文(prompt_content)
2. 过 gating(依赖检查,比如"必须装了 python3")
3. token 预算
4. 算 hash

→ 全部塞进 registry.skills: Vec<LoadedSkill>(内存里)

注意:这一步没筛选,所有合法 skill 都加载,只是"挂在那里待命"。

  ---
阶段 1:用户发消息,触发评分(agent_loop.rs:891) //自动触发

目标:从内存里"挑"出这次回复要用的 skill。

每条消息都跑一遍 select_active_skills(message, user_id):

Phase 1 — 显式激活

正则扫消息里有没有 /skill-name 这种强制指定:
extract_skill_mentions(message, &available)
- 命中 → 直接进候选,无视后续评分
- 同时把消息文本里的 /skill-name 抠掉,避免污染评分

Phase 2 — 评分筛子集

对剩下的 skill 跑确定性评分(纯字符串匹配,不调 LLM):

┌──────────────────┬───────────────────────────┐
│       命中       │           加分            │
├──────────────────┼───────────────────────────┤
│ 关键字精确匹配   │ +10(封顶 30)              │
├──────────────────┼───────────────────────────┤
│ 关键字子串匹配   │ +5(封顶 30)               │
├──────────────────┼───────────────────────────┤
│ 标签匹配         │ +3(封顶 15)               │
├──────────────────┼───────────────────────────┤
│ 正则匹配         │ +20(封顶 40)              │
├──────────────────┼───────────────────────────┤
│ exclude_keywords │ 直接 0 分(否决)           │
├──────────────────┼───────────────────────────┤
│ requires.skills  │ 链式拉同伴(深度 1,非传递) │
└──────────────────┴───────────────────────────┘

然后套两道硬性上限:
- max_active_skills(数量)
- max_context_tokens = 4000(token 预算)

→ 输出 active_skills: Vec<&LoadedSkill>,就是这次回复要用到的子集。

  ---
阶段 2:拼 XML 注入系统提示(dispatcher.rs:234)

目标:把选中的 skill "正文"塞进 LLM 看得见的提示词。

for skill in &active_skills {
let safe_content = escape_skill_content(&skill.prompt_content);
context_parts.push(format!(
"<skill name=\"{}\" version=\"{}\" trust=\"{}\">\n{}\n</skill>",
safe_name, safe_version, trust_label, safe_content,
));
}
reasoning = reasoning.with_skill_context(ctx);

- prompt_content = SKILL.md 里的"正文"部分(frontmatter 之外的内容)
- XML 转义防逃逸(防止 SKILL.md 写 </skill> 伪造 trust 级别)
- 拼成一段大 XML,塞进 system prompt

  ---
阶段 3:按 trust 衰减工具集(dispatcher.rs:314)

let initial_tool_defs = if !active_skills.is_empty() {
crate::skills::attenuate_tools(&initial_tool_defs, &active_skills).tools
} else { initial_tool_defs };

含义:
- Trusted skill → 不动工具集
- Installed skill → 按 skill 自己的 attenuate_tools 配置,砍掉某些工具(比如屏蔽 shell、file_write)

→ trust 模型在这里落地:来自注册表的 skill 看到更少的工具。

  ---
阶段 4:LLM 推理,生成回复

现在 LLM 看到的是:
- 基础 system prompt
    - <skill>...</skill> XML 块(只含命中的子集)
    - 砍过的工具列表

按这个上下文生成回复。

  ---
回到你的问题:是渐进式加载吗?

半对半错:

┌────────────┬─────────────────────────────────────────────┐
│    阶段    │               是不是"渐进式"                │
├────────────┼─────────────────────────────────────────────┤
│ 启动扫盘   │ ❌  全量加载,所有合法 skill 都进内存         │
├────────────┼─────────────────────────────────────────────┤
│ 每消息筛选 │ ✅  这部分是渐进式——只把命中的子集注入提示词 │
├────────────┼─────────────────────────────────────────────┤
│ 评分规则   │ ✅  每次都跑,不缓存,完全确定性               │
├────────────┼─────────────────────────────────────────────┤
│ 工具衰减   │ ✅  只对当前 active_skills 生效              │
└────────────┴─────────────────────────────────────────────┘

真正的"渐进"体现在两条限制:
1. Token 预算 4000:命中再多,正文总量超过 4000 token 就被截断
2. 数量上限 max_active_skills:命中再多,最多取前 N 个

→ 所以"渐进"不是"按需读盘"(那是 lazy loading),而是"按需筛选 + 按预算截断"。

  ---
全链路一句话总结

▎ 启动扫全量入内存 → 每条消息用关键词评分挑子集 → 拼 XML 注入 system prompt → 按 trust 砍工具 → LLM 推理。"渐进"指的是提示词注入量受 token 预算约束,不是磁盘读取层面。

