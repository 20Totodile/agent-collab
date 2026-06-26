# AI 协作

> 在一台 Mac mini 上让多个 AI agent（Claude Code / Codex / 其他 CLI）一起干活的"工作群"系统。Web UI 看消息流 + 派任务 + 控制 agent 上下班；agent 是 server.ts 直接管理的长寿命 stream 子进程，跨多次对话维持上下文。

## 这是什么

把多个本地跑的 AI agent 当成"团队成员"组织起来：

- **工作群频道**：所有 agent 都能看到 + 按 @mention 路由响应
- **私聊频道**：每个 agent 一个独立 DM，PM 一对一对话
- **任务系统**：issue tracker 范式（draft → 评估 → 实施 → review → 验收 → closed），每个任务自带 activity feed + 角色 gate
- **控制面板**：web 端看每个 agent 在线 / 工作中 / 静止状态，手动上下班
- **终端面板**：直接从 web 看每个 agent 当前会话的结构化 transcript（user / assistant / 工具调用 / 工具结果）

## 架构概览

```
┌─────────────┐      ┌────────────────────────┐      ┌──────────────────┐
│   Web UI    │◀────▶│  Bun Server            │◀────▶│  Agent stream    │
│  (browser)  │ HTTP │  (port 3009)           │stdio │  subprocesses    │
└─────────────┘      │  + SQLite              │      │  - claude        │
                     │  + AgentProvider layer │      │  - codex         │
                     │  + HTTP polling        │      │  - …             │
                     └────────────────────────┘      └──────────────────┘
```

- **Server (`server.ts`)**：Bun HTTP server，跑在 3009。负责消息路由 / 任务状态机 / agent 状态追踪 / **provider 子进程生命周期管理**。SQLite 存所有历史（消息 / 任务 / 事件）。
- **Web (`web/workgroup-v2/`)**：静态 HTML + 原生 JS，2.5s polling。无 build step。
- **Agent**：每个 agent 通过一个 `AgentProvider` 实现接进 server —— 服务器直接 `spawn` 一个长寿命 CLI 子进程，双向 stdio NDJSON / JSON-RPC 通信。当前内置 `claude` (Anthropic Claude Code, stream-json) 和 `codex` (OpenAI Codex CLI, `app-server` JSON-RPC 2.0) 两个 provider。

**长寿命模式**：每个 agent 一个常驻进程，跨多次 `send()` 复用同一 session（claude `--resume <sid>` / codex `thread/resume`）。没有冷启动惩罚。代价是进程始终占着 context 内存。

## 安装

### 前置依赖

- macOS / Linux（开发只在 macOS 上验证过）
- [Bun](https://bun.sh) 1.0+
- 至少一个 CLI agent 二进制：
  - **Claude Code**：从 https://claude.com/claude-code 装；CLI 名 `claude`
  - **Codex CLI**：从 https://github.com/openai/codex 装；CLI 名 `codex`，0.142+

### 启动

```bash
git clone <your-fork-url> ai-collab
cd ai-collab
bun install
bun server.ts
```

打开 `http://localhost:3009/web/workgroup-v2/index.html`：浏览器首次 GET 会被 server 302 一次自动种 `aicollab_auth` cookie，直接进 UI 不需要登录或设 token。

server 第一次启动会自动 `openssl rand -hex 24` 生成 auth token，写到 `runtime-data/state/token.txt`（chmod 600）。重启沿用同一份。

> ⚠️ **不要把 server 直接绑 0.0.0.0 / 接 LAN / 套 tunnel 暴露公网** —— 默认 auto-token 模式下，任何能 ping 通 server 端口的人都能拿全部权限。要对外暴露的话：
> 1. 显式 `export AICOLLAB_AUTH_TOKEN=<强 token>` 自己管 token
> 2. 反代后面套一层独立 auth

### 常用 env

| 变量 | 默认 | 说明 |
|---|---|---|
| `AICOLLAB_AUTH_TOKEN` |（auto-generate） | Bearer token；不设则自动生成 + 持久化。手动设以多机器同步或公网暴露。 |
| `AICOLLAB_PORT` | `3009` | server 端口 |
| `AGENT1_PROVIDER` | `claude` | agent1 的 provider 类型：`claude` / `codex` / `tmux`（见下） |
| `AGENT2_PROVIDER` | `codex` | agent2 的 provider 类型（同上三选一） |
| `AGENT1_BINARY_PATH` |（PATH lookup） | 显式指定 agent1 的 CLI 二进制路径（默认在 `$PATH` 里找） |
| `AGENT2_BINARY_PATH` |（PATH lookup） | 同上 |
| `AGENT1_CWD` |（必填） | agent1 子进程的工作目录。`.claude/settings.json` / `AGENTS.md` / hooks / MCP 配置都从这里向上找——**配错 hooks 跟 effort 会被静默丢弃**。 |
| `AGENT2_CWD` |（必填） | 同上 |
| `AGENT1_EXTRA_ARGS` |（空） | 追加到 spawn 命令行的 args，按 shell-quoting 分割：`"--model claude-opus-4-7[1m] --add-dir /path"` |
| `AGENT2_EXTRA_ARGS` |（空） | 同上 |
| `AGENT1_PROJECT_DIR` |（auto-detect） | 仅 claude provider — 覆盖 `~/.claude/projects/<encoded-cwd>/` 自动派生，给 transcript endpoint 用 |
| `AGENT2_PROJECT_DIR` |（auto-detect） | 同上 |
| `AGENT1_HEARTBEAT_PATH` |（空） | agent1 自报 context tokens 的 JSON 路径（agent runtime 自己写） |

tmux provider 专用 env（`AGENT*_PROVIDER=tmux` 才会用上）见下面《支持的 provider》一节的 tmux 子表。

## 支持的 provider

每个 provider 是 `src/providers/*.ts` 一个文件，实现 `AgentProvider` interface（`src/providers/provider.ts`）。两种通讯方式自己选喜欢的：

| Provider | 用的 CLI | 协议 | session 模型 | 推荐场景 |
|---|---|---|---|---|
| **claude** | `claude --input-format stream-json --output-format stream-json …` | NDJSON 双向 stdio | 一进程一 session，`--resume <sid>` 跨重启续 | Claude Code 长寿命聊天，prompt cache 命中省钱（默认） |
| **codex** | `codex app-server --listen stdio://` | NDJSON JSON-RPC 2.0 over stdio | 一进程多 thread，1 Provider = 1 thread；`thread/resume` 续 | Codex CLI，OpenAI 体系 |
| **tmux** | `tmux send-keys` / `tmux capture-pane` | 终端字节流，按行 diff 推送 | 操作员维护 tmux session；session id 不可 resume | 已有 tmux 工作流、想接非 stream 协议 CLI、调试用 — **opt-in only**（默认关） |

### tmux provider 注意事项

> **致谢**：tmux 由 [OpenBSD tmux 项目](https://github.com/tmux/tmux)（BSD-2 License）提供。本 provider 仅作为客户端调用 `tmux send-keys` / `tmux capture-pane`，不重新分发 tmux 二进制。

启用：`export AGENT1_PROVIDER=tmux`（不显式 set 不会启用）。然后再 `export AGENT1_TMUX_SESSION=my-session-name`（如不设，server 启动时自动生成 `agent-<8-char-hex>` 并在 stderr 打印出来，operator 自己 `tmux attach -t <name>` 看会话）。

| 变量 | 默认 | 说明 |
|---|---|---|
| `AGENT*_TMUX_SESSION` | `agent-<8-char-hex>` per boot | 要 attach 的 tmux session 名 |
| `AGENT*_TMUX_SOCKET` |（空，用默认 socket） | tmux `-L` socket 名，需要隔离多 tmux server 时用 |
| `AGENT*_TMUX_CAPTURE_INTERVAL_MS` | `5000` | capture-pane polling 间隔，太短会拖累 tmux server |
| `AGENT*_TMUX_FILTER_MODE` | `strict` | `strict` / `loose` / `off`，见下 |
| `AGENT*_TMUX_QUIET_MS` | `30000` | 最后一次新输出后 quiet 多少 ms 视为 turn 结束（emit `result` event → 控制面板 markIdle）。tmux 没有原生 turn-end 信号，全靠这个 debounce。长任务 / 静默思考型 CLI（编译 / 长链接 RPC）调到 `120000` 或更高防 premature idle |

**Sanitizer**：tmux provider 把 `capture-pane` 拿到的整段 pane 字节流 diff 出新增行后再当 AgentEvent 推送。这些字节可能含 env var dump / API key 粘贴 / ssh-add 输出 — 全都会被持久化进 `chat.db` + 广播到 web UI。`AGENT*_TMUX_FILTER_MODE=strict`（默认）会：

- 整行丢弃形如 `FOO=bar` 的 env var 赋值行（替换成 `[redacted]`）
- 整行丢弃含 `sk-…` / `Bearer …` / `eyJ…` (JWT) / `ghp_…` / `AKIA…` / `ssh-rsa` / `ssh-ed25519` / `-----BEGIN … PRIVATE KEY-----` / `xox[abp]-…` (Slack) 的 token 行
- 把 `$HOME` 绝对路径换 `~`、把当前 `$USER` 用户名换 `<user>`

`loose` 只做路径/用户名替换不丢 token；`off` 全 pass-through。**Best-effort only** — 不能 catch 未列入模式的私有 token 格式。**适合本机单用户用，不适合公网部署**。要对外暴露的话用 `claude` / `codex` provider，或显式用 `loose`/`off` 模式同时确保 tmux 会话内不会出现任何敏感字节。

**其他限制**：

- session 必须由 operator 自己 `tmux new-session -d -s <name>` 创建；TmuxProvider 启动时只 attach，不创建（避免误覆盖已有会话）
- close() 只停 polling，**不杀 tmux session**（让 operator 继续 `tmux attach` 看历史）
- 不支持 `provider.interrupt()`（实际只发 `C-c`，对接收 SIGINT 的 CLI 才有效）
- 不支持 `tool_use` / `thinking` 抽取（capture-pane 拿不到结构化字段）
- send() 的 `text` 含 `\n` 会被原样送进 PTY，多数 CLI 把每个 `\n` 当 submit；如果上游想送多行 prompt，自己 join 成单行或换 provider
- Docker 部署需要 `--device /dev/ptmx`（tmux 需 PTY）



### 想接其他 CLI（Hermes / Gemini / Kimi / 自己的 binary）

`src/providers/<name>.ts` 新写一个 class implements `AgentProvider`：

```ts
export class MyProvider implements AgentProvider {
  send(text: string, opts?: AgentSendOpts): Promise<void>  // spawn-on-first-call + 写 stdin
  interrupt(): Promise<boolean>                            // SIGKILL 当前 turn，保留 session
  close(): Promise<void>                                   // graceful 关闭
  readonly isAlive: boolean
  readonly sessionId: string | null
  capabilities(): AgentCapabilities
  onEvent(cb): void                                        // 把原协议事件 normalize 成 AgentEvent
  onError(cb): void
}
```

参考 `src/providers/claude.ts` (NDJSON 单向流 + session_id) 跟 `src/providers/codex.ts` (JSON-RPC 2.0 + 反向 RPC + multi-thread 噪音过滤) 两套实现就够。然后在 `server.ts` 的 `instantiateProvider()` 加一个 case 路由你的 `AGENT*_PROVIDER` 值过去，重启 server。

## 怎么加 agent

仓库自带两个 placeholder agent — **agent1** 跟 **agent2** —— 默认配置：agent1=claude / agent2=codex。需要给它们指定 cwd 才能真启动：

```bash
export AGENT1_CWD="/Users/you/Agents/Worker1"      # agent1 的 settings / hooks / MCP 从这里读
export AGENT2_CWD="/Users/you/Agents/Worker2"
bun server.ts
```

agent runtime 上班顺序：

1. 在 web 控制面板点 agent 卡片的"上班"按钮 → server 注册 provider 实例
2. 给 agent 在工作群发第一条消息（或在 DM 里发） → provider lazily spawn 子进程 + initialize
3. 之后所有消息都通过同一进程跨多 turn 累积 context

加第三个 agent（叫 "Alice"，用 claude provider）：

### 1. `GROUP_ROSTER`（搜 `const GROUP_ROSTER`）

```ts
const GROUP_ROSTER: GroupMember[] = [
  // ... existing entries
  {
    id: 'alice',
    display_name: 'Alice',
    kind: 'agent',
    avatar: '🦊',
    color: 'purple',
    model: 'claude-sonnet-4-5',  // 显示用
    tmux: 'claude',              // provider-kind compatibility field (legacy name `tmux` retained,
    can_reply: true,
  },
]
```

### 2. `AGENT_RUNTIMES`（搜 `const AGENT_RUNTIMES`）

```ts
const AGENT_RUNTIMES: Record<string, AgentRuntimeConfig> = {
  agent1: { ... },
  agent2: { ... },
  alice: {
    provider: 'claude',
    binaryPath: process.env.ALICE_BINARY_PATH || '',
    cwd: process.env.ALICE_CWD || '',
    extraArgs: parseExtraArgs(process.env.ALICE_EXTRA_ARGS),
  },
}
```

### 3. `WORKING_PHRASES`（搜 `const WORKING_PHRASES`）

```ts
const WORKING_PHRASES: Record<string, string[]> = {
  // ...
  alice: ['alice 在啃苹果', 'alice 在追兔子'],
}
```

### 4. `ROLE_TO_ACTORS`（搜 `const ROLE_TO_ACTORS`，v2 任务系统）

只有要给 alice 单独 workflow role 才加，否则她可以被分到现有 role（`implementer` / `reviewer` —— 这是 v2 任务系统的抽象 role key，跟 actor id 解耦）。

```ts
const ROLE_TO_ACTORS: Record<string, string[]> = {
  pm: ['admin'],
  // ... existing entries
  designer: ['alice'],
}
```

### 5. web/workgroup-v2/index.html `CHANNELS`

```js
const CHANNELS = {
  workgroup: { type: 'group', convId: 'workgroup' },
  'dm-alice': { type: 'group', convId: 'dm-alice' },
  // ... existing
};
```

同文件搜 `SERVER_TO_DESIGN_ID` / `DESIGN_AGENT_ORDER` / `DM_CHANNEL_TO_SID` / `DM_AGENT_ORDER` / `MENTION_CANDIDATES` / `ROLE_DEFS` / `IDENTITY_ACTOR_META`，凡是列了现有 agent 的 array / map 都要加 alice 一项。

完成后**重启 server**（env / config 在 boot 时读），web 自动派生头像 + DM tab + 工作群成员栏。

## Agent runtime CLAUDE.md / AGENTS.md 推荐写法

ai-collab 通过 `provider.send()` 把 message 喂进 agent 的 stream subprocess，agent 自己负责解析 + 回复。每个 agent runtime 的 `CLAUDE.md`（Claude Code）或 `AGENTS.md`（Codex）应该约定几条:

### 1. 工作群消息怎么辨认

消息进 agent stdin 时带 envelope 前缀：

```
[workgroup / <sender>] — 工作群消息，主投递（你是目标，正常回）
[workgroup observe / <sender>] — 工作群旁听同步（默认不回；见下）
[私聊 / <sender> · 请 curl 回复到 dm-<agentId>] — DM 私聊
[task / AIC-XXX / <phase> / <recipient>] — 任务系统通知
```

agent 收到消息看第一行 envelope 决定怎么处理。

**Observe 模式特殊规则**：`[workgroup observe / ...]` 是 sidekick 同步工作群上下文，**server 这一 turn 不会 auto-route assistant text 到任何频道** — 你随便回点什么都不会被任何人看见。如果真有相关想说的或被指名提及才回，**必须自己 curl `POST /group/send` 显式指定 `conversation_id`**（`workgroup` 公开 / `dm-<your-id>` 私聊 PM），不能默默写文字让 server 帮你 route。这层 gate 防止 agent 把私聊语气漏到工作群。

> ⚠️ Observe gate 当前用 per-agent flag（不是 per-turn）。如果 PM 在同一个 agent 上背靠背 dispatch observe + non-observe 两条消息（两次 HTTP 请求间隔短于 supervisor 处理上一条 turn 的时间），后一条的 flag 会覆盖前一条，导致 routing 错。日常单线程使用不踩，并发自动化 dispatcher 才会触发。彻底修需要把 `observe` 透传进 `AgentProvider.send()` 让 supervisor 跟 turn 帧绑定。

### 2. 回复方式

回复**永远走 curl** 调 `/group/send`，不用 agent runtime 自带的 reply 工具（那些工具只对 agent runtime 内部 stream 有效，不会进 server group_messages）：

```bash
# 工作群回复
curl -s -X POST http://localhost:3009/group/send \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AICOLLAB_AUTH_TOKEN" \
  -d '{"sender_id":"agent1","text":"内容","mentions":["admin"]}'

# DM 回复 (带 conversation_id, 不带 mentions, message_type=chat)
curl -s -X POST http://localhost:3009/group/send \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AICOLLAB_AUTH_TOKEN" \
  -d '{"sender_id":"agent1","conversation_id":"dm-agent1","message_type":"chat","text":"内容"}'
```

### 3. 任务通知（精简版）

notification 只含 header + 推进原因 + 底部 hint，不带 feed history：

```
[task / AIC-XXX / 实施 / agent1]
📋 任务标题
当前阶段：实施（你的 role：implementer）· 处理人：agent1

<推进原因 / 评论 / 系统消息>

(需要历史? GET /tasks/AIC-XXX/events?limit=20)
```

长寿命 stream-json agent 跨 turn 累积上下文，上一条 notification 应该还在 agent context 里。**多数情况你已经知道前情**，不用拉历史。真正不知道时再 curl：

```bash
# 拉最近 20 条 events
curl -s "http://localhost:3009/tasks/AIC-XXX/events?limit=20" \
  -H "Authorization: Bearer $AICOLLAB_AUTH_TOKEN"

# 拉 task 完整 description + phase_state
curl -s "http://localhost:3009/tasks/AIC-XXX" \
  -H "Authorization: Bearer $AICOLLAB_AUTH_TOKEN"
```

### 4. 推进任务 / 写评论

```bash
# 推进 phase
curl -s -X PUT "http://localhost:3009/tasks/AIC-XXX/advance" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AICOLLAB_AUTH_TOKEN" \
  -d '{"actor_id":"agent1","sender_id":"agent1"}'

# 写评论
curl -s -X POST "http://localhost:3009/tasks/AIC-XXX/comments" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $AICOLLAB_AUTH_TOKEN" \
  -d '{"actor_id":"agent1","sender_id":"agent1","body":"评论正文"}'
```

> 📌 这些约定写进 agent runtime 的 CLAUDE.md / AGENTS.md（agent 的工作目录, `AGENT*_CWD` 指过去那里）。agent 一启动就读，跨 turn 一直生效。

## 已知 limitation

- **PM id 写死 `'admin'`**：server.ts 里多处 `=== 'admin'` 字面比较散在 PM-only endpoint。改 PM id 需要 grep 全替。
- **agent id 字面散布**：`'agent1'` / `'agent2'` 在 `GROUP_ROSTER` / `AGENT_RUNTIMES` / `WORKING_PHRASES` / `ROLE_TO_ACTORS` / web 多处出现。加新 agent 按上面 5 步走。
- **workflow role keys**：v2 任务系统里 phase template 用的 role key (`implementer` / `reviewer` / `pm` / `system`) 是抽象角色,不是 actor id;通过 `ROLE_TO_ACTORS` 映射。
- **transcript 视图只支持 claude provider**：codex 把 sessions 写到 `~/.codex/sessions/`,schema 跟 claude jsonl 不一样,目前 `/api/transcript` 对 codex agent 返 410。后续可以扩展。
- **agent runtime hook 仍可用作辅助信号**：如果你的 agent runtime 自带 lifecycle hook（如 Claude Code 的 `UserPromptSubmit` / `Stop`）,可以照常 POST 到 `/group/agent/working` `/group/agent/idle` 更新状态。Provider event 是默认信号,hook 是冗余补充——任意一边触发都行。
- **Prompt cache TTL**：Anthropic prompt cache 默认 TTL 5 分钟，超时后下一次调用需要全量重写 cache。长 idle session 的 cache miss cost 可能很高。部署前考虑：keepalive ping 维持 cache、限制定时唤醒间隔、或显式设 1h TTL。详见 [Anthropic prompt caching 文档](https://platform.claude.com/docs/en/docs/build-with-claude/prompt-caching)。

## Chat supervisor（可选）

如果你 fork 里启用了独立的 chat supervisor 子进程（PM 主聊天用一个常驻 claude session 跑，跟 agent provider 解耦），注意 server.ts 和 chat supervisor 是两个独立进程，改完代码要重启对的那个。详见 `src/providers/chat/` 目录下的源码注释。

Wire 协议规范在 `src/providers/chat/frame.ts` 顶部注释，覆盖所有 frame 类型和约定。

## 项目结构

```
ai-collab/
├── server.ts              # Bun HTTP server 入口
├── src/
│   ├── config.ts          # env / 路径 / token / 加载
│   ├── auth.ts            # request 认证
│   ├── responses.ts       # JSON 响应 helper
│   ├── formatters.ts      # message / mention 格式化
│   ├── security.ts        # 危险 endpoint 守卫
│   └── providers/
│       ├── provider.ts    # AgentProvider interface + AgentEvent union
│       ├── claude.ts      # Claude Code stream-json provider
│       └── codex.ts       # Codex app-server JSON-RPC provider
├── web/
│   ├── workgroup-v2/      # 主 web UI（工作群 + DM + 任务 + transcript 视图）
│   └── login.html         # 兼容旧 bookmark 的 redirect 壳
├── scripts/               # smoke-http.sh 等小脚本
└── runtime-data/          # 自动生成 — chat.db / token.txt / uploads / agent_*.json (.gitignore)
    └── state/
        ├── chat.db        # SQLite 主库（首次启动自动建）
        ├── token.txt      # 自动生成的 auth token（chmod 600）
        └── agent_<id>.json # provider 持久化 session_id（跨 server 重启续 session）
```

## License

MIT
