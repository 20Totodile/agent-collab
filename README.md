# Agent-collab

让多个 AI agent 在一个平台上按流程协作。目前支持 Claude Code 和 Codex CLI。

## 为什么做这个

在一个平台上把你所有的 agent 接在一起协作，UI 可视、易用。是一份Proof of Concept，非开袋即食！

跟 agent 对话时产生的想法可以直接建成 task，不让它流失在对话记录里。各种任务按照预设的模板自己流转，只在需要你拍板的时候通知你。

## 适合什么人

这不是一个专注自动化和高效开发的工具。它是我们为了适配自己的工作流做出来的，不一定适合所有人，主要是分享思路。

我个人从事创意类工作，除了写代码之外还需要 brainstorm、调研等不同类型的任务。这只是一个一人三 AI 的小作坊。

协作之外，每个 agent 的 context 和记忆管理同样重要。

总的来说，没有标准做法，找到适合自己的就好。

## 核心功能

### 任务系统

Issue tracker 范式，每个任务有类型和阶段流程。

- **阶段是积木**：你可以创建自己的阶段，每个阶段单独配置谁负责、能否拒绝、是否自动跳过。默认提供的阶段有草稿、评估、实施、Review、验收等
- **类型是拼装**：把阶段积木拼成不同的任务类型。开发任务一套阶段、轻量开发一套、调研另一套。创建新类型就是重新拼积木
- **活动流**：每个任务页面有一条时间线，展示所有发生过的事
- **自动通知**：任务推进到某个 agent 的阶段时，agent 的私聊窗口会收到系统提示

对话中让 AI 自己开任务单，建完自动走评估 → 实施 → review。提前设好哪些环节需要人拍板，到了那步会通知你。

<img width="1276" height="1227" alt="任务列表" src="https://github.com/user-attachments/assets/909b84ca-c2d5-4df4-8c00-2b033d3e0d41" />
<img width="1187" height="255" alt="工作流" src="https://github.com/user-attachments/assets/61f176aa-da6e-491a-a915-065470a9d7dc" />
<img width="938" height="926" alt="任务评论" src="https://github.com/user-attachments/assets/8529b439-f005-4218-afed-b7452c88c7f5" />

### 工作群和私聊

- **工作群**：所有 agent 看到同一个频道，按 @mention 路由响应
- **私聊**：每个 agent 一个独立 DM 窗口，一对一对话
- Web 和 iOS 双端同步

<img width="1282" height="1225" alt="首页" src="https://github.com/user-attachments/assets/5c29d856-5e3e-464f-9e81-7f7a4228ecf1" />

### 终端面板

<img width="1277" height="637" alt="终端" src="https://github.com/user-attachments/assets/37223f2a-01ec-49f1-9947-dc326b5bb646" />


从 web 端直接看每个 agent 在干什么，支持两种显示模式：

- **Transcript 视图**：结构化展示 agent 的对话记录（user / assistant / 工具调用 / 工具结果），claude provider 专用（要读 jsonl）
- **Tmux TUI**：直接看 agent 的 tmux 终端画面，可以交互式调试，tmux provider 专用

### 工位卡

<img width="1237" height="318" alt="工位卡" src="https://github.com/user-attachments/assets/a8bc0bab-eca1-4a34-8fb5-19b9c461ca56" />


每个 agent 一张卡，显示在线状态、当前模型、用量信息。可以手动控制上下班。

## 通讯方式

Server 和 agent 子进程之间有两种通讯方式，各有取舍：

| | stream-json | tmux |
|---|---|---|
| **机制** | server 直接 spawn 子进程，双向 NDJSON over stdio | server attach 已有的 tmux session，用 `send-keys` 输入、`capture-pane` 轮询输出 |
| **实时性** | 事件流实时收（token-level delta） | 5s 轮询屏幕快照，有间隔 |
| **数据形态** | NDJSON events（Anthropic 结构化事件） | 终端字节流（raw bytes，需 sanitize 防泄漏） |
| **agent 主体** | server 拥有进程，`--resume` 跨重启续 session | 操作员拥有 tmux session，server 只 attach 不创建 |
| **可视调试** | Headless，看不到终端 | 有 TUI，可以 `tmux attach` 进去人肉操作 |
| **transcript** | claude binary 写 jsonl，web 可看 | 同上（tmux 内跑 claude binary 时也有） |
| **适合** | 默认 / 新部署 / 完整功能 | 已有 tmux 工作流 / 需要交互式调试 / 想接非 stream 协议的 CLI |

Claude Code 和 Codex CLI 都走 stream-json。tmux provider 是给已经有 tmux 工作流的用户准备的，需要显式启用。

## 快速开始

需要 [Bun](https://bun.sh) 1.0+ 和至少一个 CLI agent（[Claude Code](https://claude.com/claude-code) 或 [Codex CLI](https://github.com/openai/codex)）。

```bash
git clone https://github.com/20Totodile/ai-collab.git
cd ai-collab
bun install

# 指定 agent 的工作目录
export AGENT1_CWD="/path/to/agent1/workspace"
export AGENT2_CWD="/path/to/agent2/workspace"

bun server.ts
```

打开 `http://localhost:3009/web/workgroup-v2/index.html`，浏览器会自动登录。

## 架构

```
┌─────────────┐      ┌────────────────────────┐      ┌──────────────────┐
│   Web UI    │◀────▶│  Bun Server            │◀────▶│  Agent stream    │
│  (browser)  │ HTTP │  (port 3009)           │stdio │  subprocesses    │
└─────────────┘      │  + SQLite              │      │  - claude        │
                     │  + AgentProvider layer │      │  - codex         │
                     │  + HTTP polling        │      │  - …             │
                     └────────────────────────┘      └──────────────────┘
```

每个 agent 是一个长寿命的 CLI 子进程，跨多次对话复用同一个 session。Server 通过 `AgentProvider` 接口管理子进程生命周期。想接其他 CLI 的话，实现这个接口即可，详见 `src/providers/provider.ts`。

## 添加更多 agent

仓库自带两个 placeholder agent（agent1 / agent2）。添加第三个 agent 需要在 `server.ts` 的几个常量里加一项，详见 [README.md](README.md)。

## 技术文档

Provider 协议细节、env 配置项、任务系统 API 等深入内容见 [README.md](README.md)。

## License

MIT
