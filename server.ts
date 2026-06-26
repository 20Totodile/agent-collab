// ai-collab server — HTTP API + static asset host on IMG_PORT (default 3009).
// Clients (web/workgroup-v2 + agent runtimes) all use HTTP polling.
import { createServer } from 'http'
import { Database } from 'bun:sqlite'
import path from 'path'
import { mkdirSync, existsSync, readdirSync, readFileSync, writeFileSync, openSync, readSync, closeSync, statSync, renameSync } from 'fs'
import { homedir } from 'os'
import { jsonOk, jsonError, readJsonBody } from './src/responses.ts'
import { jsonParseSafe } from './src/formatters.ts'
import {
  IMG_PORT,
  SOURCE,
  AUTH_TOKEN,
  AGENT1_TMUX_SESSION,
  AGENT2_TMUX_SESSION,
  AGENT1_CLOCK_IN_SCRIPT,
  SERVER_STARTED_AT,
  agentRoot,
  workspaceRoot,
  runtimeDataRoot,
  runtimeStateDir,
  runtimeUploadsDir,
  generatedArtifactsDir,
  legacyImageDir,
  chatDbPath,
  legacyChatDbPath,
  heartbeatStatePath,
  ALLOW_QUERY_TOKEN_AUTH,
  ALLOW_REMOTE_CONTROL,
  AGENT1_HEARTBEAT_PATH,
  AGENT1_PROJECT_DIR,
  AGENT2_PROJECT_DIR,
  AGENT1_PROVIDER, AGENT2_PROVIDER,
  AGENT1_BINARY_PATH, AGENT2_BINARY_PATH,
  AGENT1_CWD, AGENT2_CWD,
  AGENT1_EXTRA_ARGS, AGENT2_EXTRA_ARGS,
  AGENT1_TMUX_SOCKET, AGENT2_TMUX_SOCKET,
  AGENT1_TMUX_CAPTURE_INTERVAL_MS, AGENT2_TMUX_CAPTURE_INTERVAL_MS,
  AGENT1_TMUX_FILTER_MODE, AGENT2_TMUX_FILTER_MODE,
  AGENT1_TMUX_QUIET_MS, AGENT2_TMUX_QUIET_MS,
  type AgentProviderKind,
} from './src/config.ts'
import type { AgentProvider, AgentEvent, AgentError, AgentProviderConfig } from './src/providers/provider.ts'
import { ClaudeProvider } from './src/providers/claude.ts'
import { CodexProvider } from './src/providers/codex.ts'
import { TmuxProvider } from './src/providers/tmux.ts'
import { hasRequestAuth, isLocalRequestHost, requestAuthMode } from './src/auth.ts'
// AIC-65: handleTodosRoutes import removed — iOS todo 跟 task 撞定位，整个 todo 模块退役
import { dangerousEndpointFor, hasRemoteControlConfirm, logDangerousOperation } from './src/security.ts'

const WORKSPACE_ROOT = workspaceRoot(import.meta.dir)
const RUNTIME_DATA_ROOT = runtimeDataRoot(import.meta.dir)
const RUNTIME_STATE_DIR = runtimeStateDir(import.meta.dir)
const RUNTIME_UPLOADS_DIR = runtimeUploadsDir(import.meta.dir)
const GENERATED_ARTIFACTS_DIR = generatedArtifactsDir(import.meta.dir)
const HEARTBEAT_PATH = heartbeatStatePath(import.meta.dir)

function migrateLegacyFile(legacyPath: string, nextPath: string) {
  if (legacyPath === nextPath) return
  if (!existsSync(legacyPath) || existsSync(nextPath)) return
  mkdirSync(path.dirname(nextPath), { recursive: true })
  renameSync(legacyPath, nextPath)
  process.stderr.write(`ai-collab: migrated runtime file ${legacyPath} -> ${nextPath}\n`)
}

function migrateLegacySqlite(legacyPath: string, nextPath: string) {
  migrateLegacyFile(legacyPath, nextPath)
  for (const suffix of ['-shm', '-wal']) {
    migrateLegacyFile(`${legacyPath}${suffix}`, `${nextPath}${suffix}`)
  }
}

mkdirSync(RUNTIME_DATA_ROOT, { recursive: true })
mkdirSync(RUNTIME_STATE_DIR, { recursive: true })
mkdirSync(RUNTIME_UPLOADS_DIR, { recursive: true })
mkdirSync(GENERATED_ARTIFACTS_DIR, { recursive: true })
migrateLegacySqlite(legacyChatDbPath(import.meta.dir), chatDbPath(import.meta.dir))

const LEGACY_IMG_DIR = legacyImageDir(import.meta.dir)
const UPLOADS_DIR = RUNTIME_UPLOADS_DIR
mkdirSync(LEGACY_IMG_DIR, { recursive: true })

function resolveStoredMediaPath(filename: string) {
  const candidates = [
    path.join(UPLOADS_DIR, filename),
    path.join(LEGACY_IMG_DIR, filename),
  ]
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate
  }
  return candidates[0]
}

// SQLite for chat history
const DB_PATH = chatDbPath(import.meta.dir)
const db = new Database(DB_PATH)
// SQLite defaults to FK off; v2 schemas rely on ON DELETE CASCADE for task_events / phase_role_checks / seen_marks.
// Without this, DELETE FROM tasks_v2 leaves orphan event rows.
db.run(`PRAGMA foreign_keys = ON`)

db.run(`CREATE TABLE IF NOT EXISTS group_messages (
  id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,
  conversation_id TEXT NOT NULL DEFAULT 'workgroup',
  sender_id TEXT NOT NULL,
  sender_model TEXT DEFAULT NULL,
  text TEXT NOT NULL,
  mentions TEXT NOT NULL DEFAULT '[]',
  parent_msg_id TEXT DEFAULT NULL,
  reply_to TEXT DEFAULT NULL,
  source TEXT DEFAULT 'api',
  delivery TEXT NOT NULL DEFAULT '{}',
  meta TEXT NOT NULL DEFAULT '{}',
  message_type TEXT NOT NULL DEFAULT 'chat',
  task_id TEXT DEFAULT NULL,
  parent_task_id TEXT DEFAULT NULL,
  owner TEXT DEFAULT NULL
)`)
try { db.run(`ALTER TABLE group_messages ADD COLUMN images TEXT NOT NULL DEFAULT '[]'`) } catch {}
try { db.run(`ALTER TABLE group_messages ADD COLUMN files TEXT NOT NULL DEFAULT '[]'`) } catch {}

db.run(`CREATE TABLE IF NOT EXISTS group_agent_state (
  agent_id TEXT PRIMARY KEY,
  last_seen TEXT DEFAULT NULL,
  is_typing INTEGER DEFAULT 0,
  typing_since TEXT DEFAULT NULL,
  dispatch_id TEXT DEFAULT NULL,
  status_text TEXT DEFAULT NULL
)`)
// AIC-48: last_active = 最近一次"该 agent 真在跑或刚回完"的时间戳，driver 是 mark/clear pair。
// idle 时前端用 last_active 算"上次活跃 X 分钟前"。
try { db.run(`ALTER TABLE group_agent_state ADD COLUMN last_active TEXT DEFAULT NULL`) } catch {}

db.run(`CREATE TABLE IF NOT EXISTS group_agent_settings (
  agent_id TEXT PRIMARY KEY,
  auto_reply INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
)`)

// AIC-55: per-actor last-seen-id per channel, so web + iOS app share the same
// unread state. Previously each client kept its own UserDefaults/localStorage
// copy and the two were never reconciled — PM read on web, app still showed
// the badge.
db.run(`CREATE TABLE IF NOT EXISTS actor_channel_seen (
  actor_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  last_seen_id TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (actor_id, channel)
)`)

// === Task pipeline ===
db.run(`CREATE TABLE IF NOT EXISTS group_tasks (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  category TEXT NOT NULL DEFAULT '',
  phase TEXT NOT NULL DEFAULT 'evaluate',
  evaluate_check INTEGER NOT NULL DEFAULT 0,
  pm_check_evaluate INTEGER NOT NULL DEFAULT 0,
  implement_check INTEGER NOT NULL DEFAULT 0,
  pm_check_implement INTEGER NOT NULL DEFAULT 0,
  review_check INTEGER NOT NULL DEFAULT 0,
  pm_check_review INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active',
  evaluate_started_at TEXT DEFAULT NULL,
  evaluate_ended_at TEXT DEFAULT NULL,
  implement_started_at TEXT DEFAULT NULL,
  implement_ended_at TEXT DEFAULT NULL,
  review_started_at TEXT DEFAULT NULL,
  review_ended_at TEXT DEFAULT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
)`)
try { db.run(`ALTER TABLE group_tasks ADD COLUMN category TEXT NOT NULL DEFAULT ''`) } catch {}
try { db.run(`ALTER TABLE group_tasks ADD COLUMN evaluate_started_at TEXT DEFAULT NULL`) } catch {}
try { db.run(`ALTER TABLE group_tasks ADD COLUMN evaluate_ended_at TEXT DEFAULT NULL`) } catch {}
try { db.run(`ALTER TABLE group_tasks ADD COLUMN implement_started_at TEXT DEFAULT NULL`) } catch {}
try { db.run(`ALTER TABLE group_tasks ADD COLUMN implement_ended_at TEXT DEFAULT NULL`) } catch {}
try { db.run(`ALTER TABLE group_tasks ADD COLUMN review_started_at TEXT DEFAULT NULL`) } catch {}
try { db.run(`ALTER TABLE group_tasks ADD COLUMN review_ended_at TEXT DEFAULT NULL`) } catch {}

db.run(`CREATE TABLE IF NOT EXISTS task_categories (
  name TEXT PRIMARY KEY,
  created_at TEXT NOT NULL
)`)

// === Task Tracker v2 schema (spec: docs/task-tracker-v2-spec.md §11) ===
db.run(`CREATE TABLE IF NOT EXISTS tasks_v2 (
  id            TEXT PRIMARY KEY,
  title         TEXT NOT NULL,
  description   TEXT NOT NULL DEFAULT '',
  type          TEXT NOT NULL,
  category      TEXT NOT NULL DEFAULT '',
  current_phase TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'open',
  created_at    TEXT NOT NULL,
  created_by    TEXT NOT NULL,
  closed_at     TEXT,
  closed_by     TEXT,
  updated_at    TEXT NOT NULL
)`)
// PM 决定（DM）：dev_full/dev_lite 默认 NOT auto-upload TF — too noisy. PM opts in per task.
try { db.run(`ALTER TABLE tasks_v2 ADD COLUMN auto_upload_tf INTEGER NOT NULL DEFAULT 0`) } catch {}
db.run(`CREATE INDEX IF NOT EXISTS idx_tasks_v2_status ON tasks_v2(status)`)
db.run(`CREATE INDEX IF NOT EXISTS idx_tasks_v2_type ON tasks_v2(type)`)
db.run(`CREATE INDEX IF NOT EXISTS idx_tasks_v2_phase ON tasks_v2(current_phase)`)

db.run(`CREATE TABLE IF NOT EXISTS task_events (
  id       TEXT PRIMARY KEY,
  task_id  TEXT NOT NULL,
  ts       TEXT NOT NULL,
  kind     TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  body     TEXT NOT NULL,
  meta     TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (task_id) REFERENCES tasks_v2(id) ON DELETE CASCADE
)`)
db.run(`CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id, id)`)
// /api/search/locate 走 `WHERE ts > ? ORDER BY ts ASC LIMIT 1`, 加 ts 索引避 full table scan
db.run(`CREATE INDEX IF NOT EXISTS idx_group_messages_conv_ts ON group_messages(conversation_id, ts)`)
db.run(`CREATE INDEX IF NOT EXISTS idx_task_events_task_ts ON task_events(task_id, ts)`)

db.run(`CREATE TABLE IF NOT EXISTS task_phase_role_checks (
  task_id     TEXT NOT NULL,
  phase       TEXT NOT NULL,
  cycle       INTEGER NOT NULL,
  role_id     TEXT NOT NULL,
  checked_at  TEXT,
  checked_by  TEXT,
  checked_via TEXT,
  PRIMARY KEY (task_id, phase, cycle, role_id),
  FOREIGN KEY (task_id) REFERENCES tasks_v2(id) ON DELETE CASCADE
)`)
db.run(`CREATE INDEX IF NOT EXISTS idx_task_phase_role_checks_lookup ON task_phase_role_checks(task_id, phase, cycle)`)

db.run(`CREATE TABLE IF NOT EXISTS task_seen_marks (
  task_id            TEXT NOT NULL,
  actor_id           TEXT NOT NULL,
  last_seen_event_id TEXT NOT NULL,
  last_seen_ts       TEXT,
  PRIMARY KEY (task_id, actor_id),
  FOREIGN KEY (task_id) REFERENCES tasks_v2(id) ON DELETE CASCADE
)`)

db.run(`CREATE TABLE IF NOT EXISTS task_id_seq (
  prefix TEXT PRIMARY KEY,
  next   INTEGER NOT NULL DEFAULT 1
)`)

// AIC-31: workflow_templates persisted (replaces hardcoded WORKFLOW_TEMPLATES const).
// `phases` stored as JSON-encoded TaskV2WorkflowPhase[].
// AIC-42: workflow_templates.phases now derived from phase_ids → phase_templates JOIN at reload time.
// `phases` column kept as legacy fallback (pre-migration rows only) — to be dropped later.
db.run(`CREATE TABLE IF NOT EXISTS workflow_templates (
  key        TEXT PRIMARY KEY,
  label      TEXT NOT NULL,
  phases     TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
)`)
try { db.run(`ALTER TABLE workflow_templates ADD COLUMN phase_ids TEXT NOT NULL DEFAULT '[]'`) } catch {}

// Idempotent role-key migration: renames legacy 'otto' / 'lio' role ids to
// 'implementer' / 'reviewer'. Safe to run on every boot — UPDATEs only touch rows
// that still reference the old keys.
try {
  db.run(`UPDATE task_phase_role_checks SET role_id = 'implementer' WHERE role_id = 'otto'`)
  db.run(`UPDATE task_phase_role_checks SET role_id = 'reviewer'    WHERE role_id = 'lio'`)
  // phase_templates.required_roles + workflow_templates.phases JSON strings — string-replace
  // the role ids inside the serialized JSON. Round-trip-safe because the new ids are unique
  // tokens that don't appear elsewhere in those columns.
  db.run(`UPDATE phase_templates    SET required_roles = REPLACE(required_roles, '"otto"', '"implementer"') WHERE required_roles LIKE '%"otto"%'`)
  db.run(`UPDATE phase_templates    SET required_roles = REPLACE(required_roles, '"lio"',  '"reviewer"')    WHERE required_roles LIKE '%"lio"%'`)
  db.run(`UPDATE workflow_templates SET phases         = REPLACE(phases,         '"otto"', '"implementer"') WHERE phases         LIKE '%"otto"%'`)
  db.run(`UPDATE workflow_templates SET phases         = REPLACE(phases,         '"lio"',  '"reviewer"')    WHERE phases         LIKE '%"lio"%'`)
} catch (e) { console.error('role-key migration failed:', e) }

// AIC-42: phase_templates — first-class reusable phase definitions.
// id is immutable (PRIMARY KEY); name / required_roles / can_reject / auto_advance / is_terminal
// are editable. PM-only CRUD endpoint. on_failure / retry_max columns are legacy (kept for
// schema compat, always written NULL).
// 'draft' and 'closed' are reserved ids (runtime hardcodes them as workflow start / terminal).
db.run(`CREATE TABLE IF NOT EXISTS phase_templates (
  id              TEXT PRIMARY KEY,
  name            TEXT NOT NULL,
  required_roles  TEXT NOT NULL DEFAULT '[]',
  can_reject      INTEGER NOT NULL DEFAULT 0,
  auto_advance    INTEGER NOT NULL DEFAULT 0,
  is_terminal     INTEGER NOT NULL DEFAULT 0,
  on_failure      TEXT,
  retry_max       INTEGER,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL
)`)

// AIC-63 cycle 4 (PM 简化 + design): actor-level identity, 单背景色 / 单头像，
// 不再 per-theme。每个 actor 一行。ink/accent 由 server 从 bubble_bg 计算后返回，
// 不存表（avoid drift）。旧表 actor_theme_styles 保留作 legacy 数据归档 — 不再读。
db.run(`CREATE TABLE IF NOT EXISTS actor_styles (
  actor_id     TEXT PRIMARY KEY,
  avatar_kind  TEXT NOT NULL,
  avatar_value TEXT NOT NULL,
  bubble_bg    TEXT NOT NULL,
  updated_at   TEXT NOT NULL
)`)
// Cycle 1-3 wide schema, deprecated cycle 4. Kept for safety + audit; new code
// path does NOT read or write this table.
db.run(`CREATE TABLE IF NOT EXISTS actor_theme_styles (
  actor_id     TEXT NOT NULL,
  theme_id     TEXT NOT NULL,
  avatar_kind  TEXT NOT NULL,
  avatar_value TEXT NOT NULL,
  bubble_bg    TEXT NOT NULL,
  ink          TEXT NOT NULL,
  accent       TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (actor_id, theme_id)
)`)
// Initialize AIC sequence — start above v1 task count to avoid id collision (cosmetic)
try {
  const v1Count = (db.prepare(`SELECT COUNT(*) AS n FROM group_tasks`).get() as any).n || 0
  db.run(`INSERT OR IGNORE INTO task_id_seq (prefix, next) VALUES ('AIC', ?)`, Math.max(1, v1Count + 1))
} catch {}

type GroupMember = {
  id: string
  display_name: string
  kind: 'human' | 'agent'
  avatar: string
  color: string
  model: string | null
  tmux: string | null
  can_reply: boolean
  default_responder?: boolean
}

type GroupRecord = {
  id: string
  ts: string
  conversation_id: string
  sender_id: string
  sender_model: string | null
  text: string
  images: string[]
  files: any[]
  mentions: string[]
  parent_msg_id: string | null
  reply_to: string | null
  source: string
  delivery: any
  meta: any
  message_type: string
  task_id: string | null
  parent_task_id: string | null
  owner: string | null
}

const GROUP_MESSAGE_TYPES = new Set(['task', 'decision', 'ship', 'block', 'progress', 'chat', 'system_notification'])
const GROUP_ALL_TOKEN = '__all__'
const GROUP_MENTION_RE = /@([A-Za-z0-9_\-]+|[\u4e00-\u9fff]+)/g
// AIC-116: GroupMember.tmux field 字段名是 legacy artifact (从老式 key-injection era 留下来的).
// 新语义: provider kind 字符串 ('claude' / 'codex' / null for humans). Web 显示用 — 不再驱动
// agent dispatch routing (那走 AGENT_PROVIDERS map). Long-term rename to `.runtime` 待大改动,
// 目前同字段名 + 改语义保持跨端 (server / web) 兼容.
const GROUP_ROSTER: GroupMember[] = [
  {
    id: 'admin',
    display_name: 'admin',
    kind: 'human',
    avatar: '👾',
    color: 'neutral',
    model: null,
    tmux: null,
    can_reply: false,
  },
  // Two placeholder agents below. Rename / re-skin / replace them when wiring your own
  // setup. Each agent's id (`agent1` / `agent2`) is what the rest of the codebase routes by;
  // keep them or update everywhere they're referenced (see README "怎么加 agent").
  {
    id: 'agent2',
    display_name: 'agent2',
    kind: 'agent',
    avatar: '🤖',
    color: 'green',
    model: '',
    tmux: AGENT2_PROVIDER,
    can_reply: true,
    default_responder: true,
  },
  {
    id: 'agent1',
    display_name: 'agent1',
    kind: 'agent',
    avatar: '🤖',
    color: 'blue',
    model: '',
    tmux: AGENT1_PROVIDER,
    can_reply: true,
  },
]
const GROUP_ROSTER_BY_ID = new Map(GROUP_ROSTER.map((m) => [m.id, m]))

// ===== Agent on/off-work control (控制面板) =====
// Each agent owns its own clock-in command + clock-out routine. agent1 is wired
// here; additional agents fill in their own later.
// AIC-116: per-agent provider registry. Replaces the old session-multiplexer injection model.
// Provider instances are long-lived (spawn-on-first-send, then reused across many sends
// via the captured session_id). `clockIn` ensures a provider exists for the agent;
// `clockOut` closes it. The actual subprocess is spawned lazily by the provider on first
// send() call so a clocked-in but idle agent doesn't burn process resources upfront.
//
// State file path convention: `runtime-data/state/agent_<id>.json` — used by the provider
// to persist session_id across server restarts. Co-located with the SQLite DB so a single
// runtime-data/ backup captures everything.
type AgentRuntimeConfig = {
  provider: AgentProviderKind
  binaryPath: string
  cwd: string
  extraArgs: string[]
  // AIC-129: tmux provider — only consulted when provider === 'tmux'.
  tmuxSession: string
  tmuxSocket: string
  tmuxCaptureIntervalMs: number
  tmuxFilterMode: 'strict' | 'loose' | 'off'
  tmuxQuietMs: number
}
const AGENT_RUNTIMES: Record<string, AgentRuntimeConfig> = {
  agent1: {
    provider: AGENT1_PROVIDER,
    binaryPath: AGENT1_BINARY_PATH,
    cwd: AGENT1_CWD,
    extraArgs: AGENT1_EXTRA_ARGS,
    tmuxSession: AGENT1_TMUX_SESSION,
    tmuxSocket: AGENT1_TMUX_SOCKET,
    tmuxCaptureIntervalMs: AGENT1_TMUX_CAPTURE_INTERVAL_MS,
    tmuxFilterMode: AGENT1_TMUX_FILTER_MODE,
    tmuxQuietMs: AGENT1_TMUX_QUIET_MS,
  },
  agent2: {
    provider: AGENT2_PROVIDER,
    binaryPath: AGENT2_BINARY_PATH,
    cwd: AGENT2_CWD,
    extraArgs: AGENT2_EXTRA_ARGS,
    tmuxSession: AGENT2_TMUX_SESSION,
    tmuxSocket: AGENT2_TMUX_SOCKET,
    tmuxCaptureIntervalMs: AGENT2_TMUX_CAPTURE_INTERVAL_MS,
    tmuxFilterMode: AGENT2_TMUX_FILTER_MODE,
    tmuxQuietMs: AGENT2_TMUX_QUIET_MS,
  },
}
const AGENT_PROVIDERS = new Map<string, AgentProvider>()
// AIC-116: track the conversation each agent was last dispatched into, so
// `handleProviderEvent` can route the assistant reply back to the right channel. Provider
// events have no channel context of their own — the agent runtime doesn't know which
// channel the user typed in, only the message text.
const _agentLastDispatchChannel = new Map<string, string>()
// AIC-132: per-agent flag tracking whether the current turn was dispatched as
// observe-only. `handleProviderEvent` drops the assistant text when this is true —
// agents that want to reply to an observe message must self-curl /group/send with
// an explicit conversation_id (workgroup or dm-<id>). Reset on the next `result`
// event so the following turn starts clean.
const _agentIsObserveTurn = new Map<string, boolean>()
// Dedup last-appended assistant text per agent to prevent double-emit when the runtime
// also self-curls /group/send (legacy claude CLAUDE.md convention). Hash a sliding window
// of recent appends (TTL 60s) — same hash within window is dropped silently.
const _agentReplyDedup = new Map<string, Map<string, number>>()
function _dedupAgentReply(agentId: string, text: string): boolean {
  // returns true when this text was just appended (caller should skip)
  const now = Date.now()
  let inner = _agentReplyDedup.get(agentId)
  if (!inner) { inner = new Map(); _agentReplyDedup.set(agentId, inner) }
  // expire entries > 60s
  for (const [k, ts] of inner) if (now - ts > 60_000) inner.delete(k)
  const key = text.length > 200 ? text.slice(0, 200) + '|' + text.length : text
  if (inner.has(key)) return true
  inner.set(key, now)
  return false
}

function agentStateFilePath(agentId: string): string {
  return path.join(RUNTIME_STATE_DIR, `agent_${agentId}.json`)
}

// AIC-124 fork sync: per-provider chat_supervisor runtime dir. agent1 / agent2 each get
// their own dir so supervisor.sock / supervisor.pid / child.log are fully disjoint and a
// crash in one provider's supervisor can't take down the other.
//
// Env overrides:
//   AICOLLAB_SUPERVISOR_DIR_AGENT1 -> runtime-data/chat-supervisor/agent1/ (default)
//   AICOLLAB_SUPERVISOR_DIR_AGENT2 -> runtime-data/chat-supervisor/agent2/ (default)
// Override is per-agent on purpose — sharing dirs between agents would be a bug.
function agentSupervisorDir(agentId: string): string {
  const envKey = `AICOLLAB_SUPERVISOR_DIR_${agentId.toUpperCase()}`
  const override = process.env[envKey]
  if (override) return override
  return path.join(RUNTIME_DATA_ROOT, 'chat-supervisor', agentId)
}

// Absolute path to the chat_supervisor.ts entrypoint that SupervisorClient should spawn
// when supervisor.sock is missing. Resolved relative to server.ts location so a fork
// can be moved without breaking the path.
const CHAT_SUPERVISOR_ENTRYPOINT = path.join(import.meta.dir, 'src', 'providers', 'chat', 'chat_supervisor.ts')

function instantiateProvider(agentId: string): AgentProvider | null {
  const cfg = AGENT_RUNTIMES[agentId]
  if (!cfg) return null
  // tmux provider doesn't spawn a subprocess (operator-owned session) so the cwd guard
  // below doesn't apply to it. claude/codex still need cwd for settings/hooks discovery.
  if (cfg.provider !== 'tmux' && !cfg.cwd) {
    process.stderr.write(`ai-collab: WARN agent ${agentId} has empty cwd (set AGENT${agentId === 'agent1' ? '1' : '2'}_CWD); provider will spawn with server cwd and likely miss .claude/settings.json\n`)
  }
  const providerCfg: AgentProviderConfig = {
    label: agentId,
    cwd: cfg.cwd || undefined,
    stateFilePath: agentStateFilePath(agentId),
    binaryPath: cfg.binaryPath || undefined,
    extraArgs: cfg.extraArgs.length ? cfg.extraArgs : undefined,
    onEvent: (ev: AgentEvent) => handleProviderEvent(agentId, ev),
    onError: (err: AgentError) => handleProviderError(agentId, err),
  }
  if (cfg.provider === 'codex') return new CodexProvider(providerCfg)
  if (cfg.provider === 'tmux') {
    return new TmuxProvider({
      ...providerCfg,
      session: cfg.tmuxSession,
      socket: cfg.tmuxSocket || undefined,
      captureIntervalMs: cfg.tmuxCaptureIntervalMs,
      filterMode: cfg.tmuxFilterMode,
      quietMs: cfg.tmuxQuietMs,
    })
  }
  // ClaudeProvider needs supervisorDir/entrypoint on top of the shared config (AIC-124).
  return new ClaudeProvider({
    ...providerCfg,
    supervisorDir: agentSupervisorDir(agentId),
    supervisorEntrypoint: CHAT_SUPERVISOR_ENTRYPOINT,
  })
}

function ensureProvider(agentId: string): AgentProvider | null {
  const existing = AGENT_PROVIDERS.get(agentId)
  if (existing) return existing
  const p = instantiateProvider(agentId)
  if (p) AGENT_PROVIDERS.set(agentId, p)
  return p
}

// Bridge provider events to group state + (AIC-116) write the agent's reply
// back into the conversation feed. Each `assistant` event with non-empty text becomes a
// new group_message in the conversation the agent was last dispatched into.
//
// Dedup window prevents double-emit if the agent runtime also self-curls /group/send
// (legacy claude CLAUDE.md convention) — recent identical text within 60s is dropped.
// New ai-collab CLAUDE.md / AGENTS.md templates should NOT self-curl reply; the server
// forwards stdout automatically.
function handleProviderEvent(agentId: string, ev: AgentEvent): void {
  if (ev.type === 'system_init' || ev.type === 'assistant') {
    if (!isAgentWorkingInDb(agentId)) markAgentWorking(agentId)
  } else if (ev.type === 'result') {
    if (isAgentWorkingInDb(agentId)) markAgentIdle(agentId)
    // AIC-132: clear observe flag so the next turn starts clean.
    _agentIsObserveTurn.delete(agentId)
  }

  if (ev.type === 'assistant' && typeof ev.text === 'string' && ev.text.trim()) {
    // AIC-132: observe-only turn — drop the auto-route. The agent saw the message for
    // context sync, not for response; if it actually wants to reply it must self-curl
    // /group/send with an explicit conversation_id. Without this gate the agent's
    // text leaks into the last-dispatched (or default 'workgroup') channel — that
    // surfaces as the agent's DM-style reply tone showing up in the workgroup.
    if (_agentIsObserveTurn.get(agentId) === true) {
      const preview = ev.text.trim().slice(0, 60).replace(/\n/g, ' ')
      process.stderr.write(`provider[${agentId}] observe-turn drop assistant text (${ev.text.length} chars, first="${preview}…")\n`)
      return
    }
    const conversationId = _agentLastDispatchChannel.get(agentId) || 'workgroup'
    const text = ev.text.trim()
    if (_dedupAgentReply(agentId, text)) return
    try {
      const dispatchId = groupId('dsp')
      const record = appendGroupRecord(
        {
          sender_id: agentId,
          conversation_id: conversationId,
          text,
          message_type: 'chat',
          source: 'provider_reply',
        },
        [],
        { mode: 'broadcast', targets: [], dispatch_id: dispatchId, delivered: [], failed: [] },
      )
      // Fan out to other agents in the same conversation (so they see each other's
      // replies just like a human would). Skip the sender itself.
      if (conversationId === 'workgroup') {
        const fanOut = GROUP_ROSTER
          .filter((m) => m.kind === 'agent' && m.id !== agentId && groupAgentAutoReply(m.id))
          .map((m) => m.id)
        if (fanOut.length) {
          dispatchGroupRecord(record, fanOut, 1, /* observeOnly */ true)
        }
      }
    } catch (e) {
      console.error(`provider[${agentId}] reply write-back failed:`, e)
    }
  }
}

function handleProviderError(agentId: string, err: AgentError): void {
  console.error(`provider[${agentId}] error: kind=${err.kind} msg=${err.message}`)
  if (err.kind === 'process_exited' || err.kind === 'spawn_failed') {
    if (isAgentWorkingInDb(agentId)) markAgentIdle(agentId)
    // AIC-116 cycle 2: drop the dead provider instance so the next dispatch
    // re-instantiates and re-spawns. Without this, AGENT_PROVIDERS.has() stays true →
    // groupStatusSnapshot reports `online` for a zombie that won't actually accept input.
    AGENT_PROVIDERS.delete(agentId)
    // AIC-132: provider died mid-turn; observe flag would never see its `result`
    // event so wouldn't auto-clear. Clear here so the next provider instance starts
    // with a clean slate.
    _agentIsObserveTurn.delete(agentId)
  }
}

// 上班: ensure a provider instance exists. Subprocess spawn is deferred to first send() —
// no work needed here beyond instantiation. Returns `already=true` when a provider was
// already alive (PM sees agent online), `already=false` for a fresh instantiation.
function agentClockIn(agentId: string) {
  const existing = AGENT_PROVIDERS.get(agentId)
  const wasAlive = !!existing?.isAlive
  const p = ensureProvider(agentId)
  if (!p) return { ok: false, error: `no provider runtime configured for ${agentId}` }
  markAgentIdle(agentId)
  return { ok: true, already: wasAlive }
}

// Close the agent's provider (graceful shutdown — provider.close() ends stdin and waits exit).
async function killAgentSession(agentId: string) {
  const p = AGENT_PROVIDERS.get(agentId)
  if (!p) return { ok: true, alreadyOff: true }
  try { await p.close() } catch {}
  AGENT_PROVIDERS.delete(agentId)
  updateGroupAgentState(agentId, { last_seen: groupNowIso(), is_typing: 0, typing_since: null, status_text: 'clocked out' })
  return { ok: true }
}

// 下班: graceful provider close. No more "inject a clock-out routine" intermediate state —
// the long-lived stream subprocess just terminates. If the agent runtime had its own
// shutdown sequence (memory consolidation etc), that should happen via a dedicated
// per-agent `clockOutPrompt` send() before close() rather than being smuggled through
// a legacy key-press path. Not in AIC-116 scope; future improvement.
async function agentClockOut(agentId: string) {
  const p = AGENT_PROVIDERS.get(agentId)
  if (!p) return { ok: true, alreadyOff: true }
  try { await p.close() } catch {}
  AGENT_PROVIDERS.delete(agentId)
  updateGroupAgentState(agentId, { last_seen: groupNowIso(), is_typing: 0, typing_since: null, status_text: 'clocked out' })
  return { ok: true }
}
const GROUP_REPLY_AGENT_IDS = GROUP_ROSTER.filter((m) => m.can_reply).map((m) => m.id)
const GROUP_ALIASES = new Map<string, string>([
  ['all', GROUP_ALL_TOKEN],
  ['everyone', GROUP_ALL_TOKEN],
  ['admin', 'admin'],
  ['agent1', 'agent1'],
  ['agent2', 'agent2'],
  ['agent-a', 'agent1'],
  ['agent-b', 'agent2'],
])

function groupNowIso() {
  return new Date().toISOString()
}

function groupId(prefix: string) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`
}

// === Task Tracker v2: constants, helpers (spec §10.5–§11.7, §14) ===

type PhaseDef = {
  key: string
  label: string
  required_roles: ('pm' | 'implementer' | 'reviewer' | 'system')[]
  can_reject: boolean
  auto_advance: boolean
}

// AIC-31: WORKFLOW_TEMPLATES backed by `workflow_templates` table.
// AIC-42: each workflow_template is now a {label, phase_ids[]} — phases derived from PHASE_TEMPLATES.
// Live runtime map (mutated only by reloadWorkflowTemplates from DB).
let WORKFLOW_TEMPLATES: Record<string, { label: string; phases: PhaseDef[] }> = {}

// AIC-42: live runtime map of phase templates (mutated only by reloadPhaseTemplates from DB).
let PHASE_TEMPLATES: Record<string, PhaseDef> = {}

// AIC-42: reserved phase template ids — runtime hardcodes 'draft' as start, 'closed' as terminal.
// PM cannot rename or delete these (validator + endpoint enforce). Others fully editable.
const RESERVED_PHASE_IDS = new Set(['draft', 'closed'])

// Immutable defaults seeded into DB on first run only ((fix) don't re-seed from memory).
// AIC-42: split into phase templates (DEFAULT_PHASE_TEMPLATES) + workflow phase_ids (below).
const DEFAULT_PHASE_TEMPLATES: Record<string, PhaseDef> = {
  draft:       { key: 'draft',       label: '草稿',    required_roles: ['pm'],         can_reject: false, auto_advance: false },
  evaluate:    { key: 'evaluate',    label: '评估',    required_roles: ['pm'],         can_reject: false, auto_advance: false },
  implement:   { key: 'implement',   label: '实施',    required_roles: ['implementer'], can_reject: false, auto_advance: false },
  review:      { key: 'review',      label: 'Review',  required_roles: ['reviewer'],   can_reject: true,  auto_advance: false },
  verify:      { key: 'verify',      label: '验收',    required_roles: ['pm'],         can_reject: false, auto_advance: false },
  in_progress: { key: 'in_progress', label: '进行中',  required_roles: ['pm'],         can_reject: false, auto_advance: false },
  open:        { key: 'open',        label: '开',      required_roles: ['pm'],         can_reject: false, auto_advance: false },
  closed:      { key: 'closed',      label: '已关闭',  required_roles: [],             can_reject: false, auto_advance: false },
}

const DEFAULT_WORKFLOW_TEMPLATES: Record<string, { label: string; phase_ids: string[] }> = {
  dev_full: { label: '开发任务',     phase_ids: ['draft', 'evaluate', 'implement', 'review', 'verify', 'closed'] },
  dev_lite: { label: '轻量开发任务', phase_ids: ['draft', 'implement', 'review', 'verify', 'closed'] },
  research: { label: '调研任务',     phase_ids: ['draft', 'in_progress', 'closed'] },
  tracking: { label: '跟踪/讨论',    phase_ids: ['draft', 'open', 'closed'] },
}

// AIC-31/42: validate a single phase template body (id + label + required_roles + flags).
// Used by phase_templates POST/PATCH (Step 2) and by migrate path that derives templates from
// legacy inline phases.
const VALID_ROLES = new Set(['pm', 'implementer', 'reviewer', 'system'])
function validatePhaseTemplate(p: any, { allowMissingId }: { allowMissingId?: boolean } = {}): string | null {
  if (!p || typeof p !== 'object') return 'phase template must be an object'
  if (!allowMissingId) {
    if (typeof p.id !== 'string' || !/^[a-z][a-z0-9_]{0,40}$/.test(p.id)) return `phase template id must match ^[a-z][a-z0-9_]{0,40}$`
  }
  if (typeof p.name !== 'string' || !p.name.trim()) return 'phase template name required'
  if (!Array.isArray(p.required_roles)) return 'phase template required_roles must be an array'
  for (const r of p.required_roles) if (!VALID_ROLES.has(r)) return `phase template required_roles contains unknown role: ${r}`
  if (typeof p.can_reject !== 'boolean') return 'phase template can_reject must be boolean'
  if (typeof p.auto_advance !== 'boolean') return 'phase template auto_advance must be boolean'
  // auto_advance phases are auto-skipped by handleAutoAdvancePhase, which only marks the system
  // role check (the runner is system-owned, not actor-owned). If required_roles asks for anything
  // other than ['system'], the auto-skip leaves non-system checks unchecked → maybeAdvancePhase
  // can't advance → task wedges. Push the invariant into the validator.
  if (p.auto_advance) {
    if (p.required_roles.length !== 1 || p.required_roles[0] !== 'system') {
      return 'phase template with auto_advance=true must have required_roles=["system"]'
    }
  }
  return null
}

// AIC-42: validate a workflow_template's phase_ids array. Returns error string or null if OK.
// Review checklist (4 hard constraints — see /docs/v2-phase-template-spec.md):
//   1. phase_ids globally unique within one workflow (no template appears twice)
//   2. phase_templates.id is immutable (enforced at PATCH endpoint, not here)
//   3. event labels are snapshotted (existing enterPhase already does this)
//   4. delete phase template requires no workflow references (enforced at DELETE endpoint)
// Runtime invariants (preserved from validatePhases):
//   - phase_ids[0] === 'draft' (createTask hardcodes; publish takes phase_ids[draftIdx+1])
//   - phase_ids[last] === 'closed' AND template.required_roles=[] AND template.is_terminal=1
//   - mid-flow templates must be reachable: required_roles non-empty OR auto_advance
function validatePhaseIds(phase_ids: any): string | null {
  if (!Array.isArray(phase_ids) || phase_ids.length < 2) return 'phase_ids must be an array of at least 2 items'
  const seen = new Set<string>()
  for (let i = 0; i < phase_ids.length; i++) {
    const id = phase_ids[i]
    if (typeof id !== 'string') return `phase_ids[${i}] must be a string`
    if (!PHASE_TEMPLATES[id]) return `phase_ids[${i}] references unknown phase template: ${id}`
    if (seen.has(id)) return `phase_ids contains duplicate template: ${id} (constraint #1: globally unique within workflow)`
    seen.add(id)
  }
  if (phase_ids[0] !== 'draft') return `phase_ids[0] must be 'draft' (runtime hardcodes initial phase)`
  const lastId = phase_ids[phase_ids.length - 1]
  if (lastId !== 'closed') return `phase_ids[last] must be 'closed' (runtime hardcodes terminal phase)`
  const lastDef = PHASE_TEMPLATES[lastId]
  if (!lastDef || lastDef.required_roles.length !== 0) return `phase template '${lastId}' must be terminal: required_roles=[]`
  for (let i = 1; i < phase_ids.length - 1; i++) {
    const def = PHASE_TEMPLATES[phase_ids[i]]
    if (def.required_roles.length === 0 && !def.auto_advance) {
      return `phase template '${def.key}' is mid-flow but has no required_roles and is not auto_advance — workflow would deadlock`
    }
  }
  return null
}

// Legacy: validate a fully-inline phases array (each phase is a complete PhaseDef object).
// Kept for the migration path (parses workflow_templates.phases rows from before AIC-42) only.
// New CRUD endpoints (Step 2) use validatePhaseIds + validatePhaseTemplate instead.
function validatePhases(phases: any): string | null {
  if (!Array.isArray(phases) || phases.length < 2) return 'phases must be an array of at least 2 items'
  const seenKeys = new Set<string>()
  for (let i = 0; i < phases.length; i++) {
    const p = phases[i]
    if (!p || typeof p !== 'object') return `phases[${i}] must be an object`
    if (typeof p.key !== 'string' || !/^[a-z][a-z0-9_]{0,40}$/.test(p.key)) return `phases[${i}].key must match ^[a-z][a-z0-9_]{0,40}$`
    if (seenKeys.has(p.key)) return `duplicate phase key: ${p.key}`
    seenKeys.add(p.key)
    if (typeof p.label !== 'string' || !p.label.trim()) return `phases[${i}].label required`
    if (!Array.isArray(p.required_roles)) return `phases[${i}].required_roles must be an array`
    for (const r of p.required_roles) if (!VALID_ROLES.has(r)) return `phases[${i}].required_roles contains unknown role: ${r}`
    if (typeof p.can_reject !== 'boolean') return `phases[${i}].can_reject must be boolean`
    if (typeof p.auto_advance !== 'boolean') return `phases[${i}].auto_advance must be boolean`
  }
  if (phases[0].key !== 'draft') return `phases[0].key must be 'draft' (runtime hardcodes initial phase = draft)`
  const last = phases[phases.length - 1]
  if (last.key !== 'closed') return `last phase.key must be 'closed' (runtime hardcodes terminal phase key)`
  if (last.required_roles.length !== 0) return `last phase ('${last.key}') must be terminal: required_roles=[]`
  for (let i = 1; i < phases.length - 1; i++) {
    const p = phases[i]
    if (p.required_roles.length === 0 && !p.auto_advance) {
      return `phases[${i}] ('${p.key}') is mid-flow but has no required_roles and is not auto_advance — workflow would deadlock`
    }
  }
  return null
}

// AIC-42: insert a phase template row from a PhaseDef.
function insertPhaseTemplateRow(p: PhaseDef) {
  const now = groupNowIso()
  db.run(
    `INSERT INTO phase_templates (id, name, required_roles, can_reject, auto_advance, is_terminal, on_failure, retry_max, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)`,
    [
      p.key,
      p.label,
      JSON.stringify(p.required_roles),
      p.can_reject ? 1 : 0,
      p.auto_advance ? 1 : 0,
      p.required_roles.length === 0 ? 1 : 0,
      now,
      now,
    ],
  )
}

// AIC-42 (review fix): upsert default phase templates — insert each default
// only if its id is NOT already in the table. Runs AFTER migrateInlinePhasesToPhaseIds
// so PM's legacy inline customizations (AIC-31 era) win over the hardcoded defaults
// when both share an id (e.g. PM changed implement.required_roles → that shape is in
// phase_templates first; ensure skips it). Then ensure fills in default ids that
// weren't referenced by any legacy workflow (e.g. 'in_progress' / 'open' if PM had
// never created a research/tracking type).
function ensureDefaultPhaseTemplates() {
  for (const [id, def] of Object.entries(DEFAULT_PHASE_TEMPLATES)) {
    const exists = db.prepare(`SELECT id FROM phase_templates WHERE id=?`).get(id) as any
    if (!exists) insertPhaseTemplateRow(def)
  }
}

// Deprecated phase template ids that should be removed on every boot. Add new entries
// here when a phase template is dropped from DEFAULT_PHASE_TEMPLATES so existing DBs
// don't keep a stale row. Idempotent — re-running on a clean DB is a no-op.
// Also walks workflow_templates.phase_ids and strips deprecated entries from each row.
const DEPRECATED_PHASE_IDS = ['tf_upload']
function migrateRemoveDeprecatedPhaseTemplates() {
  for (const id of DEPRECATED_PHASE_IDS) {
    db.run(`DELETE FROM phase_templates WHERE id = ?`, [id])
  }
  const rows = db.prepare(`SELECT key, phase_ids FROM workflow_templates`).all() as any[]
  const now = groupNowIso()
  for (const r of rows) {
    let ids: string[] = []
    try { ids = JSON.parse(r.phase_ids || '[]') } catch {}
    const filtered = ids.filter((id: string) => !DEPRECATED_PHASE_IDS.includes(id))
    if (filtered.length === ids.length) continue
    db.run(`UPDATE workflow_templates SET phase_ids = ?, updated_at = ? WHERE key = ?`, [JSON.stringify(filtered), now, r.key])
  }
}

// AIC-63 cycle 4 (PM 简化 + design): 5 actor，每 actor 单 bubble_bg。
// 默认色取自 design 稿 PRESET_COLORS。ink/accent 不存表，server PUT/GET 时算。
type ActorStyleRow = {
  actor_id: string
  avatar_kind: 'emoji' | 'image'
  avatar_value: string
  bubble_bg: string
}
const DEFAULT_ACTOR_STYLES: ActorStyleRow[] = [
  { actor_id: 'admin',  avatar_kind: 'emoji', avatar_value: '👾', bubble_bg: '#dccfe8' },
  { actor_id: 'agent1', avatar_kind: 'emoji', avatar_value: '🤖', bubble_bg: '#cdddec' },
  { actor_id: 'agent2', avatar_kind: 'emoji', avatar_value: '🤖', bubble_bg: '#f4d990' },
  { actor_id: 'system', avatar_kind: 'emoji', avatar_value: '⚙', bubble_bg: '#d4d0c8' },
]
const PRESET_BG_COLORS = [
  '#f7d3d0', '#f7d4b8', '#f4d990', '#cee0c5', '#b8d6cc',
  '#cdddec', '#dccfe8', '#e6dccd', '#d4d0c8', '#332b23',
]

// AIC-63 cycle 4 (PM 简化诉求): PM 只需要选「头像 + 头像背景色」, ink/accent
// server 自动派生 ——
//   accent = bubble_bg (PM 语义：头像背景色 = 气泡色 = 强调色)
//   ink    = 高对比色 (bg 浅 → 同色调深色 / bg 深 → 同色调浅色), 始终满足可读性
function deriveInkAndAccent(bubbleBg: string): { ink: string; accent: string } {
  const hex = bubbleBg.replace('#', '').trim()
  if (hex.length !== 6) return { ink: '#160923', accent: bubbleBg }
  const r = parseInt(hex.substring(0, 2), 16)
  const g = parseInt(hex.substring(2, 4), 16)
  const b = parseInt(hex.substring(4, 6), 16)
  if ([r, g, b].some(v => Number.isNaN(v))) return { ink: '#160923', accent: bubbleBg }
  // WCAG relative luminance
  const toLinear = (c: number) => { const cn = c / 255; return cn <= 0.03928 ? cn / 12.92 : Math.pow((cn + 0.055) / 1.055, 2.4) }
  const lum = 0.2126 * toLinear(r) + 0.7152 * toLinear(g) + 0.0722 * toLinear(b)
  const toHex = (n: number) => Math.max(0, Math.min(255, Math.round(n))).toString(16).padStart(2, '0')
  const ink = lum > 0.5
    // bg 浅 → ink 用同色调深版 (rgb × 0.3 保持色相)
    ? `#${toHex(r * 0.3)}${toHex(g * 0.3)}${toHex(b * 0.3)}`
    // bg 深 → ink 用同色调浅版 (各 channel 加白 70%)
    : `#${toHex(r + (255 - r) * 0.7)}${toHex(g + (255 - g) * 0.7)}${toHex(b + (255 - b) * 0.7)}`
  return { ink, accent: bubbleBg }
}

// AIC-63 cycle 4: per-actor seed (5 rows). Ensure-mode: INSERT OR IGNORE so PM
// customizations are never overwritten on restart. If the legacy actor_theme_styles
// table has PM customizations from cycle 1-3, prefer those (pastel theme row) over
// the hardcoded defaults — call sites must run migrateThemedToActorStyles first.
function seedActorStylesIfMissing() {
  const now = groupNowIso()
  for (const r of DEFAULT_ACTOR_STYLES) {
    db.run(
      `INSERT OR IGNORE INTO actor_styles (actor_id, avatar_kind, avatar_value, bubble_bg, updated_at) VALUES (?, ?, ?, ?, ?)`,
      [r.actor_id, r.avatar_kind, r.avatar_value, r.bubble_bg, now],
    )
  }
}

// AIC-63 cycle 4 migration: if PM customized rows in the old actor_theme_styles table
// (cycle 1-3), copy the pastel theme row to actor_styles. INSERT OR IGNORE so we
// never overwrite anything user did directly on the new table.
function migrateThemedStylesToActorStyles() {
  try {
    const oldExists = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='actor_theme_styles'`).get()
    if (!oldExists) return
    const oldRows = db.prepare(`SELECT actor_id, avatar_kind, avatar_value, bubble_bg FROM actor_theme_styles WHERE theme_id = 'pastel'`).all() as any[]
    const now = groupNowIso()
    for (const r of oldRows) {
      db.run(
        `INSERT OR IGNORE INTO actor_styles (actor_id, avatar_kind, avatar_value, bubble_bg, updated_at) VALUES (?, ?, ?, ?, ?)`,
        [r.actor_id, r.avatar_kind, r.avatar_value, r.bubble_bg, now],
      )
    }
  } catch (e) { console.error('migrateThemedStylesToActorStyles', e) }
}

// AIC-31/42: seed default workflow templates on first run (table empty).
// AIC-42: writes phase_ids directly (new schema). `phases` column written as '[]' (legacy column,
// reload prefers phase_ids when non-empty).
function seedDefaultsIfDbEmpty() {
  const count = (db.prepare(`SELECT COUNT(*) AS n FROM workflow_templates`).get() as any).n
  if (count > 0) return
  const now = groupNowIso()
  for (const [key, tpl] of Object.entries(DEFAULT_WORKFLOW_TEMPLATES)) {
    db.run(
      `INSERT INTO workflow_templates (key, label, phases, phase_ids, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
      [key, tpl.label, '[]', JSON.stringify(tpl.phase_ids), now, now],
    )
  }
}

// AIC-42 migration: upgrade pre-AIC-42 rows (have legacy inline phases but phase_ids='[]').
// For each such row: derive phase_templates rows from the inline phases (skip if id already exists,
// to preserve PM's customizations from AIC-31 era), then update phase_ids = phases.map(p => p.key).
// Idempotent — re-running on a fully-migrated DB is a no-op.
function migrateInlinePhasesToPhaseIds() {
  const rows = db.prepare(`SELECT key, phases, phase_ids FROM workflow_templates`).all() as any[]
  for (const r of rows) {
    let phaseIds: string[] = []
    try { phaseIds = JSON.parse(r.phase_ids || '[]') } catch {}
    if (phaseIds.length > 0) continue
    let inlinePhases: PhaseDef[] = []
    try { inlinePhases = JSON.parse(r.phases || '[]') } catch {}
    if (inlinePhases.length === 0) continue
    const derivedIds: string[] = []
    for (const p of inlinePhases) {
      const exists = db.prepare(`SELECT id FROM phase_templates WHERE id=?`).get(p.key) as any
      if (!exists) insertPhaseTemplateRow(p)
      derivedIds.push(p.key)
    }
    db.run(`UPDATE workflow_templates SET phase_ids=? WHERE key=?`, [JSON.stringify(derivedIds), r.key])
  }
}

// AIC-42: reload PHASE_TEMPLATES purely from DB. Empty DB → empty map.
function reloadPhaseTemplates() {
  const rows = db.prepare(`SELECT id, name, required_roles, can_reject, auto_advance FROM phase_templates`).all() as any[]
  const next: Record<string, PhaseDef> = {}
  for (const r of rows) {
    try {
      const def: PhaseDef = {
        key: r.id,
        label: r.name,
        required_roles: JSON.parse(r.required_roles) as PhaseDef['required_roles'],
        can_reject: !!r.can_reject,
        auto_advance: !!r.auto_advance,
      }
      next[r.id] = def
    } catch (e) {
      console.error(`phase_templates: bad row for ${r.id}`, e)
    }
  }
  PHASE_TEMPLATES = next
}

// AIC-42: reload purely from DB. phase_ids → PHASE_TEMPLATES JOIN derives phases (new path).
// Fallback to legacy inline phases JSON only if phase_ids is empty (pre-migration rows).
// Sharing PhaseDef object via PHASE_TEMPLATES across workflows is the mechanism for "edit a phase
// template once, all workflows that reference it pick up the change on next reload".
// No auto-seed — empty DB stays empty (defaults only seeded once at startup).
function reloadWorkflowTemplates() {
  const rows = db.prepare(`SELECT key, label, phases, phase_ids FROM workflow_templates`).all() as any[]
  const next: Record<string, { label: string; phases: PhaseDef[] }> = {}
  for (const r of rows) {
    try {
      let phases: PhaseDef[] = []
      let phaseIds: string[] = []
      try { phaseIds = JSON.parse(r.phase_ids || '[]') } catch {}
      if (phaseIds.length > 0) {
        const derived: PhaseDef[] = []
        for (const id of phaseIds) {
          const def = PHASE_TEMPLATES[id]
          if (!def) { console.error(`workflow_templates ${r.key}: missing phase template '${id}'`); continue }
          derived.push(def)
        }
        phases = derived
      } else {
        phases = JSON.parse(r.phases || '[]') as PhaseDef[]
      }
      next[r.key] = { label: r.label, phases }
    } catch (e) {
      console.error(`workflow_templates: bad data for ${r.key}`, e)
    }
  }
  WORKFLOW_TEMPLATES = next
}

// AIC-42 boot order (review fix — migrate BEFORE seed):
//   1. seedDefaultsIfDbEmpty       — empty DB: write 4 default workflow_templates rows (phase_ids set, phases='[]')
//   2. migrateInlinePhasesToPhaseIds — upgrade: any row with legacy inline phases → derive phase_templates rows using THOSE values (PM customizations win)
//   3. ensureDefaultPhaseTemplates — fill in any DEFAULT_PHASE_TEMPLATES id not yet inserted (safe upsert: only inserts missing ids, never overwrites)
//   4. reloadPhaseTemplates / reloadWorkflowTemplates — derive runtime maps
seedDefaultsIfDbEmpty()
migrateInlinePhasesToPhaseIds()
ensureDefaultPhaseTemplates()
migrateRemoveDeprecatedPhaseTemplates()
reloadPhaseTemplates()
reloadWorkflowTemplates()
// AIC-63 cycle 4 boot order: migrate from legacy first (PM customizations win),
// then seed defaults for any actor still missing. AIC-42 教训:「从用户数据派生 > 从代码常量 seed」.
migrateThemedStylesToActorStyles()
seedActorStylesIfMissing()

const ROLE_TO_ACTORS: Record<string, string[]> = {
  pm:          ['admin'],
  implementer: ['agent1'],
  reviewer:    ['agent2'],
  system:      ['system'],
}

const ACTOR_DISPLAY_NAMES: Record<string, string> = {
  admin:  'admin',
  agent1: 'agent1',
  agent2: 'agent2',
  system: 'System',
}

// ULID-ish: Crockford base32 timestamp (10 chars) + 16 chars randomness = 26 chars total.
// Crockford base32 alphabet is monotonic — lexicographic sort == time sort.
const ULID_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

// Monotonic mode: when two ULIDs are minted in the same millisecond, the random
// part of the second one is the first one's random + 1 (base32 big-int add).
// Without this, two events written by a single microtask burst (e.g.
// enterPhase() → handleAutoAdvancePhase() → maybeAdvancePhase() chain in v2)
// land with random ordering, making the activity feed read out-of-order.
let _ulidLastTs = 0
let _ulidLastRand: string[] = []

function encodeUlidTs(ts: number): string {
  let tsPart = ''
  for (let i = 9; i >= 0; i--) {
    tsPart = ULID_ALPHABET[ts % 32] + tsPart
    ts = Math.floor(ts / 32)
  }
  return tsPart
}

function generateULID() {
  let ts = Date.now()
  let randChars: string[]
  // Same-ms OR clock-rollback (incl. NTP correction, system sleep, manual time
  // change): anchor to _ulidLastTs and bump the random portion by 1, so the new
  // ULID is still strictly greater than the previous one. Without the <= branch
  // a backwards tick (e.g. 1000ms → 999ms) would mint a smaller-id ULID and
  // break the "lex sort == write order" guarantee that v2's activity feed relies
  // on. Caught in review.
  if (ts <= _ulidLastTs && _ulidLastRand.length === 16) {
    ts = _ulidLastTs
    randChars = _ulidLastRand.slice()
    let carry = 1
    for (let i = 15; i >= 0 && carry; i--) {
      const idx = ULID_ALPHABET.indexOf(randChars[i]) + carry
      randChars[i] = ULID_ALPHABET[idx % 32]
      carry = idx >= 32 ? 1 : 0
    }
    if (carry) {
      // Overflow (16-char base32 wraparound — astronomically unlikely):
      // bump to the next millisecond and re-randomize.
      ts = _ulidLastTs + 1
      randChars = []
      for (let i = 0; i < 16; i++) randChars.push(ULID_ALPHABET[Math.floor(Math.random() * 32)])
    }
  } else {
    randChars = []
    for (let i = 0; i < 16; i++) randChars.push(ULID_ALPHABET[Math.floor(Math.random() * 32)])
  }
  _ulidLastTs = ts
  _ulidLastRand = randChars
  return encodeUlidTs(ts) + randChars.join('')
}

function findPhaseDef(type: string, phase: string): PhaseDef | null {
  const tpl = WORKFLOW_TEMPLATES[type]
  if (!tpl) return null
  return tpl.phases.find(p => p.key === phase) || null
}

function roleForActor(actor_id: string): string | null {
  for (const [role, actors] of Object.entries(ROLE_TO_ACTORS)) {
    if (actors.includes(actor_id)) return role
  }
  return null
}

function actorsForRoles(roles: string[]): string[] {
  const set = new Set<string>()
  for (const r of roles) {
    for (const a of ROLE_TO_ACTORS[r] || []) set.add(a)
  }
  return [...set]
}

function displayOwnerLabel(actors: string[]): string {
  if (!actors.length) return '—'
  return actors.map(a => ACTOR_DISPLAY_NAMES[a] || a).join(' + ')
}

// Get max cycle for (task, phase); 0 if no row yet
function getMaxCycle(task_id: string, phase: string): number {
  const row = db.prepare(`SELECT COALESCE(MAX(cycle), 0) AS c FROM task_phase_role_checks WHERE task_id=? AND phase=?`).get(task_id, phase) as any
  return row?.c || 0
}

// Get pending_actors for current cycle of current phase
function getPendingActors(task_id: string, phase: string): string[] {
  const cycle = getMaxCycle(task_id, phase)
  if (cycle === 0) return []
  const rows = db.prepare(`SELECT role_id FROM task_phase_role_checks WHERE task_id=? AND phase=? AND cycle=? AND checked_at IS NULL`).all(task_id, phase, cycle) as any[]
  return actorsForRoles(rows.map(r => r.role_id))
}

function formatNotificationText(event: any, task: any): string {
  const tid = task.id
  const meta = event.meta || {}
  switch (meta.event_type) {
    case 'task_published':       return `📋 ${tid} 新任务发布 · 进入 ${meta.to_phase_label || meta.to_phase} 阶段`
    case 'phase_enter': {
      // v0.7.1: phase_enter event derived from advance/reject/rollback; differentiate by triggered_by
      const trig = meta.triggered_by
      if (trig === 'reject')   return `↩ ${tid} ${meta.from_phase_label || meta.from_phase} 不通过 · 退回${meta.to_phase_label || meta.to_phase} · 处理人：你`
      if (trig === 'rollback') return `↩ ${tid} 被打回到${meta.to_phase_label || meta.to_phase} · 处理人：你`
      return `📋 ${tid} 进入 ${meta.to_phase_label || meta.to_phase} 阶段 · 处理人：你`
    }
    case 'task_closed':          return `🎉 ${tid} 已关闭`
    case 'user_comment_mention': return `💬 ${tid}：${ACTOR_DISPLAY_NAMES[event.actor_id] || event.actor_id} 在评论里 @ 了你`
    default:                     return `📋 ${tid} 有更新`
  }
}

// Send a DM system_notification to a single actor. Insert into group_messages, then dispatch
// (which triggers provider dispatch for agents). 'admin' is human-only, no agent dispatch.
// Caller is responsible for NOT calling this with actor_id='system' (would be a self-loop).
function notifyActorViaDM(actor_id: string, task: any, event: any) {
  if (actor_id === 'system') return
  const text = formatNotificationText(event, task)
  const ts = groupNowIso()
  const recordId = groupId('grp')
  const dispatchId = groupId('dsp')
  const delivery = { mode: 'dm', targets: [actor_id], dispatch_id: dispatchId, delivered: [], failed: [] }
  const meta = { task_id: task.id, event_id: event.id, event_type: event.meta?.event_type }
  db.run(
    `INSERT INTO group_messages (id, ts, conversation_id, sender_id, sender_model, text, images, files, mentions, parent_msg_id, reply_to, source, delivery, meta, message_type, task_id, parent_task_id, owner)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [recordId, ts, `dm-${actor_id}`, 'system', null, text, '[]', '[]', '[]', null, null, 'task_notify', JSON.stringify(delivery), JSON.stringify(meta), 'system_notification', task.id, null, actor_id],
  )
  // Tmux injection for agents only (admin is human, no agent session for DM)
  if (actor_id !== 'admin') {
    const record: GroupRecord = {
      id: recordId, ts,
      conversation_id: `dm-${actor_id}`,
      sender_id: 'system', sender_model: null,
      text, images: [], files: [], mentions: [],
      parent_msg_id: null, reply_to: null,
      source: 'task_notify', delivery, meta,
      message_type: 'system_notification', task_id: task.id, parent_task_id: null, owner: actor_id,
    }
    try { dispatchGroupRecord(record, [actor_id], 0, false) } catch (e) { console.error('notifyActorViaDM dispatch failed:', e) }
  }
}

// Notify a set of actors (dedup, skip 'system')
function notifyActorsViaDM(actor_ids: string[], task: any, event: any) {
  const seen = new Set<string>()
  for (const a of actor_ids) {
    if (a === 'system' || seen.has(a)) continue
    seen.add(a)
    notifyActorViaDM(a, task, event)
  }
}

// Insert a task_event row and return the event object (with id/ts populated)
function insertTaskEvent(task_id: string, kind: 'system_event' | 'user_comment', actor_id: string, body: string, meta: any) {
  const id = 'evt_' + generateULID()
  const ts = groupNowIso()
  db.run(
    `INSERT INTO task_events (id, task_id, ts, kind, actor_id, body, meta) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [id, task_id, ts, kind, actor_id, body, JSON.stringify(meta || {})],
  )
  return { id, task_id, ts, kind, actor_id, body, meta: meta || {} }
}

// Enter a phase: bump cycle, insert role check rows, update task.current_phase, write phase_advance event.
// If new_phase has auto_advance=true, also schedule the auto handler.
// Returns the event object for the caller to use as notification payload.
function enterPhase(task: any, new_phase: string, fromPhase: string | null, triggerKind: 'advance' | 'reject' | 'rollback' | 'publish') {
  const phaseDef = findPhaseDef(task.type, new_phase)
  if (!phaseDef) throw new Error(`unknown phase ${new_phase} for type ${task.type}`)
  const nextCycle = getMaxCycle(task.id, new_phase) + 1
  const now = groupNowIso()
  for (const role of phaseDef.required_roles) {
    db.run(
      `INSERT INTO task_phase_role_checks (task_id, phase, cycle, role_id, checked_at, checked_by, checked_via) VALUES (?, ?, ?, ?, NULL, NULL, NULL)`,
      [task.id, new_phase, nextCycle, role],
    )
  }
  // Update task main row
  const isClosed = new_phase === 'closed'
  if (isClosed) {
    db.run(`UPDATE tasks_v2 SET current_phase = ?, status = 'closed', closed_at = ?, closed_by = ?, updated_at = ? WHERE id = ?`,
      [new_phase, now, task.created_by || 'system', now, task.id])
  } else {
    db.run(`UPDATE tasks_v2 SET current_phase = ?, updated_at = ? WHERE id = ?`, [new_phase, now, task.id])
  }
  const fromDef = fromPhase ? findPhaseDef(task.type, fromPhase) : null
  // v0.7.1 Fix 2: enterPhase only writes ONE event per call. Action events (phase_reject/phase_rollback
  // with comment + rejected_by_actor) are written by the endpoint BEFORE calling enterPhase, so this
  // function writes the "arrival" event with type derived from context. Avoids double-write.
  const eventType =
    triggerKind === 'publish' ? 'task_published' :
    (isClosed ? 'task_closed' : 'phase_enter')
  const meta: any = {
    event_type: eventType,
    from_phase: fromPhase,
    from_phase_label: fromDef?.label,
    to_phase: new_phase,
    to_phase_label: phaseDef.label,
    cycle: nextCycle,
    triggered_by: triggerKind,
  }
  const body = `${triggerKind === 'rollback' ? '↩' : triggerKind === 'reject' ? '✗' : '→'} 进入 ${phaseDef.label}（cycle ${nextCycle}）`
  const event = insertTaskEvent(task.id, 'system_event', 'system', body, meta)

  // Refresh task object with new fields for downstream notify
  task.current_phase = new_phase
  if (isClosed) { task.status = 'closed'; task.closed_at = now }

  // Notify per §14.1
  if (eventType === 'task_published') {
    // Notify first non-pm required actor (PM is the publisher, no self-ping)
    const firstActors = actorsForRoles(phaseDef.required_roles).filter(a => a !== 'admin')
    notifyActorsViaDM(firstActors, task, event)
  } else if (eventType === 'task_closed') {
    // v0.7.1 Fix 3: participants = events.actor_id ∪ phase_role_checks.checked_by (minus system)
    const fromEvents = db.prepare(`SELECT DISTINCT actor_id FROM task_events WHERE task_id=? AND actor_id != 'system'`).all(task.id) as any[]
    const fromChecks = db.prepare(`SELECT DISTINCT checked_by FROM task_phase_role_checks WHERE task_id=? AND checked_by IS NOT NULL AND checked_by != 'system'`).all(task.id) as any[]
    const actors = new Set<string>()
    for (const r of fromEvents) actors.add(r.actor_id)
    for (const r of fromChecks) actors.add(r.checked_by)
    notifyActorsViaDM([...actors], task, event)
  } else {
    // phase_enter (advance / reject / rollback) → notify new-phase owners
    const owners = actorsForRoles(phaseDef.required_roles).filter(a => a !== 'system')
    notifyActorsViaDM(owners, task, event)
  }

  // Schedule auto_advance — no auto_advance phase in default workflows after tf_upload removal,
  // but the hook is kept here for future use (e.g. external CI integrations registering custom phases).
  if (phaseDef.auto_advance) {
    queueMicrotask(() => handleAutoAdvancePhase(task.id, new_phase, nextCycle))
  }

  return event
}

// Auto-advance handler. No auto_advance phases ship in the default templates; this is the
// extension point for custom phase templates with auto_advance=true. Default behavior: skip
// instantly (mark system check checked + advance) so a misconfigured template doesn't wedge.
function handleAutoAdvancePhase(task_id: string, phase: string, cycle: number) {
  const task = loadTask(task_id)
  if (!task) return
  insertTaskEvent(task_id, 'system_event', 'system', `⏭ 自动推进阶段 ${phase}`, { event_type: 'phase_auto_skipped', phase })
  db.run(`UPDATE task_phase_role_checks SET checked_at = ?, checked_by = 'system', checked_via = 'auto' WHERE task_id=? AND phase=? AND cycle=? AND role_id='system'`, [groupNowIso(), task_id, phase, cycle])
  maybeAdvancePhase(loadTask(task_id))
  wakeTaskWaiters(task_id)
}

// Check whether current cycle of current phase is fully checked → auto-advance to next phase.
// Returns true if advanced.
function maybeAdvancePhase(task: any): boolean {
  const phaseDef = findPhaseDef(task.type, task.current_phase)
  if (!phaseDef) return false
  if (!phaseDef.required_roles.length) return false // closed phase
  const cycle = getMaxCycle(task.id, task.current_phase)
  const unchecked = (db.prepare(
    `SELECT COUNT(*) AS n FROM task_phase_role_checks WHERE task_id=? AND phase=? AND cycle=? AND checked_at IS NULL`
  ).get(task.id, task.current_phase, cycle) as any).n
  if (unchecked > 0) return false
  // All required_roles checked → advance to next phase
  const phases = WORKFLOW_TEMPLATES[task.type]?.phases || []
  const idx = phases.findIndex(p => p.key === task.current_phase)
  if (idx < 0 || idx >= phases.length - 1) return false
  const next = phases[idx + 1]
  enterPhase(task, next.key, task.current_phase, 'advance')
  return true
}

// Find previous non-auto_advance phase. Used by reject (reviewer rejects → back to implement).
function previousNonAutoPhase(type: string, current: string): string | null {
  const phases = WORKFLOW_TEMPLATES[type]?.phases || []
  const idx = phases.findIndex(p => p.key === current)
  for (let i = idx - 1; i >= 0; i--) {
    if (!phases[i].auto_advance && phases[i].key !== 'draft') return phases[i].key
  }
  return null
}

// Refresh task row from DB
function loadTask(task_id: string): any | null {
  return db.prepare(`SELECT * FROM tasks_v2 WHERE id = ?`).get(task_id) || null
}

// Long poll waiters: events (per task) and summary (all open tasks)
type TaskEventWaiter = { taskId: string; sinceEventId: string; resolve: (r: Response) => void; timer: ReturnType<typeof setTimeout> }
type TaskSummaryWaiter = { sinceCursor: string; resolve: (r: Response) => void; timer: ReturnType<typeof setTimeout> }
const taskEventWaiters = new Set<TaskEventWaiter>()
const taskSummaryWaiters = new Set<TaskSummaryWaiter>()

// Wake any long poll waiters for a task after a write.
// taskEventWaiters: wake those listening to this task.
// taskSummaryWaiters: wake all (any write may change the cursor).
// opts.deleted = true means the task was just deleted; resolve event waiters with 410 so detail
// pages don't have to wait 30s to discover the task is gone.
function wakeTaskWaiters(taskId: string, opts: { deleted?: boolean } = {}) {
  for (const w of [...taskEventWaiters]) {
    if (w.taskId !== taskId) continue
    if (opts.deleted) {
      clearTimeout(w.timer)
      taskEventWaiters.delete(w)
      w.resolve(Response.json({ ok: false, deleted: true, error: 'task deleted' }, { status: 410 }))
      continue
    }
    const rows = db.prepare(`SELECT * FROM task_events WHERE task_id=? AND id > ? ORDER BY id ASC`).all(taskId, w.sinceEventId) as any[]
    if (rows.length === 0) continue
    clearTimeout(w.timer)
    taskEventWaiters.delete(w)
    w.resolve(Response.json({ ok: true, events: rows.map(e => ({ ...e, meta: JSON.parse(e.meta || '{}') })) }))
  }
  for (const w of [...taskSummaryWaiters]) {
    clearTimeout(w.timer)
    taskSummaryWaiters.delete(w)
    // Build a fresh snapshot inline (logic duplicated from the summary handler; kept simple intentionally)
    const rows = db.prepare(`SELECT * FROM tasks_v2 WHERE status='open' ORDER BY updated_at DESC`).all() as any[]
    for (const row of rows) {
      attachPendingFields(row)
      const latest = db.prepare(`SELECT id, ts, kind, actor_id, meta, body FROM task_events WHERE task_id=? ORDER BY id DESC LIMIT 1`).get(row.id) as any
      row.latest_event_id = latest?.id || null
      row.latest_event_ts = latest?.ts || null
      row.latest_event_kind = latest?.kind || null
      row.latest_event_actor = latest?.actor_id || null
      row.latest_event_body = latest?.body ? String(latest.body).slice(0, 120) : null
      // AIC-61 cycle 3: mention 字段 for toast trigger (评论 @ admin 弹通知)
      let latestEventMentions: string[] = []
      if (latest?.meta) { try { const m = JSON.parse(latest.meta); if (Array.isArray(m?.mentions)) latestEventMentions = m.mentions } catch {} }
      row.latest_event_mentions = latestEventMentions
      const unread: Record<string, number> = {}
      for (const actor of ['admin', 'agent1', 'agent2']) {
        const mark = db.prepare(`SELECT last_seen_event_id FROM task_seen_marks WHERE task_id=? AND actor_id=?`).get(row.id, actor) as any
        const lastId = mark?.last_seen_event_id || ''
        const n = (db.prepare(`SELECT COUNT(*) AS n FROM task_events WHERE task_id=? AND id > ?`).get(row.id, lastId) as any).n
        unread[actor] = n
      }
      row.unread_count = unread
    }
    let cursor = ''
    for (const r of rows) { if (r.latest_event_id && r.latest_event_id > cursor) cursor = r.latest_event_id }
    w.resolve(Response.json({ ok: true, cursor, tasks: rows }))
  }
}

// Build pending_actors + display_owner_label for an API row
function attachPendingFields(task: any) {
  const pending = getPendingActors(task.id, task.current_phase)
  task.pending_actors = pending
  task.display_owner_label = displayOwnerLabel(pending)
  const phaseDef = findPhaseDef(task.type, task.current_phase)
  task.current_phase_label = phaseDef?.label || task.current_phase
  task.type_label = WORKFLOW_TEMPLATES[task.type]?.label || task.type
  const phases = WORKFLOW_TEMPLATES[task.type]?.phases || []
  task.phase_index = phases.findIndex(p => p.key === task.current_phase)
  task.phase_total = phases.length
  return task
}

// AIC-116: legacy session helpers (tmuxSessionExists / safeTmuxSession / sendTmuxText / sendTmuxKey)
// removed. Agent communication goes through `provider.send()` (long-lived stream subprocess);
// agent "online" status derives from `AgentProvider.isAlive`. See groupStatusSnapshot below.

function commandForPid(pid: number) {
  if (!pid || pid < 1) return ''
  try {
    return Bun.spawnSync(['ps', '-p', String(pid), '-ww', '-o', 'command=']).stdout.toString().trim()
  } catch {
    return ''
  }
}

function parentPidFor(pid: number) {
  if (!pid || pid < 1) return 0
  try {
    return Number(Bun.spawnSync(['ps', '-p', String(pid), '-o', 'ppid=']).stdout.toString().trim()) || 0
  } catch {
    return 0
  }
}

function normalizeGroupMentions(raw: any, text = '') {
  const items: string[] = []
  if (typeof raw === 'string') {
    items.push(...raw.split(',').map((x) => x.trim()).filter(Boolean))
  } else if (Array.isArray(raw)) {
    items.push(...raw.map((x) => String(x).trim()).filter(Boolean))
  }
  for (const match of text.matchAll(GROUP_MENTION_RE)) items.push(match[1])

  const out: string[] = []
  const seen = new Set<string>()
  for (const item of items) {
    const key = item.trim().replace(/^@/, '').toLowerCase()
    const id = GROUP_ALIASES.get(key)
    if (id && !seen.has(id)) {
      out.push(id)
      seen.add(id)
    }
  }
  return out
}

function defaultGroupResponder() {
  const defaultOn = GROUP_ROSTER.find((m) => m.default_responder && groupAgentAutoReply(m.id))?.id
  return defaultOn || GROUP_REPLY_AGENT_IDS.find((id) => groupAgentAutoReply(id)) || null
}

function groupTargetsFor(senderId: string, mentions: string[], hopCount = 0) {
  const sender = GROUP_ROSTER_BY_ID.get(senderId)
  const replyable = GROUP_REPLY_AGENT_IDS.filter((id) => groupAgentAutoReply(id))
  if (!sender?.can_reply) {
    const effective = mentions.length ? mentions : replyable
    if (effective.includes(GROUP_ALL_TOKEN)) return replyable
    return effective.filter((id) => replyable.includes(id))
  }
  if (hopCount >= 3 || mentions.length === 0 || mentions.includes(GROUP_ALL_TOKEN)) return []
  return mentions.filter((id) => replyable.includes(id) && id !== senderId)
}

function groupObserverTargetsFor(senderId: string, responseTargets: string[]) {
  const sender = GROUP_ROSTER_BY_ID.get(senderId)
  const responseSet = new Set(responseTargets)
  return GROUP_REPLY_AGENT_IDS.filter((id) => {
    if (id === senderId || responseSet.has(id)) return false
    return groupAgentAutoReply(id)
  })
}

// AIC-50: per-session creation timestamp (epoch seconds) for the "本 session"
// uptime panel. cycle 2 (review fix): the cycle-1 永久 cache was unsafe — if a
// same-name session was killed and immediately re-created without any poll
// catching the offline moment in between, the stale created_at would keep
// being served and uptime would never reset. The probe returns
// nonzero on missing session so this also implicitly probes existence in one
// spawn (a few ms via the local socket — cheap enough at 2.5s poll cadence).
// AIC-116: per-session creation timestamp removed. Long-lived provider doesn't expose
// a stable "session started" wall-clock — `group_agent_state.last_seen` is the closest analog
// and already in the snapshot. Web 不再显示 "本 session" 工时, 不丢核心功能。

// AIC-60: per-agent session context tokens — mirrors Hub iOS CTX card.
// Each agent writes its own heartbeat from a Stop/SessionStart hook (or equivalent).
const AGENT_HEARTBEAT_PATHS: Record<string, string> = {
  agent1: AGENT1_HEARTBEAT_PATH,
}
function readAgentContext(agentId: string): { tokens: number | null; percent: number | null; detail: string | null } {
  const path = AGENT_HEARTBEAT_PATHS[agentId]
  if (!path) return { tokens: null, percent: null, detail: null }
  try {
    if (!existsSync(path)) return { tokens: null, percent: null, detail: null }
    const hb = JSON.parse(readFileSync(path, 'utf-8')) as any
    const tokens = typeof hb?.context_tokens === 'number' ? hb.context_tokens : 0
    if (tokens <= 0) return { tokens: null, percent: null, detail: null }
    const percent = Math.min(100, Math.round(tokens / 10000))   // 假设 1M context window
    const detail = `~${Math.round(tokens / 1000)}K / 1M`
    return { tokens, percent, detail }
  } catch { return { tokens: null, percent: null, detail: null } }
}

// AIC-115/116: transcript jsonl path resolution — driven by provider session_id, not by external pane PID.
// Resolution: env override (`AGENT{1,2}_PROJECT_DIR`) → agent-configured cwd → ~/.claude/projects
// glob by sid. Only supported for ClaudeProvider (CodexProvider keeps its sessions in
// `~/.codex/sessions/`, different schema — endpoint returns 410 for codex agents).

// Anthropic CLI's project dir naming convention: cwd `/Users/foo/bar` → `~/.claude/projects/-Users-foo-bar/`.
function claudeProjectDirForCwd(cwd: string): string {
  const encoded = (cwd.startsWith('/') ? cwd : '/' + cwd).replace(/\//g, '-')
  return path.join(homedir(), '.claude/projects', encoded)
}

// Scan ~/.claude/projects/* for the project dir containing `${sid}.jsonl`.
// sid is globally unique so at most one dir matches. Cheap (~10ms for ~10 dirs).
function findProjectDirContainingSid(sid: string): string | null {
  const root = path.join(homedir(), '.claude/projects')
  if (!existsSync(root)) return null
  try {
    for (const d of readdirSync(root)) {
      const full = path.join(root, d)
      const p = path.join(full, `${sid}.jsonl`)
      if (existsSync(p)) return full
    }
  } catch {}
  return null
}

function agentProjectDirOverride(agentId: string): string {
  if (agentId === 'agent1') return AGENT1_PROJECT_DIR
  if (agentId === 'agent2') return AGENT2_PROJECT_DIR
  return ''
}

// Map agentId → { transcript jsonl path, sid }. Drives /api/transcript + /api/agent-billing.
// Returns null when: agent has no live provider / provider isn't claude / provider hasn't yet
// captured a session_id (no assistant event yet) / file missing on disk.
function resolveTranscriptPath(agentId: string): { path: string; sid: string } | null {
  const provider = AGENT_PROVIDERS.get(agentId)
  if (!provider || provider.capabilities().providerName !== 'claude') return null
  const sid = provider.sessionId
  if (!sid) return null

  // 1. env override (operator-set explicit project dir)
  const overrideDir = agentProjectDirOverride(agentId)
  if (overrideDir) {
    const p = path.join(overrideDir, `${sid}.jsonl`)
    if (existsSync(p)) return { path: p, sid }
  }
  // 2. derive from agent-configured cwd
  const cfg = AGENT_RUNTIMES[agentId]
  if (cfg?.cwd) {
    const dir = claudeProjectDirForCwd(cfg.cwd)
    const p = path.join(dir, `${sid}.jsonl`)
    if (existsSync(p)) return { path: p, sid }
  }
  // 3. glob fallback — sid is unique, dir scan is cheap
  const dir = findProjectDirContainingSid(sid)
  if (dir) {
    const p = path.join(dir, `${sid}.jsonl`)
    if (existsSync(p)) return { path: p, sid }
  }
  return null
}

// Opus 4.x pricing (per-token; Anthropic listed $15/M input · $75/M output ·
// $1.5/M cache_read · $18.75/M cache_creation). Used for per-turn cost chip.
const OPUS_PRICING = {
  input_per_tok: 15e-6,
  output_per_tok: 75e-6,
  cache_read_per_tok: 1.5e-6,
  cache_creation_per_tok: 18.75e-6,
}
function calcOpusTurnCostUsd(input: number, output: number, cacheRead: number, cacheCreation: number): number {
  return input * OPUS_PRICING.input_per_tok
    + output * OPUS_PRICING.output_per_tok
    + cacheRead * OPUS_PRICING.cache_read_per_tok
    + cacheCreation * OPUS_PRICING.cache_creation_per_tok
}

// Reverse-scan transcript jsonl tail to find the most recent assistant event's
// `message.usage`. Tail-read 1MB cap, not full file — at 1.5s poll cadence a
// 16MB+ jsonl would double IO; the latest assistant event is always near the
// tail. Mirrors main repo readAgentLastTurnUsage (AIC-113).
function readAgentLastTurnUsage(agentId: string): {
  input_tokens: number
  output_tokens: number
  cache_read_input_tokens: number
  cache_creation_input_tokens: number
  cost_usd: number
} | null {
  const resolved = resolveTranscriptPath(agentId)
  if (!resolved) return null
  try {
    const TAIL_CAP = 1_000_000
    const stat = statSync(resolved.path)
    let raw: string
    if (stat.size > TAIL_CAP) {
      const fd = openSync(resolved.path, 'r')
      const startByte = stat.size - TAIL_CAP
      const buf = Buffer.alloc(TAIL_CAP)
      try { readSync(fd, buf, 0, TAIL_CAP, startByte) } finally { closeSync(fd) }
      raw = buf.toString('utf-8')
      const nl = raw.indexOf('\n')
      if (nl !== -1) raw = raw.slice(nl + 1)  // drop partial first line
    } else {
      raw = readFileSync(resolved.path, 'utf-8')
    }
    const lines = raw.split('\n')
    let checked = 0
    for (let i = lines.length - 1; i >= 0 && checked < 20; i--) {
      const line = lines[i].trim()
      if (!line) continue
      try {
        const ev = JSON.parse(line) as any
        if (ev.type === 'assistant') {
          const usage = ev.message?.usage || {}
          const inp = usage.input_tokens || 0
          const out = usage.output_tokens || 0
          const cr = usage.cache_read_input_tokens || 0
          const cc = usage.cache_creation_input_tokens || 0
          if (inp + out + cr + cc > 0) {
            return {
              input_tokens: inp,
              output_tokens: out,
              cache_read_input_tokens: cr,
              cache_creation_input_tokens: cc,
              cost_usd: calcOpusTurnCostUsd(inp, out, cr, cc),
            }
          }
          checked++
        }
      } catch {}
    }
  } catch {}
  return null
}

function groupStatusSnapshot() {
  const agents: Record<string, any> = {}
  for (const member of GROUP_ROSTER.filter((m) => m.kind === 'agent')) {
    const row = db.prepare(`SELECT * FROM group_agent_state WHERE agent_id = ?`).get(member.id) as any
    // AIC-116: online iff a long-lived provider instance is registered for this agent. Even if
    // the underlying subprocess is between sends (process spawns on first send, exits on close),
    // "online" tracks operator intent — the agent slot is configured + provisioned.
    const provider = AGENT_PROVIDERS.get(member.id)
    const online = !!provider
    const ctx = readAgentContext(member.id)
    agents[member.id] = {
      state: online ? 'online' : 'offline',
      // provider-kind field (legacy name `tmux` retained for response-shape compatibility with
      // existing web/iOS consumers; value is the configured provider kind so dashboards can
      // show "claude" / "codex" directly without server-side derivation). Field will be
      // renamed `.runtime` in a later breaking change.
      tmux: AGENT_RUNTIMES[member.id]?.provider ?? null,
      last_seen: row?.last_seen || null,
      last_active: row?.last_active || null,
      is_typing: Boolean(row?.is_typing),
      typing_since: row?.typing_since || null,
      dispatch_id: row?.dispatch_id || null,
      status_text: row?.status_text || null,
      auto_reply: groupAgentAutoReply(member.id),
      session_started_at: null,  // AIC-116: no equivalent for long-lived providers
      // AIC-60: session context — null if the agent's runtime doesn't surface it.
      context_tokens: ctx.tokens,
      context_percent: ctx.percent,
      context_detail: ctx.detail,
    }
  }
  return { agents }
}

function groupMembersPayload() {
  const status = groupStatusSnapshot().agents
  return GROUP_ROSTER.map((member) => ({
    ...member,
    name: member.display_name,
    online: status[member.id]?.state === 'online',
    typing: Boolean(status[member.id]?.is_typing),
  }))
}

function groupRowToRecord(row: any): GroupRecord {
  return {
    id: row.id,
    ts: row.ts,
    conversation_id: row.conversation_id || 'workgroup',
    sender_id: row.sender_id,
    sender_model: row.sender_model || null,
    text: row.text,
    images: jsonParseSafe(row.images, []),
    files: jsonParseSafe(row.files, []),
    mentions: jsonParseSafe(row.mentions, []),
    parent_msg_id: row.parent_msg_id || null,
    reply_to: row.reply_to || null,
    source: row.source || 'api',
    delivery: jsonParseSafe(row.delivery, {}),
    meta: jsonParseSafe(row.meta, {}),
    message_type: row.message_type || 'chat',
    task_id: row.task_id || null,
    parent_task_id: row.parent_task_id || null,
    owner: row.owner || null,
  }
}

function readGroupRecords(since: string | null, limit: number, conversationId?: string, beforeId?: string | null) {
  const safeLimit = Math.min(Math.max(limit || 120, 1), 500)
  let rows: any[]
  // AIC-90: before_id 历史分页 — 取 ts < anchor.ts (按 ts DESC) limit 条 + reverse 让最早在头
  if (beforeId) {
    const anchor = db.prepare(`SELECT ts FROM group_messages WHERE id = ?`).get(beforeId) as any
    if (!anchor) return []
    rows = conversationId
      ? (db.prepare(`SELECT * FROM group_messages WHERE conversation_id = ? AND ts < ? ORDER BY ts DESC LIMIT ?`).all(conversationId, anchor.ts, safeLimit) as any[]).reverse()
      : (db.prepare(`SELECT * FROM group_messages WHERE ts < ? ORDER BY ts DESC LIMIT ?`).all(anchor.ts, safeLimit) as any[]).reverse()
  } else if (conversationId) {
    rows = since
      ? db.prepare(`SELECT * FROM group_messages WHERE conversation_id = ? AND ts > ? ORDER BY ts ASC LIMIT ?`).all(conversationId, since, safeLimit) as any[]
      : (db.prepare(`SELECT * FROM group_messages WHERE conversation_id = ? ORDER BY ts DESC LIMIT ?`).all(conversationId, safeLimit) as any[]).reverse()
  } else {
    rows = since
      ? db.prepare(`SELECT * FROM group_messages WHERE ts > ? ORDER BY ts ASC LIMIT ?`).all(since, safeLimit) as any[]
      : (db.prepare(`SELECT * FROM group_messages ORDER BY ts DESC LIMIT ?`).all(safeLimit) as any[]).reverse()
  }
  return rows.map(groupRowToRecord)
}

function updateGroupAgentState(agentId: string, patch: Record<string, any>) {
  const existing = db.prepare(`SELECT agent_id FROM group_agent_state WHERE agent_id = ?`).get(agentId) as any
  if (!existing) {
    db.run(
      `INSERT INTO group_agent_state (agent_id, last_seen, is_typing, typing_since, dispatch_id, status_text, last_active) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [agentId, null, 0, null, null, null, null],
    )
  }
  const current = db.prepare(`SELECT * FROM group_agent_state WHERE agent_id = ?`).get(agentId) as any
  const next = { ...current, ...patch }
  db.run(
    `UPDATE group_agent_state SET last_seen = ?, is_typing = ?, typing_since = ?, dispatch_id = ?, status_text = ?, last_active = ? WHERE agent_id = ?`,
    [next.last_seen || null, next.is_typing ? 1 : 0, next.typing_since || null, next.dispatch_id || null, next.status_text || null, next.last_active || null, agentId],
  )
}

// AIC-48: per-agent casual "working" phrase pool. PM-curated final list (see task feed).
// Mark/clear pair below replaces the old `status_text = 'reading dm' / 'reading group task'`
// hardcoded strings — those felt robotic. Now the agent gets a randomly-picked phrase from
// its animal-themed pool every time we mark it busy.
// "working" status text shown on each agent's worker card. Per-agent phrase pools — pick
// something playful or just leave the generic defaults. Customize per agent personality.
const WORKING_PHRASES: Record<string, string[]> = {
  agent1: ['正在思考...', '处理中...', '写代码中...', 'Thinking...', 'Working...'],
  agent2: ['正在思考...', '处理中...', '审查中...', 'Reviewing...', 'Working...'],
}
function pickWorkingPhrase(agentId: string): string {
  const pool = WORKING_PHRASES[agentId]
  if (!pool || !pool.length) return '工作中...'
  return pool[Math.floor(Math.random() * pool.length)]
}
// AIC-48: mark an agent as actively working — random phrase from its pool + bump last_active.
// Called from dispatchGroupRecord (DM/group injection).
function markAgentWorking(agentId: string, opts: { dispatch_id?: string | null } = {}) {
  const now = groupNowIso()
  updateGroupAgentState(agentId, {
    last_seen: now,
    last_active: now,
    is_typing: 1,
    typing_since: now,
    dispatch_id: opts.dispatch_id ?? null,
    status_text: pickWorkingPhrase(agentId),
  })
}
// AIC-48: mark an agent as idle — clear status text but record last_active so frontend can
// show "上次活跃 X 分钟前". Called when the agent posts back a reply via /group/send (its
// "I'm done with this round" signal).
function markAgentIdle(agentId: string) {
  const now = groupNowIso()
  updateGroupAgentState(agentId, {
    last_seen: now,
    last_active: now,
    is_typing: 0,
    typing_since: null,
    status_text: null,
  })
}

// AIC-116: AGENT_WATCHERS (legacy visual screen capture diff探活) removed. Working/idle status is now
// derived from `AgentProvider.onEvent` lifecycle (system_init/assistant → working;
// result → idle). The CLI runtime's own lifecycle hooks (e.g. Claude Code's UserPromptSubmit
// / Stop POSTing /group/agent/working / idle) still work as a secondary signal — provider
// events just cover the case where the agent runtime doesn't have hooks installed.

// AIC-119: per-agent statusline cache. agent runtime's statusline.sh fire-and-forget POST 整 stdin
// JSON 进来 → web 终端面板底部 row 5s poll cached + age_ms 自己判 stale. server 重启 cache 丢失无害.
const AGENT_STATUSLINE_CACHE = new Map<string, { data: any; ts: number }>()
const AGENT_STATUSLINE_IDS = new Set(['agent1', 'agent2'])

function isAgentWorkingInDb(agentId: string): boolean {
  const row = db.prepare(`SELECT status_text, is_typing FROM group_agent_state WHERE agent_id = ?`).get(agentId) as any
  if (!row) return false
  return Boolean((row.status_text && String(row.status_text).trim()) || row.is_typing)
}

function groupAgentAutoReply(agentId: string) {
  const row = db.prepare(`SELECT auto_reply FROM group_agent_settings WHERE agent_id = ?`).get(agentId) as any
  if (row) return Boolean(row.auto_reply)
  return agentId === 'agent1' || agentId === 'agent2'
}

function setGroupAgentAutoReply(agentId: string, enabled: boolean) {
  if (!GROUP_ROSTER_BY_ID.has(agentId)) throw new Error(`unknown agent_id: ${agentId}`)
  const member = GROUP_ROSTER_BY_ID.get(agentId)
  if (!member?.can_reply) throw new Error(`agent cannot reply: ${agentId}`)
  db.run(
    `INSERT INTO group_agent_settings (agent_id, auto_reply, updated_at)
     VALUES (?, ?, ?)
     ON CONFLICT(agent_id) DO UPDATE SET auto_reply = excluded.auto_reply, updated_at = excluded.updated_at`,
    [agentId, enabled ? 1 : 0, groupNowIso()],
  )
}

function appendGroupRecord(input: any, mentions: string[], delivery: any) {
  const senderId = String(input.sender_id || 'admin').trim()
  const member = GROUP_ROSTER_BY_ID.get(senderId)
  if (!member) throw new Error(`unknown sender_id: ${senderId}`)
  const text = String(input.text || '').trim()
  const images: string[] = Array.isArray(input.images) ? input.images.filter((x: any) => typeof x === 'string') : []
  const files: any[] = Array.isArray(input.files) ? input.files : []
  if (!text && images.length === 0 && files.length === 0) throw new Error('text or attachment required')
  let messageType = String(input.message_type || 'chat').trim().toLowerCase()
  if (!GROUP_MESSAGE_TYPES.has(messageType)) throw new Error(`bad message_type: ${messageType}`)
  let taskId = String(input.task_id || '').trim() || null
  if (messageType === 'task' && !taskId) taskId = groupId('task')
  const ts = groupNowIso()
  const record: GroupRecord = {
    id: groupId('grp'),
    ts,
    conversation_id: String(input.conversation_id || 'workgroup'),
    sender_id: senderId,
    sender_model: input.model || member.model || null,
    text,
    images,
    files,
    mentions,
    parent_msg_id: input.parent_msg_id || null,
    reply_to: input.reply_to || null,
    source: String(input.source || 'api'),
    delivery,
    meta: input.meta && typeof input.meta === 'object' ? input.meta : {},
    message_type: messageType,
    task_id: taskId,
    parent_task_id: String(input.parent_task_id || '').trim() || null,
    owner: String(input.owner || '').trim() || (delivery.targets?.[0] || null),
  }
  db.run(
    `INSERT INTO group_messages (id, ts, conversation_id, sender_id, sender_model, text, images, files, mentions, parent_msg_id, reply_to, source, delivery, meta, message_type, task_id, parent_task_id, owner)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      record.id,
      record.ts,
      record.conversation_id,
      record.sender_id,
      record.sender_model,
      record.text,
      JSON.stringify(record.images),
      JSON.stringify(record.files),
      JSON.stringify(record.mentions),
      record.parent_msg_id,
      record.reply_to,
      record.source,
      JSON.stringify(record.delivery),
      JSON.stringify(record.meta),
      record.message_type,
      record.task_id,
      record.parent_task_id,
      record.owner,
    ],
  )
  // AIC-51: previously agent post → markAgentIdle as a "round done" heuristic.
  // Removed because it fires mid-round when the agent sends a progress message
  // while still working, falsely flipping state to idle before Stop hook /
  // provider event (the real signal) gets a chance to fire.
  return record
}

function saveGroupDelivery(recordId: string, delivery: any) {
  db.run(`UPDATE group_messages SET delivery = ? WHERE id = ?`, [JSON.stringify(delivery), recordId])
}

function groupContextLines(limit = 16) {
  return readGroupRecords(null, limit, 'workgroup').map((rec) => {
    const member = GROUP_ROSTER_BY_ID.get(rec.sender_id)
    const name = member?.display_name || rec.sender_id
    const text = rec.text.replace(/\s+/g, ' ').slice(0, 180)
    return `[${rec.ts.slice(11, 16)}] ${name}: ${text}`
  }).join('\n')
}

// v2 子任务 3/4: build envelope for system_notification DM messages so agent sees [task / ...] prefix
// instead of generic [私聊 / system]. Recipient gets task card + recent 10 events (each comment ≤200 chars).
function buildTaskInjection(record: GroupRecord, recipientActor: string): string | null {
  if (record.sender_id !== 'system' || record.message_type !== 'system_notification') return null
  const meta = (record.meta && typeof record.meta === 'object') ? record.meta : {}
  const taskId = record.task_id || meta.task_id
  if (!taskId) return null
  const task = loadTask(String(taskId))
  if (!task) return null
  // Closed-state special case (PM 2026-06-21 ask): skip the 10-event feed history — it
  // wastes tokens since recipients already saw those events in previous notifications.
  // Just send the headline + record.text (e.g. "🎉 AIC-XX 已关闭").
  if (task.current_phase === 'closed') {
    return [
      `[task / ${taskId} / closed / ${recipientActor}]`,
      `📋 ${task.title}`,
      '',
      record.text,
    ].join('\n')
  }
  // AIC-117: notification 精简到 ~50-100 tokens. 老版每次塞最近 10 条 feed history
  // (10 events × 200 char ≈ 2000 字 ≈ 500-800 tokens), 在长寿命 stream-json agent
  // (AIC-104 主仓 / AIC-116 ai-collab) 已跨 turn 累积上下文的前提下 = 冗余浪费. agent
  // 想看历史改成按需主动拉:
  //   curl http://localhost:<PORT>/tasks/<id>/events?limit=20 -H "Authorization: Bearer ..."
  // README.md 「Agent runtime template」段教 agent 这么用.
  const phaseDef = findPhaseDef(task.type, task.current_phase)
  const phaseLabel = phaseDef?.label || task.current_phase
  const recipientRole = roleForActor(recipientActor) || ''
  const ownerActors = getPendingActors(taskId, task.current_phase)
  const ownerLabel = displayOwnerLabel(ownerActors)
  const cycle = getMaxCycle(taskId, task.current_phase)
  const head = `[task / ${taskId} / ${task.current_phase} / ${recipientActor}]`
  // AIC-117: 含 cycle 让 agent 看到当前第几轮 (reject 回滚 cycle 2/3 时关键).
  const roleAndCycle = `${recipientRole ? `你的 role：${recipientRole}, ` : ''}cycle ${cycle}`
  const ctxLine = `当前阶段：${phaseLabel}（${roleAndCycle}）· 处理人：${ownerLabel}`
  const result = [
    head,
    `📋 ${task.title}`,
    ctxLine,
    '',
    record.text,
    '',
    `(需要历史? GET /tasks/${taskId}/events?limit=20)`,
  ].join('\n')
  if (process.env.AIC117_ENVELOPE_DUMP) {
    process.stderr.write(`[AIC-117 ENVELOPE] task=${taskId} phase=${task.current_phase} len=${result.length}\n${result}\n---END---\n`)
  }
  return result
}

function buildAgent2Injection(record: GroupRecord, hopCount: number, observeOnly = false) {
  // v2 task system_notification → [task / ...] envelope
  const taskEnv = buildTaskInjection(record, 'agent2')
  if (taskEnv) return taskEnv
  const sender = GROUP_ROSTER_BY_ID.get(record.sender_id)?.display_name || record.sender_id
  const isDm = !!dmAgentFor(record.conversation_id)
  const head = isDm
    ? `[私聊 / ${sender} · 请 curl 回复到 dm-agent2]`
    : `[workgroup${observeOnly ? ' observe' : ''} / ${sender}]`
  const meta = [
    `message_id=${record.id}`,
    record.task_id ? `task_id=${record.task_id}` : '',
    `hop_count=${hopCount}`,
  ].filter(Boolean).join('\n')
  const footer = observeOnly && !isDm ? OBSERVE_FOOTER : ''
  return `${head}\n${meta}\n${record.text}${groupAttachmentsBlock(record)}${footer}`
}

// Direct-message conversation id like 'dm-agent1' → the agent id ('agent1'),
// or null if it isn't a DM to a known replyable agent.
function dmAgentFor(conversationId: string | null | undefined): string | null {
  if (!conversationId || !conversationId.startsWith('dm-')) return null
  const agent = conversationId.slice(3)
  const member = GROUP_ROSTER_BY_ID.get(agent)
  return member && member.kind === 'agent' && member.can_reply ? agent : null
}

function allowedGroupConversationsForSender(senderId: string): Set<string> {
  if (senderId === 'admin') {
    return new Set(['workgroup', ...GROUP_REPLY_AGENT_IDS.map((id) => `dm-${id}`)])
  }
  const member = GROUP_ROSTER_BY_ID.get(senderId)
  if (member?.kind === 'agent' && member.can_reply) {
    return new Set(['workgroup', `dm-${senderId}`])
  }
  return new Set(['workgroup'])
}

function assertGroupConversationAllowed(senderId: string, conversationId: string | null | undefined) {
  const normalizedConversationId = String(conversationId || 'workgroup').trim() || 'workgroup'
  const allowed = allowedGroupConversationsForSender(senderId)
  if (!allowed.has(normalizedConversationId)) {
    throw new Error(`sender ${senderId} cannot post to ${normalizedConversationId}`)
  }
}

// Local file paths for any images/files on a group/DM record, so an agent can Read them directly.
function groupAttachmentsBlock(record: GroupRecord): string {
  const paths: string[] = []
  for (const n of record.images || []) paths.push(path.join(UPLOADS_DIR, n))
  for (const f of record.files || []) {
    const server = typeof f === 'string' ? f : f?.server
    if (server) paths.push(path.join(UPLOADS_DIR, server))
  }
  if (paths.length === 0) return ''
  return `\n[附件，可直接 Read 这些本地文件]\n${paths.join('\n')}`
}

// AIC-132 observe-only footer. When server.ts dispatches a workgroup message as
// observe (sidekick syncing context, not the addressee), this footer tells the
// agent that the server will silently drop this turn's assistant text — replying
// must be an explicit self-curl with a chosen conversation_id.
const OBSERVE_FOOTER = `\n\n（旁听消息，用来同步工作群上下文。默认不回；server 这一 turn 不会 auto-route 你的 assistant text 到任何频道。真想说话必须自己 curl /group/send，由你显式指定 conversation_id（"workgroup" 公开回 / "dm-<your-id>" 私聊回）。）`

function buildPlainBridgePrompt(record: GroupRecord, observeOnly = false) {
  // v2 task system_notification → [task / ...] envelope (recipient = agent1 when injecting via this path)
  const taskEnv = buildTaskInjection(record, 'agent1')
  if (taskEnv) return taskEnv
  const sender = GROUP_ROSTER_BY_ID.get(record.sender_id)?.display_name || record.sender_id
  const head = dmAgentFor(record.conversation_id)
    ? `[私聊 / ${sender} · 请 curl 回复到 dm-agent1]`
    : `[workgroup${observeOnly ? ' observe' : ''} / ${sender}]`
  const footer = observeOnly && !dmAgentFor(record.conversation_id) ? OBSERVE_FOOTER : ''
  return `${head}\n${record.text}${groupAttachmentsBlock(record)}${footer}`
}

// AIC-116: injectToTmux removed. Dispatch goes through `provider.send()` in dispatchGroupRecord.

function dispatchGroupRecord(record: GroupRecord, targets: string[], hopCount: number, observeOnly = false) {
  // AIC-116: legacy key injection → provider.send(). Provider auto-spawns its subprocess on
  // first call and stays alive across many sends (long-lived chat context per PM 06-25).
  // observeOnly (sidekick passively listening to a thread) doesn't trigger working status.
  //
  // Async dispatch model: provider.send() rejects on spawn / init / handshake failure
  // (agent can't accept this turn), resolves once the handshake passes. Mid-turn failures
  // (parse errors / turn protocol errors) fire through provider.onError + don't reject.
  // Optimistically mark `delivered` then correct after-the-fact if send() rejects async.
  const delivered: string[] = []
  const failed: string[] = []
  const correctableTargets: string[] = []  // targets we optimistically pushed but may need to demote
  for (const target of targets) {
    const member = GROUP_ROSTER_BY_ID.get(target)
    if (!member || member.kind !== 'agent') {
      failed.push(target)
      continue
    }
    const provider = ensureProvider(target)
    if (!provider) {
      console.error(`dispatchGroupRecord: no provider runtime configured for ${target}`)
      failed.push(target)
      continue
    }
    const text = target === 'agent2'
      ? buildAgent2Injection(record, hopCount + 1, observeOnly)
      : buildPlainBridgePrompt(record, observeOnly)
    if (!observeOnly) markAgentWorking(target, { dispatch_id: record.delivery.dispatch_id })
    // AIC-116: record which conversation this agent was dispatched into so
    // handleProviderEvent can write its assistant reply back into the right channel.
    if (!observeOnly) _agentLastDispatchChannel.set(target, record.conversation_id)
    // AIC-132: tag this turn as observe-only so `handleProviderEvent` drops the
    // assistant text instead of auto-appending it to a channel. Set BEFORE provider.send
    // so the agent's first assistant event (which can arrive before send() resolves on
    // long streams) already sees the flag.
    _agentIsObserveTurn.set(target, observeOnly)
    // Optimistic delivered; corrected via the .catch() below when send() rejects async.
    delivered.push(target)
    correctableTargets.push(target)
    provider.send(text).catch((e: any) => {
      // AIC-116 cycle 2: send() rejected → spawn / init / handshake failure.
      // Agent never accepted the message. Demote from delivered → failed, re-persist delivery,
      // and mark idle so the UI dot goes neutral instead of "working forever".
      console.error(`dispatchGroupRecord: provider.send to ${target} rejected:`, e)
      if (isAgentWorkingInDb(target)) markAgentIdle(target)
      // Re-read the current delivery (other concurrent .catch() handlers may have updated
      // it) and atomically demote this target.
      try {
        const row = db.prepare(`SELECT delivery FROM group_messages WHERE id=?`).get(record.id) as any
        if (row?.delivery) {
          const cur = JSON.parse(row.delivery)
          const list = observeOnly ? cur.observed : cur.delivered
          const failList = observeOnly ? (cur.observe_failed = cur.observe_failed || []) : (cur.failed = cur.failed || [])
          const idx = Array.isArray(list) ? list.indexOf(target) : -1
          if (idx !== -1) list.splice(idx, 1)
          if (!failList.includes(target)) failList.push(target)
          saveGroupDelivery(record.id, cur)
        }
      } catch (saveErr) {
        console.error(`dispatchGroupRecord: failed to demote ${target} in delivery record:`, saveErr)
      }
    })
  }
  if (observeOnly) {
    record.delivery.observed = delivered
    record.delivery.observe_failed = failed
  } else {
    record.delivery.delivered = delivered
    record.delivery.failed = failed
  }
  saveGroupDelivery(record.id, record.delivery)
  return { delivered, failed }
}

async function readOptionalJsonObject(req: Request) {
  const raw = await req.text()
  if (!raw.trim()) return {}
  try {
    return JSON.parse(raw)
  } catch {
    return {}
  }
}


function requireRemoteControlConfirm(url: URL, operation: string, body: any) {
  const local = isLocalRequestHost(url.hostname)
  const confirmed = hasRemoteControlConfirm(body, operation)
  if (local) {
    if (!confirmed) {
      logDangerousOperation('remote_control_local_without_confirm', {
        operation,
        path: url.pathname,
        host: url.hostname,
      })
    }
    return null
  }
  if (!ALLOW_REMOTE_CONTROL) {
    logDangerousOperation('remote_control_disabled', {
      operation,
      path: url.pathname,
      host: url.hostname,
    })
    return jsonError('remote_control_disabled', 403)
  }
  if (confirmed) return null
  logDangerousOperation('remote_control_confirm_required', {
    operation,
    path: url.pathname,
    host: url.hostname,
  })
  return jsonError('remote_control_confirmation_required', 403, { confirm: operation })
}

async function convertOggToM4a(oggPath: string, m4aPath: string): Promise<boolean> {
  try {
    const result = Bun.spawnSync(['ffmpeg', '-y', '-i', oggPath, '-c:a', 'aac', '-b:a', '128k', m4aPath])
    return result.exitCode === 0
  } catch (e) {
    process.stderr.write(`ai-collab: ffmpeg conversion failed: ${e}\n`)
    return false
  }
}

// Read cumulative assistant usage from CC's JSONL transcript (all turns since last user message)
function updateHeartbeat() {
  try {
    if (!existsSync(HEARTBEAT_PATH)) return
    const state = JSON.parse(readFileSync(HEARTBEAT_PATH, 'utf-8'))
    state.last_message_from_admin = new Date().toISOString()
    writeFileSync(HEARTBEAT_PATH, JSON.stringify(state, null, 2))
  } catch (err) {
    process.stderr.write(`ai-collab: failed to update heartbeat: ${err}\n`)
  }
}

let claudeUsageCache: { data: any, ts: number } | null = null

// AIC-53: extracted from /api/claude-usage handler so /usage/:agentId can reuse the
// same 60s-cached fetch + keychain plan-meta extraction. Returns either { ...usage,
// subscriptionType, rateLimitTier } or { error: '...' } — caller maps to HTTP code.
async function fetchClaudeUsageCached(): Promise<any> {
  if (claudeUsageCache && Date.now() - claudeUsageCache.ts < 60_000) return claudeUsageCache.data
  try {
    const cred = Bun.spawnSync(['security', 'find-generic-password', '-s', 'Claude Code-credentials', '-w'])
    const credObj = JSON.parse(cred.stdout.toString()).claudeAiOauth || {}
    const token = credObj.accessToken
    if (!token) return { error: 'no oauth token' }
    const resp = await fetch('https://api.anthropic.com/api/oauth/usage', {
      headers: { 'Authorization': `Bearer ${token}`, 'anthropic-beta': 'oauth-2025-04-20' }
    })
    if (!resp.ok) return { error: `upstream ${resp.status}` }
    const data = await resp.json()
    const merged = { ...data, subscriptionType: credObj.subscriptionType || null, rateLimitTier: credObj.rateLimitTier || null }
    claudeUsageCache = { data: merged, ts: Date.now() }
    return merged
  } catch (e: any) {
    return { error: e.message }
  }
}

// AIC-54: Codex CLI usage from ~/.codex/logs_2.sqlite. Codex desktop app holds
// the WAL — readonly via the `sqlite3` CLI (not bun:sqlite) avoids racing the
// writer. Returns:
//   - total_usage_tokens / auto_compact_limit  ← latest "post sampling token
//     usage" row, drives the context bar
//   - weekly_total_tokens                       ← sum of (input+output) per
//     response.completed event since Monday 00:00 (local time)
//   - model + reasoning_effort                  ← read from ~/.codex/config.toml
// 60s cache; spawn cost stays trivial under any sane poll rate.
let codexUsageCache: { data: any, ts: number } | null = null
function fetchCodexUsageCached(): any {
  if (codexUsageCache && Date.now() - codexUsageCache.ts < 60_000) return codexUsageCache.data
  try {
    const codexDb = `${homedir()}/.codex/logs_2.sqlite`
    if (!existsSync(codexDb)) return { error: 'codex logs db not found' }

    // (1) latest post-sampling row: total_usage_tokens + auto_compact_scope_limit.
    // cycle 2 (review fix): pin target='codex_core::session::turn'. The generic
    // `log` target and `codex_api::endpoint::responses_websocket` also contain
    // these strings, but only as user-input / tool-call echoes (including the
    // cycle-1 debug sqlite queries I ran in my own terminal — which Codex's
    // session loop dutifully recorded). Filtering by the real telemetry target
    // is the only way to keep the Codex usage endpoint honest.
    const ctxRes = Bun.spawnSync(['sqlite3', '-readonly', codexDb,
      "SELECT feedback_log_body FROM logs WHERE target='codex_core::session::turn' AND feedback_log_body LIKE '%total_usage_tokens=%' ORDER BY ts DESC LIMIT 1;"])
    const ctxLine = ctxRes.stdout.toString()
    const totalMatch = ctxLine.match(/total_usage_tokens=(\d+)/)
    const limitMatch = ctxLine.match(/auto_compact_scope_limit=(\d+)/)
    const total_usage_tokens = totalMatch ? parseInt(totalMatch[1], 10) : 0
    const auto_compact_limit = limitMatch ? parseInt(limitMatch[1], 10) : 244800

    // (2) weekly accumulated: sum input+output per response.completed since Monday 00:00 local.
    // cycle 2 (review fix): pin to single source `codex_otel.log_only` (sse_event lands in
    //   both log_only and trace_safe, summing both would double-count).
    // 2026-06-20 (PM verify): newer Codex desktop builds stopped writing OTEL
    //   sse_event entirely — the `codex_otel.log_only` source returns 0 even
    //   when Codex is clearly active (post-sampling rows still present). Fall
    //   back to summing `total_usage_tokens` from `codex_core::session::turn`
    //   post-sampling rows in the same window and flag the result as estimate.
    //   This overcounts within a thread (each turn's reading is cumulative,
    //   not delta) but is monotonic-ish across turns and beats showing 0 when
    //   the user knows Codex did run. Mark with weekly_is_estimate so UI can
    //   surface "估算".
    const now = new Date()
    const dow = now.getDay() === 0 ? 7 : now.getDay()  // Sun→7, Mon→1
    const mondayMs = new Date(now.getFullYear(), now.getMonth(), now.getDate() - (dow - 1), 0, 0, 0, 0).getTime()
    const mondayEpoch = Math.floor(mondayMs / 1000)
    const weeklyRes = Bun.spawnSync(['sqlite3', '-readonly', codexDb,
      `SELECT feedback_log_body FROM logs WHERE target='codex_otel.log_only' AND feedback_log_body LIKE '%event.kind=response.completed%input_token_count=%' AND ts > ${mondayEpoch};`])
    const weeklyLines = weeklyRes.stdout.toString().split('\n')
    let weekly_total_tokens = 0
    for (const line of weeklyLines) {
      const i = line.match(/input_token_count=(\d+)/)
      const o = line.match(/output_token_count=(\d+)/)
      if (i) weekly_total_tokens += parseInt(i[1], 10)
      if (o) weekly_total_tokens += parseInt(o[1], 10)
    }
    let weekly_is_estimate = false
    if (weekly_total_tokens === 0) {
      const fallbackRes = Bun.spawnSync(['sqlite3', '-readonly', codexDb,
        `SELECT feedback_log_body FROM logs WHERE target='codex_core::session::turn' AND feedback_log_body LIKE '%total_usage_tokens=%' AND ts > ${mondayEpoch};`])
      for (const line of fallbackRes.stdout.toString().split('\n')) {
        const m = line.match(/total_usage_tokens=(\d+)/)
        if (m) weekly_total_tokens += parseInt(m[1], 10)
      }
      if (weekly_total_tokens > 0) weekly_is_estimate = true
    }

    // (3) plan label from config.toml — quick regex, no need to pull a TOML parser
    let model = 'gpt-5.4', reasoning_effort = ''
    try {
      const tomlPath = `${homedir()}/.codex/config.toml`
      if (existsSync(tomlPath)) {
        const toml = readFileSync(tomlPath, 'utf-8')
        const mm = toml.match(/^model\s*=\s*"([^"]+)"/m)
        const rm = toml.match(/^model_reasoning_effort\s*=\s*"([^"]+)"/m)
        if (mm) model = mm[1]
        if (rm) reasoning_effort = rm[1]
      }
    } catch {}

    const data = { total_usage_tokens, auto_compact_limit, weekly_total_tokens, weekly_is_estimate, model, reasoning_effort }
    codexUsageCache = { data, ts: Date.now() }
    return data
  } catch (e: any) {
    return { error: e.message }
  }
}


// AIC-116: startAgentLifecycleWatcher() removed — provider events drive working/idle status.

// Image HTTP server for upload/download
Bun.serve({
  port: IMG_PORT,
  // AIC-52: was '0.0.0.0' which exposed 3009 on every interface — anyone on the
  // same wifi could hit the API. cloudflared connects from this host, so the
  // public tunnel keeps working; LAN devices that previously skipped auth lose
  // their backdoor.
  hostname: '127.0.0.1',
  async fetch(req) {
    const url = new URL(req.url)
    const dangerousEndpoint = dangerousEndpointFor(req.method, url.pathname)
    const authMode = requestAuthMode(req, url, AUTH_TOKEN, {
      
      allowQueryToken: ALLOW_QUERY_TOKEN_AUTH,
    })

    // Global auth: all routes require auth or localhost origin
    const hasAuth = authMode !== null
    if (authMode === 'query') {
      logDangerousOperation('deprecated_query_token_auth', {
        method: req.method,
        path: url.pathname,
        host: url.hostname,
      })
    }

    // Login page & login POST: exempt from auth
    // AIC-119: statusline POST 也 exempt (本机 fire-and-forget, 不带 token), 但限 localhost.
    if (url.pathname === '/web/login.html' || (req.method === 'POST' && url.pathname === '/web/login')
        || (req.method === 'POST' && url.pathname === '/api/agent-statusline' && isLocalRequestHost(url.hostname))) {
      // fall through to handlers below
    } else if (!hasAuth) {
      if (dangerousEndpoint) {
        logDangerousOperation('unauthorized_dangerous_request', {
          method: req.method,
          path: url.pathname,
          risk: dangerousEndpoint.risk,
          reason: dangerousEndpoint.reason,
          host: url.hostname,
        })
      }
      // For web page GETs (HTML entry points only — NOT static assets like .js/.css/.png),
      // auto-set the aicollab_auth cookie and redirect to the original page. The browser will then
      // include the cookie on subsequent XHR / asset fetches that DO require auth.
      // Open-source onboarding: users don't type a token; the server-derived AUTH_TOKEN (from
      // env or runtime-data/state/token.txt) is bound to the cookie on first GET. Anyone able
      // to reach the server thus gets in — bind the server to 127.0.0.1 if you don't want that.
      const isHtmlEntry = url.pathname === '/' || url.pathname === '/web' || url.pathname === '/web/'
        || url.pathname.endsWith('.html')
        || url.pathname === '/web/workgroup-v2'
      if (req.method === 'GET' && isHtmlEntry) {
        const target = url.pathname === '/' || url.pathname === '/web' || url.pathname === '/web/' || url.pathname === '/web/workgroup-v2'
          ? '/web/workgroup-v2/index.html'
          : url.pathname
        return new Response(null, {
          status: 302,
          headers: {
            'Location': target,
            'Set-Cookie': `aicollab_auth=${AUTH_TOKEN}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${30 * 24 * 3600}`,
          },
        })
      }
      return Response.json({ error: 'unauthorized' }, { status: 401 })
    }

    if (req.method === 'POST' && url.pathname === '/upload') {
      const origName = url.searchParams.get('name') || 'image.jpg'
      const ext = origName.split('.').pop() || 'jpg'
      const filename = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}.${ext}`
      const data = await req.arrayBuffer()
      await Bun.write(path.join(UPLOADS_DIR, filename), data)
      return Response.json({ filename, original: origName, size: data.byteLength })
    }

    if (req.method === 'GET' && url.pathname.startsWith('/images/')) {
      const filename = decodeURIComponent(url.pathname.slice(8))
      const filePath = resolveStoredMediaPath(filename)
      const file = Bun.file(filePath)
      if (await file.exists()) return new Response(file)
      return new Response('not found', { status: 404 })
    }

    // Login POST handler
    if (req.method === 'POST' && url.pathname === '/web/login') {
      try {
        const body = await req.json() as any
        if (body.password === AUTH_TOKEN) {
          return new Response(JSON.stringify({ ok: true }), {
            headers: {
              'Content-Type': 'application/json',
              'Set-Cookie': `aicollab_auth=${AUTH_TOKEN}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=${30 * 24 * 3600}`
            }
          })
        }
        return Response.json({ error: 'wrong password' }, { status: 401 })
      } catch {
        return Response.json({ error: 'bad request' }, { status: 400 })
      }
    }

    // /, /web, /web/ 全 redirect 到 workgroup-v2 主入口（防外网书签 404）
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/web' || url.pathname === '/web/')) {
      return new Response(null, { status: 302, headers: { Location: '/web/workgroup-v2/index.html' } })
    }

    // AIC-76: v1 workgroup.html retired 2026-06-21 — redirect 防外网书签 404
    if (req.method === 'GET' && url.pathname === '/web/workgroup.html') {
      return new Response(null, { status: 302, headers: { Location: '/web/workgroup-v2/index.html' } })
    }
    // v2 dir without trailing index.html — server doesn't auto-resolve, redirect.
    if (req.method === 'GET' && url.pathname === '/web/workgroup-v2/') {
      return new Response(null, { status: 302, headers: { Location: '/web/workgroup-v2/index.html' } })
    }

    if (req.method === 'GET' && url.pathname.startsWith('/web/') && url.pathname.endsWith('.html')) {
      const webPath = path.join(import.meta.dir, url.pathname)
      const file = Bun.file(webPath)
      if (await file.exists()) return new Response(file, { headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache, no-store, must-revalidate' } })
    }

    if (req.method === 'GET' && url.pathname.startsWith('/web/')) {
      const webPath = path.join(import.meta.dir, url.pathname)
      const file = Bun.file(webPath)
      if (await file.exists()) {
        const ext = webPath.split('.').pop() || ''
        const ct = ext === 'js' ? 'application/javascript; charset=utf-8'
                 : ext === 'css' ? 'text/css; charset=utf-8'
                 : ext === 'json' ? 'application/json; charset=utf-8'
                 : ext === 'png' ? 'image/png'
                 : ext === 'jpg' || ext === 'jpeg' ? 'image/jpeg'
                 : ext === 'svg' ? 'image/svg+xml'
                 : ext === 'woff2' ? 'font/woff2'
                 : 'application/octet-stream'
        return new Response(file, { headers: { 'Content-Type': ct, 'Cache-Control': 'no-cache, no-store, must-revalidate' } })
      }
    }

    // === HTTP Polling API (auth required) ===
    const isAuthed = hasRequestAuth(req, url, AUTH_TOKEN, {
      
      allowQueryToken: ALLOW_QUERY_TOKEN_AUTH,
    })

    // Workgroup roster: GET /group/roster
    if (req.method === 'GET' && url.pathname === '/group/roster' && isAuthed) {
      return Response.json({
        ok: true,
        roster: GROUP_ROSTER,
        members: groupMembersPayload(),
        status: groupStatusSnapshot(),
      })
    }

    // === AIC-63 cycle 4: actor-level identity (avatar + single bg per actor) ===
    // Server hydrates ink/accent into every response so clients never see drift.
    const _hydrateStyle = (row: any) => {
      const derived = deriveInkAndAccent(row.bubble_bg)
      return { ...row, ink: derived.ink, accent: derived.accent }
    }

    // GET /api/actor-styles → all 5 actor rows + preset bg colors for the picker
    if (req.method === 'GET' && url.pathname === '/api/actor-styles' && isAuthed) {
      const rows = db.prepare(`SELECT actor_id, avatar_kind, avatar_value, bubble_bg, updated_at FROM actor_styles ORDER BY actor_id`).all() as any[]
      return Response.json({ ok: true, styles: rows.map(_hydrateStyle), presets: PRESET_BG_COLORS })
    }

    // PUT /api/actor-styles/:actor_id (PM only) → update one actor's avatar + bg.
    if (req.method === 'PUT' && /^\/api\/actor-styles\/[^/]+$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json().catch(() => ({})) as any
        const authMode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
        const isPm = authMode === 'cookie' || authMode === 'local' || (authMode === 'header' && body.sender_id === 'admin')
        if (!isPm) return Response.json({ ok: false, error: 'only PM can edit actor styles' }, { status: 403 })
        const actorId = decodeURIComponent(url.pathname.split('/')[3])
        const VALID_ACTORS = new Set(['admin', 'agent1', 'agent2', 'system'])
        if (!VALID_ACTORS.has(actorId)) return Response.json({ ok: false, error: `unknown actor_id '${actorId}'` }, { status: 400 })
        const patch: Record<string, string> = {}
        for (const field of ['avatar_kind', 'avatar_value', 'bubble_bg'] as const) {
          const v = body[field]
          if (typeof v === 'string' && v.length > 0) patch[field] = v
        }
        if (patch.avatar_kind && patch.avatar_kind !== 'emoji' && patch.avatar_kind !== 'image') {
          return Response.json({ ok: false, error: `avatar_kind must be 'emoji' or 'image'` }, { status: 400 })
        }
        if (patch.bubble_bg && !/^#[0-9a-fA-F]{6}$/.test(patch.bubble_bg)) {
          return Response.json({ ok: false, error: `bubble_bg must be #RRGGBB hex` }, { status: 400 })
        }
        if (Object.keys(patch).length === 0) return Response.json({ ok: false, error: 'no editable fields supplied' }, { status: 400 })
        const updates: string[] = []
        const values: any[] = []
        for (const [field, v] of Object.entries(patch)) { updates.push(`${field} = ?`); values.push(v) }
        updates.push(`updated_at = ?`); values.push(groupNowIso())
        values.push(actorId)
        const result = db.prepare(`UPDATE actor_styles SET ${updates.join(', ')} WHERE actor_id = ?`).run(...values)
        if (result.changes === 0) return Response.json({ ok: false, error: 'row not found (seed missing?)' }, { status: 404 })
        const row = db.prepare(`SELECT actor_id, avatar_kind, avatar_value, bubble_bg, updated_at FROM actor_styles WHERE actor_id = ?`).get(actorId) as any
        return Response.json({ ok: true, style: _hydrateStyle(row) })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // === AIC-63 cycle 1-3 legacy endpoints removed (per-theme + avatar broadcast). ===
    // Routes below were the dead code that 重写 cycle 4 replaces; keeping a guard
    // here so callers still pinned to the old paths get a clear 410 instead of a
    // silent 404, helping any stale client surface itself.
    if (req.method === 'PUT' && /^\/api\/actor-styles\/[^/]+\/[^/]+$/.test(url.pathname) && isAuthed) {
      return Response.json({ ok: false, error: 'AIC-63 cycle 4: per-theme + avatar-broadcast endpoints removed. Use PUT /api/actor-styles/:actor_id instead.' }, { status: 410 })
    }

    // Workgroup response gates: GET /group/settings
    if (req.method === 'GET' && url.pathname === '/group/settings' && isAuthed) {
      return Response.json({
        ok: true,
        auto_reply: Object.fromEntries(GROUP_REPLY_AGENT_IDS.map((id) => [id, groupAgentAutoReply(id)])),
      })
    }

    // Workgroup response gates: POST /group/settings { agent_id, auto_reply }
    if (req.method === 'POST' && url.pathname === '/group/settings' && isAuthed) {
      try {
        const body = await req.json() as any
        const agentId = String(body.agent_id || '').trim()
        setGroupAgentAutoReply(agentId, Boolean(body.auto_reply))
        return Response.json({
          ok: true,
          auto_reply: Object.fromEntries(GROUP_REPLY_AGENT_IDS.map((id) => [id, groupAgentAutoReply(id)])),
          status: groupStatusSnapshot(),
        })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }

    // Workgroup poll: GET /group/poll?since=<iso>&limit=120
    if (req.method === 'GET' && url.pathname === '/group/poll' && isAuthed) {
      const since = url.searchParams.get('since')
      const beforeId = url.searchParams.get('before_id')        // AIC-90: 历史分页
      const limit = parseInt(url.searchParams.get('limit') || '120')
      const conversationId = url.searchParams.get('conversation_id') || 'workgroup'
      const records = readGroupRecords(since, limit, conversationId, beforeId)
      // AIC-90: 历史分页返 cursor (= 最早一条 id, client 下次传 before_id 用) + has_more
      // 兼容老 caller (不传 before_id 行为不变, cursor=null has_more=false)
      const isHistoryFetch = !!beforeId
      const cursor = isHistoryFetch && records.length > 0 ? records[0].id : null
      const hasMore = isHistoryFetch ? records.length === Math.min(Math.max(limit || 120, 1), 500) : false
      return Response.json({
        ok: true,
        records,
        count: records.length,
        last_ts: records.at(-1)?.ts || since || null,
        cursor,
        has_more: hasMore,
        roster: GROUP_ROSTER,
        members: groupMembersPayload(),
        status: groupStatusSnapshot(),
      })
    }

    // Workgroup send: POST /group/send
    if (req.method === 'POST' && url.pathname === '/group/send' && isAuthed) {
      try {
        const body = await req.json() as any
        const text = String(body.text || '')
        const senderId = String(body.sender_id || 'admin').trim()
        // v2 system virtual identity: external clients cannot impersonate system.
        // notifyActorViaDM (server-internal) writes directly to group_messages without going through this endpoint.
        if (senderId === 'system') return Response.json({ ok: false, error: 'sender_id=system is internal-only' }, { status: 403 })
        // Reject any message targeting dm-system (system has no DM channel to reply into).
        if (String(body.conversation_id || '') === 'dm-system') return Response.json({ ok: false, error: 'dm-system is not a valid conversation' }, { status: 400 })
        assertGroupConversationAllowed(senderId, body.conversation_id)
        const hopCount = parseInt(String(body.hop_count || '0')) || 0

        // Direct message (1:1) conversation, e.g. conversation_id='dm-agent1'.
        // Route strictly between the human and that agent — no mentions, no observers,
        // so the DM never leaks into the workgroup or to other agents.
        // Channel restriction: only the DM owner (the agent) or PM can send here.
        const dmAgent = dmAgentFor(String(body.conversation_id || ''))
        if (dmAgent) {
          const dmAuthMode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
          const dmIsPm = dmAuthMode === 'cookie' || dmAuthMode === 'local' || (dmAuthMode === 'header' && senderId === 'admin')
          if (senderId !== dmAgent && !dmIsPm) {
            return Response.json({ ok: false, error: `only ${dmAgent} or PM can send to dm-${dmAgent}` }, { status: 403 })
          }
          const dmTargets = senderId === dmAgent ? [] : [dmAgent]
          const dmDelivery = { targets: dmTargets, mode: 'dm', dispatch_id: groupId('dsp'), delivered: [], failed: [] }
          const dmRecord = appendGroupRecord({ ...body, sender_id: senderId }, [], dmDelivery)
          if (dmTargets.length > 0) dispatchGroupRecord(dmRecord, dmTargets, hopCount)
          return Response.json({ ok: true, record: dmRecord, targets: dmTargets, observers: [] })
        }

        const mentions = normalizeGroupMentions(body.mentions, text)
        const targets = groupTargetsFor(senderId, mentions, hopCount)
        const delivery = {
          targets,
          mode: mentions.includes(GROUP_ALL_TOKEN) ? 'all' : (mentions.length ? 'mention' : 'default'),
          dispatch_id: groupId('dsp'),
          delivered: [],
          failed: [],
        }
        const record = appendGroupRecord({ ...body, sender_id: senderId }, mentions, delivery)
        if (targets.length > 0) dispatchGroupRecord(record, targets, hopCount)
        const observers = groupObserverTargetsFor(senderId, targets)
        if (observers.length > 0) dispatchGroupRecord(record, observers, hopCount, true)
        return Response.json({ ok: true, record, targets, observers })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }

    // Workgroup typing/status: POST /group/typing
    if (req.method === 'POST' && url.pathname === '/group/typing' && isAuthed) {
      try {
        const body = await req.json() as any
        const agentId = String(body.sender_id || body.agent_id || '').trim()
        if (!GROUP_ROSTER_BY_ID.has(agentId)) {
          return Response.json({ ok: false, error: 'unknown agent_id' }, { status: 400 })
        }
        const typing = Boolean(body.is_typing ?? body.typing ?? false)
        updateGroupAgentState(agentId, {
          last_seen: groupNowIso(),
          is_typing: typing ? 1 : 0,
          typing_since: typing ? groupNowIso() : null,
          dispatch_id: body.dispatch_id || null,
          status_text: body.status_text || null,
        })
        return Response.json({ ok: true, status: groupStatusSnapshot() })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }

    // Workgroup heartbeat: POST /group/roster_heartbeat
    // Control panel: 上班 (clock in)
    if (req.method === 'POST' && url.pathname === '/group/agent/clock-in' && isAuthed) {
      try {
        const body = await req.json() as any
        const result = agentClockIn(String(body.agent_id || '').trim())
        return Response.json(result, { status: result.ok ? 200 : 400 })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }

    // Control panel: 下班 (clock out) — inject checkout, wait for ready handshake
    if (req.method === 'POST' && url.pathname === '/group/agent/clock-out' && isAuthed) {
      try {
        const body = await req.json() as any
        const result = agentClockOut(String(body.agent_id || '').trim())
        return Response.json(result, { status: result.ok ? 200 : 400 })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }

    // Agent reports it finished checkout and is safe to close → kill its session
    if (req.method === 'POST' && url.pathname === '/group/agent/clock-out-ready' && isAuthed) {
      try {
        const body = await req.json() as any
        const result = killAgentSession(String(body.agent_id || '').trim())
        return Response.json(result, { status: result.ok ? 200 : 400 })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }

    // AIC-48 v2: Claude Code lifecycle hook signals — UserPromptSubmit / Stop
    // 直接给 server "I'm working now" / "I just stopped" 真信号，不再靠 agent 发消息推断。
    // 修了 PM 抓的核心 bug：agent 在 terminal 跑 tool 但不刷工作群 → status 卡 working 不释放。
    if (req.method === 'POST' && url.pathname === '/group/agent/working' && isAuthed) {
      try {
        const body = await req.json() as any
        const agentId = String(body.agent_id || '').trim()
        const m = GROUP_ROSTER_BY_ID.get(agentId)
        if (!m || m.kind !== 'agent') return Response.json({ ok: false, error: 'unknown agent_id' }, { status: 400 })
        markAgentWorking(agentId)
        return Response.json({ ok: true })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }
    if (req.method === 'POST' && url.pathname === '/group/agent/idle' && isAuthed) {
      try {
        const body = await req.json() as any
        const agentId = String(body.agent_id || '').trim()
        const m = GROUP_ROSTER_BY_ID.get(agentId)
        if (!m || m.kind !== 'agent') return Response.json({ ok: false, error: 'unknown agent_id' }, { status: 400 })
        markAgentIdle(agentId)
        return Response.json({ ok: true })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }

    // AIC-55: multi-device unread sync — actor pushes last_seen_id per channel
    // on read, other devices pull on boot/poll. Auth: actor self (Bearer +
    // matching sender_id) or PM (cookie / local / Bearer + admin). agents don't
    // need this (they don't have unread badges), but the table is actor-scoped
    // so any actor in the namespace can use it.
    if (req.method === 'POST' && url.pathname === '/seen' && isAuthed) {
      try {
        const body = await req.json() as any
        const actorId = String(body.actor_id || '').trim()
        const channel = String(body.channel || '').trim()
        const lastSeenId = String(body.last_seen_id || '').trim()
        if (!actorId || !channel || !lastSeenId) {
          return Response.json({ ok: false, error: 'actor_id, channel, last_seen_id required' }, { status: 400 })
        }
        const mode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
        const isSelf = mode === 'header' && body.sender_id === actorId
        const isPm = mode === 'cookie' || mode === 'local' || (mode === 'header' && body.sender_id === 'admin')
        if (!isSelf && !isPm) {
          return Response.json({ ok: false, error: `actor ${actorId} must auth as self or PM proxy` }, { status: 403 })
        }
        const now = groupNowIso()
        db.run(
          `INSERT INTO actor_channel_seen (actor_id, channel, last_seen_id, updated_at) VALUES (?, ?, ?, ?)
           ON CONFLICT(actor_id, channel) DO UPDATE SET last_seen_id = excluded.last_seen_id, updated_at = excluded.updated_at`,
          [actorId, channel, lastSeenId, now],
        )
        return Response.json({ ok: true, updated_at: now })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }

    if (req.method === 'GET' && url.pathname === '/seen' && isAuthed) {
      const actorId = (url.searchParams.get('actor_id') || '').trim()
      if (!actorId) return Response.json({ ok: false, error: 'actor_id query param required' }, { status: 400 })
      const rows = db.prepare(`SELECT channel, last_seen_id, updated_at FROM actor_channel_seen WHERE actor_id = ?`).all(actorId) as any[]
      const channels: Record<string, { last_seen_id: string; updated_at: string }> = {}
      for (const r of rows) channels[r.channel] = { last_seen_id: r.last_seen_id, updated_at: r.updated_at }
      return Response.json({ ok: true, actor_id: actorId, channels })
    }

    if (req.method === 'POST' && url.pathname === '/group/roster_heartbeat' && isAuthed) {
      try {
        const body = await req.json() as any
        const agentId = String(body.sender_id || body.agent_id || '').trim()
        if (!GROUP_ROSTER_BY_ID.has(agentId)) {
          return Response.json({ ok: false, error: 'unknown agent_id' }, { status: 400 })
        }
        updateGroupAgentState(agentId, {
          last_seen: groupNowIso(),
          is_typing: Boolean(body.is_typing ?? false) ? 1 : 0,
          typing_since: body.is_typing ? groupNowIso() : null,
          status_text: body.status_text || null,
        })
        return Response.json({ ok: true, status: groupStatusSnapshot() })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message || String(e) }, { status: 400 })
      }
    }

    // === Task pipeline API ===

    // List tasks: GET /group/tasks
    // status='draft'  → 待发布（status=active AND phase=draft）
    // status='active' → 进行中（排除 draft：status=active AND phase != 'draft'）
    // status='done'   → 已完成
    // status 为空     → 全部
    if (req.method === 'GET' && url.pathname === '/group/tasks' && isAuthed) {
      const status = url.searchParams.get('status') || ''
      const category = url.searchParams.get('category') || ''
      const where: string[] = []
      const params: any[] = []
      if (status === 'draft') {
        where.push(`status = 'active' AND phase = 'draft'`)
      } else if (status === 'active') {
        where.push(`status = 'active' AND phase != 'draft'`)
      } else if (status) {
        where.push(`status = ?`); params.push(status)
      }
      if (category) { where.push(`category = ?`); params.push(category) }
      const sql = `SELECT * FROM group_tasks ${where.length ? 'WHERE ' + where.join(' AND ') : ''} ORDER BY created_at DESC`
      const rows = db.prepare(sql).all(...params)
      const categories = db.prepare(`SELECT name FROM task_categories ORDER BY name`).all().map((r: any) => r.name)
      return Response.json({ ok: true, tasks: rows, categories })
    }

    // AIC-87: v1 task pipeline 写操作全 410 sealed (POST/PUT/PATCH/DELETE)。GET 保留兼容
    // 历史 client / 外网书签。所有新写操作必须走 v2 POST /tasks (line 4238) — 之前有 caller 误用
    // /group/tasks 派任务暴露了这个 read-only gate 一直没真做的 hole (Memory 06-18 拍板留口
    // 等 v2 稳定后单独清退)。一次性拦掉下面 6+ 个 v1 write handler，避免散落改。
    // categories POST/DELETE 也一并 410 — 跟 description 承诺对齐。分类表 (task_categories)
    // 是 v1/v2 共享的，所以 v2 section 加 GET/POST/DELETE /tasks/categories 接替。
    // web (workgroup-v2/index.html) caller 同步改到 /tasks/categories。
    if (
      (req.method === 'POST' || req.method === 'PUT' || req.method === 'PATCH' || req.method === 'DELETE') &&
      (url.pathname === '/group/tasks' || url.pathname.startsWith('/group/tasks/'))
    ) {
      return Response.json({
        ok: false,
        error: 'v1 task endpoint deprecated (AIC-87), use v2 /tasks',
        v2: {
          create: 'POST /tasks',
          publish: 'PUT /tasks/:id/publish',
          advance: 'PUT /tasks/:id/advance',
          reject: 'PUT /tasks/:id/reject',
          rollback: 'PUT /tasks/:id/rollback (PM-only)',
          comment: 'POST /tasks/:id/comments',
          patch: 'PATCH /tasks/:id',
          delete: 'DELETE /tasks/:id',
        },
      }, { status: 410 })
    }

    // Categories: GET/POST /group/tasks/categories
    if (req.method === 'GET' && url.pathname === '/group/tasks/categories' && isAuthed) {
      const rows = db.prepare(`SELECT name FROM task_categories ORDER BY name`).all()
      return Response.json({ ok: true, categories: rows.map((r: any) => r.name) })
    }
    if (req.method === 'POST' && url.pathname === '/group/tasks/categories' && isAuthed) {
      const catAuthMode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
      if (catAuthMode !== 'cookie' && catAuthMode !== 'local') {
        return Response.json({ ok: false, error: 'only PM can manage categories' }, { status: 403 })
      }
      const body = await req.json() as any
      const name = String(body.name || '').trim()
      if (!name) return Response.json({ ok: false, error: 'name required' }, { status: 400 })
      try { db.run(`INSERT INTO task_categories (name, created_at) VALUES (?, ?)`, [name, groupNowIso()]) } catch {}
      return Response.json({ ok: true })
    }
    if (req.method === 'DELETE' && url.pathname === '/group/tasks/categories' && isAuthed) {
      const catDelAuthMode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
      if (catDelAuthMode !== 'cookie' && catDelAuthMode !== 'local') {
        return Response.json({ ok: false, error: 'only PM can manage categories' }, { status: 403 })
      }
      const body = await req.json() as any
      const name = String(body.name || '').trim()
      if (!name) return Response.json({ ok: false, error: 'name required' }, { status: 400 })
      db.run(`DELETE FROM task_categories WHERE name = ?`, name)
      db.run(`UPDATE group_tasks SET category = '' WHERE category = ?`, name)
      return Response.json({ ok: true })
    }

    // Create task: POST /group/tasks (creates as draft, not published until PUT /publish)
    if (req.method === 'POST' && url.pathname === '/group/tasks' && isAuthed) {
      try {
        const body = await req.json() as any
        const title = String(body.title || '').trim()
        if (!title) return Response.json({ ok: false, error: 'title required' }, { status: 400 })
        const id = `task_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
        const now = groupNowIso()
        db.run(
          `INSERT INTO group_tasks (id, title, description, category, phase, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'draft', 'active', ?, ?)`,
          [id, title, body.description || '', body.category || '', now, now]
        )
        const task = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(id)
        return Response.json({ ok: true, task })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // Publish draft task: PUT /group/tasks/:id/publish (PM only)
    if (req.method === 'PUT' && /^\/group\/tasks\/[^/]+\/publish$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json().catch(() => ({})) as any
        const publishAuthMode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
        const isPm = publishAuthMode === 'cookie' || publishAuthMode === 'local' || (publishAuthMode === 'header' && body.sender_id === 'admin')
        if (!isPm) return Response.json({ ok: false, error: 'only PM can publish tasks' }, { status: 403 })
        const taskId = decodeURIComponent(url.pathname.slice('/group/tasks/'.length, -'/publish'.length))
        const task = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(taskId) as any
        if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
        if (task.phase !== 'draft') return Response.json({ ok: false, error: `task is in phase '${task.phase}', not draft` }, { status: 400 })
        const now = groupNowIso()
        db.run(`UPDATE group_tasks SET phase = 'evaluate', evaluate_started_at = ?, updated_at = ? WHERE id = ?`, [now, now, taskId])
        const updated = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(taskId)
        const taskMsg = `📋 新任务：**${task.title}**${task.description ? '\n' + task.description : ''}\n\n阶段：评估`
        await fetch(`http://127.0.0.1:${IMG_PORT}/group/send`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH_TOKEN}` },
          body: JSON.stringify({ sender_id: 'admin', text: taskMsg, mentions: [], task_id: taskId })
        })
        return Response.json({ ok: true, task: updated })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // Edit task: PATCH /group/tasks/:id (PM only)
    if (req.method === 'PATCH' && url.pathname.startsWith('/group/tasks/') && isAuthed) {
      const patchAuthMode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
      if (patchAuthMode !== 'cookie' && patchAuthMode !== 'local') {
        return Response.json({ ok: false, error: 'only PM can edit tasks' }, { status: 403 })
      }
      const taskId = decodeURIComponent(url.pathname.slice('/group/tasks/'.length))
      const body = await req.json() as any
      const task = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(taskId) as any
      if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
      const sets: string[] = []
      const vals: any[] = []
      if (body.title !== undefined) { sets.push('title = ?'); vals.push(String(body.title).trim()) }
      if (body.description !== undefined) { sets.push('description = ?'); vals.push(String(body.description)) }
      if (body.category !== undefined) { sets.push('category = ?'); vals.push(String(body.category)) }
      if (!sets.length) return Response.json({ ok: false, error: 'nothing to update' }, { status: 400 })
      sets.push('updated_at = ?'); vals.push(groupNowIso()); vals.push(taskId)
      db.run(`UPDATE group_tasks SET ${sets.join(', ')} WHERE id = ?`, vals)
      const updated = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(taskId)
      return Response.json({ ok: true, task: updated })
    }

    // Update task check: PUT /group/tasks/:id
    if (req.method === 'PUT' && url.pathname.startsWith('/group/tasks/') && isAuthed) {
      try {
        const taskId = decodeURIComponent(url.pathname.slice('/group/tasks/'.length))
        const body = await req.json() as any
        const task = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(taskId) as any
        if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })

        const now = groupNowIso()

        // Handle rollback — PM only (cookie / local / app)
        const rollbackAuthMode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
        const rollbackIsPm = rollbackAuthMode === 'cookie' || rollbackAuthMode === 'local' || (rollbackAuthMode === 'header' && body.sender_id === 'admin')
        if ((body.action === 'rollback_implement' || body.action === 'rollback_evaluate') && !rollbackIsPm) {
          return Response.json({ ok: false, error: 'only PM can rollback tasks' }, { status: 403 })
        }
        const rollbackComment = body.comment ? String(body.comment).trim() : ''
        if (body.action === 'rollback_implement' && task.phase === 'evaluate') {
          return Response.json({ ok: false, error: 'cannot rollback to implement from evaluate phase' }, { status: 400 })
        }
        if (body.action === 'rollback_implement') {
          db.run(`UPDATE group_tasks SET phase = 'implement', implement_check = 0, pm_check_implement = 0, review_check = 0, pm_check_review = 0, implement_started_at = ?, implement_ended_at = NULL, review_started_at = NULL, review_ended_at = NULL, updated_at = ? WHERE id = ?`, [now, now, taskId])
          const updated = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(taskId)
          const msg = `🔄 任务「${task.title}」打回到实施阶段 → @agent1 请重新实施${rollbackComment ? '\n💬 ' + rollbackComment : ''}`
          await fetch(`http://127.0.0.1:${IMG_PORT}/group/send`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH_TOKEN}` },
            body: JSON.stringify({ sender_id: 'admin', text: msg, mentions: ['agent1'], task_id: taskId })
          })
          return Response.json({ ok: true, task: updated })
        }

        if (body.action === 'rollback_evaluate') {
          db.run(`UPDATE group_tasks SET phase = 'evaluate', evaluate_check = 0, pm_check_evaluate = 0, implement_check = 0, pm_check_implement = 0, review_check = 0, pm_check_review = 0, evaluate_started_at = ?, evaluate_ended_at = NULL, implement_started_at = NULL, implement_ended_at = NULL, review_started_at = NULL, review_ended_at = NULL, updated_at = ? WHERE id = ?`, [now, now, taskId])
          const updated = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(taskId)
          const msg = `🔄 任务「${task.title}」打回到评估阶段${rollbackComment ? '\n💬 ' + rollbackComment : ''}`
          await fetch(`http://127.0.0.1:${IMG_PORT}/group/send`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH_TOKEN}` },
            body: JSON.stringify({ sender_id: 'admin', text: msg, mentions: [], task_id: taskId })
          })
          return Response.json({ ok: true, task: updated })
        }

        // Handle check toggles — only current phase checks allowed
        const phaseChecks: Record<string, {agent: string, pm: string}> = {
          evaluate:  {agent: 'evaluate_check',  pm: 'pm_check_evaluate'},
          implement: {agent: 'implement_check', pm: 'pm_check_implement'},
          review:    {agent: 'review_check',  pm: 'pm_check_review'},
        }
        const checkField = body.check as string
        const currentPhaseChecks = phaseChecks[task.phase]
        if (checkField && currentPhaseChecks) {
          // Only allow checks belonging to the current phase
          if (checkField !== currentPhaseChecks.agent && checkField !== currentPhaseChecks.pm) {
            return Response.json({ ok: false, error: `check '${checkField}' not allowed in phase '${task.phase}'` }, { status: 403 })
          }
          // Role guard: PM = cookie / local / app (Bearer + sender_id=admin); agent = Bearer + matching sender_id
          const authMode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
          const isPm = authMode === 'cookie' || authMode === 'local' || (authMode === 'header' && body.sender_id === 'admin')
          const agentForCheck: Record<string, string> = { implement_check: 'agent1', review_check: 'agent2' }
          if (checkField === currentPhaseChecks.agent) {
            const expectedAgent = agentForCheck[checkField]
            if (body.sender_id !== expectedAgent) {
              return Response.json({ ok: false, error: `only ${expectedAgent} can toggle ${checkField}` }, { status: 403 })
            }
            if (isPm) {
              return Response.json({ ok: false, error: 'PM cannot toggle agent checks' }, { status: 403 })
            }
          }
          if (checkField === currentPhaseChecks.pm) {
            if (!isPm) {
              return Response.json({ ok: false, error: 'only PM (cookie auth) can toggle PM checks' }, { status: 403 })
            }
          }

          const val = body.value !== undefined ? (body.value ? 1 : 0) : (task[checkField] ? 0 : 1)
          db.run(`UPDATE group_tasks SET ${checkField} = ?, updated_at = ? WHERE id = ?`, [val, now, taskId])

          // Check if phase should advance
          const fresh = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(taskId) as any
          let advanced = false

          if (fresh.phase === 'evaluate' && fresh.evaluate_check && fresh.pm_check_evaluate) {
            db.run(`UPDATE group_tasks SET phase = 'implement', evaluate_ended_at = ?, implement_started_at = ?, implement_check = 0, pm_check_implement = 0, review_check = 0, pm_check_review = 0, updated_at = ? WHERE id = ?`, [now, now, now, taskId])
            const msg = `✅ 任务「${task.title}」评估通过 → @agent1 请开始实施`
            await fetch(`http://127.0.0.1:${IMG_PORT}/group/send`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH_TOKEN}` },
              body: JSON.stringify({ sender_id: 'admin', text: msg, mentions: ['agent1'], task_id: taskId })
            })
            advanced = true
          } else if (fresh.phase === 'implement' && fresh.implement_check && fresh.pm_check_implement) {
            db.run(`UPDATE group_tasks SET phase = 'review', implement_ended_at = ?, review_started_at = ?, review_check = 0, pm_check_review = 0, updated_at = ? WHERE id = ?`, [now, now, now, taskId])
            const msg = `✅ 任务「${task.title}」实施完成 → @agent2 请 review`
            await fetch(`http://127.0.0.1:${IMG_PORT}/group/send`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH_TOKEN}` },
              body: JSON.stringify({ sender_id: 'admin', text: msg, mentions: ['agent2'], task_id: taskId })
            })
            advanced = true
          } else if (fresh.phase === 'review' && fresh.review_check && fresh.pm_check_review) {
            db.run(`UPDATE group_tasks SET phase = 'done', status = 'done', review_ended_at = ?, updated_at = ? WHERE id = ?`, [now, now, taskId])
            const msg = `🎉 任务「${task.title}」全部完成！`
            await fetch(`http://127.0.0.1:${IMG_PORT}/group/send`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH_TOKEN}` },
              body: JSON.stringify({ sender_id: 'admin', text: msg, mentions: [], task_id: taskId })
            })
            advanced = true
          }

          const updated = db.prepare(`SELECT * FROM group_tasks WHERE id = ?`).get(taskId)
          return Response.json({ ok: true, task: updated, advanced })
        }

        return Response.json({ ok: false, error: 'no valid action or check field' }, { status: 400 })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 500 })
      }
    }

    // Delete task: DELETE /group/tasks/:id (PM only)
    if (req.method === 'DELETE' && url.pathname.startsWith('/group/tasks/') && isAuthed) {
      const delAuthMode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
      if (delAuthMode !== 'cookie' && delAuthMode !== 'local') {
        return Response.json({ ok: false, error: 'only PM can delete tasks' }, { status: 403 })
      }
      const taskId = decodeURIComponent(url.pathname.slice('/group/tasks/'.length))
      const task = db.prepare(`SELECT id FROM group_tasks WHERE id = ?`).get(taskId)
      if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
      db.run(`DELETE FROM group_tasks WHERE id = ?`, taskId)
      return Response.json({ ok: true, deleted: taskId })
    }

    // ====================================================================
    // === Task Tracker v2 API (spec: docs/task-tracker-v2-spec.md §12)  ===
    // ====================================================================

    // Helper: resolve PM auth (cookie / local / Bearer+sender_id=admin) and "actor as me" auth (Bearer + matching sender_id).
    const v2AuthInfo = () => {
      const mode = requestAuthMode(req, url, AUTH_TOKEN, { allowQueryToken: ALLOW_QUERY_TOKEN_AUTH })
      return { mode }
    }
    const v2IsPm = (info: { mode: string }, body: any) =>
      info.mode === 'cookie' || info.mode === 'local' || (info.mode === 'header' && body?.sender_id === 'admin')

    // AIC-87: v2 categories CRUD — 接替 v1 /group/tasks/categories (后者已 410)。
    // 数据共用 task_categories 表 (v1/v2 共享分类元数据,v2 tasks_v2.category 也读这张)。
    if (req.method === 'GET' && url.pathname === '/tasks/categories' && isAuthed) {
      const rows = db.prepare(`SELECT name FROM task_categories ORDER BY name`).all()
      return Response.json({ ok: true, categories: rows.map((r: any) => r.name) })
    }
    if (req.method === 'POST' && url.pathname === '/tasks/categories' && isAuthed) {
      const body = await req.json().catch(() => ({})) as any
      if (!v2IsPm(v2AuthInfo(), body)) return Response.json({ ok: false, error: 'only PM can manage categories' }, { status: 403 })
      const name = String(body.name || '').trim()
      if (!name) return Response.json({ ok: false, error: 'name required' }, { status: 400 })
      try { db.run(`INSERT INTO task_categories (name, created_at) VALUES (?, ?)`, [name, groupNowIso()]) } catch {}
      return Response.json({ ok: true })
    }
    if (req.method === 'DELETE' && url.pathname === '/tasks/categories' && isAuthed) {
      const body = await req.json().catch(() => ({})) as any
      if (!v2IsPm(v2AuthInfo(), body)) return Response.json({ ok: false, error: 'only PM can manage categories' }, { status: 403 })
      const name = String(body.name || '').trim()
      if (!name) return Response.json({ ok: false, error: 'name required' }, { status: 400 })
      db.run(`DELETE FROM task_categories WHERE name = ?`, name)
      db.run(`UPDATE group_tasks SET category = '' WHERE category = ?`, name)
      db.run(`UPDATE tasks_v2 SET category = '' WHERE category = ?`, name)
      return Response.json({ ok: true })
    }

    // 12.11 GET /workflow_templates — phase definitions for UI rendering
    if (req.method === 'GET' && url.pathname === '/tasks/workflow_templates' && isAuthed) {
      return Response.json({ ok: true, templates: WORKFLOW_TEMPLATES })
    }

    // AIC-42 · GET /tasks/phase_templates — list all phase templates with usage_count (for settings UI)
    if (req.method === 'GET' && url.pathname === '/tasks/phase_templates' && isAuthed) {
      const usageByTemplate: Record<string, string[]> = {}
      for (const [wfKey, wf] of Object.entries(WORKFLOW_TEMPLATES)) {
        for (const p of wf.phases) {
          if (!usageByTemplate[p.key]) usageByTemplate[p.key] = []
          if (!usageByTemplate[p.key].includes(wfKey)) usageByTemplate[p.key].push(wfKey)
        }
      }
      const templates = Object.values(PHASE_TEMPLATES).map(def => ({
        id: def.key,
        name: def.label,
        required_roles: def.required_roles,
        can_reject: def.can_reject,
        auto_advance: def.auto_advance,
        is_terminal: def.required_roles.length === 0,
        is_reserved: RESERVED_PHASE_IDS.has(def.key),
        usage_count: usageByTemplate[def.key]?.length || 0,
        used_by: usageByTemplate[def.key] || [],
      }))
      return Response.json({ ok: true, templates })
    }

    // AIC-42 · POST /tasks/phase_templates — create phase template (PM only).
    // Server auto-generates id if client doesn't supply one (PM never sees ids).
    if (req.method === 'POST' && url.pathname === '/tasks/phase_templates' && isAuthed) {
      try {
        const body = await req.json() as any
        const info = v2AuthInfo()
        if (!v2IsPm(info, body)) return Response.json({ ok: false, error: 'only PM can manage phase templates' }, { status: 403 })
        const id = body.id ? String(body.id).trim() : `p_${generateULID().slice(0, 10).toLowerCase()}`
        const normalized = {
          id,
          name: typeof body.name === 'string' ? body.name.trim() : '',
          required_roles: Array.isArray(body.required_roles) ? body.required_roles : [],
          can_reject: !!body.can_reject,
          auto_advance: !!body.auto_advance,
        }
        const err = validatePhaseTemplate(normalized)
        if (err) return Response.json({ ok: false, error: err }, { status: 400 })
        if (PHASE_TEMPLATES[id]) return Response.json({ ok: false, error: `phase template ${id} already exists; use PATCH` }, { status: 409 })
        const def: PhaseDef = {
          key: id, label: normalized.name,
          required_roles: normalized.required_roles as PhaseDef['required_roles'],
          can_reject: normalized.can_reject, auto_advance: normalized.auto_advance,
        }
        insertPhaseTemplateRow(def)
        reloadPhaseTemplates()
        reloadWorkflowTemplates()
        return Response.json({ ok: true, template: PHASE_TEMPLATES[id] })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // AIC-42 · PATCH /tasks/phase_templates/:id — edit (PM only).
    // Reserved ids (draft/closed) fully locked — runtime hardcodes their semantics.
    // id is immutable (constraint #2) — body.id ignored even if supplied.
    // Open-task deadlock gate: changing mid-flow phase to required_roles=[] + auto_advance=false
    // while any open task is at this phase in any workflow → 409.
    if (req.method === 'PATCH' && /^\/tasks\/phase_templates\/[^/]+$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json() as any
        const info = v2AuthInfo()
        if (!v2IsPm(info, body)) return Response.json({ ok: false, error: 'only PM can manage phase templates' }, { status: 403 })
        const id = decodeURIComponent(url.pathname.slice('/tasks/phase_templates/'.length))
        if (!PHASE_TEMPLATES[id]) return Response.json({ ok: false, error: 'phase template not found' }, { status: 404 })
        if (RESERVED_PHASE_IDS.has(id)) return Response.json({ ok: false, error: `phase template '${id}' is reserved (runtime hardcoded); not editable` }, { status: 409 })
        const cur = PHASE_TEMPLATES[id]
        const normalized = {
          id,
          name: typeof body.name === 'string' && body.name.trim() ? body.name.trim() : cur.label,
          required_roles: Array.isArray(body.required_roles) ? body.required_roles : cur.required_roles,
          can_reject: typeof body.can_reject === 'boolean' ? body.can_reject : cur.can_reject,
          auto_advance: typeof body.auto_advance === 'boolean' ? body.auto_advance : cur.auto_advance,
        }
        const err = validatePhaseTemplate(normalized)
        if (err) return Response.json({ ok: false, error: err }, { status: 400 })
        // Behavior fields are locked while ANY open task is currently at this phase. The task's
        // task_phase_role_checks rows for its current cycle were INSERTed against the OLD phase
        // shape — if PM swaps required_roles from ['implementer'] to ['reviewer'], the advance handler would
        // query the new shape but the check row only exists for 'implementer', and 0-row UPDATE wedges
        // the task. auto_advance / can_reject have analogous problems (execution state already
        // captured the old behavior). Only `name` is safe to hot-swap.
        const sortedRoles = (arr: any[]) => [...(arr || [])].sort().join(',')
        const behaviorChanged = (
          sortedRoles(normalized.required_roles) !== sortedRoles(cur.required_roles) ||
          normalized.can_reject !== cur.can_reject ||
          normalized.auto_advance !== cur.auto_advance
        )
        if (behaviorChanged) {
          let occupied = 0
          const occupiedDetails: string[] = []
          for (const [wfKey, wf] of Object.entries(WORKFLOW_TEMPLATES)) {
            if (!wf.phases.some(p => p.key === id)) continue
            const n = (db.prepare(`SELECT COUNT(*) AS n FROM tasks_v2 WHERE type=? AND status='open' AND current_phase=?`).get(wfKey, id) as any).n
            if (n > 0) { occupied += n; occupiedDetails.push(`${wfKey}:${n}`) }
          }
          if (occupied > 0) {
            return Response.json({ ok: false, error: `cannot change behavior fields of phase '${id}' while ${occupied} open task(s) are currently at this phase (${occupiedDetails.join(', ')}) — their phase execution state (task_phase_role_checks) is locked to the old shape. Only 'name' is editable until they advance.` }, { status: 409 })
          }
        }
        const now = groupNowIso()
        db.run(
          `UPDATE phase_templates SET name=?, required_roles=?, can_reject=?, auto_advance=?, is_terminal=?, on_failure=NULL, retry_max=NULL, updated_at=? WHERE id=?`,
          [normalized.name, JSON.stringify(normalized.required_roles), normalized.can_reject ? 1 : 0, normalized.auto_advance ? 1 : 0, normalized.required_roles.length === 0 ? 1 : 0, now, id],
        )
        reloadPhaseTemplates()
        reloadWorkflowTemplates()
        return Response.json({ ok: true, template: PHASE_TEMPLATES[id] })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // AIC-42 · DELETE /tasks/phase_templates/:id — PM only.
    // Reserved ids cannot be deleted. Constraint #4: refuse if ANY workflow references this
    // template (even if no open task at this phase) — would leave dangling phase_ids in DB.
    if (req.method === 'DELETE' && /^\/tasks\/phase_templates\/[^/]+$/.test(url.pathname) && isAuthed) {
      try {
        const info = v2AuthInfo()
        if (info.mode !== 'cookie' && info.mode !== 'local') {
          return Response.json({ ok: false, error: 'only PM (cookie/local) can delete phase templates' }, { status: 403 })
        }
        const id = decodeURIComponent(url.pathname.slice('/tasks/phase_templates/'.length))
        if (!PHASE_TEMPLATES[id]) return Response.json({ ok: false, error: 'phase template not found' }, { status: 404 })
        if (RESERVED_PHASE_IDS.has(id)) return Response.json({ ok: false, error: `phase template '${id}' is reserved (runtime hardcoded); not deletable` }, { status: 409 })
        const refs = Object.entries(WORKFLOW_TEMPLATES).filter(([, wf]) => wf.phases.some(p => p.key === id))
        if (refs.length > 0) {
          return Response.json({ ok: false, error: `cannot delete phase template '${id}': still referenced by ${refs.length} workflow(s): ${refs.map(r => r[0]).join(', ')}` }, { status: 409 })
        }
        db.run(`DELETE FROM phase_templates WHERE id = ?`, id)
        reloadPhaseTemplates()
        reloadWorkflowTemplates()
        return Response.json({ ok: true, deleted: id })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // AIC-31/42 · POST /tasks/workflow_templates — create new type (PM only).
    // AIC-42: body shape is {key, label, phase_ids: string[]} (no longer inline phases).
    if (req.method === 'POST' && url.pathname === '/tasks/workflow_templates' && isAuthed) {
      try {
        const body = await req.json() as any
        const info = v2AuthInfo()
        if (!v2IsPm(info, body)) return Response.json({ ok: false, error: 'only PM can manage workflow templates' }, { status: 403 })
        const key = String(body.key || '').trim()
        const label = String(body.label || '').trim()
        if (!/^[a-z][a-z0-9_]{1,40}$/.test(key)) return Response.json({ ok: false, error: 'key must match ^[a-z][a-z0-9_]{1,40}$' }, { status: 400 })
        if (!label) return Response.json({ ok: false, error: 'label required' }, { status: 400 })
        const phase_ids = body.phase_ids
        const err = validatePhaseIds(phase_ids)
        if (err) return Response.json({ ok: false, error: err }, { status: 400 })
        if (WORKFLOW_TEMPLATES[key]) return Response.json({ ok: false, error: `type ${key} already exists; use PATCH to edit` }, { status: 409 })
        const now = groupNowIso()
        db.run(
          `INSERT INTO workflow_templates (key, label, phases, phase_ids, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
          [key, label, '[]', JSON.stringify(phase_ids), now, now],
        )
        reloadWorkflowTemplates()
        return Response.json({ ok: true, template: WORKFLOW_TEMPLATES[key] })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // AIC-31/42 · PATCH /tasks/workflow_templates/:key — edit label and/or phase_ids (PM only).
    // AIC-42: body shape is {label?, phase_ids?} (no longer inline phases).
    if (req.method === 'PATCH' && /^\/tasks\/workflow_templates\/[^/]+$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json() as any
        const info = v2AuthInfo()
        if (!v2IsPm(info, body)) return Response.json({ ok: false, error: 'only PM can manage workflow templates' }, { status: 403 })
        const key = decodeURIComponent(url.pathname.slice('/tasks/workflow_templates/'.length))
        if (!WORKFLOW_TEMPLATES[key]) return Response.json({ ok: false, error: 'template not found' }, { status: 404 })
        const sets: string[] = []
        const vals: any[] = []
        if (typeof body.label === 'string' && body.label.trim()) { sets.push('label = ?'); vals.push(body.label.trim()) }
        if (Array.isArray(body.phase_ids)) {
          const err = validatePhaseIds(body.phase_ids)
          if (err) return Response.json({ ok: false, error: err }, { status: 400 })
          const openTasks = db.prepare(`SELECT current_phase FROM tasks_v2 WHERE type = ? AND status = 'open'`).all(key) as any[]
          const newSet = new Set(body.phase_ids as string[])
          for (const t of openTasks) {
            if (!newSet.has(t.current_phase)) {
              return Response.json({ ok: false, error: `cannot remove phase '${t.current_phase}' while open task(s) of type ${key} are in it` }, { status: 409 })
            }
          }
          sets.push('phase_ids = ?'); vals.push(JSON.stringify(body.phase_ids))
        }
        if (!sets.length) return Response.json({ ok: false, error: 'nothing to update' }, { status: 400 })
        sets.push('updated_at = ?'); vals.push(groupNowIso())
        vals.push(key)
        db.run(`UPDATE workflow_templates SET ${sets.join(', ')} WHERE key = ?`, vals)
        reloadWorkflowTemplates()
        return Response.json({ ok: true, template: WORKFLOW_TEMPLATES[key] })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // AIC-31 · DELETE /tasks/workflow_templates/:key — PM only; gate refuses if any open task uses this type
    if (req.method === 'DELETE' && /^\/tasks\/workflow_templates\/[^/]+$/.test(url.pathname) && isAuthed) {
      try {
        const info = v2AuthInfo()
        if (info.mode !== 'cookie' && info.mode !== 'local') {
          return Response.json({ ok: false, error: 'only PM (cookie/local) can delete workflow templates' }, { status: 403 })
        }
        const key = decodeURIComponent(url.pathname.slice('/tasks/workflow_templates/'.length))
        if (!WORKFLOW_TEMPLATES[key]) return Response.json({ ok: false, error: 'template not found' }, { status: 404 })
        const openCount = (db.prepare(`SELECT COUNT(*) AS n FROM tasks_v2 WHERE type = ? AND status = 'open'`).get(key) as any).n
        if (openCount > 0) {
          return Response.json({ ok: false, error: `cannot delete type ${key}: ${openCount} open task(s) still using it` }, { status: 409 })
        }
        db.run(`DELETE FROM workflow_templates WHERE key = ?`, key)
        reloadWorkflowTemplates()
        return Response.json({ ok: true, deleted: key })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // 12.1 GET /tasks — list with status / type / category filters
    if (req.method === 'GET' && url.pathname === '/tasks' && isAuthed) {
      const status = url.searchParams.get('status') || ''
      const type = url.searchParams.get('type') || ''
      const category = url.searchParams.get('category') || ''
      const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200)
      const offset = parseInt(url.searchParams.get('offset') || '0')
      const where: string[] = []
      const params: any[] = []
      // 2026-06-20: mirror /group/tasks's 3-bucket semantics on v2. Previously this
      // only matched 'open' / 'closed'; passing status='draft' silently fell
      // through to no filter and returned every task in the table.
      if (status === 'draft') {
        where.push("status = 'open' AND current_phase = 'draft'")
      } else if (status === 'active') {
        where.push("status = 'open' AND current_phase != 'draft'")
      } else if (status === 'done') {
        where.push("status = 'closed'")
      } else if (status === 'open' || status === 'closed') {
        where.push('status = ?'); params.push(status)
      }
      if (type) { where.push('type = ?'); params.push(type) }
      if (category) { where.push('category = ?'); params.push(category) }
      const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : ''
      const total = (db.prepare(`SELECT COUNT(*) AS n FROM tasks_v2 ${whereSql}`).get(...params) as any).n
      const rows = db.prepare(`SELECT * FROM tasks_v2 ${whereSql} ORDER BY created_at DESC LIMIT ? OFFSET ?`).all(...params, limit, offset) as any[]
      for (const row of rows) {
        attachPendingFields(row)
        const latest = db.prepare(`SELECT id, ts, kind, actor_id, meta, body FROM task_events WHERE task_id=? ORDER BY id DESC LIMIT 1`).get(row.id) as any
        row.latest_event_id = latest?.id || null
        row.latest_event_ts = latest?.ts || null
        row.latest_event_kind = latest?.kind || null
        row.latest_event_actor = latest?.actor_id || null
        row.latest_event_body = latest?.body ? String(latest.body).slice(0, 120) : null
        // AIC-61 cycle 3: mention 字段 for toast trigger
        let latestEventMentions: string[] = []
        if (latest?.meta) { try { const m = JSON.parse(latest.meta); if (Array.isArray(m?.mentions)) latestEventMentions = m.mentions } catch {} }
        row.latest_event_mentions = latestEventMentions
      }
      const categories = db.prepare(`SELECT name FROM task_categories ORDER BY name`).all().map((r: any) => r.name)
      return Response.json({ ok: true, total, tasks: rows, categories, types: Object.keys(WORKFLOW_TEMPLATES) })
    }

    // 12.2 GET /tasks/:id — full detail with events + phase_state + workflow
    if (req.method === 'GET' && url.pathname.startsWith('/tasks/') && !url.pathname.includes('/events') && isAuthed && !url.pathname.endsWith('/summary')) {
      const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length))
      if (!taskId || taskId.includes('/')) {
        // fall through; might be sub-resource path
      } else {
        const task = loadTask(taskId)
        if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
        attachPendingFields(task)
        const events = (db.prepare(`SELECT * FROM task_events WHERE task_id=? ORDER BY id ASC`).all(taskId) as any[]).map(e => ({
          ...e,
          meta: JSON.parse(e.meta || '{}'),
        }))
        const phase_state = db.prepare(`SELECT * FROM task_phase_role_checks WHERE task_id=? ORDER BY phase, cycle, role_id`).all(taskId)
        const workflow = WORKFLOW_TEMPLATES[task.type]?.phases || []
        return Response.json({ ok: true, task, workflow, phase_state, events })
      }
    }

    // 12.13 GET /tasks/:id/events — long poll for new events on a task
    if (req.method === 'GET' && url.pathname.startsWith('/tasks/') && url.pathname.endsWith('/events') && isAuthed) {
      const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length, -'/events'.length))
      // AIC-117 cycle 2: accept `since` as alias for `since_event_id` (scope 字面写的是 `since=ULID`).
      const sinceEventId = url.searchParams.get('since_event_id') || url.searchParams.get('since') || ''
      // before_id 历史分页 (跟 /group/poll 同款, before_event_id 兼容老 caller)
      const beforeId = url.searchParams.get('before_id') || url.searchParams.get('before_event_id') || ''
      const limit = Math.min(parseInt(url.searchParams.get('limit') || '0') || 0, 500)
      const timeout = Math.min(parseInt(url.searchParams.get('timeout') || '30'), 30) * 1000
      // AIC-90: before_id 历史分页 — short-circuit 不走 long poll, cursor=最早一条 id
      if (beforeId) {
        const lim = limit || 50
        const rows = db.prepare(`SELECT * FROM task_events WHERE task_id=? AND id < ? ORDER BY id DESC LIMIT ?`).all(taskId, beforeId, lim) as any[]
        const events = rows.reverse().map(e => ({ ...e, meta: JSON.parse(e.meta || '{}') }))
        const cursor = events.length > 0 ? events[0].id : null
        const hasMore = events.length === lim
        return Response.json({ ok: true, events, cursor, has_more: hasMore })
      }
      // AIC-117: 三路 fetch —
      // (a) sinceEventId 有值 → 拉 id > sinceEventId (long poll 增量, 不 limit, caller 想拿全部新事件)
      // (b) sinceEventId 空 + limit 有值 → 拉最近 N 条 (agent 主动拉 history 路径; ORDER BY DESC LIMIT N 然后 reverse 保 ASC 输出)
      // (c) sinceEventId 空 + limit 空 → 拉全表 ASC (long poll 首次拿全部 backlog, 跟旧 caller 兼容)
      const fetchNew = () => {
        let rows: any[]
        if (sinceEventId) {
          rows = db.prepare(`SELECT * FROM task_events WHERE task_id=? AND id > ? ORDER BY id ASC`).all(taskId, sinceEventId) as any[]
        } else if (limit > 0) {
          rows = (db.prepare(`SELECT * FROM task_events WHERE task_id=? ORDER BY id DESC LIMIT ?`).all(taskId, limit) as any[]).reverse()
        } else {
          rows = db.prepare(`SELECT * FROM task_events WHERE task_id=? ORDER BY id ASC`).all(taskId) as any[]
        }
        return rows.map(e => ({ ...e, meta: JSON.parse(e.meta || '{}') }))
      }
      const immediate = fetchNew()
      if (immediate.length > 0 || !sinceEventId) return Response.json({ ok: true, events: immediate })
      return new Promise<Response>((resolve) => {
        const timer = setTimeout(() => {
          taskEventWaiters.delete(waiter)
          resolve(Response.json({ ok: true, events: [] }))
        }, timeout)
        const waiter = { taskId, sinceEventId, resolve, timer }
        taskEventWaiters.add(waiter)
      })
    }

    // 12.12 GET /tasks/summary — long poll for list-level summary (cursor-based)
    if (req.method === 'GET' && url.pathname === '/tasks/summary' && isAuthed) {
      const sinceCursor = url.searchParams.get('since_cursor') || ''
      const timeout = Math.min(parseInt(url.searchParams.get('timeout') || '30'), 30) * 1000
      const buildSnapshot = () => {
        const rows = db.prepare(`SELECT * FROM tasks_v2 WHERE status='open' ORDER BY updated_at DESC`).all() as any[]
        for (const row of rows) {
          attachPendingFields(row)
          const latest = db.prepare(`SELECT id, ts, kind, actor_id, meta, body FROM task_events WHERE task_id=? ORDER BY id DESC LIMIT 1`).get(row.id) as any
          row.latest_event_id = latest?.id || null
          row.latest_event_ts = latest?.ts || null
          row.latest_event_kind = latest?.kind || null
          row.latest_event_actor = latest?.actor_id || null
          row.latest_event_body = latest?.body ? String(latest.body).slice(0, 120) : null
          // AIC-61 cycle 3: mention 字段 for toast trigger
          let latestEventMentions: string[] = []
          if (latest?.meta) { try { const m = JSON.parse(latest.meta); if (Array.isArray(m?.mentions)) latestEventMentions = m.mentions } catch {} }
          row.latest_event_mentions = latestEventMentions
          // unread_count per actor
          const unread: Record<string, number> = {}
          for (const actor of ['admin', 'agent1', 'agent2']) {
            const mark = db.prepare(`SELECT last_seen_event_id FROM task_seen_marks WHERE task_id=? AND actor_id=?`).get(row.id, actor) as any
            const lastId = mark?.last_seen_event_id || ''
            const n = (db.prepare(`SELECT COUNT(*) AS n FROM task_events WHERE task_id=? AND id > ?`).get(row.id, lastId) as any).n
            unread[actor] = n
          }
          row.unread_count = unread
        }
        // Cursor: max latest_event_id across all open tasks (string compare)
        let cursor = ''
        for (const r of rows) { if (r.latest_event_id && r.latest_event_id > cursor) cursor = r.latest_event_id }
        return { cursor, tasks: rows }
      }
      const snap = buildSnapshot()
      if (snap.cursor !== sinceCursor || !sinceCursor) return Response.json({ ok: true, ...snap })
      return new Promise<Response>((resolve) => {
        const timer = setTimeout(() => {
          taskSummaryWaiters.delete(waiter)
          resolve(Response.json({ ok: true, ...buildSnapshot() }))
        }, timeout)
        const waiter = { sinceCursor, resolve, timer }
        taskSummaryWaiters.add(waiter)
      })
    }

    // 12.3 POST /tasks — create a draft task (PM only)
    if (req.method === 'POST' && url.pathname === '/tasks' && isAuthed) {
      try {
        const body = await req.json() as any
        const info = v2AuthInfo()
        // PM (admin) can always create; agent1 can create on its own (agent2 is
        // review-only). sender_id is trusted because Bearer + matching sender_id
        // is the agent's own auth mode, and PM cookie/local goes through v2IsPm.
        const AGENT_CREATORS = new Set(['agent1'])
        const isPm = v2IsPm(info, body)
        const isAuthedAgent = info.mode === 'header' && AGENT_CREATORS.has(body.sender_id)
        if (!isPm && !isAuthedAgent) {
          return Response.json({ ok: false, error: 'creating tasks requires PM auth, or Bearer as agent1' }, { status: 403 })
        }
        const title = String(body.title || '').trim()
        const type = String(body.type || '').trim()
        if (!title) return Response.json({ ok: false, error: 'title required' }, { status: 400 })
        if (!WORKFLOW_TEMPLATES[type]) return Response.json({ ok: false, error: `unknown type: ${type}` }, { status: 400 })
        // Allocate AIC-NNN
        const seqRow = db.prepare(`SELECT next FROM task_id_seq WHERE prefix='AIC'`).get() as any
        const seq = seqRow?.next || 1
        db.run(`UPDATE task_id_seq SET next = next + 1 WHERE prefix='AIC'`)
        const id = `AIC-${seq}`
        const now = groupNowIso()
        db.run(
          `INSERT INTO tasks_v2 (id, title, description, type, category, current_phase, status, created_at, created_by, updated_at) VALUES (?, ?, ?, ?, ?, 'draft', 'open', ?, ?, ?)`,
          [id, title, body.description || '', type, body.category || '', now, body.sender_id || 'admin', now],
        )
        // Insert the draft phase's role checks (PM is the "owner" of draft, no actual check needed but row exists for cycle tracking)
        db.run(
          `INSERT INTO task_phase_role_checks (task_id, phase, cycle, role_id) VALUES (?, 'draft', 1, 'pm')`, [id],
        )
        insertTaskEvent(id, 'system_event', body.sender_id || 'admin', `${ACTOR_DISPLAY_NAMES[body.sender_id || 'admin']} 创建了任务`, { event_type: 'task_created' })
        const task = attachPendingFields(loadTask(id))
        wakeTaskWaiters(id)
        return Response.json({ ok: true, task })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // 12.4 PUT /tasks/:id/publish — draft → first non-draft phase (PM only)
    if (req.method === 'PUT' && /^\/tasks\/[^/]+\/publish$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json().catch(() => ({})) as any
        const info = v2AuthInfo()
        if (!v2IsPm(info, body)) return Response.json({ ok: false, error: 'only PM can publish tasks' }, { status: 403 })
        const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length, -'/publish'.length))
        const task = loadTask(taskId)
        if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
        if (task.current_phase !== 'draft') return Response.json({ ok: false, error: `task is in '${task.current_phase}', not draft` }, { status: 400 })
        const phases = WORKFLOW_TEMPLATES[task.type]?.phases || []
        const draftIdx = phases.findIndex(p => p.key === 'draft')
        const firstPhase = phases[draftIdx + 1]
        if (!firstPhase) return Response.json({ ok: false, error: 'no next phase defined' }, { status: 500 })
        enterPhase(task, firstPhase.key, 'draft', 'publish')
        const updated = attachPendingFields(loadTask(taskId))
        wakeTaskWaiters(taskId)
        return Response.json({ ok: true, task: updated })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // 12.5 PUT /tasks/:id/advance — owner / PM confirms current phase
    if (req.method === 'PUT' && /^\/tasks\/[^/]+\/advance$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json() as any
        const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length, -'/advance'.length))
        const task = loadTask(taskId)
        if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
        const phaseDef = findPhaseDef(task.type, task.current_phase)
        if (!phaseDef) return Response.json({ ok: false, error: `bad phase ${task.current_phase}` }, { status: 500 })
        const actorId = String(body.actor_id || '').trim()
        if (!actorId) return Response.json({ ok: false, error: 'actor_id required' }, { status: 400 })
        if (actorId === 'pm') return Response.json({ ok: false, error: "actor_id='pm' not accepted; use 'admin'" }, { status: 400 })
        const role = roleForActor(actorId)
        if (!role) return Response.json({ ok: false, error: `unknown actor: ${actorId}` }, { status: 400 })
        if (!phaseDef.required_roles.includes(role as any)) {
          return Response.json({ ok: false, error: `actor ${actorId} (role ${role}) not in required_roles for phase ${task.current_phase}` }, { status: 403 })
        }
        // Auth check per role
        const info = v2AuthInfo()
        if (role === 'pm' && !v2IsPm(info, body)) return Response.json({ ok: false, error: 'PM auth required' }, { status: 403 })
        if (role === 'system') return Response.json({ ok: false, error: 'system advance not callable externally' }, { status: 403 })
        // Agent roles: either the agent itself (Bearer + matching sender_id) or
        // PM cookie/local auth proxying on the agent's behalf (the "强制推进"
        // path from v2 web). PM proxy is recorded as checked_via='advance_proxy_pm'
        // so the audit trail distinguishes self-check from PM override.
        const isPmProxy = role !== 'pm' && v2IsPm(info, body)
        const isAgentSelf = info.mode === 'header' && body.sender_id === actorId
        if (role !== 'pm' && !isPmProxy && !isAgentSelf) {
          return Response.json({ ok: false, error: `agent ${actorId} must auth with Bearer + sender_id=${actorId}, or PM cookie/local auth to proxy` }, { status: 403 })
        }
        const cycle = getMaxCycle(taskId, task.current_phase)
        if (cycle === 0) return Response.json({ ok: false, error: 'no phase row to advance' }, { status: 500 })
        // Update check
        const now = groupNowIso()
        const checkedVia = isPmProxy ? 'advance_proxy_pm' : 'advance'
        // checked_by is the real operator (per v2 audit semantics). For PM
        // proxy that's 'admin', not the proxied agent — otherwise the audit
        // log would look like the agent self-checked.
        // role_id still records the workflow role being ticked.
        const checkedBy = isPmProxy ? 'admin' : actorId
        db.run(
          `UPDATE task_phase_role_checks SET checked_at = ?, checked_by = ?, checked_via = ? WHERE task_id=? AND phase=? AND cycle=? AND role_id=?`,
          [now, checkedBy, checkedVia, taskId, task.current_phase, cycle, role],
        )
        // Check whether phase fully checked → advance
        const advanced = maybeAdvancePhase(loadTask(taskId))
        const updated = attachPendingFields(loadTask(taskId))
        wakeTaskWaiters(taskId)
        return Response.json({ ok: true, task: updated, advanced })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // 12.6 PUT /tasks/:id/reject — owner rejects current phase (must have can_reject=true)
    if (req.method === 'PUT' && /^\/tasks\/[^/]+\/reject$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json() as any
        const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length, -'/reject'.length))
        const task = loadTask(taskId)
        if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
        const phaseDef = findPhaseDef(task.type, task.current_phase)
        if (!phaseDef) return Response.json({ ok: false, error: `bad phase ${task.current_phase}` }, { status: 500 })
        if (!phaseDef.can_reject) return Response.json({ ok: false, error: `phase ${task.current_phase} does not allow reject` }, { status: 400 })
        const actorId = String(body.actor_id || '').trim()
        const role = roleForActor(actorId)
        if (!role || !phaseDef.required_roles.includes(role as any)) {
          return Response.json({ ok: false, error: `actor ${actorId} cannot reject this phase` }, { status: 403 })
        }
        const info = v2AuthInfo()
        // Auth: agent itself (Bearer + sender_id=actorId) OR PM proxy (PM cookie/local).
        // Parity with /advance handler (AIC-74 cycle 2: auth 不一致 bug).
        const isPmProxy = role !== 'pm' && v2IsPm(info, body)
        const isAgentSelf = info.mode === 'header' && body.sender_id === actorId
        if (!isPmProxy && !isAgentSelf) {
          return Response.json({ ok: false, error: `actor ${actorId} must auth with Bearer + sender_id=${actorId}, or PM cookie/local auth to proxy` }, { status: 403 })
        }
        const comment = String(body.comment || '').trim()
        const prevPhase = previousNonAutoPhase(task.type, task.current_phase)
        if (!prevPhase) return Response.json({ ok: false, error: 'no previous phase to reject to' }, { status: 400 })
        // v0.7.1: action event (actor-owned, for feed display) then enterPhase (system phase_enter event)
        insertTaskEvent(taskId, 'system_event', actorId, `${ACTOR_DISPLAY_NAMES[actorId]} 拒绝 ${phaseDef.label}${comment ? ': ' + comment : ''}`, {
          event_type: 'phase_reject_action', from_phase: task.current_phase, from_phase_label: phaseDef.label, to_phase: prevPhase, to_phase_label: findPhaseDef(task.type, prevPhase)?.label, comment, rejected_by_actor: actorId,
        })
        enterPhase(task, prevPhase, task.current_phase, 'reject')
        const updated = attachPendingFields(loadTask(taskId))
        wakeTaskWaiters(taskId)
        return Response.json({ ok: true, task: updated })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // 12.7 PUT /tasks/:id/rollback — PM rollback to a specific phase
    if (req.method === 'PUT' && /^\/tasks\/[^/]+\/rollback$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json() as any
        const info = v2AuthInfo()
        if (!v2IsPm(info, body)) return Response.json({ ok: false, error: 'only PM can rollback' }, { status: 403 })
        const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length, -'/rollback'.length))
        const task = loadTask(taskId)
        if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
        const toPhase = String(body.to_phase || '').trim()
        const phases = WORKFLOW_TEMPLATES[task.type]?.phases || []
        const toIdx = phases.findIndex(p => p.key === toPhase)
        const curIdx = phases.findIndex(p => p.key === task.current_phase)
        if (toIdx < 0 || toIdx >= curIdx) return Response.json({ ok: false, error: `cannot rollback to ${toPhase}` }, { status: 400 })
        if (toPhase === 'draft' || toPhase === 'closed') return Response.json({ ok: false, error: `cannot rollback to terminal/initial phase ${toPhase}` }, { status: 400 })
        const comment = String(body.comment || '').trim()
        // v0.7.1: action event (actor-owned PM) + enterPhase (system phase_enter)
        insertTaskEvent(taskId, 'system_event', 'admin', `admin 打回到 ${findPhaseDef(task.type, toPhase)?.label}${comment ? ': ' + comment : ''}`, {
          event_type: 'phase_rollback_action', from_phase: task.current_phase, from_phase_label: findPhaseDef(task.type, task.current_phase)?.label, to_phase: toPhase, to_phase_label: findPhaseDef(task.type, toPhase)?.label, comment,
        })
        enterPhase(task, toPhase, task.current_phase, 'rollback')
        const updated = attachPendingFields(loadTask(taskId))
        wakeTaskWaiters(taskId)
        return Response.json({ ok: true, task: updated })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // 12.8 POST /tasks/:id/comments — add user comment with optional mentions
    if (req.method === 'POST' && /^\/tasks\/[^/]+\/comments$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json() as any
        const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length, -'/comments'.length))
        const task = loadTask(taskId)
        if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
        const actorId = String(body.actor_id || '').trim()
        if (!actorId) return Response.json({ ok: false, error: 'actor_id required' }, { status: 400 })
        if (actorId === 'system') return Response.json({ ok: false, error: 'system cannot post comments' }, { status: 403 })
        const text = String(body.body || '').trim()
        if (!text) return Response.json({ ok: false, error: 'body required' }, { status: 400 })
        // Auth: PM uses cookie/local/admin; agents use Bearer+sender_id, or PM proxy.
        const info = v2AuthInfo()
        if (actorId === 'admin') {
          if (!v2IsPm(info, body)) return Response.json({ ok: false, error: 'PM auth required' }, { status: 403 })
        } else {
          const isPmProxy = v2IsPm(info, body)
          const isAgentSelf = info.mode === 'header' && body.sender_id === actorId
          if (!isPmProxy && !isAgentSelf) {
            return Response.json({ ok: false, error: `actor ${actorId} must auth with Bearer + sender_id=${actorId}, or PM cookie/local auth to proxy` }, { status: 403 })
          }
        }
        const mentions: string[] = Array.isArray(body.mentions) ? body.mentions.filter((x: any) => typeof x === 'string' && x !== actorId) : []
        const meta = { mentions, reply_to_event_id: body.reply_to_event_id || null, attachments: body.attachments || [] }
        const event = insertTaskEvent(taskId, 'user_comment', actorId, text, meta)
        // Notify mentioned actors via DM
        for (const mention of mentions) {
          const notifyEvent = { ...event, meta: { event_type: 'user_comment_mention', task_id: taskId } }
          notifyActorViaDM(mention, task, notifyEvent)
        }
        wakeTaskWaiters(taskId)
        return Response.json({ ok: true, event })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // 12.9 PATCH /tasks/:id — edit title/description/category (PM only)
    if (req.method === 'PATCH' && /^\/tasks\/[^/]+$/.test(url.pathname) && !url.pathname.includes('/tasks/workflow_templates') && isAuthed) {
      try {
        const body = await req.json() as any
        const info = v2AuthInfo()
        if (!v2IsPm(info, body)) return Response.json({ ok: false, error: 'only PM can edit tasks' }, { status: 403 })
        const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length))
        const task = loadTask(taskId)
        if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
        const sets: string[] = []
        const vals: any[] = []
        if (typeof body.title === 'string') { sets.push('title = ?'); vals.push(body.title.trim()) }
        if (typeof body.description === 'string') { sets.push('description = ?'); vals.push(body.description) }
        if (typeof body.category === 'string') { sets.push('category = ?'); vals.push(body.category) }
        if (!sets.length) return Response.json({ ok: false, error: 'no fields to update' }, { status: 400 })
        sets.push('updated_at = ?'); vals.push(groupNowIso())
        vals.push(taskId)
        db.run(`UPDATE tasks_v2 SET ${sets.join(', ')} WHERE id = ?`, vals)
        const updated = attachPendingFields(loadTask(taskId))
        wakeTaskWaiters(taskId)
        return Response.json({ ok: true, task: updated })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // 12.10 DELETE /tasks/:id — delete task and cascade events/checks (PM only)
    if (req.method === 'DELETE' && /^\/tasks\/[^/]+$/.test(url.pathname) && isAuthed) {
      const info = v2AuthInfo()
      if (info.mode !== 'cookie' && info.mode !== 'local') {
        return Response.json({ ok: false, error: 'only PM (cookie/local) can delete tasks' }, { status: 403 })
      }
      const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length))
      const task = loadTask(taskId)
      if (!task) return Response.json({ ok: false, error: 'task not found' }, { status: 404 })
      // Cascade delete (FK ON DELETE CASCADE handles events / checks / seen_marks)
      db.run(`DELETE FROM tasks_v2 WHERE id = ?`, taskId)
      // v0.7.2 Fix 6: events table is now empty for this task; pass deleted=true so detail waiters resolve 410
      wakeTaskWaiters(taskId, { deleted: true })
      return Response.json({ ok: true, deleted: taskId })
    }

    // POST /tasks/:id/seen — mark task events as seen (for unread count)
    if (req.method === 'POST' && /^\/tasks\/[^/]+\/seen$/.test(url.pathname) && isAuthed) {
      try {
        const body = await req.json() as any
        const taskId = decodeURIComponent(url.pathname.slice('/tasks/'.length, -'/seen'.length))
        const actorId = String(body.actor_id || '').trim()
        const eventId = String(body.last_seen_event_id || '').trim()
        if (!actorId || !eventId) return Response.json({ ok: false, error: 'actor_id + last_seen_event_id required' }, { status: 400 })
        const ts = groupNowIso()
        db.run(
          `INSERT INTO task_seen_marks (task_id, actor_id, last_seen_event_id, last_seen_ts) VALUES (?, ?, ?, ?)
           ON CONFLICT(task_id, actor_id) DO UPDATE SET last_seen_event_id = excluded.last_seen_event_id, last_seen_ts = excluded.last_seen_ts`,
          [taskId, actorId, eventId, ts],
        )
        wakeTaskWaiters(taskId)
        return Response.json({ ok: true })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }

    // ====================================================================
    // === End Task Tracker v2 API                                       ===
    // ====================================================================

    // 跨频道搜索 endpoint：group_messages (workgroup + dm-*) + task_events (user_comment) union
    // LIKE %q% + filter channels/senders/since/before。不索引附件 / 编辑历史。
    if (req.method === 'GET' && url.pathname === '/api/search' && isAuthed) {
      try {
        const q = (url.searchParams.get('q') || '').trim()
        if (!q) {
          return Response.json({ ok: true, results: [], total: 0, by_channel_count: {}, by_sender_count: {} })
        }
        const limit = Math.min(Math.max(parseInt(url.searchParams.get('limit') || '50') || 50, 1), 200)
        const channels = (url.searchParams.get('channels') || '').split(',').map(s => s.trim()).filter(Boolean)
        const senders = (url.searchParams.get('senders') || '').split(',').map(s => s.trim()).filter(Boolean)
        const since = url.searchParams.get('since') || null
        const before = url.searchParams.get('before') || null

        // SQL LIKE escape — q 内含 % _ \ 都转义防误匹配
        const likePattern = '%' + q.replace(/[\\%_]/g, m => '\\' + m) + '%'

        const all: any[] = []

        // ---- group_messages 表 (workgroup + dm-agent1 + dm-agent2) ----
        const groupChs = channels.filter(c => !c.startsWith('task:'))
        const includeGroup = channels.length === 0 || groupChs.length > 0
        if (includeGroup) {
          const conds: string[] = [`text LIKE ? ESCAPE '\\'`, `(message_type IS NULL OR message_type != 'system_notification')`]
          const params: any[] = [likePattern]
          if (groupChs.length > 0) {
            conds.push(`conversation_id IN (${groupChs.map(() => '?').join(',')})`)
            params.push(...groupChs)
          }
          if (senders.length > 0) {
            conds.push(`sender_id IN (${senders.map(() => '?').join(',')})`)
            params.push(...senders)
          }
          if (since) { conds.push(`ts >= ?`); params.push(since) }
          if (before) { conds.push(`ts <= ?`); params.push(before) }
          const rows = db.prepare(`SELECT id, sender_id, text, ts, conversation_id FROM group_messages WHERE ${conds.join(' AND ')} ORDER BY ts DESC LIMIT ?`).all(...params, limit) as any[]
          for (const r of rows) {
            all.push({
              channel: r.conversation_id,
              message_id: r.id,
              sender_id: r.sender_id,
              ts: r.ts,
              text: String(r.text || ''),
              task_id: null,
              kind: 'chat',
            })
          }
        }

        // ---- task_events 表 (kind=user_comment) ----
        const taskChs = channels.filter(c => c.startsWith('task:')).map(c => c.slice('task:'.length))
        const includeTask = channels.length === 0 || taskChs.length > 0 || channels.includes('task')
        if (includeTask) {
          const conds: string[] = [`body LIKE ? ESCAPE '\\'`, `kind = 'user_comment'`]
          const params: any[] = [likePattern]
          if (taskChs.length > 0) {
            conds.push(`task_id IN (${taskChs.map(() => '?').join(',')})`)
            params.push(...taskChs)
          }
          if (senders.length > 0) {
            conds.push(`actor_id IN (${senders.map(() => '?').join(',')})`)
            params.push(...senders)
          }
          if (since) { conds.push(`ts >= ?`); params.push(since) }
          if (before) { conds.push(`ts <= ?`); params.push(before) }
          const rows = db.prepare(`SELECT id, task_id, actor_id, body, ts, kind FROM task_events WHERE ${conds.join(' AND ')} ORDER BY ts DESC LIMIT ?`).all(...params, limit) as any[]
          for (const r of rows) {
            all.push({
              channel: `task:${r.task_id}`,
              message_id: r.id,
              sender_id: r.actor_id,
              ts: r.ts,
              text: String(r.body || ''),
              task_id: r.task_id,
              kind: 'user_comment',
            })
          }
        }

        // merge sort ts desc + truncate
        all.sort((a, b) => (b.ts > a.ts ? 1 : b.ts < a.ts ? -1 : 0))
        const results = all.slice(0, limit)

        // by_channel_count / by_sender_count 从全集派生 (不只 truncate 后), 让 filter chip count 准确
        const byChannel: Record<string, number> = {}
        const bySender: Record<string, number> = {}
        for (const r of all) {
          byChannel[r.channel] = (byChannel[r.channel] || 0) + 1
          bySender[r.sender_id] = (bySender[r.sender_id] || 0) + 1
        }

        return Response.json({
          ok: true,
          results,
          total: all.length,
          by_channel_count: byChannel,
          by_sender_count: bySender,
        })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 500 })
      }
    }

    // Locate a message in DB so client can fetch its surrounding batch in one round-trip
    // instead of loop-load-older "guess where it is". Used by web `openMessage` to jump to
    // old messages from search results / URL ?msg= without 30 rounds of prepend abuse.
    // Returns `before_id` = newer-by-1 message id, so client calls `?before_id=<that>&limit=50`
    // and that batch includes the target.
    if (req.method === 'GET' && url.pathname === '/api/search/locate' && isAuthed) {
      const channel = url.searchParams.get('channel') || ''
      const messageId = url.searchParams.get('message_id') || ''
      if (!channel || !messageId) return jsonError('channel + message_id required', 400)
      try {
        let result: { found: boolean; source_table?: 'group_messages' | 'task_events'; timestamp?: string; before_id?: string | null } = { found: false }
        if (channel.startsWith('task:')) {
          const taskId = channel.slice('task:'.length)
          const row = db.prepare(`SELECT id, ts FROM task_events WHERE id=? AND task_id=?`).get(messageId, taskId) as any
          if (row) {
            const next = db.prepare(`SELECT id FROM task_events WHERE task_id=? AND ts > ? ORDER BY ts ASC LIMIT 1`).get(taskId, row.ts) as any
            result = { found: true, source_table: 'task_events', timestamp: row.ts, before_id: next?.id ?? null }
          }
        } else {
          // workgroup / dm-agent1 / dm-agent2 → group_messages
          const grpRow = db.prepare(`SELECT id, ts FROM group_messages WHERE id=? AND conversation_id=?`).get(messageId, channel) as any
          if (grpRow) {
            const next = db.prepare(`SELECT id FROM group_messages WHERE conversation_id=? AND ts > ? ORDER BY ts ASC LIMIT 1`).get(channel, grpRow.ts) as any
            result = { found: true, source_table: 'group_messages', timestamp: grpRow.ts, before_id: next?.id ?? null }
          }
        }
        return Response.json({ ok: true, channel, message_id: messageId, ...result })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 500 })
      }
    }

// System stats: GET /api/system/stats — host-machine RAM / disk / uptime introspection.
    // Off by default in open-source mode (otherwise any web client sees the operator's host
    // hardware footprint). Opt-in via env to enable the workstation USAGE card.
    if (req.method === 'GET' && url.pathname === '/api/system/stats' && isAuthed) {
      if (process.env.AICOLLAB_SYSTEM_STATS !== '1') {
        return Response.json({
          available: false,
          reason: 'system stats disabled — set AICOLLAB_SYSTEM_STATS=1 to enable host-machine RAM/disk/uptime introspection',
        })
      }
      try {
        const os = await import('os')
        const totalMem = os.totalmem()
        const freeMem = os.freemem()
        const usedMem = totalMem - freeMem
        const formatGB = (b: number) => (b / 1073741824).toFixed(1) + 'G'

        // Use APFS container stats for accurate disk usage
        const apfsOut = Bun.spawnSync(['/usr/sbin/diskutil', 'apfs', 'list']).stdout.toString()
        const ceilMatch = apfsOut.match(/Size \(Capacity Ceiling\):\s+(\d+)/)
        const usedMatch = apfsOut.match(/Capacity In Use By Volumes:\s+(\d+)/)
        const containerTotal = ceilMatch ? Number(ceilMatch[1]) : 0
        const containerUsed = usedMatch ? Number(usedMatch[1]) : 0
        const diskTotal = containerTotal ? formatGB(containerTotal) : '?'
        const diskUsed = containerUsed ? formatGB(containerUsed) : '?'
        const diskPercent = containerTotal ? Math.round(containerUsed / containerTotal * 100) : 0

        const uptimeOut = Bun.spawnSync(['uptime']).stdout.toString().trim()
        const uptimeMatch = uptimeOut.match(/up\s+(.+?),\s+\d+\s+user/)
        const uptime = uptimeMatch?.[1]?.trim() || '?'

        // Context usage — read cached value from heartbeat (nudge updates every 5min)
        let contextPercent = 0
        let contextDetail = ''
        try {
          const hb = JSON.parse(readFileSync(HEARTBEAT_PATH, 'utf-8'))
          const tokens = hb.context_tokens || 0
          if (tokens > 0) {
            const estimatedK = Math.round(tokens / 1000)
            contextPercent = Math.min(100, Math.round(tokens / 10000))
            contextDetail = `~${estimatedK}K / 1M`
          }
        } catch {}

        return Response.json({
          ram_used: formatGB(usedMem),
          ram_total: formatGB(totalMem),
          disk_used: diskUsed,
          disk_total: diskTotal,
          disk_percent: diskPercent,
          uptime: uptime,
          context_percent: contextPercent,
          context_detail: contextDetail
        })
      } catch (e: any) {
        return Response.json({ error: e.message }, { status: 500 })
      }
    }

    // AIC-119: agent statusline cache — statusline.sh POST 整 stdin JSON, web 终端面板 5s GET cached.
    // 自治 auth (不走 outer isAuthed): POST 要求 localhost OR Bearer + x-sender-id=agent_id;
    // GET 走 outer isAuthed (web 端已带 cookie). statusline.sh 是本机 fire-and-forget, 不带 token.
    if (req.method === 'POST' && url.pathname === '/api/agent-statusline') {
      try {
        const agent = url.searchParams.get('agent') || ''
        if (!AGENT_STATUSLINE_IDS.has(agent)) {
          return Response.json({ ok: false, error: `unknown agent: ${agent}` }, { status: 400 })
        }
        const isLocal = isLocalRequestHost(url.hostname)
        const isBearer = req.headers.get('authorization') === `Bearer ${AUTH_TOKEN}`
        const senderId = req.headers.get('x-sender-id') || ''
        const isSelf = isBearer && senderId === agent
        if (!isLocal && !isSelf) {
          return Response.json({ ok: false, error: 'unauthorized; agent must POST own statusline locally or via Bearer + x-sender-id' }, { status: 403 })
        }
        const data = await req.json().catch(() => null)
        if (!data || typeof data !== 'object') {
          return Response.json({ ok: false, error: 'body must be JSON object' }, { status: 400 })
        }
        AGENT_STATUSLINE_CACHE.set(agent, { data, ts: Date.now() })
        return Response.json({ ok: true })
      } catch (e: any) {
        return Response.json({ ok: false, error: e.message }, { status: 400 })
      }
    }
    if (req.method === 'GET' && url.pathname === '/api/agent-statusline' && isAuthed) {
      const agent = url.searchParams.get('agent') || ''
      if (!AGENT_STATUSLINE_IDS.has(agent)) {
        return Response.json({ ok: false, error: `unknown agent: ${agent}` }, { status: 400 })
      }
      const entry = AGENT_STATUSLINE_CACHE.get(agent)
      if (!entry) {
        return Response.json({ ok: true, data: null, ts: null, age_ms: null })
      }
      // AIC-119: description A2 字面要求返 { data, ts, age_ms }, 补 ts (epoch ms).
      return Response.json({ ok: true, data: entry.data, ts: entry.ts, age_ms: Date.now() - entry.ts })
    }

// Usage probes (Claude / Codex) are HOST-MACHINE introspection — fetchClaudeUsageCached
    // reads `Claude Code-credentials` from the macOS keychain to call Anthropic's OAuth usage
    // API; fetchCodexUsageCached reads `~/.codex/logs_2.sqlite`. If the host has those, the
    // returned numbers belong to whoever owns the machine, NOT to whoever opened the open-source
    // UI. Opt-in via env so a default open-source deployment never leaks host-side AI quotas.
    const usageProbeEnabled = process.env.AICOLLAB_USAGE_PROBE === '1'
    const usagePlaceholderResp = () => Response.json({
      available: false,
      reason: 'usage probe disabled — set AICOLLAB_USAGE_PROBE=1 to enable host-side Claude/Codex introspection',
    })

    // Claude usage monitor: GET /api/claude-usage — proxies Anthropic OAuth usage API
    // (token stays on the Mac; 60s cache so the app's 30s poll doesn't hammer Anthropic)
    if (req.method === 'GET' && url.pathname === '/api/claude-usage' && isAuthed) {
      if (!usageProbeEnabled) return usagePlaceholderResp()
      const data = await fetchClaudeUsageCached()
      if ('error' in data) return Response.json(data, { status: data.error === 'no oauth token' ? 500 : 502 })
      return Response.json(data)
    }

    // AIC-53: GET /usage/:agentId — per-agent workstation USAGE panel data source.
    // agent1 maps to the local Claude.ai account (shared keychain seat). agent2
    // maps to the local Codex CLI logs DB. Add cases per your own deployment.
    // Frontend derives colors / warning text from raw fields client-side (mirrors design).
    if (req.method === 'GET' && /^\/usage\/[^/]+$/.test(url.pathname) && isAuthed) {
      if (!usageProbeEnabled) return usagePlaceholderResp()
      const agentId = decodeURIComponent(url.pathname.slice('/usage/'.length))
      if (agentId === 'agent1') {
        const usage = await fetchClaudeUsageCached()
        if ('error' in usage) return Response.json({ available: false, reason: usage.error }, { status: 200 })
        const planLabel = usage.subscriptionType ? `Claude ${String(usage.subscriptionType).replace(/^./, c => c.toUpperCase())} ${(usage.rateLimitTier || '').replace(/^default_claude_max_/, '').replace(/^claude_/, '') || ''}`.trim() : 'Claude'
        return Response.json({
          available: true,
          plan_label: planLabel,
          plan_seat: 'shared seat',
          session_pct: Math.round(usage.five_hour?.utilization ?? 0),
          session_resets_at: usage.five_hour?.resets_at || null,
          weekly_pct: Math.round(usage.seven_day?.utilization ?? 0),
          weekly_resets_at: usage.seven_day?.resets_at || null,
        })
      }
      if (agentId === 'agent2') {
        // AIC-54: real Codex usage data from ~/.codex/logs_2.sqlite.
        const c = fetchCodexUsageCached()
        if ('error' in c) return Response.json({ available: false, reason: c.error })
        const sessionPct = c.auto_compact_limit > 0
          ? Math.round((c.total_usage_tokens / c.auto_compact_limit) * 100)
          : 0
        const reasoning = c.reasoning_effort ? ` ${c.reasoning_effort}` : ''
        return Response.json({
          available: true,
          plan_label: `Codex · ${c.model}${reasoning}`,
          plan_seat: 'local thread',
          session_pct: sessionPct,
          session_label_override: 'context 用量',
          session_resets_at: null,  // Codex auto-compacts on limit; no calendar reset
          weekly_no_limit: true,
          weekly_total_tokens: c.weekly_total_tokens,
          weekly_label_override: c.weekly_is_estimate ? '本周累计（估算）' : '本周累计',
        })
      }
      return Response.json({ available: false, reason: 'unknown agent' }, { status: 404 })
    }

    // AIC-115/116: GET /api/transcript?agent=&limit=N&after=<uuid>
    // Returns parsed jsonl events for the given agent's running provider session.
    // - agent1 / agent2 (claude provider) → provider.sessionId → ~/.claude/projects/<cwd>/<sid>.jsonl
    // - Other agent ids OR codex provider → 410 (no claude transcript source)
    // Noise filter: 6 internal Anthropic CLI event types (last-prompt / mode / permission-mode /
    // file-history-snapshot / ai-title / queue-operation) — operator UI noise, never useful for
    // PM-facing debug. `after=<uuid>` returns events after the cursor (incremental poll). When
    // file > 5MB, tail last 5MB and drop partial first line.
    if (req.method === 'GET' && url.pathname === '/api/transcript' && isAuthed) {
      try {
        const agent = (url.searchParams.get('agent') || 'agent1').trim()
        if (agent !== 'agent1' && agent !== 'agent2') {
          return Response.json({ ok: false, error: `transcript not supported for agent: ${agent}`, agent }, { status: 410 })
        }
        const resolved = resolveTranscriptPath(agent)
        if (!resolved) {
          return Response.json({ ok: false, error: `unable to resolve transcript for agent: ${agent} (session not running, sid missing, or not a claude binary?)`, agent })
        }
        const { path: tpath, sid } = resolved
        const limit = Math.max(1, Math.min(parseInt(url.searchParams.get('limit') || '50'), 500))
        const after = (url.searchParams.get('after') || '').trim()
        const stat = statSync(tpath)
        const SIZE_CAP = 5_000_000
        let raw: string
        if (stat.size > SIZE_CAP) {
          const fd = openSync(tpath, 'r')
          const startByte = stat.size - SIZE_CAP
          const buf = Buffer.alloc(SIZE_CAP)
          try { readSync(fd, buf, 0, SIZE_CAP, startByte) } finally { closeSync(fd) }
          raw = buf.toString('utf-8')
          const nl = raw.indexOf('\n')
          if (nl !== -1) raw = raw.slice(nl + 1)
        } else {
          raw = readFileSync(tpath, 'utf-8')
        }
        const allLines = raw.split('\n').filter(l => l.trim())
        const events: any[] = []
        const SKIP_TYPES = new Set(['last-prompt', 'mode', 'permission-mode', 'file-history-snapshot', 'ai-title', 'queue-operation'])
        for (const line of allLines) {
          try {
            const ev = JSON.parse(line)
            if (SKIP_TYPES.has(ev.type)) continue
            events.push(ev)
          } catch (e) {
            events.push({ type: 'parse_error', raw: line.slice(0, 200), error: String(e) })
          }
        }
        let result = events
        let usedAfter = false
        if (after) {
          const idx = events.findIndex(e => e.uuid === after)
          if (idx !== -1) {
            // Incremental cursor: caller wants idx+1 onwards. Don't slice(-limit) here —
            // that would silently drop (N-limit) old events that the caller already saw,
            // breaking next-poll cursor continuity.
            result = events.slice(idx + 1)
            usedAfter = true
          }
        }
        if (!usedAfter) {
          result = result.slice(-limit)
        }
        const lastEvent = result[result.length - 1]
        // AIC-129: transcript_path used to be returned verbatim (absolute path under
        // $HOME) — leaks operator username + dir layout to any client. Web UI never
        // displayed it; just dropped. Recompute server-side if you need it.
        return Response.json({
          ok: true,
          events: result,
          total_events: events.length,
          total_lines: allLines.length,
          sid,
          agent,
          last_event_uuid: lastEvent?.uuid || null,
          timestamp: new Date().toISOString(),
        })
      } catch (e) {
        return Response.json({ ok: false, error: String(e) })
      }
    }

    // AIC-115 (2026-06-25): GET /api/agent-billing/:agentId
    // Returns the most recent turn's usage + Opus-priced cost for the chip rendered next to
    // each assistant event in the transcript view. agent1/agent2: reverse-scan jsonl for the
    // last `assistant` event's message.usage. Tail 1MB cap (poll cadence ≠ full-file IO).
    // Other agent ids → 410.
    if (req.method === 'GET' && /^\/api\/agent-billing\/[^/]+$/.test(url.pathname) && isAuthed) {
      const agentId = decodeURIComponent(url.pathname.slice('/api/agent-billing/'.length))
      if (agentId !== 'agent1' && agentId !== 'agent2') {
        return Response.json({ ok: false, error: `agent-billing not supported for agent: ${agentId}`, agentId }, { status: 410 })
      }
      const usage = readAgentLastTurnUsage(agentId)
      if (!usage) {
        return Response.json({ ok: true, agentId, available: false, reason: 'no usage data (session not running / no assistant event yet)', turn_count: null })
      }
      return Response.json({
        ok: true,
        agentId,
        available: true,
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_input_tokens: usage.cache_read_input_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        cost_usd: usage.cost_usd,
        // turn_count: null for ai-collab — only main repo tracks brain_loop turn counter for ian/chat.
        // null surfaces the field so consumers can rely on a stable response shape (spec § Acceptance).
        turn_count: null,
      })
    }

    // AIC-116: /api/terminal visual screen capture endpoint retired. Use /api/transcript for structured
    // jsonl events (claude agents only). No equivalent for sending keystrokes to a long-lived
    // stream subprocess — write a normal user message via /group/send instead.
    if (req.method === 'GET' && url.pathname === '/api/terminal' && isAuthed) {
      return Response.json({
        ok: false,
        error: 'Legacy visual screen capture endpoint retired in AIC-116. Use /api/transcript?agent= for structured events.',
      }, { status: 410 })
    }
    if (req.method === 'POST' && url.pathname === '/api/terminal/send' && isAuthed) {
      return Response.json({
        ok: false,
        error: 'Legacy key injection endpoint retired in AIC-116. Send via /group/send (provider.send streams to the agent).',
      }, { status: 410 })
    }



    return new Response('not found', { status: 404 })
  },
})
process.stderr.write(`ai-collab: HTTP server on http://127.0.0.1:${IMG_PORT}\n`)
// AIC-129: warn when tmux provider is enabled — capture-pane reads raw terminal bytes,
// strict sanitizer drops obvious secrets but cannot catch novel formats.
for (const [agentId, cfg] of Object.entries(AGENT_RUNTIMES)) {
  if (cfg.provider === 'tmux') {
    process.stderr.write(`ai-collab: [tmux-provider] ${agentId} attached to tmux session '${cfg.tmuxSession}' (filter=${cfg.tmuxFilterMode}, quiet=${cfg.tmuxQuietMs}ms) — capture-pane will read terminal output; ensure the session contains no secrets\n`)
  }
}
