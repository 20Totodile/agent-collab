import path from 'path'
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs'
import { randomBytes } from 'crypto'

export const IMG_PORT = Number(process.env.AICOLLAB_PORT || 3009)
export const SOURCE = 'ai-collab'
// AUTH_TOKEN resolution order (open-source onboarding without manual setup):
//   1. AICOLLAB_AUTH_TOKEN env (≥ 16 chars) — explicit, recommended for LAN / tunnel deployments
//   2. runtime-data/state/token.txt persistent file — auto-generated on first boot, reused after
//   3. Generate a new random token, persist to (2)
// Cookie is auto-set by the /web static handler so end users never type or copy the token.
export const AUTH_TOKEN = (() => {
  // config.ts lives at <plugin>/src/config.ts → parent of import.meta.dir is the plugin root.
  const pluginRoot = path.dirname(import.meta.dir)
  const tokenFile = path.join(pluginRoot, 'runtime-data', 'state', 'token.txt')
  const fromEnv = process.env.AICOLLAB_AUTH_TOKEN

  // Helper: ensure tokenFile contains `t` (idempotent so env-mode and file-mode end up
  // with the same disk state; without this, toggling env between runs would silently
  // change the live token and invalidate every cookie issued during the previous run).
  const persist = (t: string) => {
    try {
      mkdirSync(path.dirname(tokenFile), { recursive: true })
      let existing = ''
      if (existsSync(tokenFile)) existing = readFileSync(tokenFile, 'utf-8').trim()
      if (existing !== t) writeFileSync(tokenFile, t, { mode: 0o600 })
    } catch (e) {
      process.stderr.write(`ai-collab: WARN failed to persist auth token to ${tokenFile}: ${e}\n`)
    }
  }

  if (fromEnv && fromEnv.length >= 16) {
    persist(fromEnv)
    return fromEnv
  }
  if (existsSync(tokenFile)) {
    const t = readFileSync(tokenFile, 'utf-8').trim()
    if (t.length >= 16) return t
    // File exists but is corrupt/short — surface this loudly before overwriting, so the
    // operator notices that any previously-issued cookies are about to stop working.
    process.stderr.write(`ai-collab: WARN ${tokenFile} contained <16 chars, regenerating (previous cookies invalidated)\n`)
  }
  const t = randomBytes(24).toString('hex')
  persist(t)
  process.stderr.write(`ai-collab: auto-generated auth token written to ${tokenFile}\n`)
  return t
})()
// Per-agent heartbeat-state file paths (json with context_tokens). Empty = no context card.
export const AGENT1_HEARTBEAT_PATH = process.env.AGENT1_HEARTBEAT_PATH || ''
// AIC-115/116: per-agent Claude Code project dir (override). When set, resolveTranscriptPath
// uses `<dir>/<sid>.jsonl` directly instead of deriving from the agent's working cwd.
// Useful when the agent runtime is configured to write transcripts somewhere non-standard.
export const AGENT1_PROJECT_DIR = process.env.AGENT1_PROJECT_DIR || ''
export const AGENT2_PROJECT_DIR = process.env.AGENT2_PROJECT_DIR || ''

// AIC-116/129: per-agent provider type.
//   'claude' — Anthropic Claude Code CLI, stream-json NDJSON
//   'codex'  — Codex CLI, `app-server --listen stdio://` JSON-RPC 2.0
//   'tmux'   — tmux capture-pane / send-keys (opt-in; capture output is raw terminal bytes,
//              capture-pane sanitizer strips obvious secrets — see src/providers/tmux.ts)
// New providers added in future just extend the union — see src/providers/*.ts.
export type AgentProviderKind = 'claude' | 'codex' | 'tmux'
function parseProvider(raw: string | undefined, fallback: AgentProviderKind): AgentProviderKind {
  const v = (raw || '').toLowerCase()
  if (v === 'claude' || v === 'codex' || v === 'tmux') return v
  return fallback
}
export const AGENT1_PROVIDER: AgentProviderKind = parseProvider(process.env.AGENT1_PROVIDER, 'claude')
export const AGENT2_PROVIDER: AgentProviderKind = parseProvider(process.env.AGENT2_PROVIDER, 'codex')

// AIC-116: per-agent binary path override + working directory. Falls back to PATH lookup
// of the provider's expected binary (`claude` / `codex`) when empty.
export const AGENT1_BINARY_PATH = process.env.AGENT1_BINARY_PATH || ''
export const AGENT2_BINARY_PATH = process.env.AGENT2_BINARY_PATH || ''
// Agent working directory — passed to subprocess spawn cwd. The agent's `.claude/settings.json`
// / `AGENTS.md` / hooks / MCP config are loaded based on this. CRITICAL to set correctly,
// else effort/tools/hooks are silently dropped (main repo AIC-104 lesson).
export const AGENT1_CWD = process.env.AGENT1_CWD || ''
export const AGENT2_CWD = process.env.AGENT2_CWD || ''

// AIC-116: per-agent extra args appended to the spawn command line (provider-specific).
// Example: AGENT1_EXTRA_ARGS="--model claude-opus-4-7[1m] --add-dir /path/to/repo"
function parseExtraArgs(raw: string | undefined): string[] {
  if (!raw) return []
  // Split on whitespace respecting double-quoted segments. Simple parser sufficient for
  // CLI args (no escape sequences supported).
  const out: string[] = []
  const re = /"([^"]*)"|(\S+)/g
  let m: RegExpExecArray | null
  while ((m = re.exec(raw)) !== null) out.push(m[1] ?? m[2])
  return out
}
export const AGENT1_EXTRA_ARGS = parseExtraArgs(process.env.AGENT1_EXTRA_ARGS)
export const AGENT2_EXTRA_ARGS = parseExtraArgs(process.env.AGENT2_EXTRA_ARGS)
export const ALLOW_QUERY_TOKEN_AUTH = process.env.AICOLLAB_ALLOW_QUERY_TOKEN_AUTH === '1'
export const ALLOW_REMOTE_CONTROL = process.env.AICOLLAB_ALLOW_REMOTE_CONTROL !== '0'
// AIC-129: tmux provider configuration (only used when AGENT*_PROVIDER=tmux).
//   AGENT*_TMUX_SESSION         — session name to attach to (default `agent-<8-char-hex>` per boot)
//   AGENT*_TMUX_SOCKET          — optional `-L` socket name; empty = tmux default socket
//   AGENT*_TMUX_CAPTURE_INTERVAL_MS — polling cadence for capture-pane diff (default 5000)
//   AGENT*_TMUX_FILTER_MODE     — strict (default) | loose | off; see tmux-sanitizer.ts
function defaultTmuxSession(): string {
  return `agent-${randomBytes(4).toString('hex')}`
}
export const AGENT1_TMUX_SESSION = process.env.AGENT1_TMUX_SESSION || defaultTmuxSession()
export const AGENT2_TMUX_SESSION = process.env.AGENT2_TMUX_SESSION || defaultTmuxSession()
export const AGENT1_TMUX_SOCKET = process.env.AGENT1_TMUX_SOCKET || ''
export const AGENT2_TMUX_SOCKET = process.env.AGENT2_TMUX_SOCKET || ''
function parseIntervalMs(raw: string | undefined, fallback: number): number {
  if (!raw) return fallback
  const n = Number(raw)
  if (!Number.isFinite(n) || n <= 0) return fallback
  return Math.max(500, Math.floor(n))
}
export const AGENT1_TMUX_CAPTURE_INTERVAL_MS = parseIntervalMs(process.env.AGENT1_TMUX_CAPTURE_INTERVAL_MS, 5000)
export const AGENT2_TMUX_CAPTURE_INTERVAL_MS = parseIntervalMs(process.env.AGENT2_TMUX_CAPTURE_INTERVAL_MS, 5000)
// AIC-129 cycle 2: tmux provider has no native turn-end signal, so we synthesize one
// after `quietMs` of no new capture activity (provider emits `result` event → server
// markAgentIdle). Default 30s matches main repo AGENT_WATCHERS.quietMs (AIC-49).
export const AGENT1_TMUX_QUIET_MS = parseIntervalMs(process.env.AGENT1_TMUX_QUIET_MS, 30_000)
export const AGENT2_TMUX_QUIET_MS = parseIntervalMs(process.env.AGENT2_TMUX_QUIET_MS, 30_000)
function parseFilterModeEnv(raw: string | undefined): 'strict' | 'loose' | 'off' {
  const v = (raw || '').toLowerCase()
  if (v === 'strict' || v === 'loose' || v === 'off') return v
  return 'strict'
}
export const AGENT1_TMUX_FILTER_MODE = parseFilterModeEnv(process.env.AGENT1_TMUX_FILTER_MODE)
export const AGENT2_TMUX_FILTER_MODE = parseFilterModeEnv(process.env.AGENT2_TMUX_FILTER_MODE)
// Optional agent startup script.
export const AGENT1_CLOCK_IN_SCRIPT = process.env.AGENT1_CLOCK_IN_SCRIPT || ''
export const SERVER_STARTED_AT = new Date().toISOString()

export function workspaceRoot(pluginDir: string) {
  return path.dirname(pluginDir)
}

export function agentRoot(pluginDir: string) {
  return path.dirname(path.dirname(pluginDir))
}

export function runtimeDataRoot(pluginDir: string) {
  return path.join(pluginDir, 'runtime-data')
}

export function runtimeStateDir(pluginDir: string) {
  return path.join(runtimeDataRoot(pluginDir), 'state')
}

export function runtimeUploadsDir(pluginDir: string) {
  return path.join(runtimeDataRoot(pluginDir), 'uploads')
}

export function generatedArtifactsDir(pluginDir: string) {
  return path.join(runtimeDataRoot(pluginDir), 'generated')
}

export function legacyImageDir(pluginDir: string) {
  return path.join(pluginDir, 'images')
}

export function chatDbPath(pluginDir: string) {
  return path.join(runtimeStateDir(pluginDir), 'chat.db')
}

export function legacyChatDbPath(pluginDir: string) {
  return path.join(pluginDir, 'chat.db')
}

export function heartbeatStatePath(pluginDir: string) {
  return path.join(agentRoot(pluginDir), 'heartbeat_state.json')
}
