// AIC-124: ClaudeProvider — thin AgentProvider over a long-lived chat_supervisor daemon.
//
// History: pre-AIC-124 this class spawned the `claude` subprocess directly inside the
// server.ts process and pumped its stdout/stderr. That meant any server.ts restart killed
// the conversation context. AIC-124 moves the subprocess into a separate `chat_supervisor`
// daemon (per-provider, isolated runtimeDir) so server.ts can be restarted without losing
// the claude session, and so that crashes in one provider can't take down the other.
//
// Wire shape (see ./chat/frame.ts WireMessage):
//   - server.ts (this client) opens a unix socket to the supervisor under `supervisorDir`
//   - supervisor forwards child stdout to us as raw `child_out` byte chunks (NOT pre-parsed)
//   - we line-buffer those chunks here and parse Anthropic stream-json into AgentEvent
//
// Per-provider isolation (Q4 06-26): each ClaudeProvider instance gets its own supervisorDir
// (e.g. runtime-data/chat-supervisor/agent1/ vs .../agent2/) and therefore its own
// supervisor.sock / supervisor.pid / child.log / supervisor child process. Agent1 crashing
// can't blow up agent2's supervisor.
//
// State file: `cfg.stateFilePath` is BOTH where this class persists the captured
// session_id (so we can answer `provider.sessionId` synchronously for transcript routing)
// AND what we pass to the supervisor via `AICOLLAB_CHAT_STATE_FILE` so the supervisor
// itself reads it on (re)spawn for `--resume <sid>`. Keeping both readers pointed at one
// file means a server.ts restart doesn't desync the two views.

import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs'
import { dirname } from 'path'
import { SupervisorClient } from './chat/supervisor_client.ts'
import type {
  AgentProvider, AgentEvent, AgentSendOpts, AgentCapabilities,
  AgentError, AgentProviderConfig, AgentUsage,
} from './provider.ts'

const CAPABILITIES: AgentCapabilities = {
  providerName: 'claude',
  supportsSessionResume: true,
  supportsAttachments: false, // future: forward as supervisor StdinPayload.imagePaths
  supportsToolUse: true,
  supportsThinking: true,
  supportsPartialEvents: true,
}

type PersistedState = { session_id?: string | null; updated_at?: string }

export interface ClaudeProviderConfig extends AgentProviderConfig {
  /**
   * Absolute path to this provider's chat_supervisor runtime dir. Sock/pid/log/child.log
   * live under it. Each provider instance MUST use a disjoint directory or two providers
   * will fight over one supervisor — see AIC-124 Q4.
   */
  supervisorDir: string
  /**
   * Absolute path to the `chat_supervisor.ts` entrypoint that the client should spawn
   * when supervisor.sock is missing. Required (no PATH lookup; we want explicit failure
   * if the operator forgot to set it).
   */
  supervisorEntrypoint: string
}

export class ClaudeProvider implements AgentProvider {
  private cfg: ClaudeProviderConfig
  private client: SupervisorClient
  private _sessionId: string | null = null
  private _connected = false
  private _connecting: Promise<void> | null = null
  private _alive = true
  private eventCb: ((ev: AgentEvent) => void | Promise<void>) | null = null
  private errorCb: ((err: AgentError) => void) | null = null
  private label: string
  /** Per-connection NDJSON line buffer (Anthropic stream-json from child_out). */
  private stdoutBuf = ''

  constructor(cfg: ClaudeProviderConfig) {
    if (!cfg.supervisorDir) {
      throw new Error('ClaudeProvider: supervisorDir required (AIC-124 per-provider isolation)')
    }
    if (!cfg.supervisorEntrypoint) {
      throw new Error('ClaudeProvider: supervisorEntrypoint required (path to chat_supervisor.ts)')
    }
    this.cfg = cfg
    this.label = cfg.label || 'claude'
    if (cfg.onEvent) this.eventCb = cfg.onEvent
    if (cfg.onError) this.errorCb = cfg.onError
    this._sessionId = this._loadPersistedSessionId()

    // Compose env for the supervisor child. Forwarded via supervisorLauncher argv ->
    // spawn(env: process.env), so we set the AICOLLAB_CHAT_* keys in this process's
    // env before constructing the launcher list. This is safe because each provider
    // instance has a disjoint AICOLLAB_SUPERVISOR_DIR derived path; the env writes
    // happen inside _supervisorLauncher() at connect() time, one supervisor at a time.
    this.client = new SupervisorClient({
      supervisorDir: cfg.supervisorDir,
      autoSpawn: true,
      supervisorLauncher: this._supervisorLauncher(),
      clientLabel: `ai-collab:${this.label}`,
    })

    this.client.onChildOut((data: string) => { void this._handleChildOut(data) })
    this.client.onChildCrashed((info) => {
      // Treat child crash as process_exited so the server.ts dispatch layer drops us +
      // re-instantiates the provider (server.ts handleProviderError already wires that).
      this.errorCb?.({
        kind: 'process_exited',
        message: `${this.label}: claude child crashed (exit=${info.exit_code} signal=${info.signal})`,
      })
      this._alive = false
    })
    this.client.onError((err: Error) => {
      this.errorCb?.({ kind: 'protocol_error', message: `${this.label}: supervisor client: ${err.message}` })
    })
  }

  get sessionId(): string | null { return this._sessionId }

  get isAlive(): boolean { return this._alive && this._connected }

  capabilities(): AgentCapabilities { return CAPABILITIES }
  onEvent(cb: (ev: AgentEvent) => void | Promise<void>): void { this.eventCb = cb }
  onError(cb: (err: AgentError) => void): void { this.errorCb = cb }

  /**
   * Send a turn. Lazily connects (and auto-spawns the supervisor) on first call.
   * Resolves once the wire frame has been written to the supervisor socket;
   * downstream events stream in via onEvent as child_out arrives.
   *
   * Image attachments: supervisor takes raw paths and base64-encodes them into Anthropic
   * content blocks. capabilities().supportsAttachments stays false until server.ts wires
   * them through; opts.attachments is currently ignored here.
   */
  async send(text: string, _opts?: AgentSendOpts): Promise<void> {
    await this._ensureConnected()
    await this.client.send({ prompt: text })
  }

  /** Hard-interrupt the current turn. The supervisor owns the child PID; we kill it via
   *  process group from the client side. Session id is kept so the next send resumes. */
  async interrupt(): Promise<boolean> {
    // We don't have a wire frame for interrupt in the AIC-124 contract. The closest
    // equivalent: disconnect the socket so the supervisor's writer-lease becomes
    // vacant, then reconnect — but that does NOT actually stop the running turn.
    // For now, surface this as a no-op + warn through errorCb so operators see it.
    // A proper fix needs a new `interrupt` WireMessage; out of scope for AIC-124 fork sync.
    this.errorCb?.({
      kind: 'protocol_error',
      message: `${this.label}: interrupt() not yet implemented over chat_supervisor wire (AIC-124 TODO)`,
    })
    return false
  }

  /** Graceful close: disconnect the socket. Supervisor stays alive across server.ts
   *  restart — that's the whole point of moving to a daemon. Calling close() does NOT
   *  terminate the supervisor or the claude child. */
  async close(): Promise<void> {
    if (!this._connected && !this._connecting) return
    try { await this.client.disconnect() } catch {}
    this._connected = false
    this._alive = false
  }

  // ───────────── private ─────────────

  /** Build the argv that SupervisorClient uses when supervisor.sock is missing. */
  private _supervisorLauncher(): string[] {
    // We rely on `bun` being on PATH (same constraint as the ai-collab `start` script).
    return ['bun', 'run', this.cfg.supervisorEntrypoint]
  }

  /** Set the AICOLLAB_CHAT_* env keys for the supervisor's child spawn. Mutates
   *  process.env (necessary because SupervisorClient.spawnSupervisor uses
   *  Bun.spawn without an explicit env, inheriting the parent's). */
  private _stampSupervisorEnv(): void {
    process.env.AICOLLAB_SUPERVISOR_DIR = this.cfg.supervisorDir
    process.env.AICOLLAB_CHAT_CHILD_CWD = this.cfg.cwd || process.cwd()
    process.env.AICOLLAB_CHAT_CHILD_BIN = this.cfg.binaryPath || 'claude'
    process.env.AICOLLAB_CHAT_PROVIDER = this.label
    if (this.cfg.stateFilePath) {
      process.env.AICOLLAB_CHAT_STATE_FILE = this.cfg.stateFilePath
    }
    if (this.cfg.extraArgs && this.cfg.extraArgs.length > 0) {
      process.env.AICOLLAB_CHAT_CHILD_EXTRA_ARGS = JSON.stringify(this.cfg.extraArgs)
    } else {
      delete process.env.AICOLLAB_CHAT_CHILD_EXTRA_ARGS
    }
  }

  private async _ensureConnected(): Promise<void> {
    if (this._connected) return
    if (this._connecting) return this._connecting
    this._connecting = (async () => {
      this._stampSupervisorEnv()
      try {
        await this.client.connect()
        this._connected = true
        this._alive = true
        this.stdoutBuf = ''
      } catch (e: any) {
        this.errorCb?.({ kind: 'spawn_failed', message: String(e?.message || e), raw: e })
        throw e
      }
    })()
    try { await this._connecting } finally { this._connecting = null }
  }

  /** Called for every child_out chunk from the supervisor. Same NDJSON line framing
   *  and normalize logic as the pre-AIC-124 in-process spawn path. */
  private async _handleChildOut(chunk: string): Promise<void> {
    this.stdoutBuf += chunk
    let nl = this.stdoutBuf.indexOf('\n')
    while (nl !== -1) {
      const line = this.stdoutBuf.slice(0, nl).trim()
      this.stdoutBuf = this.stdoutBuf.slice(nl + 1)
      if (line) await this._handleLine(line)
      nl = this.stdoutBuf.indexOf('\n')
    }
  }

  private async _handleLine(line: string): Promise<void> {
    let raw: any
    try {
      raw = JSON.parse(line)
    } catch (e) {
      this.errorCb?.({ kind: 'parse_error', message: String(e), raw: line.slice(0, 200) })
      return
    }
    // Anthropic error responses (subtype='error_during_execution', is_error=true) echo
    // the INVALID sid we asked for. Capturing it here would re-poison _sessionId and
    // re-write state.json with the expired sid, defeating the fresh-spawn we want.
    // (Main repo AIC-104 sid-poison fix, mirrored verbatim.)
    const isErrorResult = raw.type === 'result' && (raw.is_error === true || raw.subtype === 'error_during_execution')
    if (!isErrorResult && raw.session_id && raw.session_id !== this._sessionId) {
      this._sessionId = raw.session_id
    }
    if (isErrorResult) {
      this._sessionId = null
      this._persistSessionId()
      this.errorCb?.({ kind: 'session_expired', message: 'error_during_execution', raw })
    } else if (raw.type === 'result') {
      this._persistSessionId()
    }

    const ev = this._normalize(raw)
    try { await this.eventCb?.(ev) }
    catch (e) { console.error(this.label, 'onEvent threw', e) }
  }

  private _normalize(raw: any): AgentEvent {
    const t = raw.type as string
    if (t === 'system' && raw.subtype === 'init') {
      return { type: 'system_init', session_id: raw.session_id, raw }
    }
    if (t === 'assistant') {
      const content = raw.message?.content || []
      let text = ''
      let thinking = ''
      const tool_uses: Array<{ name: string; input: any }> = []
      for (const b of content) {
        if (b?.type === 'text') text += (b.text || '')
        else if (b?.type === 'thinking') thinking += (b.thinking || '')
        else if (b?.type === 'tool_use') tool_uses.push({ name: b.name, input: b.input })
      }
      return { type: 'assistant', text, thinking: thinking || undefined, tool_uses, raw }
    }
    if (t === 'result') {
      const u = raw.usage || raw.message?.usage
      const usage: AgentUsage | undefined = u ? {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        cache_read_input_tokens: u.cache_read_input_tokens,
        cache_creation_input_tokens: u.cache_creation_input_tokens,
      } : undefined
      return { type: 'result', usage, raw }
    }
    return { type: 'other', raw }
  }

  private _loadPersistedSessionId(): string | null {
    if (!this.cfg.stateFilePath) return null
    try {
      if (!existsSync(this.cfg.stateFilePath)) return null
      const raw = readFileSync(this.cfg.stateFilePath, 'utf-8')
      const obj = JSON.parse(raw) as PersistedState
      return obj.session_id || null
    } catch { return null }
  }

  private _persistSessionId(): void {
    if (!this.cfg.stateFilePath) return
    try {
      mkdirSync(dirname(this.cfg.stateFilePath), { recursive: true })
      const obj: PersistedState = { session_id: this._sessionId, updated_at: new Date().toISOString() }
      writeFileSync(this.cfg.stateFilePath, JSON.stringify(obj, null, 2), { mode: 0o600 })
    } catch (e) {
      console.error(this.label, 'persist sid failed', e)
    }
  }
}
