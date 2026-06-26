// AIC-129: TmuxProvider — AgentProvider over a tmux session (capture-pane / send-keys).
//
// Use case: operators who already drive a CLI agent inside their own tmux session and
// want ai-collab to talk to it as-is, without rehosting the agent under stream-json or
// JSON-RPC. Trade-off: capture-pane reads raw terminal bytes, so any secret the agent
// dumps (env exports, API key paste-overs, ssh-add output) flows through into the
// AgentEvent stream unless the strict sanitizer drops it. See tmux-sanitizer.ts.
//
// Lifecycle: lazy attach on first send(). We never create the session — operator owns
// the tmux process. close() stops polling but leaves the session alive (so the operator
// can attach to inspect history). isAlive flips false on the first capture-pane error.
//
// Capture diff strategy: tmux has no "what's new since last frame" primitive, so we
// snapshot `capture-pane -p -S -<CAPTURE_LINES>` and reconcile against the previous
// snapshot by anchoring on the LAST non-empty line of the prior snapshot inside the
// new one. New lines are everything after the anchor. When scrollback rotates past the
// anchor (slow polling + chatty session) we emit the whole frame; better duplicate than
// silently swallow output.
//
// Not implemented: file/image attachments (capabilities reports false), tool_use
// extraction (raw text only), thinking field.

import { spawnSync } from 'child_process'
import { randomBytes } from 'crypto'
import type {
  AgentProvider, AgentEvent, AgentSendOpts, AgentCapabilities,
  AgentError, AgentProviderConfig,
} from './provider.ts'
import { sanitizeFrame, parseFilterMode, type FilterMode } from './tmux-sanitizer.ts'

const CAPABILITIES: AgentCapabilities = {
  providerName: 'tmux',
  supportsSessionResume: false, // tmux session id is opaque, not a resumable handle
  supportsAttachments: false,
  supportsToolUse: false,
  supportsThinking: false,
  supportsPartialEvents: false, // we emit per-poll batches, not per-token
}

const DEFAULT_CAPTURE_INTERVAL_MS = 5_000
const DEFAULT_QUIET_MS = 30_000 // matches main repo AGENT_WATCHERS quietMs (AIC-49)
const CAPTURE_LINES = 500 // tmux -S -<n>; covers ~5-10s of busy output between polls
const SEND_KEYS_TIMEOUT_MS = 5_000
const HAS_SESSION_TIMEOUT_MS = 3_000

export interface TmuxProviderConfig extends AgentProviderConfig {
  /** tmux session name (required). Operator must have created the session beforehand. */
  session: string
  /** Optional tmux -L socket name. Empty = default socket. */
  socket?: string
  /** capture-pane polling interval in ms. Default 5000. */
  captureIntervalMs?: number
  /** Sanitizer mode. Default 'strict'. */
  filterMode?: FilterMode
  /** ms of capture quiet after the last assistant emission before we emit a synthetic
   *  `result` event (server interprets it as turn-end → markAgentIdle). Defaults to
   *  30000 — matches main repo AGENT_WATCHERS.quietMs (AIC-49). The result event is
   *  the only idle signal a tmux provider has; without it the agent stays "working"
   *  forever in the control panel. */
  quietMs?: number
}

export class TmuxProvider implements AgentProvider {
  private cfg: TmuxProviderConfig
  private label: string
  private session: string
  private socket: string | null
  private captureIntervalMs: number
  private filterMode: FilterMode
  private quietMs: number
  private _sessionId: string
  private _alive: boolean = false
  private _attached: boolean = false
  private _pollHandle: ReturnType<typeof setInterval> | null = null
  private _quietHandle: ReturnType<typeof setTimeout> | null = null
  private _turnActive: boolean = false
  private _lastCaptureRaw: string = ''
  private _baselineSet: boolean = false
  private eventCb: ((ev: AgentEvent) => void | Promise<void>) | null = null
  private errorCb: ((err: AgentError) => void) | null = null

  constructor(cfg: TmuxProviderConfig) {
    if (!cfg.session) {
      throw new Error('TmuxProvider: session required')
    }
    this.cfg = cfg
    this.label = cfg.label || 'tmux'
    this.session = cfg.session
    this.socket = cfg.socket && cfg.socket.length > 0 ? cfg.socket : null
    this.captureIntervalMs = Math.max(500, cfg.captureIntervalMs || DEFAULT_CAPTURE_INTERVAL_MS)
    this.filterMode = cfg.filterMode || 'strict'
    // Floor matches config.ts parseIntervalMs (500ms). config-level validation already
    // clamps env input; this is just belt-and-suspenders for direct constructor callers.
    this.quietMs = Math.max(500, cfg.quietMs || DEFAULT_QUIET_MS)
    // sessionId is opaque to the wire; we shape it like other providers so transcript
    // routing code that branches on prefix can recognize the provider class quickly.
    this._sessionId = `tmux:${this.session}:${Math.floor(Date.now() / 1000).toString(36)}`
    if (cfg.onEvent) this.eventCb = cfg.onEvent
    if (cfg.onError) this.errorCb = cfg.onError
  }

  get sessionId(): string | null { return this._sessionId }
  get isAlive(): boolean { return this._alive }
  capabilities(): AgentCapabilities { return CAPABILITIES }
  onEvent(cb: (ev: AgentEvent) => void | Promise<void>): void { this.eventCb = cb }
  onError(cb: (err: AgentError) => void): void { this.errorCb = cb }

  async send(text: string, _opts?: AgentSendOpts): Promise<void> {
    if (!this._attached) await this._attach() // throws on missing session — bubbles to caller

    // send-keys -l sends literal (no key binding interpretation); separate Enter so that
    // operators who bind Enter to something custom still get a newline.
    const t1 = this._tmux(['send-keys', '-t', this.session, '-l', text])
    if (t1.code !== 0) {
      const msg = `${this.label}: tmux send-keys (text) failed: ${t1.stderr.trim() || 'exit ' + t1.code}`
      this.errorCb?.({ kind: 'protocol_error', message: msg })
      throw new Error(msg)
    }
    const t2 = this._tmux(['send-keys', '-t', this.session, 'Enter'])
    if (t2.code !== 0) {
      const msg = `${this.label}: tmux send-keys (Enter) failed: ${t2.stderr.trim() || 'exit ' + t2.code}`
      this.errorCb?.({ kind: 'protocol_error', message: msg })
      throw new Error(msg)
    }
    // Turn started — arm the quiet timer so we can emit a `result` event after the
    // session goes silent for quietMs (server reads result as turn-end → markAgentIdle).
    this._turnActive = true
    this._resetQuietTimer()
  }

  async interrupt(): Promise<boolean> {
    if (!this._attached) return false
    const r = this._tmux(['send-keys', '-t', this.session, 'C-c'])
    if (r.code !== 0) {
      this.errorCb?.({
        kind: 'protocol_error',
        message: `${this.label}: tmux send-keys C-c failed: ${r.stderr.trim() || 'exit ' + r.code}`,
      })
      return false
    }
    return true
  }

  async close(): Promise<void> {
    if (this._pollHandle) {
      clearInterval(this._pollHandle)
      this._pollHandle = null
    }
    if (this._quietHandle) {
      clearTimeout(this._quietHandle)
      this._quietHandle = null
    }
    this._turnActive = false
    this._attached = false
    this._alive = false
    // Intentionally do NOT kill the tmux session — operator owns it.
  }

  // ───────────── private ─────────────

  /** First-time attach: verify session exists, take baseline capture, start polling. */
  private async _attach(): Promise<void> {
    if (this._attached) return
    if (!this._hasSession()) {
      const msg = `${this.label}: tmux session '${this.session}' not found (create it before clocking in this agent)`
      this.errorCb?.({ kind: 'spawn_failed', message: msg })
      throw new Error(msg)
    }
    this._attached = true
    this._alive = true
    // emit a system_init event so downstream UI (terminal panel etc) sees the provider
    // came online with a stable session_id — symmetric to claude/codex behavior.
    try {
      this.eventCb?.({
        type: 'system_init',
        session_id: this._sessionId,
        raw: { source: 'tmux', session: this.session, socket: this.socket || null },
      })
    } catch (e) { console.error(this.label, 'onEvent threw on system_init', e) }

    // Take baseline now so the first poll diff doesn't dump pre-existing pane content.
    const cap = this._tmux(['capture-pane', '-p', '-t', this.session, '-S', `-${CAPTURE_LINES}`])
    if (cap.code === 0) {
      this._lastCaptureRaw = cap.stdout
      this._baselineSet = true
    }

    this._pollHandle = setInterval(() => { this._pollOnce() }, this.captureIntervalMs)
    // Unref so we don't keep the event loop alive for closed providers; the server
    // shutdown path explicitly closes us when needed.
    if (typeof (this._pollHandle as any)?.unref === 'function') (this._pollHandle as any).unref()
  }

  private _pollOnce(): void {
    if (!this._attached) return
    const cap = this._tmux(['capture-pane', '-p', '-t', this.session, '-S', `-${CAPTURE_LINES}`])
    if (cap.code !== 0) {
      this._alive = false
      // Reset _attached so the operator's next `send()` re-runs the has-session check
      // and re-takes a baseline. Without this, a transient tmux death leaves the
      // provider in a half-attached state forever (send-keys still succeeds against
      // a freshly-recreated session, but capture-pane polling never restarts).
      this._attached = false
      this._baselineSet = false
      this._lastCaptureRaw = ''
      this._turnActive = false
      this.errorCb?.({
        kind: 'process_exited',
        message: `${this.label}: capture-pane failed (session lost?): ${cap.stderr.trim() || 'exit ' + cap.code}`,
      })
      if (this._pollHandle) { clearInterval(this._pollHandle); this._pollHandle = null }
      if (this._quietHandle) { clearTimeout(this._quietHandle); this._quietHandle = null }
      return
    }
    const curr = cap.stdout
    if (!this._baselineSet) {
      this._lastCaptureRaw = curr
      this._baselineSet = true
      return
    }
    const appended = this._diffAppendedLines(this._lastCaptureRaw, curr)
    this._lastCaptureRaw = curr
    const meaningful = appended.filter(l => l.length > 0)
    if (meaningful.length === 0) return
    const text = sanitizeFrame(meaningful.join('\n'), this.filterMode)
    if (!text.trim()) return
    try {
      const r = this.eventCb?.({
        type: 'assistant',
        text,
        raw: { source: 'tmux-capture', session: this.session, line_count: meaningful.length },
      })
      // eventCb signature allows Promise<void> — guard async rejection too so a downstream
      // handler throwing won't kill the poll interval silently.
      if (r && typeof (r as any).catch === 'function') (r as any).catch((e: any) => console.error(this.label, 'onEvent rejected on assistant', e))
    } catch (e) { console.error(this.label, 'onEvent threw on assistant', e) }
    // Push back the quiet deadline. Unconditional: any new capture output — whether it
    // followed our own send() or came from operator typing / spontaneous program output —
    // counts as turn activity. We must keep re-arming the timer so a future quiet window
    // emits another `result` event. Without this, the first quiet-fire would permanently
    // leave _turnActive=false and any subsequent unsolicited output would only ever flip
    // the agent BACK to working (server reads assistant as markWorking) with no way to
    // ever flip back to idle.
    this._turnActive = true
    this._resetQuietTimer()
  }

  /** Reschedule the quiet timer; fires `result` after `quietMs` of no new capture activity. */
  private _resetQuietTimer(): void {
    if (this._quietHandle) clearTimeout(this._quietHandle)
    this._quietHandle = setTimeout(() => {
      this._quietHandle = null
      if (!this._turnActive) return
      this._turnActive = false
      try {
        this.eventCb?.({
          type: 'result',
          raw: { source: 'tmux-quiet', quiet_ms: this.quietMs, session: this.session },
        })
      } catch (e) { console.error(this.label, 'onEvent threw on quiet-result', e) }
    }, this.quietMs)
    if (typeof (this._quietHandle as any)?.unref === 'function') (this._quietHandle as any).unref()
  }

  private _diffAppendedLines(prev: string, curr: string): string[] {
    return diffAppendedLines(prev, curr)
  }

  private _hasSession(): boolean {
    const r = this._tmux(['has-session', '-t', this.session], HAS_SESSION_TIMEOUT_MS)
    return r.code === 0
  }

  /** Wrap tmux invocation with -L socket prefix + a synchronous timeout. Bun's spawnSync
   *  lacks a per-call timeout; node:child_process.spawnSync does and is available in Bun. */
  private _tmux(args: string[], timeoutMs: number = SEND_KEYS_TIMEOUT_MS): {
    code: number; stdout: string; stderr: string;
  } {
    const argv: string[] = []
    if (this.socket) argv.push('-L', this.socket)
    argv.push(...args)
    try {
      const r = spawnSync('tmux', argv, { timeout: timeoutMs, encoding: 'utf-8' })
      if (r.error) {
        return { code: -1, stdout: '', stderr: String((r.error as any).message || r.error) }
      }
      return {
        code: typeof r.status === 'number' ? r.status : -1,
        stdout: typeof r.stdout === 'string' ? r.stdout : '',
        stderr: typeof r.stderr === 'string' ? r.stderr : '',
      }
    } catch (e: any) {
      return { code: -1, stdout: '', stderr: String(e?.message || e) }
    }
  }
}

/** Generate a default tmux session name when the operator didn't set AGENT*_TMUX_SESSION.
 *  Stable per server boot (computed once at module load in config.ts). */
export function defaultTmuxSessionName(): string {
  return `agent-${randomBytes(4).toString('hex')}`
}

/** Pure diff used by TmuxProvider._diffAppendedLines + tests. Anchors on the LAST
 *  non-empty line of `prev`, then advances through `curr` until we've seen that anchor
 *  as many times as it appeared in `prev`; everything after is the new tail.
 *
 *  Counting matters because chat CLIs commonly render a fixed prompt line ("▷ ", "> ")
 *  that repeats every turn. A naive rightmost-match would always anchor on the freshest
 *  prompt in `curr` and slice everything after it (= nothing), silently swallowing the
 *  new output between the previous prompt and the latest one. Falls back to the full
 *  `curr` when the anchor count exceeds what `curr` still has (scrollback rotated past). */
export function diffAppendedLines(prev: string, curr: string): string[] {
  if (!prev) return curr.split('\n')
  const prevLines = prev.split('\n')
  const currLines = curr.split('\n')
  let anchor = ''
  for (let i = prevLines.length - 1; i >= 0; i--) {
    const l = prevLines[i]
    if (l && l.trim()) { anchor = l; break }
  }
  if (!anchor) return currLines
  let prevCount = 0
  for (const l of prevLines) if (l === anchor) prevCount++
  let seen = 0
  for (let j = 0; j < currLines.length; j++) {
    if (currLines[j] === anchor) {
      seen++
      if (seen === prevCount) return currLines.slice(j + 1)
    }
  }
  // Rotated past: emit full frame rather than silently drop.
  return currLines
}
