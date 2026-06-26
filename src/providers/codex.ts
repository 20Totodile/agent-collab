// AIC-116: CodexProvider — long-lived `codex app-server --listen stdio://` wrapper.
//
// Codex CLI speaks NDJSON JSON-RPC 2.0 over stdio (one JSON object per line, the
// `"jsonrpc":"2.0"` envelope field is optional). One process can host N threads;
// we follow the "1 Provider instance = 1 thread" convention from multica — spawn
// the process once, complete the `initialize` → `initialized` → `thread/start`
// handshake, then reuse the same threadId across many `send()` calls (PM 06-25:
// long-lived chat context, not multica's per-task agent-as-tool).
//
// Protocol shape (verified via probe spawn against codex 0.142.0-alpha.6):
//   client → server  request       : { id, method, params }
//   server → client  response      : { id, result } or { id, error: { code, message } }
//   server → client  notification  : { method, params }     (no id; streaming events)
//   server → client  ServerRequest : { id, method, params } (reverse RPC — we MUST respond)
//
// Multi-thread noise: codex spawns internal subagent threads (memory consolidation,
// rate-limit recheck etc) on the same stdio channel. Filter by
// `params.threadId !== this._threadId` and drop foreign-thread notifications, else
// they pollute our event stream.

import { spawn, type Subprocess } from 'bun'
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs'
import { dirname } from 'path'
import type {
  AgentProvider, AgentEvent, AgentSendOpts, AgentCapabilities,
  AgentError, AgentProviderConfig, AgentUsage,
} from './provider.ts'

const CAPABILITIES: AgentCapabilities = {
  providerName: 'codex',
  supportsSessionResume: true,
  supportsAttachments: false,
  supportsToolUse: true,
  supportsThinking: false,    // codex reasoning is opaque, not emitted as separate field
  supportsPartialEvents: true, // item/agentMessage/delta
}

type PersistedState = { session_id?: string | null; updated_at?: string }
type PendingRequest = {
  resolve: (result: any) => void
  reject: (err: any) => void
  timeoutHandle: ReturnType<typeof setTimeout>
}

const REQUEST_TIMEOUT_MS = 60_000
const APPROVAL_ACCEPTING_METHODS = new Set([
  'item/commandExecution/requestApproval',
  'item/fileChange/requestApproval',
  // Legacy approval methods (older codex versions)
  'execCommandApproval',
  'applyPatchApproval',
])

export class CodexProvider implements AgentProvider {
  private cfg: AgentProviderConfig
  private proc: Subprocess<'pipe', 'pipe', 'pipe'> | null = null
  private stdoutBuf = ''
  private stderrBuf = ''
  private spawning: Promise<void> | null = null
  private _initialized = false
  private _threadId: string | null = null
  // AIC-116: distinguish "this process already started/resumed its thread" from
  // "thread id loaded from disk, needs resume on first send". Without this, every send()
  // after the first one re-fires `thread/resume` on a thread we already own — codex may
  // return an error response or treat it as a no-op depending on version.
  private _threadStartedInThisProcess = false
  private _nextId = 1
  private _pending = new Map<number, PendingRequest>()
  private eventCb: ((ev: AgentEvent) => void | Promise<void>) | null = null
  private errorCb: ((err: AgentError) => void) | null = null
  private label: string

  constructor(cfg: AgentProviderConfig) {
    this.cfg = cfg
    this.label = cfg.label || 'codex'
    if (cfg.onEvent) this.eventCb = cfg.onEvent
    if (cfg.onError) this.errorCb = cfg.onError
    // Persisted session_id maps to threadId for codex (resume across server restarts).
    const persisted = this._loadPersistedSessionId()
    if (persisted) this._threadId = persisted
  }

  get sessionId(): string | null { return this._threadId }

  get isAlive(): boolean {
    if (!this.proc) return false
    if (this.proc.exitCode !== null) return false
    if ((this.proc as any).signalCode != null) return false
    return true
  }

  capabilities(): AgentCapabilities { return CAPABILITIES }
  onEvent(cb: (ev: AgentEvent) => void | Promise<void>): void { this.eventCb = cb }
  onError(cb: (err: AgentError) => void): void { this.errorCb = cb }

  /**
   * Send a user message. On first call: spawns the codex app-server, runs the
   * initialize handshake, starts (or resumes) a thread, then issues `turn/start`.
   * Subsequent calls reuse the same process + threadId. Resolves once `turn/start`
   * is acknowledged; downstream notifications fire through onEvent as they arrive.
   */
  async send(text: string, _opts?: AgentSendOpts): Promise<void> {
    // AIC-116 cycle 2: send() reject semantics — rejects on spawn / initialize /
    // thread handshake failure (callers learn "agent can't accept this turn at all"), but
    // resolves once the turn/start request is dispatched fire-and-forget. Mid-turn protocol
    // errors (turn/start ack failure, codex JSON-RPC errors during a turn) surface through
    // onError so callers can take corrective action without polluting send() rejection paths.
    if (!this.isAlive) {
      await this._spawn()  // throws on spawn fail → bubbles up to caller
      this._initialized = false
      this._threadStartedInThisProcess = false
    }
    if (!this._initialized) await this._initialize()  // throws on init fail
    // AIC-116: first send in this process starts or resumes the thread;
    // subsequent sends reuse it directly (no extra thread/resume call).
    if (!this._threadStartedInThisProcess) {
      if (this._threadId) await this._resumeOrStartThread()  // throws on thread fail
      else await this._startThread()
    }

    // AIC-116 cycle 3: await turn/start request — codex must ack the new
    // turn before we consider this dispatch delivered. JSON-RPC ack is sub-second for a
    // healthy app-server; cap timeout at 10s so a hung process surfaces fast (dispatch
    // demote → failed) instead of stalling the optimistic-delivered UI for 60s.
    await this._request('turn/start', {
      threadId: this._threadId,
      input: [{ type: 'text', text, text_elements: [] }],
    }, 10_000)
  }

  async interrupt(): Promise<boolean> {
    if (!this.isAlive) return false
    // Best-effort turn cancellation before SIGKILL — codex supports turn/interrupt.
    if (this._threadId) {
      try {
        await this._request('turn/interrupt', { threadId: this._threadId }, 5_000)
      } catch { /* fall through to SIGKILL */ }
    }
    try { this.proc!.kill('SIGKILL') } catch {}
    try { await this.proc!.exited } catch {}
    return true
  }

  async close(): Promise<void> {
    if (!this.isAlive) return
    try {
      const writer = this.proc!.stdin as any
      if (writer && typeof writer.end === 'function') writer.end()
    } catch {}
    try { await this.proc!.exited } catch {}
  }

  // ───────────── private: spawn / handshake ─────────────

  private async _spawn(): Promise<void> {
    if (this.spawning) return this.spawning
    this.spawning = (async () => {
      const codexBin = this.cfg.binaryPath || 'codex'
      const args: string[] = ['app-server', '--listen', 'stdio://']
      if (this.cfg.extraArgs) args.push(...this.cfg.extraArgs)
      const spawnOpts: any = {
        cmd: [codexBin, ...args],
        stdin: 'pipe', stdout: 'pipe', stderr: 'pipe',
      }
      if (this.cfg.cwd) spawnOpts.cwd = this.cfg.cwd

      try {
        this.proc = spawn(spawnOpts)
      } catch (e: any) {
        this.errorCb?.({ kind: 'spawn_failed', message: String(e?.message || e), raw: e })
        this.proc = null
        throw e
      }

      this.stdoutBuf = ''
      this.stderrBuf = ''
      this._pumpStdout()
      this._pumpStderr()

      this.proc.exited.then((code) => {
        const signal = (this.proc as any)?.signalCode ?? null
        if ((code !== null && code !== 0) || signal) {
          this.errorCb?.({
            kind: 'process_exited',
            message: `${this.label} exited code=${code} signal=${signal}`,
          })
        }
        // Reject all pending requests so callers don't hang forever after a crash.
        for (const [id, p] of this._pending) {
          clearTimeout(p.timeoutHandle)
          p.reject(new Error(`${this.label}: process exited before request ${id} completed`))
        }
        this._pending.clear()
      })
    })()
    try { await this.spawning } finally { this.spawning = null }
  }

  private async _initialize(): Promise<void> {
    await this._request('initialize', {
      clientInfo: { name: 'ai-collab', title: null, version: '0.1.0' },
      capabilities: null,
    })
    this._notify('initialized', undefined)
    this._initialized = true
  }

  private async _startThread(): Promise<void> {
    const res: any = await this._request('thread/start', {
      cwd: this.cfg.cwd || process.cwd(),
      approvalPolicy: 'never',
      sandbox: 'workspace-write',
    })
    this._threadId = res?.thread?.id ?? res?.thread?.sessionId ?? null
    if (!this._threadId) throw new Error(`${this.label}: thread/start returned no thread id`)
    this._persistSessionId()
    this._threadStartedInThisProcess = true
    this.eventCb?.({ type: 'system_init', session_id: this._threadId, raw: res })
  }

  private async _resumeOrStartThread(): Promise<void> {
    if (!this._threadId) return this._startThread()
    try {
      const res: any = await this._request('thread/resume', { sessionId: this._threadId })
      const newId = res?.thread?.id ?? res?.thread?.sessionId
      if (newId && newId !== this._threadId) this._threadId = newId
      this._persistSessionId()
      this._threadStartedInThisProcess = true
      this.eventCb?.({ type: 'system_init', session_id: this._threadId, raw: res })
    } catch (e: any) {
      // Resume failed — sid expired or thread/resume unsupported. Fall back to fresh thread.
      this._threadId = null
      this._persistSessionId()
      await this._startThread()
    }
  }

  // ───────────── private: JSON-RPC plumbing ─────────────

  private _request(method: string, params: any, timeoutMs = REQUEST_TIMEOUT_MS): Promise<any> {
    return new Promise((resolve, reject) => {
      const id = this._nextId++
      const timeoutHandle = setTimeout(() => {
        if (this._pending.has(id)) {
          this._pending.delete(id)
          reject(new Error(`${this.label}: request ${method} #${id} timed out after ${timeoutMs}ms`))
        }
      }, timeoutMs)
      this._pending.set(id, { resolve, reject, timeoutHandle })
      try {
        this._writeLine({ id, method, params })
      } catch (e: any) {
        // stdin write failed — reject the request immediately rather than waiting out
        // the timeout. AIC-116 cycle 3.
        this._pending.delete(id)
        clearTimeout(timeoutHandle)
        reject(e)
      }
    })
  }

  private _notify(method: string, params: any): void {
    try { this._writeLine({ method, params }) }
    catch (e: any) { this.errorCb?.({ kind: 'protocol_error', message: `${this.label}: notify ${method} write failed: ${e?.message || e}` }) }
  }

  private _respond(id: number | string, result: any): void {
    try { this._writeLine({ id, result }) }
    catch (e: any) { this.errorCb?.({ kind: 'protocol_error', message: `${this.label}: respond #${id} write failed: ${e?.message || e}` }) }
  }

  private _respondError(id: number | string, code: number, message: string): void {
    try { this._writeLine({ id, error: { code, message } }) }
    catch (e: any) { this.errorCb?.({ kind: 'protocol_error', message: `${this.label}: respondError #${id} write failed: ${e?.message || e}` }) }
  }

  // Throws on dead subprocess / stdin write failure so callers (especially `_request`)
  // can synchronously reject the matching pending request instead of waiting out the
  // 60s timeout. AIC-116 cycle 3: silent stdin write swallow let send() return
  // resolve "success" while the actual user input never reached the agent.
  private _writeLine(obj: any): void {
    if (!this.isAlive) {
      throw new Error(`${this.label}: write to dead subprocess`)
    }
    const writer = this.proc!.stdin as any
    writer.write(JSON.stringify(obj) + '\n')
    if (typeof writer.flush === 'function') writer.flush()
  }

  private async _pumpStdout(): Promise<void> {
    if (!this.proc) return
    const reader = (this.proc.stdout as ReadableStream<Uint8Array>).getReader()
    const decoder = new TextDecoder('utf-8')
    try {
      while (true) {
        const { value, done } = await reader.read()
        if (done) break
        if (!value) continue
        this.stdoutBuf += decoder.decode(value, { stream: true })
        let nl = this.stdoutBuf.indexOf('\n')
        while (nl !== -1) {
          const line = this.stdoutBuf.slice(0, nl).trim()
          this.stdoutBuf = this.stdoutBuf.slice(nl + 1)
          if (line) this._handleLine(line)
          nl = this.stdoutBuf.indexOf('\n')
        }
      }
      if (this.stdoutBuf.trim()) this._handleLine(this.stdoutBuf.trim())
    } catch (e) {
      this.errorCb?.({ kind: 'protocol_error', message: `stdout pump error: ${e}` })
    } finally {
      try { reader.releaseLock() } catch {}
    }
  }

  private async _pumpStderr(): Promise<void> {
    if (!this.proc) return
    const reader = (this.proc.stderr as ReadableStream<Uint8Array>).getReader()
    const decoder = new TextDecoder('utf-8')
    try {
      while (true) {
        const { value, done } = await reader.read()
        if (done) break
        if (!value) continue
        this.stderrBuf += decoder.decode(value, { stream: true })
        let nl = this.stderrBuf.indexOf('\n')
        while (nl !== -1) {
          const line = this.stderrBuf.slice(0, nl).trim()
          this.stderrBuf = this.stderrBuf.slice(nl + 1)
          if (line) {
            // codex's normal logging goes to stderr at info level — most lines are noise.
            // The "panic" / "thread '...' panicked" / "fatal:" prefixes are the ones worth
            // surfacing. Anything else stays as console.error for debug visibility but
            // doesn't emit AgentError (would spam callers).
            if (/panic|fatal:|thread '.*' panicked|error:/i.test(line)) {
              this.errorCb?.({ kind: 'protocol_error', message: `${this.label} stderr: ${line}`, raw: line })
            } else {
              console.error(this.label, 'stderr:', line)
            }
          }
          nl = this.stderrBuf.indexOf('\n')
        }
      }
    } catch {}
    finally { try { reader.releaseLock() } catch {} }
  }

  private _handleLine(line: string): void {
    let raw: any
    try {
      raw = JSON.parse(line)
    } catch (e) {
      this.errorCb?.({ kind: 'parse_error', message: String(e), raw: line.slice(0, 200) })
      return
    }
    // Dispatch by shape (see protocol header comment).
    const hasId = raw.id !== undefined && raw.id !== null
    const hasResult = Object.prototype.hasOwnProperty.call(raw, 'result')
    const hasError = Object.prototype.hasOwnProperty.call(raw, 'error')
    const hasMethod = typeof raw.method === 'string'
    if (hasId && (hasResult || hasError)) {
      this._handleResponse(raw)
      return
    }
    if (hasId && hasMethod) {
      this._handleServerRequest(raw)
      return
    }
    if (hasMethod) {
      this._handleNotification(raw)
      return
    }
    // Unknown shape — surface as raw 'other' event for debug.
    this.eventCb?.({ type: 'other', raw })
  }

  private _handleResponse(raw: any): void {
    const id = Number(raw.id)
    const p = this._pending.get(id)
    if (!p) return
    this._pending.delete(id)
    clearTimeout(p.timeoutHandle)
    if (raw.error) p.reject(raw.error)
    else p.resolve(raw.result)
  }

  private _handleServerRequest(raw: any): void {
    const { id, method } = raw
    // Daemon mode: auto-accept all approval requests. No human in the loop.
    if (APPROVAL_ACCEPTING_METHODS.has(method)) {
      this._respond(id, { decision: 'accept' })
      return
    }
    if (method === 'item/permissions/requestApproval') {
      this._respond(id, { decision: 'accept', scope: 'turn' })
      return
    }
    if (method === 'mcpServer/elicitation/request') {
      this._respond(id, { action: 'accept', content: null })
      return
    }
    // Unknown server-request: respond with method-not-found rather than hanging the turn.
    this._respondError(id, -32601, `${this.label}: unhandled server-request ${method}`)
  }

  private _handleNotification(raw: any): void {
    const method = raw.method as string
    const params = raw.params || {}
    // Filter cross-thread noise. Some notifications (remoteControl/status, account/*) have
    // no threadId — let those through. Anything threadId-scoped must match ours.
    if (params.threadId && this._threadId && params.threadId !== this._threadId) {
      return
    }

    // AIC-116 cycle 2: do NOT emit partial deltas as `assistant` events. Each delta
    // is one streaming token chunk — emitting them as assistant text would let the server
    // write N intermediate fragments + 1 final copy to group_messages.
    // Buffer them silently; the matching `item/completed` carries the full text and is the
    // single emission point.
    if (method === 'item/agentMessage/delta') {
      this.eventCb?.({ type: 'other', raw })  // surface for transcript UI debug, NOT as assistant
      return
    }
    if (method === 'item/completed') {
      const item = params.item
      if (item?.type === 'agentMessage') {
        this.eventCb?.({ type: 'assistant', text: item.text || '', raw })
        return
      }
      if (item?.type === 'commandExecution') {
        const tool_uses = [{ name: 'exec_command', input: { command: item.command, output: item.aggregatedOutput } }]
        this.eventCb?.({ type: 'assistant', tool_uses, raw })
        return
      }
      if (item?.type === 'fileChange') {
        const tool_uses = [{ name: 'patch_apply', input: item }]
        this.eventCb?.({ type: 'assistant', tool_uses, raw })
        return
      }
      // userMessage / reasoning / others — surface as 'other' for debug; don't double-emit.
      this.eventCb?.({ type: 'other', raw })
      return
    }
    if (method === 'thread/tokenUsage/updated') {
      // AIC-116 cycle 2 non-blocking: token-usage updates can fire mid-turn (before turn/completed).
      // Emitting `result` here flips the agent state to idle prematurely — surface as 'other'
      // so callers that care (token meters) can still read it, but state machine stays "working"
      // until the real turn/completed lands below.
      this.eventCb?.({ type: 'other', raw })
      return
    }
    if (method === 'turn/completed') {
      const status = params.turn?.status
      if (status === 'failed' || status === 'cancelled' || status === 'aborted') {
        const msg = params.turn?.error?.message || `turn ${status}`
        this.errorCb?.({ kind: 'protocol_error', message: msg, raw })
      }
      // Pull token usage off the turn payload itself if available (codex 0.142+ embeds
      // `turn.usage` on completed). Mirror the old thread/tokenUsage/updated normalization
      // shape so downstream code only sees one canonical `result` event per turn.
      const u = params.turn?.usage || params.tokenUsage?.total
      const usage: AgentUsage | undefined = u ? {
        input_tokens: u.inputTokens ?? u.input_tokens,
        output_tokens: u.outputTokens ?? u.output_tokens,
        cache_read_input_tokens: u.cachedInputTokens ?? u.cache_read_input_tokens,
      } : undefined
      this.eventCb?.({ type: 'result', usage, raw })
      return
    }
    if (method === 'error') {
      // Mid-turn error notification. `willRetry: true` means codex is retrying internally
      // and the turn is still alive — don't surface as fatal. `false` is terminal.
      const willRetry = params.willRetry === true
      const msg = params.error?.message || 'codex error notification'
      if (!willRetry) {
        this.errorCb?.({ kind: 'protocol_error', message: msg, raw })
      }
      this.eventCb?.({ type: 'error', message: msg, raw })
      return
    }
    // Unhandled notifications (turn/started, item/started, mcpServer/*, remoteControl/*, etc)
    // surface as 'other' so the UI can render them if needed but don't add specific mapping.
    this.eventCb?.({ type: 'other', raw })
  }

  // ───────────── private: session persistence ─────────────

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
      const obj: PersistedState = { session_id: this._threadId, updated_at: new Date().toISOString() }
      writeFileSync(this.cfg.stateFilePath, JSON.stringify(obj, null, 2), { mode: 0o600 })
    } catch (e) {
      console.error(this.label, 'persist sid failed', e)
    }
  }
}
