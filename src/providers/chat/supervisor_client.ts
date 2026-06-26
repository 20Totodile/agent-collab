/**
 * supervisor_client.ts — server.ts side of the chat-supervisor split (AIC-124).
 *
 * Connects to the long-lived per-provider chat_supervisor daemon over a unix
 * socket, forwards stdin (turns + image attachments) to the running claude
 * child, and surfaces child_out / child_crashed back to the caller (server.ts).
 *
 * Not a drop-in for ChatProcess — the public surface here is intentionally
 * narrower (only what server.ts truly needs after the split): connect / send /
 * onChildOut / onChildCrashed / disconnect, plus indirect reconnect handling.
 *
 * Framing + WireMessage union live in ./frame so both sides share the wire
 * contract. We re-export the small subset relevant to callers.
 */

import {
  existsSync,
  openSync,
  mkdirSync,
  closeSync,
} from 'node:fs'
import { randomUUID } from 'node:crypto'
import { join } from 'node:path'
import {
  FrameTooLargeError,
  LineFramer,
  encodeMessage,
  type WireMessage,
} from './frame'

// ─────────────────────────── public types ───────────────────────────────────

export interface SupervisorClientOpts {
  /** Absolute path to runtime-data/chat-supervisor/<provider>/. Must match the
   *  daemon's runtimeDir; that's where supervisor.sock lives. */
  supervisorDir: string
  /** If supervisor.sock is absent on connect(), spawn supervisor via supervisorLauncher.
   *  Default true (lazy-spawn semantics from Q2). */
  autoSpawn?: boolean
  /**
   * argv for spawning the supervisor daemon, e.g.
   *   ['bun', 'run', '/abs/path/to/chat_supervisor.ts', '/abs/path/to/config.json']
   * Required iff autoSpawn=true and supervisor.sock is missing.
   */
  supervisorLauncher?: string[]
  /** Free-form label sent in `take_writer`. Defaults to 'server.ts'. */
  clientLabel?: string
  /** Connect handshake timeout (ms). Default 2000. */
  helloTimeoutMs?: number
  /** Overall connect timeout incl. supervisor boot + sock appearance (ms). Default 5000. */
  connectTimeoutMs?: number
  /** Max reconnect attempts after an unsolicited socket close. Default 10. */
  maxReconnectAttempts?: number
}

export interface ConnectInfo {
  supervisor_pid: number
  child_pid: number
}

export interface ChildCrashedInfo {
  exit_code: number | null
  signal: string | null
  /** Whether crash hit mid-turn. server.ts uses this to decide whether to clear
   *  typing state + push an error to PM. */
  during_active_turn: boolean
  /** Last ~4KB of child.log so server.ts can surface the actual error to PM. */
  tail_log: string
}

/** Fired when the supervisor reports a different child_spawn_id than we last saw
 *  (i.e. supervisor lazy-respawned during our reconnect window). Caller must
 *  drop any pending turn state — the old child is dead, its result event will
 *  never arrive. */
export type ChildRespawnedInfo = {
  old_child_spawn_id: string | undefined
  new_child_spawn_id: string | undefined
}

/** Send payload — mirrors StdinPayload on the wire (image fix scope). */
export interface SendPayload {
  prompt: string
  imagePaths?: string[]
}

/** Cycle 2: supervisor's interrupt acknowledgement. child_alive=false means
 *  the supervisor's child died (either claude binary exits on SIGINT, or the
 *  child was already gone when interrupt arrived). Next send() will lazy-respawn. */
export interface InterruptAckInfo {
  child_alive: boolean
  log_offset: number
  request_id?: string
}

/** Snapshot of supervisor + connection state, used by the
 *  /admin/chat-supervisor/health endpoint. All fields reflect the in-memory
 *  state of this client (the supervisor itself is NOT round-tripped — the
 *  hello/take_writer cache covers everything operators ask about, and a sync
 *  snapshot is the right shape for a read-only health check). */
export interface SupervisorHealth {
  connected: boolean
  supervisor_pid: number | null
  child_pid: number | null
  child_spawn_id: string | null
  child_log_offset: number | null
  /** ms since epoch when the most recent hello arrived (initial connect OR a
   *  reconnect handshake). null before first connect. */
  connected_at_ms: number | null
  /** ms since epoch when ANY wire frame (child_out / writer_granted / ping /
   *  ack) was last received. null before first connect. Used as a proxy for
   *  "supervisor is still alive on the other end". */
  last_frame_at_ms: number | null
  /** Currently we always run one supervisor client per server.ts. The supervisor
   *  itself tracks attached_count internally (heartbeat log), but does not
   *  surface it on the wire. Operators reading this endpoint should know the
   *  answer is "1" in steady state; null when disconnected. */
  attached_count: number | null
}

// ─────────────────────────── module constants ───────────────────────────────

const SOCK_FILENAME = 'supervisor.sock'
const SUPERVISOR_LOG_FILENAME = 'supervisor.client.log'
const SOCK_POLL_INTERVAL_MS = 100
const RECONNECT_BACKOFF_BASE_MS = 200
const RECONNECT_BACKOFF_CAP_MS = 5000

// ─────────────────────────── class ──────────────────────────────────────────

export class SupervisorClient {
  private readonly opts: Required<
    Omit<SupervisorClientOpts, 'supervisorLauncher'>
  > & { supervisorLauncher?: string[] }

  /** Active socket, or null when disconnected. Bun typing kept loose because
   *  Bun.connect resolves to Socket<undefined> by default and we don't need
   *  per-socket data. */
  private socket: unknown | null = null

  /** Per-socket framer (line buffer + parser). Replaced on every reconnect. */
  private framer: LineFramer = new LineFramer()

  /** Last received hello — used for ConnectInfo + reconnect identity check. */
  private hello: {
    supervisor_pid: number
    child_pid: number
    child_spawn_id?: string
  } | null = null

  /** True once connect() has succeeded at least once and we haven't been
   *  asked to disconnect(). Drives auto-reconnect on unexpected close. */
  private wantConnected = false

  /** Counter for exponential backoff. Reset on successful reconnect. */
  private reconnectAttempts = 0

  /** Last observed child.log byte offset — used to seed since_log_offset on
   *  reconnect take_writer so the supervisor's ring buffer / disk catchup can
   *  replay anything we missed during the socket gap. null = never received any
   *  child_out / catchup yet (first connect). */
  private lastChildLogOffset: number | null = null

  /** ms since epoch of the most recent hello (initial connect OR a reconnect
   *  handshake). null before first connect. Used by getHealth(). */
  private lastHelloAtMs: number | null = null

  /** ms since epoch when any wire frame was last received from the supervisor.
   *  Updated on every dispatch() call. null before first connect. */
  private lastFrameAtMs: number | null = null

  /** Pending hello-wait — resolved on first hello after socket open. */
  private helloWaiter: {
    resolve: (info: ConnectInfo) => void
    reject: (err: Error) => void
    timer: ReturnType<typeof setTimeout>
  } | null = null

  /** Callbacks. */
  private cbChildOut: ((data: string) => void) | null = null
  private cbChildCrashed: ((info: ChildCrashedInfo) => void) | null = null
  private cbReconnecting: ((attempt: number, delayMs: number) => void) | null = null
  private cbError: ((err: Error) => void) | null = null
  private cbChildRespawned: ((info: ChildRespawnedInfo) => void) | null = null
  /** Cycle 2: interrupt_ack callback. ChatProcess.interrupt() awaits this to
   *  decide whether to clear typing state (always) and whether to expect a
   *  lazy respawn on next send (child_alive=false). */
  private cbInterruptAck: ((info: InterruptAckInfo) => void) | null = null

  constructor(opts: SupervisorClientOpts) {
    if (!opts || typeof opts.supervisorDir !== 'string' || !opts.supervisorDir) {
      throw new Error('SupervisorClient: supervisorDir required')
    }
    this.opts = {
      supervisorDir: opts.supervisorDir,
      autoSpawn: opts.autoSpawn ?? true,
      supervisorLauncher: opts.supervisorLauncher,
      clientLabel: opts.clientLabel ?? 'server.ts',
      helloTimeoutMs: opts.helloTimeoutMs ?? 2000,
      connectTimeoutMs: opts.connectTimeoutMs ?? 5000,
      maxReconnectAttempts: opts.maxReconnectAttempts ?? 10,
    }
  }

  // ────────── public API ──────────

  /**
   * Discover or spawn the supervisor, open the unix socket, wait for hello,
   * return supervisor + child PIDs. Subsequent connect() calls while already
   * connected resolve immediately with the cached hello.
   */
  async connect(): Promise<ConnectInfo> {
    if (this.socket && this.hello) {
      return {
        supervisor_pid: this.hello.supervisor_pid,
        child_pid: this.hello.child_pid,
      }
    }
    this.wantConnected = true
    const sockPath = this.sockPath()

    // 1. Discovery / autoSpawn.
    if (!existsSync(sockPath)) {
      if (!this.opts.autoSpawn) {
        const err = new Error(
          `SupervisorClient: supervisor.sock missing at ${sockPath} and autoSpawn=false`,
        )
        this.fireError(err)
        throw err
      }
      this.spawnSupervisor()
    }

    // 2. Poll for sock file appearance (covers fresh spawn race).
    await this.waitForSock(sockPath, this.opts.connectTimeoutMs)

    // 3. Connect Bun unix socket.
    const info = await this.openSocketAndAwaitHello(sockPath)
    this.reconnectAttempts = 0
    return info
  }

  /**
   * Send a turn to the child. `imagePaths` (if any) are forwarded verbatim;
   * supervisor reads + base64-encodes them and builds the Anthropic content
   * blocks. Must be connected (or auto-reconnect-in-progress will throw).
   */
  async send(msg: SendPayload): Promise<void> {
    if (!msg || typeof msg.prompt !== 'string') {
      throw new Error('SupervisorClient.send: prompt (string) required')
    }
    if (!this.socket) {
      throw new Error('SupervisorClient.send: not connected')
    }
    const wire: WireMessage = {
      type: 'stdin',
      // Wire contract says `data: StdinPayload` (object), not raw string.
      // imagePaths intentionally omitted from wire if empty/undefined to keep
      // the line short for the common text-only path.
      data:
        msg.imagePaths && msg.imagePaths.length > 0
          ? { prompt: msg.prompt, imagePaths: msg.imagePaths }
          : { prompt: msg.prompt },
    } as WireMessage
    this.writeFrame(wire)
  }

  onChildOut(cb: (data: string) => void): void {
    this.cbChildOut = cb
  }

  onChildCrashed(cb: (info: ChildCrashedInfo) => void): void {
    this.cbChildCrashed = cb
  }

  /** notified when supervisor reports a NEW child_spawn_id on
   *  reconnect. Caller MUST drop any in-flight turn state — old child is gone,
   *  its result event will never arrive. If no callback is registered,
   *  SupervisorClient falls back to firing onError. */
  onChildRespawned(cb: (info: ChildRespawnedInfo) => void): void {
    this.cbChildRespawned = cb
  }

  /** Cycle 2: notified when supervisor acknowledges an interrupt. The callback
   *  fires once per interrupt round-trip (sendInterrupt → ack). */
  onInterruptAck(cb: (info: InterruptAckInfo) => void): void {
    this.cbInterruptAck = cb
  }

  /** Cycle 2: send an InterruptMessage to the supervisor. Non-blocking — the
   *  caller observes the result via onInterruptAck (or the global onError). The
   *  socket stays open; this is *not* a disconnect. Returns the request_id so
   *  the caller can correlate. */
  sendInterrupt(): string {
    const reqId = randomUUID()
    this.writeFrame({ type: 'interrupt', request_id: reqId } as WireMessage)
    return reqId
  }

  /** Notified each time a reconnect attempt is scheduled. Attempt is 1-based. */
  onReconnecting(cb: (attempt: number, delayMs: number) => void): void {
    this.cbReconnecting = cb
  }

  /** Fired on any error surfaced through the client (handshake failure,
   *  framing overflow, parse errors that breach the buffer cap, etc.). */
  onError(cb: (err: Error) => void): void {
    this.cbError = cb
  }

  /** Graceful close: stop wanting connection, end socket, drop callbacks state. */
  async disconnect(): Promise<void> {
    this.wantConnected = false
    // Cancel any pending hello wait.
    if (this.helloWaiter) {
      clearTimeout(this.helloWaiter.timer)
      this.helloWaiter.reject(new Error('SupervisorClient: disconnect() during handshake'))
      this.helloWaiter = null
    }
    const sock = this.socket as { end?: () => void } | null
    this.socket = null
    if (sock && typeof sock.end === 'function') {
      try {
        sock.end()
      } catch {
        // ignore — socket may already be half-closed
      }
    }
    this.framer = new LineFramer()
    this.hello = null
    this.reconnectAttempts = 0
    // Caller asked to fully disconnect — drop offset so a future connect()
    // (e.g. ChatProcess.interrupt() + send()) starts fresh. We preserve
    // lastChildLogOffset across automatic reconnects (handleSocketClose path),
    // not across explicit disconnect.
    this.lastChildLogOffset = null
    // Health snapshot reflects current connection — clear on explicit disconnect.
    // Automatic reconnect path (handleSocketClose) does NOT reset these so the
    // /admin/chat-supervisor/health endpoint can show stale-but-recent values
    // during a reconnect window (which is itself diagnostic information).
    this.lastHelloAtMs = null
    this.lastFrameAtMs = null
  }

  /** Snapshot health state for the /admin/chat-supervisor/health endpoint. Cheap
   *  + sync — does not touch the socket. Reflects whatever the client cached at
   *  the last hello + last frame. Returns null fields when disconnected. */
  getHealth(): SupervisorHealth {
    const connected = this.socket !== null && this.hello !== null
    return {
      connected,
      supervisor_pid: this.hello?.supervisor_pid ?? null,
      child_pid: this.hello?.child_pid ?? null,
      child_spawn_id: this.hello?.child_spawn_id ?? null,
      child_log_offset: this.lastChildLogOffset,
      connected_at_ms: this.lastHelloAtMs,
      last_frame_at_ms: this.lastFrameAtMs,
      // Single-client-per-server.ts assumption — see SupervisorHealth doc.
      attached_count: connected ? 1 : null,
    }
  }

  // ────────── internals: connect plumbing ──────────

  private sockPath(): string {
    return join(this.opts.supervisorDir, SOCK_FILENAME)
  }

  private spawnSupervisor(): void {
    const argv = this.opts.supervisorLauncher
    if (!argv || argv.length === 0) {
      const err = new Error(
        'SupervisorClient: autoSpawn=true but supervisorLauncher not provided',
      )
      this.fireError(err)
      throw err
    }
    // Ensure runtimeDir + log file exist so we have somewhere to point detached
    // stdio at — supervisor itself will (re)create child.log, but its early
    // boot messages need to land somewhere readable.
    try {
      mkdirSync(this.opts.supervisorDir, { recursive: true })
    } catch (e) {
      const err = new Error(
        `SupervisorClient: cannot create supervisorDir ${this.opts.supervisorDir}: ${
          (e as Error).message
        }`,
      )
      this.fireError(err)
      throw err
    }
    const logPath = join(this.opts.supervisorDir, SUPERVISOR_LOG_FILENAME)
    let logFd: number
    try {
      logFd = openSync(logPath, 'a')
    } catch (e) {
      const err = new Error(
        `SupervisorClient: cannot open supervisor log ${logPath}: ${(e as Error).message}`,
      )
      this.fireError(err)
      throw err
    }
    try {
      // Detached so supervisor outlives our process; stdio piped to log fd so
      // early errors are diagnosable. We don't keep a handle to the subproc.
      const bunGlobal = (globalThis as unknown as {
        Bun?: { spawn: (argv: string[], opts: Record<string, unknown>) => unknown }
      }).Bun
      if (!bunGlobal || typeof bunGlobal.spawn !== 'function') {
        throw new Error('Bun.spawn unavailable — SupervisorClient requires Bun runtime')
      }
      bunGlobal.spawn(argv, {
        detached: true,
        stdio: ['ignore', logFd, logFd],
      })
    } catch (e) {
      const err = new Error(
        `SupervisorClient: failed to spawn supervisor: ${(e as Error).message}`,
      )
      this.fireError(err)
      throw err
    } finally {
      // We dup'd into the child via Bun.spawn; safe to close the parent fd.
      try {
        closeSync(logFd)
      } catch {
        // ignore
      }
    }
  }

  /** Poll every SOCK_POLL_INTERVAL_MS for sock file. Throws on timeout. */
  private async waitForSock(sockPath: string, timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      if (existsSync(sockPath)) return
      await new Promise<void>((r) => setTimeout(r, SOCK_POLL_INTERVAL_MS))
    }
    if (existsSync(sockPath)) return
    const err = new Error(
      `SupervisorClient: timed out (${timeoutMs}ms) waiting for ${sockPath}`,
    )
    this.fireError(err)
    throw err
  }

  /** Opens Bun unix socket and resolves once a hello message arrives or rejects on timeout. */
  private openSocketAndAwaitHello(sockPath: string): Promise<ConnectInfo> {
    const bunGlobal = (globalThis as unknown as {
      Bun?: {
        connect: (opts: Record<string, unknown>) => Promise<unknown>
      }
    }).Bun
    if (!bunGlobal || typeof bunGlobal.connect !== 'function') {
      const err = new Error('Bun.connect unavailable — SupervisorClient requires Bun runtime')
      this.fireError(err)
      return Promise.reject(err)
    }

    return new Promise<ConnectInfo>((resolve, reject) => {
      const helloTimer = setTimeout(() => {
        if (this.helloWaiter) {
          this.helloWaiter = null
          const err = new Error(
            `SupervisorClient: hello timeout (${this.opts.helloTimeoutMs}ms)`,
          )
          this.fireError(err)
          // Tear socket down so reconnect path can retry cleanly.
          const sock = this.socket as { end?: () => void } | null
          this.socket = null
          try { sock?.end?.() } catch { /* ignore */ }
          reject(err)
        }
      }, this.opts.helloTimeoutMs)

      this.helloWaiter = { resolve, reject, timer: helloTimer }

      const self = this
      const framer = new LineFramer()
      this.framer = framer

      bunGlobal
        .connect({
          unix: sockPath,
          socket: {
            data(_socket: unknown, chunk: Uint8Array) {
              self.onSocketData(chunk)
            },
            open(socket: unknown) {
              self.socket = socket
            },
            close() {
              self.handleSocketClose()
            },
            error(_socket: unknown, error: Error) {
              self.fireError(error)
              self.handleSocketClose()
            },
          },
        })
        .catch((err: Error) => {
          if (this.helloWaiter) {
            clearTimeout(this.helloWaiter.timer)
            this.helloWaiter = null
          }
          this.fireError(err)
          reject(err)
        })
    })
  }

  // ────────── internals: read path ──────────

  private onSocketData(chunk: Uint8Array): void {
    let msgs: WireMessage[]
    try {
      msgs = this.framer.feed(chunk)
    } catch (e) {
      // Frame cap exceeded or fatal parse — surface + drop socket. Reconnect
      // loop will retry with a fresh framer. Distinguish cap
      // exceed via prefix so server.ts ianSessionExpiryStderrHandler can
      // identify it without parsing the raw framer message.
      let err: Error
      if (e instanceof FrameTooLargeError) {
        err = new Error(
          `SupervisorClient: frame cap exceeded (${e.bytes}b > ${
            (e as FrameTooLargeError & { bytes: number }).bytes
          }b); dropping socket`,
        )
      } else if (e instanceof Error) {
        err = new Error(`SupervisorClient: framer threw: ${e.message}`)
      } else {
        err = new Error(`SupervisorClient: framer threw ${String(e)}`)
      }
      this.fireError(err)
      const sock = this.socket as { end?: () => void } | null
      this.socket = null
      try { sock?.end?.() } catch { /* ignore */ }
      return
    }
    for (const msg of msgs) this.dispatch(msg)
  }

  private dispatch(msg: WireMessage): void {
    // Track wire-level liveness for getHealth(). Every dispatched frame —
    // including ping / no-op — proves the supervisor still has us attached.
    this.lastFrameAtMs = Date.now()
    switch (msg.type) {
      case 'hello': {
        const m = msg as Extract<WireMessage, { type: 'hello' }>
        // explicit protocol_version assertion. Future bump to 2
        // (schema change) must NOT silently parse old field names.
        if (m.protocol_version !== 1) {
          this.fireError(
            new Error(
              `SupervisorClient: unsupported supervisor protocol_version=${m.protocol_version}, expected 1`,
            ),
          )
          this.wantConnected = false
          const sock = this.socket as { end?: () => void } | null
          this.socket = null
          try { sock?.end?.() } catch { /* ignore */ }
          return
        }
        // detect supervisor lazy-respawn during our reconnect gap.
        // First connect (this.hello === null) is not a respawn — only treat it
        // as one if we had a prior hello with a different child_spawn_id.
        const prevSpawnId = this.hello?.child_spawn_id
        const newSpawnId = (m as { child_spawn_id?: string }).child_spawn_id
        if (prevSpawnId && newSpawnId && prevSpawnId !== newSpawnId) {
          if (this.cbChildRespawned) {
            try {
              this.cbChildRespawned({
                old_child_spawn_id: prevSpawnId,
                new_child_spawn_id: newSpawnId,
              })
            } catch (e) {
              console.error('SupervisorClient: onChildRespawned handler threw', e)
            }
          } else {
            this.fireError(
              new Error(
                `SupervisorClient: child respawned during reconnect (old=${prevSpawnId} new=${newSpawnId})`,
              ),
            )
          }
        }
        // latch initial child.log offset from hello so first
        // catchup window is anchored even if no child_out has arrived yet.
        const helloOffset = (m as { child_log_offset?: number }).child_log_offset
        if (typeof helloOffset === 'number' && Number.isFinite(helloOffset)) {
          // Only initialize from hello if we haven't tracked an offset yet.
          // On reconnect we WANT to preserve lastChildLogOffset so take_writer
          // can request a catchup from where we left off.
          if (this.lastChildLogOffset === null) {
            this.lastChildLogOffset = helloOffset
          }
        }
        this.hello = {
          supervisor_pid: m.supervisor_pid,
          child_pid: m.child_pid,
          child_spawn_id: newSpawnId,
        }
        this.lastHelloAtMs = Date.now()
        // Immediately claim writer lease — server.ts is the legitimate
        // replacement (force=true matches Q2 boot behavior).
        // include since_log_offset so the supervisor replays anything we
        // missed during the socket gap (the entire point of AIC-124 split).
        // Cycle 2 (2026-06-26): the emergency `since_log_offset: undefined`
        // workaround is removed. The catchup wire bug it was masking (big
        // single-frame + UTF-8 chunk-boundary corruption) is fixed by:
        //   - Splitting catchup into a sequence of small child_out frames
        //     (see chat_supervisor.ts take_writer handler).
        //   - StringDecoder-based byte buffering in LineFramer.feed.
        try {
          this.writeFrame({
            type: 'take_writer',
            client_label: this.opts.clientLabel,
            force: true,
            since_log_offset: this.lastChildLogOffset ?? undefined,
          } as WireMessage)
        } catch (e) {
          this.fireError(
            e instanceof Error ? e : new Error(`take_writer write failed: ${String(e)}`),
          )
        }
        if (this.helloWaiter) {
          clearTimeout(this.helloWaiter.timer)
          const w = this.helloWaiter
          this.helloWaiter = null
          w.resolve({ supervisor_pid: m.supervisor_pid, child_pid: m.child_pid })
        }
        return
      }
      case 'child_out': {
        // Cycle 2: catchup is no longer a separate frame type. Replay arrives
        // as child_out frames with catchup:true; live tailer broadcast comes
        // through with catchup omitted (or false). We treat both identically
        // on the read path — same line buffer, same onChildOut callback — and
        // only log the catchup flag for observability.
        const m = msg as Extract<WireMessage, { type: 'child_out' }>
        if (this.cbChildOut) {
          try { this.cbChildOut(m.data) } catch (e) {
            console.error('SupervisorClient: onChildOut handler threw', e)
          }
        }
        // track running offset for next reconnect's since_log_offset.
        if (typeof m.log_offset === 'number' && Number.isFinite(m.log_offset)) {
          this.lastChildLogOffset = m.log_offset
        }
        return
      }
      case 'child_crashed': {
        const m = msg as Extract<WireMessage, { type: 'child_crashed' }> & {
          exit_code: number | null
          signal: string | null
          during_active_turn?: boolean
          tail_log?: string
        }
        if (this.cbChildCrashed) {
          try {
            // forward during_active_turn + tail_log so server.ts
            // can clear typing state and surface the actual error to PM.
            this.cbChildCrashed({
              exit_code: m.exit_code,
              signal: m.signal,
              during_active_turn: m.during_active_turn === true,
              tail_log: typeof m.tail_log === 'string' ? m.tail_log : '',
            })
          } catch (e) {
            console.error('SupervisorClient: onChildCrashed handler threw', e)
          }
        }
        return
      }
      case 'error': {
        const m = msg as Extract<WireMessage, { type: 'error' }> & {
          code: string
          message: string
        }
        this.fireError(new Error(`supervisor error[${m.code}]: ${m.message}`))
        return
      }
      case 'writer_granted': {
        // Cycle 2: writer_granted may carry catchup meta (catchup_log_offset,
        // catchup_from_disk) when the take_writer included since_log_offset.
        // Use them to (a) advance lastChildLogOffset past any catchup that
        // arrived before this frame and (b) surface a ring-overflow warning
        // when the supervisor had to fall back to disk replay (operational
        // signal — operator may want to raise CHAT_CATCHUP_RING).
        const m = msg as Extract<WireMessage, { type: 'writer_granted' }>
        if (typeof m.catchup_log_offset === 'number' && Number.isFinite(m.catchup_log_offset)) {
          this.lastChildLogOffset = m.catchup_log_offset
        }
        if (m.catchup_from_disk === true) {
          this.fireError(
            new Error(
              `SupervisorClient: catchup served from disk (ring overflow; consider raising CHAT_CATCHUP_RING)`,
            ),
          )
        }
        return
      }
      case 'interrupt_ack': {
        const m = msg as Extract<WireMessage, { type: 'interrupt_ack' }>
        if (this.cbInterruptAck) {
          try {
            this.cbInterruptAck({
              child_alive: m.child_alive === true,
              log_offset: typeof m.log_offset === 'number' ? m.log_offset : 0,
              request_id: m.request_id,
            })
          } catch (e) {
            console.error('SupervisorClient: onInterruptAck handler threw', e)
          }
        }
        if (typeof m.log_offset === 'number' && Number.isFinite(m.log_offset)) {
          this.lastChildLogOffset = m.log_offset
        }
        return
      }
      case 'writer_revoked':
      case 'ping':
        // Currently no-op on client side; supervisor manages lease state and
        // the lone server.ts client never voluntarily relinquishes. Keep cases
        // explicit so unknown types fall through to the default warning.
        return
      default:
        console.warn('SupervisorClient: unknown wire message type', (msg as { type?: string }).type)
    }
  }

  // ────────── internals: write path ──────────

  private writeFrame(msg: WireMessage): void {
    const sock = this.socket as { write?: (data: string) => number } | null
    if (!sock || typeof sock.write !== 'function') {
      throw new Error('SupervisorClient.writeFrame: socket not writable')
    }
    sock.write(encodeMessage(msg))
  }

  // ────────── internals: reconnect ──────────

  private handleSocketClose(): void {
    // Reject any in-flight hello wait.
    if (this.helloWaiter) {
      clearTimeout(this.helloWaiter.timer)
      const w = this.helloWaiter
      this.helloWaiter = null
      w.reject(new Error('SupervisorClient: socket closed during handshake'))
    }
    this.socket = null
    this.framer = new LineFramer()
    this.hello = null
    if (!this.wantConnected) return
    this.scheduleReconnect()
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= this.opts.maxReconnectAttempts) {
      this.fireError(
        new Error(
          `SupervisorClient: reconnect gave up after ${this.reconnectAttempts} attempts`,
        ),
      )
      this.wantConnected = false
      return
    }
    const attempt = ++this.reconnectAttempts
    const delay = Math.min(
      RECONNECT_BACKOFF_CAP_MS,
      RECONNECT_BACKOFF_BASE_MS * 2 ** (attempt - 1),
    )
    if (this.cbReconnecting) {
      try { this.cbReconnecting(attempt, delay) } catch (e) {
        console.error('SupervisorClient: onReconnecting handler threw', e)
      }
    }
    setTimeout(() => {
      if (!this.wantConnected) return
      this.connect().catch((err) => {
        // connect() already fires onError + re-schedules via handleSocketClose
        // if it fails post-open. For pre-open failures (sock missing past
        // timeout, Bun.connect rejection) we need to schedule explicitly.
        if (this.wantConnected && !this.socket) {
          this.scheduleReconnect()
        }
        void err
      })
    }, delay)
  }

  private fireError(err: Error): void {
    if (this.cbError) {
      try { this.cbError(err) } catch (e) {
        console.error('SupervisorClient: onError handler threw', e)
      }
    }
  }
}
