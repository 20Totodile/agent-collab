// AIC-124 chat_supervisor.ts — long-lived daemon owning the claude stream-json child.
//
// Architecture (from POC h4 + AIC-124 contract + Q1-Q5 decisions):
//   - Owns ONE claude subprocess via Bun.spawn (detached:true; stdout/stderr -> child.log).
//     Long-lived across server.ts restarts. server.ts attaches via unix socket.
//   - Unix socket NDJSON wire (frame.ts LineFramer + encodeMessage).
//   - Single-writer lease enforced: only one client can write stdin at a time. Reads are
//     fan-out broadcast to ALL attached sockets.
//   - Image attachment conversion lives HERE (not in client) per 06-26 PM scope: client
//     only knows server-side paths; supervisor reads + base64-encodes + builds Anthropic
//     content blocks. Lets us swap block format without touching client.
//   - Lazy spawn on child crash (Q2): broadcast child_crashed to all attached, then wait
//     for next stdin before respawning. Avoids busy-loop respawn on persistent failure.
//   - Catchup: in-memory ring of last N child_out frames (Q5). On take_writer with
//     since_log_offset, replay from ring; if cursor older than ring, fall back to reading
//     child.log tail off disk and tag from_disk=true.
//
// Env vars (paths/config derived in main()):
//   CHAT_SUPERVISOR_DIR   absolute runtime dir (sock/pid/log live under it). REQUIRED.
//   CHAT_CHILD_BIN        claude binary name/path. Default 'claude'.
//   CHAT_CHILD_MODEL      optional --model arg.
//   CHAT_CHILD_EXTRA_ARGS extra CLI args, JSON-encoded array. Default '[]'.
//   CHAT_CHILD_CWD        cwd for child. REQUIRED (see chat-process.ts AIC-104 note).
//   CHAT_PROVIDER         provider tag, included in hello. Default 'agent'.
//   CHAT_PRE_RESUME_MARKER optional path to touch before --resume spawn.
//   CHAT_STATE_FILE       optional path holding {session_id} JSON for --resume.
//   CHAT_CATCHUP_RING     ring buffer size in lines. Default '200'.
//   CHAT_MAX_IMAGE_BYTES  per-image byte cap (pre-base64). Default 5*1024*1024.
//
// Fork prefix policy (AIC fork sync):
//   When this file is run inside the ai-collab fork, ClaudeProvider stamps env
//   keys with an `AICOLLAB_` prefix (AICOLLAB_SUPERVISOR_DIR / AICOLLAB_CHAT_*)
//   so they don't collide with the upstream main repo's 3009-process env. configFromEnv
//   prefers the AICOLLAB_-prefixed name when present and falls back to the bare
//   CHAT_* name otherwise. This lets the SAME chat_supervisor.ts source serve
//   both the main repo (CHAT_*) and the ai-collab fork (AICOLLAB_*) without
//   diverging. Keep both prefixes in sync if you add a new var here.
//
// Failure policy (Wiki rule 1 / 12 rules #12):
//   - pidfile O_EXCL collision with alive holder -> exit(2). Dead holder -> reclaim.
//   - socket bind / log open / runtime dir create failure -> log + exit(1).
//   - frame overflow on client -> drop that one socket; supervisor stays up.
//   - child spawn failure -> log + exit(3) on first spawn; lazy-respawn failure handled
//     inline on next stdin (client gets ErrorMessage).
//   - uncaughtException / unhandledRejection -> log + shutdown cleanup + exit(4).

import {
  appendFileSync,
  closeSync,
  existsSync,
  fstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readSync,
  statSync,
  unlinkSync,
  writeFileSync,
  writeSync,
} from 'node:fs'
import { StringDecoder } from 'node:string_decoder'
import { dirname, join } from 'node:path'

import {
  encodeMessage,
  FrameTooLargeError,
  InvalidFrameError,
  LineFramer,
  type ChildCrashedMessage,
  type ChildOutMessage,
  type ErrorCode,
  type ErrorMessage,
  type HelloMessage,
  type StdinPayload,
  type WireMessage,
} from './frame'

import type { Socket, Subprocess, TCPSocketListener } from 'bun'

/* ────── config ────── */

export interface SupervisorConfig {
  /** Provider tag echoed in HelloMessage. AIC-124 Q4: per-provider supervisor. */
  provider: string
  /** Absolute runtime dir. supervisor.{sock,pid,log} + child.{pid,log} live under here. */
  runtimeDir: string
  /** claude binary path or 'claude'. */
  claudeBin: string
  /** claude CLI args, NOT including --resume (that's added per-spawn). */
  claudeArgs: string[]
  /** cwd for claude subprocess. REQUIRED — see AIC-104 note in chat-process.ts. */
  claudeCwd: string
  /** Touched before --resume spawn so SessionStart hook can skip briefing. */
  preResumeMarkerPath?: string
  /** Persisted last session_id ({"session_id": "..."}). Used for --resume on respawn. */
  stateFilePath?: string
  /** Ring buffer size (lines) for catchup. Q5: default 200. */
  catchupRingLines: number
  /** Per-image byte cap (pre-base64). */
  maxImageBytes: number
}

/* ────── derived paths ────── */

interface RuntimePaths {
  sockPath: string
  supPidPath: string
  supLogPath: string
  childPidPath: string
  childLogPath: string
}

function derivePaths(runtimeDir: string): RuntimePaths {
  return {
    sockPath: join(runtimeDir, 'supervisor.sock'),
    supPidPath: join(runtimeDir, 'supervisor.pid'),
    supLogPath: join(runtimeDir, 'supervisor.log'),
    childPidPath: join(runtimeDir, 'child.pid'),
    childLogPath: join(runtimeDir, 'child.log'),
  }
}

/* ────── ULID-ish for child_spawn_id ────── */
// Lightweight Crockford base32 ts(10) + rand(16). Same shape as server.ts ULID
// but without monotonic carry — supervisor mints at most one per child spawn so
// monotonicity within ms is not a concern.

const ULID_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

function mintSpawnId(): string {
  let ts = Date.now()
  let tsPart = ''
  for (let i = 9; i >= 0; i--) {
    tsPart = ULID_ALPHABET[ts % 32] + tsPart
    ts = Math.floor(ts / 32)
  }
  let randPart = ''
  for (let i = 0; i < 16; i++) {
    randPart += ULID_ALPHABET[Math.floor(Math.random() * 32)]
  }
  return tsPart + randPart
}

/* ────── log helper ────── */

let _supLogPathForLogger: string | null = null
function slog(msg: string): void {
  const line = `[sup ${new Date().toISOString()} pid=${process.pid}] ${msg}\n`
  // Always echo to stderr so journald / start-server.sh logs see it too.
  process.stderr.write(line)
  if (_supLogPathForLogger) {
    try {
      appendFileSync(_supLogPathForLogger, line)
    } catch {
      // log write should never crash us
    }
  }
}

/* ────── runtime state ────── */

interface RingEntry {
  data: string
  logOffset: number
}

export interface ClientState {
  label: string
  frame: LineFramer
  isWriter: boolean
  /** since_log_offset latched from take_writer, used to seed catchup. */
  lastSeenOffset: number | null
  /** AIC-127 cycle 3: per-socket send queue. Bun unix Socket.write is sync
   *  number return — short counts mean kernel send buffer is full and the
   *  unwritten tail is silently dropped (Bun does NOT internally queue, unlike
   *  Node net.Socket). We buffer pending bytes here and retry from drain().
   *  Capped at MAX_SEND_QUEUE_BYTES; overflow closes the socket.
   *  (Cycle 2 catchup loop wrote ~47 × 8KB frames synchronously, dwarfing the
   *  ~64KB kernel buffer; the tail silently dropped → client framer saw
   *  truncated NDJSON → JSON.parse 'Expected }'. This queue makes the writes
   *  backpressure-aware.) */
  sendBuf: Buffer[]
  sendBufBytes: number
}

interface SupervisorState {
  pid: number
  paths: RuntimePaths
  /** Current child Subprocess. null when lazy-respawn pending after crash. */
  child: Subprocess<'pipe', number, number> | null
  childSpawnId: string
  /** Append-only fd for child.log. Reused across spawns (file is appended to). */
  childLogFd: number
  /** Read fd for child.log tail. */
  childLogReadFd: number
  /** Running byte offset for child_out broadcast. */
  childLogOffset: number
  /** Ring buffer (oldest first). */
  catchupRing: RingEntry[]
  catchupRingMax: number
  /** Attached sockets, keyed by Bun socket object. */
  attached: Map<Socket<ClientState>, ClientState>
  /** Current writer-lease holder (subset of attached). */
  writer: Socket<ClientState> | null
  writerEpoch: number
  /** True from stdin sent until next `result` event (or child_out parse can't tell, so
   *  we approximate: true whenever writer wrote stdin since last result). Drives
   *  ChildCrashedMessage.during_active_turn. */
  activeTurnInFlight: boolean
  /** Cycle 2 interrupt: true between InterruptMessage receipt and the next
   *  `result` event (or child exit). While true, tailer still writes child.log
   *  and pushes to catchupRing, but DOES NOT broadcast child_out to attached
   *  clients — so PM UI no longer sees a "stopped" turn keep spewing tokens. */
  interruptPending: boolean
  /** Cycle 2: childLogOffset captured at the moment of interrupt receipt.
   *  Used as a diagnostic for the interrupt_ack frame's log_offset field.
   *  null when no interrupt in flight. */
  interruptBoundary: number | null
  /** Server handle for shutdown. */
  server: TCPSocketListener<ClientState> | null
  /** Heartbeat interval handle. */
  heartbeatTimer: ReturnType<typeof setInterval> | null
  /** Tailer interval handle. */
  tailerTimer: ReturnType<typeof setInterval> | null
  /** UTF-8 StringDecoder to safely span codepoints across 64KB
   *  tailer buffer boundaries. Without this, multibyte chars (中文, emoji) get
   *  replaced with U+FFFD (3 bytes), which throws off ring oldestStart math
   *  AND occasionally explodes JSON.parse downstream. */
  tailerDecoder: StringDecoder
  /** Accumulator for partial NDJSON lines, so we only
   *  check 'type":"result"' on complete lines (not arbitrary chunk
   *  boundaries — which can split the literal mid-stream). */
  tailerLineBuf: string
  /** Inhibit shutdown re-entrance. */
  shuttingDown: boolean
}

/* ────── pidfile reclaim ────── */

/**
 * AIC-124 cycle 5: async probe unix socket 是否有 live listener.
 * 'live' = 连得上 → 有 supervisor 跑; 'dead' = ECONNREFUSED 或 timeout → stale 可 unlink.
 * Bun.connect 是异步的 — sync busy-wait 包它会让 open() callback 永远 fire 不了
 * (busy-wait 阻塞 event loop). cycle 4 踩过, e2e 复现.
 * 修法: 改 Promise, caller await.
 */
async function probeSocketLive(sockPath: string, timeoutMs: number): Promise<'live' | 'dead'> {
  return new Promise(resolve => {
    let resolved = false
    const settle = (val: 'live' | 'dead') => {
      if (resolved) return
      resolved = true
      clearTimeout(timer)
      resolve(val)
    }
    const timer = setTimeout(() => settle('dead'), timeoutMs)
    // @ts-expect-error Bun global
    const BunRt = (globalThis as any).Bun
    if (!BunRt?.connect) {
      settle('dead')
      return
    }
    try {
      BunRt.connect({
        unix: sockPath,
        socket: {
          open(s: any) { settle('live'); try { s?.end?.() } catch {} },
          data() {},
          close() {},
          error() { settle('dead') },
        },
      }).catch(() => settle('dead'))
    } catch {
      settle('dead')
    }
  })
}

/**
 * Acquire pidfile with O_CREAT|O_EXCL. On EEXIST: kill -0 to probe holder; alive -> throw,
 * dead -> unlink + retry once. Returns the open fd (kept open for ownership signaling).
 */
export async function reclaimRuntimeFiles(paths: RuntimePaths): Promise<void> {
  if (existsSync(paths.supPidPath)) {
    const raw = (() => {
      try {
        return readFileSync(paths.supPidPath, 'utf8').trim()
      } catch {
        return ''
      }
    })()
    const oldPid = parseInt(raw, 10)
    if (!Number.isNaN(oldPid) && oldPid > 0) {
      let alive = false
      try {
        process.kill(oldPid, 0)
        alive = true
      } catch {
        alive = false
      }
      if (alive) {
        slog(`supervisor already running pid=${oldPid}`)
        process.exit(2)
      }
      slog(`stale supervisor.pid (pid=${oldPid} dead); reclaiming`)
    } else {
      slog(`unparseable supervisor.pid (${JSON.stringify(raw)}); reclaiming`)
    }
    try {
      unlinkSync(paths.supPidPath)
    } catch {}
  }
  // AIC-124 cycle 4 fix: 不能无条件 unlink sockPath —
  // 否则 emergency 起的 orphan supervisor + 新 lazy spawn 撞同款 path 双跑.
  // 必须先 probe socket 是否仍有 live listener; 只有 ECONNREFUSED / ENOENT / timeout
  // 才认为真死 + unlink. 连上 + hello 收到 → sock live → exit(2) 复用.
  if (existsSync(paths.sockPath)) {
    const probeResult = await probeSocketLive(paths.sockPath, 500)
    if (probeResult === 'live') {
      slog(`socket ${paths.sockPath} has live listener — another supervisor alive; exiting`)
      process.exit(2)
    }
    slog(`socket ${paths.sockPath} probe=${probeResult}; unlinking stale`)
    try {
      unlinkSync(paths.sockPath)
    } catch (e) {
      throw new Error(`failed to unlink stale socket ${paths.sockPath}: ${(e as Error).message}`)
    }
  }
  // O_CREAT|O_EXCL guarantees we never race another supervisor in the gap between
  // the kill-probe above and the bind below.
  let fd: number
  try {
    fd = openSync(paths.supPidPath, 'wx', 0o600)
  } catch (e: any) {
    if (e?.code === 'EEXIST') {
      // Another process snuck in between our reclaim + open. Surface explicitly.
      slog(`race: another supervisor bound supervisor.pid; aborting`)
      process.exit(2)
    }
    throw new Error(`failed to open supervisor.pid: ${e?.message ?? e}`)
  }
  // write THROUGH the O_EXCL fd, not via writeFileSync (which
  // re-opens the path without EXCL, leaving a window between openSync(wx)
  // success and writeFileSync where a throw would leave an empty pidfile and
  // the EXCL fd unused. writeSync on the fd we already hold closes that
  // window entirely.
  try {
    const buf = Buffer.from(String(process.pid))
    writeSync(fd, buf, 0, buf.length, 0)
  } finally {
    try {
      closeSync(fd)
    } catch {}
  }
}

/* ────── spawn child ────── */

export function spawnChild(state: SupervisorState, cfg: SupervisorConfig): void {
  // Build args. --resume gets prepended if state file has a session_id.
  const args: string[] = []
  const sid = loadPersistedSessionId(cfg)
  if (sid) {
    args.push('--resume', sid)
    if (cfg.preResumeMarkerPath) {
      try {
        mkdirSync(dirname(cfg.preResumeMarkerPath), { recursive: true })
        writeFileSync(cfg.preResumeMarkerPath, '', { mode: 0o600 })
      } catch (e) {
        slog(`preResumeMarker write failed (continuing): ${(e as Error).message}`)
      }
    }
  }
  for (const a of cfg.claudeArgs) args.push(a)

  // child.log fd is opened append-mode in main(); we just hand it to Bun.spawn.
  // Bun documents number fd is valid for stdout/stderr.
  let proc: Subprocess<'pipe', number, number>
  try {
    proc = Bun.spawn({
      cmd: [cfg.claudeBin, ...args],
      stdin: 'pipe',
      stdout: state.childLogFd,
      stderr: state.childLogFd,
      cwd: cfg.claudeCwd,
      // detached:true detaches the child from supervisor's process group so any
      // external `kill -- -<pgid>` aimed at server.ts cannot accidentally kill the
      // child via pgroup-fanout. Supervisor is itself robust to its own pgroup
      // signals via SIGHUP-ignore below.
      env: process.env,
      // @ts-expect-error Bun's TS lib for Bun.spawn doesn't yet expose detached on
      // the public type but the runtime accepts it (see POC h1).
      detached: true,
    }) as Subprocess<'pipe', number, number>
  } catch (e) {
    throw new Error(`Bun.spawn failed: ${(e as Error).message}`)
  }

  state.child = proc
  state.childSpawnId = mintSpawnId()
  state.activeTurnInFlight = false

  try {
    writeFileSync(state.paths.childPidPath, String(proc.pid))
  } catch (e) {
    slog(`failed to write child.pid: ${(e as Error).message}`)
  }
  slog(`spawned child pid=${proc.pid} spawn_id=${state.childSpawnId} args=${JSON.stringify(args)}`)

  // Wire crash detection. exited fires regardless of how the child died.
  proc.exited.then((code) => {
    if (state.shuttingDown) return
    const signal = (proc as any).signalCode ?? null
    slog(`child exited code=${code} signal=${signal}`)
    handleChildExit(state, cfg, code ?? null, signal)
  }).catch((e) => {
    slog(`proc.exited rejected: ${(e as Error).message}`)
  })
}

function handleChildExit(
  state: SupervisorState,
  cfg: SupervisorConfig,
  code: number | null,
  signal: string | null,
): void {
  const wasActive = state.activeTurnInFlight
  const wasInterrupt = state.interruptPending
  const oldPid = state.child?.pid ?? -1
  state.child = null
  state.activeTurnInFlight = false
  try {
    if (existsSync(state.paths.childPidPath)) unlinkSync(state.paths.childPidPath)
  } catch {}
  if (wasInterrupt) {
    // Cycle 2: this exit was caused by our SIGINT (claude binary may handle
    // SIGINT by exiting rather than just aborting the turn). Don't surface
    // child_crashed — that would (correctly) trip server.ts crash handling.
    // Instead send a final interrupt_ack with child_alive=false so the writer
    // knows the next send will lazy-respawn. Clear gate state.
    state.interruptPending = false
    state.interruptBoundary = null
    if (state.writer) {
      writeWire(state.writer, {
        type: 'interrupt_ack',
        child_alive: false,
        log_offset: state.childLogOffset,
      })
    }
    slog(`child exited during interrupt window (pid=${oldPid} code=${code} signal=${signal}); sent interrupt_ack child_alive=false`)
    return
  }
  const tailLog = readChildLogTail(cfg, 4 * 1024)
  const crash: ChildCrashedMessage = {
    type: 'child_crashed',
    child_pid: oldPid,
    exit_code: code,
    signal,
    during_active_turn: wasActive,
    tail_log: tailLog,
  }
  broadcastWire(state, crash)
  // Q2 lazy spawn: do NOT respawn here. Next stdin from a writer triggers respawn.
}

/* ────── reading child.log tail for catchup + crash tail ────── */

export function readChildLogTail(cfg: SupervisorConfig, maxBytes: number): string {
  const path = join(cfg.runtimeDir, 'child.log')
  try {
    if (!existsSync(path)) return ''
    const st = statSync(path)
    const size = st.size
    const start = Math.max(0, size - maxBytes)
    const len = size - start
    if (len <= 0) return ''
    const fd = openSync(path, 'r')
    try {
      const buf = Buffer.alloc(len)
      const n = readSync(fd, buf, 0, len, start)
      return buf.slice(0, n).toString('utf8')
    } finally {
      try { closeSync(fd) } catch {}
    }
  } catch (e) {
    slog(`readChildLogTail failed: ${(e as Error).message}`)
    return ''
  }
}

/**
 * Resolve catchup bytes for a since_log_offset cursor.
 * - If sinceOffset >= current childLogOffset: nothing to send.
 * - If ring covers [sinceOffset, current]: assemble from ring, from_disk=false.
 * - Else: read disk tail, from_disk=true (Q5).
 */
export function buildCatchup(
  state: SupervisorState,
  cfg: SupervisorConfig,
  sinceOffset: number,
): { data: string; logOffset: number; from_disk: boolean } {
  const current = state.childLogOffset
  if (sinceOffset >= current) {
    return { data: '', logOffset: current, from_disk: false }
  }
  // Try ring. Ring entries store the END offset of each broadcast chunk.
  // We can satisfy from ring iff oldest ring entry's start <= sinceOffset.
  // Each entry's start = previousEntry.logOffset (or 0 for very first).
  const ring = state.catchupRing
  if (ring.length > 0) {
    // The "start" of entry[0] is unknown precisely (it's wherever the broadcast
    // started). We approximate: oldest-coverable offset = entry[0].logOffset - entry[0].data.length.
    const oldestStart = ring[0].logOffset - Buffer.byteLength(ring[0].data, 'utf8')
    if (oldestStart <= sinceOffset) {
      // Walk ring, skipping entries fully before sinceOffset.
      const pieces: string[] = []
      for (const ent of ring) {
        const entStart = ent.logOffset - Buffer.byteLength(ent.data, 'utf8')
        if (ent.logOffset <= sinceOffset) continue
        if (entStart >= sinceOffset) {
          pieces.push(ent.data)
        } else {
          // Partial: slice off the bytes < sinceOffset. data is UTF-8 string; we
          // can't byte-slice safely for arbitrary multibyte, so fall back to disk
          // to avoid producing invalid UTF-8 mid-codepoint.
          return readDiskCatchup(cfg, sinceOffset, current)
        }
      }
      return { data: pieces.join(''), logOffset: current, from_disk: false }
    }
  }
  return readDiskCatchup(cfg, sinceOffset, current)
}

function readDiskCatchup(
  cfg: SupervisorConfig,
  sinceOffset: number,
  current: number,
): { data: string; logOffset: number; from_disk: boolean } {
  const path = join(cfg.runtimeDir, 'child.log')
  try {
    const len = current - sinceOffset
    if (len <= 0) return { data: '', logOffset: current, from_disk: true }
    const fd = openSync(path, 'r')
    try {
      const buf = Buffer.alloc(len)
      const n = readSync(fd, buf, 0, len, sinceOffset)
      return { data: buf.slice(0, n).toString('utf8'), logOffset: current, from_disk: true }
    } finally {
      try { closeSync(fd) } catch {}
    }
  } catch (e) {
    slog(`readDiskCatchup failed (sinceOffset=${sinceOffset}): ${(e as Error).message}`)
    return { data: '', logOffset: current, from_disk: true }
  }
}

/* ────── session_id state file ────── */

function loadPersistedSessionId(cfg: SupervisorConfig): string | null {
  if (!cfg.stateFilePath) return null
  try {
    if (!existsSync(cfg.stateFilePath)) return null
    const raw = readFileSync(cfg.stateFilePath, 'utf-8')
    const obj = JSON.parse(raw) as { session_id?: string }
    return obj.session_id || null
  } catch {
    return null
  }
}

/* ────── socket server ────── */

export function startSocketServer(
  state: SupervisorState,
  cfg: SupervisorConfig,
): TCPSocketListener<ClientState> {
  let server: TCPSocketListener<ClientState>
  try {
    server = Bun.listen<ClientState>({
      unix: state.paths.sockPath,
      socket: {
        open(socket) {
          const cs: ClientState = {
            label: '',
            frame: new LineFramer(),
            isWriter: false,
            lastSeenOffset: null,
            sendBuf: [],
            sendBufBytes: 0,
          }
          socket.data = cs
          state.attached.set(socket, cs)
          slog(`client attached; total=${state.attached.size}`)
          // First message MUST be hello, per contract.
          const hello: HelloMessage = {
            type: 'hello',
            supervisor_pid: state.pid,
            child_pid: state.child?.pid ?? -1,
            child_spawn_id: state.childSpawnId,
            child_log_path: state.paths.childLogPath,
            child_log_offset: state.childLogOffset,
            provider: cfg.provider,
            protocol_version: 1,
          }
          writeWire(socket, hello)
        },
        data(socket, chunk) {
          const cs = socket.data
          let msgs: WireMessage[]
          try {
            msgs = cs.frame.feed(chunk)
          } catch (e) {
            if (e instanceof FrameTooLargeError) {
              slog(`client ${cs.label || '<unlabeled>'} overflow ${e.bytes}b; dropping`)
              // emit ErrorMessage before close so the client sees
              // *why* the socket got torn down instead of a silent close.
              // The review checklist explicitly called for "close +
              // ErrorMessage to client" on cap breach.
              const ofErr: ErrorMessage = {
                type: 'error',
                code: 'protocol_violation',
                message: `frame buffer overflow ${e.bytes}b > 1 MiB cap`,
              }
              writeWire(socket, ofErr)
              try { socket.end() } catch {}
              return
            }
            if (e instanceof InvalidFrameError) {
              slog(`client ${cs.label || '<unlabeled>'} sent invalid frame: ${e.message}`)
              const err: ErrorMessage = {
                type: 'error',
                code: 'protocol_violation',
                message: e.message,
              }
              writeWire(socket, err)
              try { socket.end() } catch {}
              return
            }
            // Unknown framing error — log + close (rule 12).
            slog(`framer threw unexpected: ${(e as Error).message}`)
            try { socket.end() } catch {}
            return
          }
          for (const m of msgs) {
            // handleClientMessage is async (stdin drain await).
            // We deliberately don't await sequentially here — each message
            // resolves independently and stdin ordering is preserved by the
            // single-writer lease (one socket = one writer queue). The catch
            // handles both sync throws and async rejections.
            handleClientMessage(state, cfg, socket, m).catch((e) => {
              slog(`handleClientMessage threw: ${(e as Error).message}`)
            })
          }
        },
        close(socket) {
          const cs = socket.data
          state.attached.delete(socket)
          if (state.writer === socket) {
            slog(`writer ${cs?.label ?? ''} disconnected; lease released`)
            state.writer = null
          }
          if (cs) {
            cs.sendBuf = []
            cs.sendBufBytes = 0
          }
          slog(`client detached; total=${state.attached.size}`)
        },
        error(socket, err) {
          slog(`socket error (${socket.data?.label ?? ''}): ${err.message}`)
          state.attached.delete(socket)
          if (state.writer === socket) state.writer = null
          const cs = socket.data
          if (cs) {
            cs.sendBuf = []
            cs.sendBufBytes = 0
          }
        },
        drain(socket) {
          // AIC-127 cycle 3: kernel send buffer has room again — retry queued
          // bytes from the per-socket sendBuf. Without this, a single partial
          // write would leave the tail stuck forever (no other wake-up signal
          // exists for unix-socket backpressure; Bun's only out-of-band drain
          // signal is this callback per the docs).
          flushSendBuf(socket)
        },
      },
    })
  } catch (e) {
    throw new Error(`Bun.listen failed on ${state.paths.sockPath}: ${(e as Error).message}`)
  }
  slog(`listening on ${state.paths.sockPath}`)
  return server
}

/* ────── client message dispatch ────── */

/**
 * AIC-127 cycle 2 helper: Promise.race with a timeout reject so
 * a wedged child stdin (Bun returns a pending thenable that never resolves
 * when the child isn't draining) doesn't permanently block handleClientMessage.
 * Caller wraps the call in try/catch; timeout rejection follows the same
 * `child_stdin_write_failed` path as a normal write error.
 *
 * Exported for verify scripts (aic127-stdin-backpressure-verify timeout cases).
 */
export async function awaitOrTimeout<T>(
  promise: PromiseLike<T>,
  ctx: string,
  timeoutMs: number,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${ctx} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        )
      }),
    ])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

export async function handleClientMessage(
  state: SupervisorState,
  cfg: SupervisorConfig,
  socket: Socket<ClientState>,
  msg: WireMessage,
): Promise<void> {
  const cs = socket.data
  switch (msg.type) {
    case 'ping':
      return
    case 'take_writer': {
      cs.label = msg.client_label || cs.label || 'unknown'
      // Force-revoke existing holder if requested.
      if (state.writer && state.writer !== socket) {
        if (msg.force) {
          const oldCs = state.writer.data
          oldCs.isWriter = false
          const rev = {
            type: 'writer_revoked',
            new_holder_label: cs.label,
            epoch: state.writerEpoch + 1,
          } as const
          writeWire(state.writer, rev)
          slog(`writer revoked: ${oldCs.label} -> ${cs.label}`)
          state.writer = null
        } else {
          // Non-forced contention: refuse via ErrorMessage. Per contract,
          // ErrorMessage codes don't include "writer_busy" — fold into
          // protocol_violation (caller should always use force=true on boot).
          const err: ErrorMessage = {
            type: 'error',
            code: 'protocol_violation',
            message: `writer lease held by ${state.writer.data.label}; resend take_writer with force=true`,
          }
          writeWire(socket, err)
          return
        }
      }
      state.writer = socket
      cs.isWriter = true
      state.writerEpoch += 1
      cs.lastSeenOffset = msg.since_log_offset ?? null
      // Cycle 2: catchup is no longer its own frame type. Replay bytes go out
      // as a sequence of child_out frames (catchup:true) followed by a single
      // writer_granted frame carrying catchup meta. Reasons:
      //   1. Removes the giant single-frame "catchup" wire path that previously
      //      hit MAX_FRAME_BYTES on long sessions and corrupted JSON on
      //      multibyte boundaries.
      //   2. Reuses the prod-validated live tailer path on the client side.
      // Frame ordering (per design): N×child_out (catchup:true) → 1×writer_granted.
      // We emit child_out chunks synchronously here; Bun is single-threaded per
      // event loop tick, so tailer broadcast can't interleave between these
      // sync writeWire calls.
      let catchupLogOffset: number | undefined = undefined
      let catchupFromDisk: boolean | undefined = undefined
      if (cs.lastSeenOffset !== null) {
        const cu = buildCatchup(state, cfg, cs.lastSeenOffset)
        catchupLogOffset = cu.logOffset
        catchupFromDisk = cu.from_disk
        if (cu.data.length > 0) {
          // Split into ≤8KB chunks to match live tailer broadcast sizing. Slice
          // by UTF-8 byte length, walking codepoint-by-codepoint via Buffer to
          // avoid breaking a multibyte sequence mid-chunk.
          const buf = Buffer.from(cu.data, 'utf8')
          const totalBytes = buf.length
          const startOffset = cu.logOffset - totalBytes
          const CHUNK = 8 * 1024
          let pos = 0
          while (pos < totalBytes) {
            let end = Math.min(pos + CHUNK, totalBytes)
            // Back off to codepoint boundary. UTF-8 continuation bytes start
            // with 10xxxxxx (0x80..0xBF); a lead byte starts otherwise. We must
            // not split between a lead and its continuations.
            if (end < totalBytes) {
              while (end > pos && (buf[end] & 0xc0) === 0x80) end--
            }
            if (end === pos) {
              // Should be unreachable (a single codepoint > 8KB is malformed)
              // but guard against infinite loop: emit one byte and let the
              // client surface the malformed sequence.
              end = Math.min(pos + 1, totalBytes)
            }
            const slice = buf.slice(pos, end).toString('utf8')
            pos = end
            writeWire(socket, {
              type: 'child_out',
              data: slice,
              log_offset: startOffset + pos,
              catchup: true,
            })
          }
        }
      }
      writeWire(socket, {
        type: 'writer_granted',
        client_label: cs.label,
        epoch: state.writerEpoch,
        ...(catchupLogOffset !== undefined ? { catchup_log_offset: catchupLogOffset } : {}),
        ...(catchupFromDisk !== undefined ? { catchup_from_disk: catchupFromDisk } : {}),
      })
      slog(`writer granted: ${cs.label} epoch=${state.writerEpoch}${catchupLogOffset !== undefined ? ` catchup_to=${catchupLogOffset} from_disk=${catchupFromDisk}` : ''}`)
      return
    }
    case 'stdin': {
      if (!cs.isWriter || state.writer !== socket) {
        const err: ErrorMessage = {
          type: 'error',
          code: 'no_writer_lease',
          message: 'stdin sent without writer lease; send take_writer first',
          request_id: msg.request_id,
        }
        writeWire(socket, err)
        return
      }
      // Lazy spawn (Q2): if child died, respawn now before forwarding.
      if (!state.child) {
        try {
          spawnChild(state, cfg)
        } catch (e) {
          slog(`lazy respawn failed: ${(e as Error).message}`)
          const err: ErrorMessage = {
            type: 'error',
            code: 'child_stdin_write_failed',
            message: `child respawn failed: ${(e as Error).message}`,
            request_id: msg.request_id,
          }
          writeWire(socket, err)
          return
        }
      }
      let line: string
      try {
        line = buildStreamJsonUserLine(msg.data, { maxImageBytes: cfg.maxImageBytes })
      } catch (e) {
        const code = (e as Error & { code?: ErrorCode }).code ?? 'bad_stdin_payload'
        const err: ErrorMessage = {
          type: 'error',
          code: code as ErrorCode,
          message: (e as Error).message,
          request_id: msg.request_id,
        }
        writeWire(socket, err)
        return
      }
      try {
        const writer = state.child!.stdin as any
        // Cycle 2: respect Bun's actual stdin-writer backpressure
        // contract. Per probing on bun 1.3.13 (FileSink):
        //   - writer.write(s) returns a `number` (sync bytes accepted) when
        //     the kernel pipe buffer (~64KB on macOS) has room.
        //   - writer.write(s) returns a `Promise<number>` (async) when the
        //     buffer is full — must be awaited or subsequent writes pile up
        //     unbounded in Bun's internal queue.
        //   - writer.flush() returns `Promise<number>` when there are pending
        //     bytes to drain, `number` otherwise.
        //   - .once('drain', ...) does NOT exist; cycle 1 code that gated on
        //     `ok === false` was dead (Bun never returns boolean false), and
        //     the fallback path resolved immediately — effectively letting a
        //     5MB image base64 (~6.7MB string + '\n') get queued in one shot.
        // Fix: chunk the payload at 64KB, await any Promise returned from
        // write() OR flush(), and only mark activeTurnInFlight after the full
        // payload has been accepted by the kernel pipe. Pipe-buffer-sized
        // chunks keep memory bounded; awaiting the returned Promise honors
        // real backpressure instead of pretending to.
        // Encode once to bytes, then chunk on byte boundaries. Slicing the JS
        // string would split surrogate pairs at exactly the wrong offset and
        // turn 🌸-class emojis into U+FFFD on the wire (Bun encodes lone
        // surrogates to the replacement char). The receiving claude binary
        // reassembles raw bytes until '\n' so chunking is safe at any byte
        // offset (no per-chunk JSON validity needed); only the FULL line is
        // parsed.
        const payloadBytes = Buffer.from(line + '\n', 'utf8')
        const CHUNK_BYTES = 64 * 1024
        // AIC-127 cycle 2: every await on writer.write / writer.flush
        // MUST have a timeout. Bun 1.3.13 returns a pending thenable when the
        // child isn't draining stdin (e.g. claude binary stuck or input parser
        // wedged). Without Promise.race, handleClientMessage blocks forever
        // here — no child_stdin_write_failed ErrorMessage emitted, no
        // activeTurnInFlight rollback. Per acceptance "大 image 失败显性化",
        // timeout → reject → caught below → client sees the error frame.
        // 5s per chunk is generous for kernel pipe drain (typical sub-millisecond
        // when child is healthy) while still bounding wedge detection.
        const STDIN_WRITE_TIMEOUT_MS = 5000
        for (let pos = 0; pos < payloadBytes.length; pos += CHUNK_BYTES) {
          const slice = payloadBytes.subarray(pos, pos + CHUNK_BYTES)
          const ret = writer.write(slice)
          if (typeof ret === 'number' && ret < 0) {
            // AIC-127 cycle 3: scope explicitly requires "writer.write
            // 返 -1 → handler error 显性化". Bun's FileSink contract: a negative
            // return means the underlying fd is closed / shutting down. Without
            // this check we'd silently proceed and end up setting
            // activeTurnInFlight=true on a sink the child can never see.
            throw new Error(`child stdin write returned ${ret} (closed) at pos=${pos}`)
          }
          if (ret && typeof (ret as PromiseLike<unknown>).then === 'function') {
            await awaitOrTimeout(ret, `write chunk pos=${pos}`, STDIN_WRITE_TIMEOUT_MS)
          }
        }
        if (typeof writer.flush === 'function') {
          const fret = writer.flush()
          if (typeof fret === 'number' && fret < 0) {
            throw new Error(`child stdin flush returned ${fret} (closed)`)
          }
          if (fret && typeof (fret as PromiseLike<unknown>).then === 'function') {
            await awaitOrTimeout(fret, 'flush', STDIN_WRITE_TIMEOUT_MS)
          }
        }
        state.activeTurnInFlight = true
      } catch (e) {
        slog(`child stdin write failed: ${(e as Error).message}`)
        // restore in-flight flag so a child crash that comes after
        // this write failure doesn't tag during_active_turn=true spuriously.
        state.activeTurnInFlight = false
        const err: ErrorMessage = {
          type: 'error',
          code: 'child_stdin_write_failed',
          message: (e as Error).message,
          request_id: msg.request_id,
        }
        writeWire(socket, err)
      }
      return
    }
    case 'interrupt': {
      // Cycle 2: PM stop path. Semantics: stop the *turn*, keep the session
      // alive. We:
      //   1. Validate lease (only the writer can interrupt their own turn).
      //   2. If no child, ack immediately with child_alive=false.
      //   3. SIGINT child PID (not pgroup — supervisor lives in same group).
      //   4. Set interruptPending=true so tailer-broadcast gate drops residual
      //      child_out frames to attached writer clients until the next result
      //      event (or child exit). Bytes are still written to child.log + ring
      //      so reconnect catchup stays complete.
      //   5. Sync-ack the client. socket stays open.
      const reqId = (msg as { request_id?: string }).request_id
      if (!cs.isWriter || state.writer !== socket) {
        const err: ErrorMessage = {
          type: 'error',
          code: 'no_writer_lease',
          message: 'interrupt sent without writer lease; send take_writer first',
          request_id: reqId,
        }
        writeWire(socket, err)
        return
      }
      if (!state.child) {
        writeWire(socket, {
          type: 'interrupt_ack',
          child_alive: false,
          log_offset: state.childLogOffset,
          ...(reqId !== undefined ? { request_id: reqId } : {}),
        })
        slog(`interrupt: no child (already gone); ack child_alive=false`)
        return
      }
      state.interruptPending = true
      state.interruptBoundary = state.childLogOffset
      const childPid = state.child.pid
      let killOk = true
      let killErrMsg = ''
      try {
        // SIGINT the child PID directly (not the negative pgroup form). Per
        // design: SIGINT semantics — claude CLI's stream-json mode interrupts
        // the current turn. If the binary instead exits on SIGINT, the
        // existing onChildExit path handles it (see handleChildExit override).
        process.kill(childPid, 'SIGINT')
      } catch (e) {
        killOk = false
        killErrMsg = (e as Error).message
      }
      if (!killOk) {
        // Roll back the gate so future broadcasts aren't permanently muted.
        state.interruptPending = false
        state.interruptBoundary = null
        const err: ErrorMessage = {
          type: 'error',
          code: 'interrupt_failed',
          message: `SIGINT child pid=${childPid} failed: ${killErrMsg}`,
          request_id: reqId,
        }
        writeWire(socket, err)
        slog(`interrupt failed: ${killErrMsg}`)
        return
      }
      slog(`interrupt: SIGINT sent to child pid=${childPid} boundary=${state.interruptBoundary}`)
      writeWire(socket, {
        type: 'interrupt_ack',
        child_alive: true,
        log_offset: state.childLogOffset,
        ...(reqId !== undefined ? { request_id: reqId } : {}),
      })
      return
    }
    default: {
      // Server -> client message types arriving FROM client are protocol violations.
      const err: ErrorMessage = {
        type: 'error',
        code: 'protocol_violation',
        message: `unexpected client message type: ${msg.type}`,
      }
      writeWire(socket, err)
      return
    }
  }
}

/* ────── image / stream-json envelope ────── */

const MIME_BY_EXT: Record<string, string> = {
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
}

class TaggedError extends Error {
  code: ErrorCode
  constructor(code: ErrorCode, msg: string) {
    super(msg)
    this.code = code
  }
}

export function buildStreamJsonUserLine(
  payload: StdinPayload,
  cfg: { maxImageBytes: number },
): string {
  if (!payload || typeof payload.prompt !== 'string') {
    throw new TaggedError('bad_stdin_payload', 'StdinPayload.prompt must be a string')
  }
  const imagePaths = payload.imagePaths ?? []
  if (!Array.isArray(imagePaths)) {
    throw new TaggedError('bad_stdin_payload', 'StdinPayload.imagePaths must be an array')
  }
  if (payload.prompt === '' && imagePaths.length === 0) {
    throw new TaggedError('bad_stdin_payload', 'empty prompt with no images')
  }
  const blocks: any[] = []
  // Always include the text block (even if empty? — contract says required field; we
  // skip pushing only when prompt is "" AND there are images, so the user message is
  // image-only. Anthropic stream-json accepts content blocks without leading text.).
  if (payload.prompt !== '') {
    blocks.push({ type: 'text', text: payload.prompt })
  }
  for (const p of imagePaths) {
    if (typeof p !== 'string' || p.length === 0) {
      throw new TaggedError('bad_stdin_payload', `imagePaths entry not a string: ${JSON.stringify(p)}`)
    }
    const lower = p.toLowerCase()
    const dot = lower.lastIndexOf('.')
    const ext = dot >= 0 ? lower.slice(dot) : ''
    const mime = MIME_BY_EXT[ext]
    if (!mime) {
      throw new TaggedError('image_unsupported_mime', `unsupported image extension: ${ext || '<none>'} (${p})`)
    }
    let buf: Buffer
    try {
      buf = readFileSync(p)
    } catch (e) {
      throw new TaggedError('image_read_failed', `cannot read ${p}: ${(e as Error).message}`)
    }
    if (buf.length > cfg.maxImageBytes) {
      throw new TaggedError(
        'image_too_large',
        `${p} is ${buf.length}b, max ${cfg.maxImageBytes}b`,
      )
    }
    blocks.push({
      type: 'image',
      source: {
        type: 'base64',
        media_type: mime,
        data: buf.toString('base64'),
      },
    })
  }
  const envelope = {
    type: 'user',
    message: { role: 'user', content: blocks },
  }
  return JSON.stringify(envelope)
}

/* ────── tailer + broadcast ────── */

const TAILER_INTERVAL_MS = 20
const TAILER_BUF_BYTES = 64 * 1024

function startTailer(state: SupervisorState): void {
  const buf = Buffer.alloc(TAILER_BUF_BYTES)
  state.tailerTimer = setInterval(() => {
    try {
      const st = fstatSync(state.childLogReadFd)
      if (st.size <= state.childLogOffset) return
      // Loop until caught up (could be multiple buf-fulls in a tick).
      while (state.childLogOffset < st.size) {
        const want = Math.min(buf.length, st.size - state.childLogOffset)
        const n = readSync(state.childLogReadFd, buf, 0, want, state.childLogOffset)
        if (n <= 0) break
        state.childLogOffset += n
        // StringDecoder.write internally buffers any trailing
        // bytes that don't form a complete UTF-8 codepoint, so we never
        // emit U+FFFD-replaced half-chars mid-stream. The leftover bytes
        // get prepended automatically on the next .write() call.
        const text = state.tailerDecoder.write(buf.slice(0, n))
        if (text.length === 0) continue
        broadcastChildOut(state, text)
        // Concern 4: only sniff "type":"result" on full lines. The literal
        // can be split across two 64KB chunks (between `result"` and `,`),
        // and naive includes() on each chunk would miss it on both halves,
        // leaving activeTurnInFlight=true forever and tainting future
        // crash messages with during_active_turn=true.
        state.tailerLineBuf += text
        let nl = state.tailerLineBuf.indexOf('\n')
        while (nl !== -1) {
          const line = state.tailerLineBuf.slice(0, nl)
          state.tailerLineBuf = state.tailerLineBuf.slice(nl + 1)
          if (line.includes('"type":"result"')) {
            state.activeTurnInFlight = false
            // Cycle 2: result event closes the interrupt window. Drop the gate
            // and clear boundary so subsequent broadcasts flow normally. We do
            // NOT re-send the residual bytes that got dropped during the gate —
            // they remain in child.log + ring for reconnect catchup.
            if (state.interruptPending) {
              slog(`interrupt window closed by result event; clearing gate (boundary=${state.interruptBoundary})`)
              state.interruptPending = false
              state.interruptBoundary = null
            }
          }
          nl = state.tailerLineBuf.indexOf('\n')
        }
      }
    } catch (e) {
      slog(`tailer error: ${(e as Error).message}`)
    }
  }, TAILER_INTERVAL_MS)
}

export function broadcastChildOut(state: SupervisorState, chunk: string): void {
  // Push to ring (cap at catchupRingMax). Always — even when interruptPending
  // is gating the live broadcast — so that a future reconnect with
  // since_log_offset can catch up the byte stream completely.
  state.catchupRing.push({ data: chunk, logOffset: state.childLogOffset })
  while (state.catchupRing.length > state.catchupRingMax) {
    state.catchupRing.shift()
  }
  // Cycle 2 interrupt gate: while interruptPending is true (i.e. SIGINT
  // delivered, awaiting result-event boundary), suppress live broadcast to ALL
  // attached clients. Bytes still flow to disk + ring, but PM UI does not see
  // the dying turn keep emitting tokens. Cleared by tailer's result-event
  // detector (or by onChildExit if the binary exits on SIGINT).
  if (state.interruptPending) return
  const msg: ChildOutMessage = {
    type: 'child_out',
    data: chunk,
    log_offset: state.childLogOffset,
  }
  broadcastWire(state, msg)
}

/* ────── wire write helpers (cycle 3 backpressure-aware) ────── */

/** AIC-127 cycle 3: per-socket send queue hard cap. Overflow → socket.end().
 *  4 MiB is generous (full catchup payload ≈ 380KB; 10× headroom) but bounded
 *  so a genuinely-stalled client can't OOM the supervisor.
 *  Exported for verify scripts. */
export const MAX_SEND_QUEUE_BYTES = 4 * 1024 * 1024

/** AIC-127 cycle 3: drain the per-socket sendBuf queue against the kernel
 *  send buffer. Called both inline from writeWire (eager attempt) and from
 *  the SocketHandler `drain()` callback (when kernel buffer reports room).
 *
 *  Per Bun docs (bun.sh/docs/api/tcp): socket.write returns a sync number —
 *    n >= 0   bytes accepted by the kernel
 *    n < 0    socket closed / shutting down
 *    n < length  partial write; remaining bytes are NOT auto-queued by Bun
 *
 *  We stash the unwritten tail at the head of sendBuf and bail; drain() will
 *  call us back when the kernel buffer has room again. Without this, a
 *  partial write silently drops the tail and the client sees a truncated
 *  NDJSON line (`JSON.parse: Expected '}'`). This was the framer-spam bug
 *  observed during catchup's 47-sync-writes-of-8KB-each pattern. */
export function flushSendBuf(socket: Socket<ClientState>): void {
  const cs = socket.data
  if (!cs) return
  while (cs.sendBuf.length > 0) {
    const buf = cs.sendBuf[0]
    let wrote: number
    try {
      wrote = socket.write(buf)
    } catch (e) {
      slog(`flushSendBuf: socket.write threw (${cs.label}): ${(e as Error).message}`)
      cs.sendBuf = []
      cs.sendBufBytes = 0
      return
    }
    if (wrote < 0) {
      // Socket closed/shutting down — drop pending; nothing else will succeed.
      slog(`flushSendBuf: socket closed (${cs.label}); dropping ${cs.sendBufBytes}b pending`)
      cs.sendBuf = []
      cs.sendBufBytes = 0
      return
    }
    if (wrote < buf.length) {
      // Partial write — kernel buffer full. Stash tail at head; wait for drain().
      cs.sendBuf[0] = buf.subarray(wrote)
      cs.sendBufBytes -= wrote
      return
    }
    // Full frame written; pop and continue.
    cs.sendBuf.shift()
    cs.sendBufBytes -= buf.length
  }
}

export function writeWire(socket: Socket<ClientState>, msg: WireMessage): void {
  const cs = socket.data
  if (!cs) {
    slog(`writeWire: socket.data missing; dropping ${msg.type}`)
    return
  }
  const buf = Buffer.from(encodeMessage(msg), 'utf8')
  if (cs.sendBufBytes + buf.length > MAX_SEND_QUEUE_BYTES) {
    // Overflow: client is too slow / disconnected silently. Tear down the
    // socket so a runaway broadcast doesn't OOM the supervisor (mirrors the
    // 1 MiB framer cap policy on the receive side).
    slog(
      `writeWire: send queue overflow (${cs.sendBufBytes + buf.length}b > ${MAX_SEND_QUEUE_BYTES}b) ` +
      `for ${cs.label || '<unlabeled>'}; closing socket`,
    )
    cs.sendBuf = []
    cs.sendBufBytes = 0
    try { socket.end() } catch {}
    return
  }
  cs.sendBuf.push(buf)
  cs.sendBufBytes += buf.length
  flushSendBuf(socket)
}

function broadcastWire(state: SupervisorState, msg: WireMessage): void {
  // Per-socket queue path — each attached socket gets its own backpressure
  // accounting. Re-encoding per socket is fine: there's normally one attached
  // (server.ts SupervisorClient) and the buffer is small relative to catchup
  // payloads. Avoids cross-socket head-of-line blocking that a shared buffer
  // would impose.
  for (const sock of state.attached.keys()) {
    writeWire(sock, msg)
  }
}

/* ────── shutdown ────── */

export async function shutdown(
  state: SupervisorState,
  cfg: SupervisorConfig,
  signal: string,
): Promise<never> {
  if (state.shuttingDown) {
    // Already shutting down — give it a moment then hard-exit.
    setTimeout(() => process.exit(0), 200)
    // Type assertion: never returns.
    return new Promise<never>(() => {})
  }
  state.shuttingDown = true
  slog(`shutdown signal=${signal}`)
  // Stop timers.
  if (state.heartbeatTimer) {
    clearInterval(state.heartbeatTimer)
    state.heartbeatTimer = null
  }
  if (state.tailerTimer) {
    clearInterval(state.tailerTimer)
    state.tailerTimer = null
  }
  // Stop accepting new connections.
  try { state.server?.stop() } catch {}
  // Close child stdin so it can flush + exit naturally.
  try { (state.child?.stdin as any)?.end?.() } catch {}
  // Give child 500ms; then SIGTERM; then SIGKILL after another 500ms.
  await new Promise((r) => setTimeout(r, 500))
  if (state.child) {
    let stillAlive = false
    try { process.kill(state.child.pid, 0); stillAlive = true } catch {}
    if (stillAlive) {
      try { state.child.kill('SIGTERM') } catch {}
      await new Promise((r) => setTimeout(r, 500))
      try { process.kill(state.child.pid, 0); state.child.kill('SIGKILL') } catch {}
    }
  }
  // Unlink runtime files.
  for (const p of [state.paths.sockPath, state.paths.supPidPath, state.paths.childPidPath]) {
    try { if (existsSync(p)) unlinkSync(p) } catch {}
  }
  // Close fds.
  try { closeSync(state.childLogFd) } catch {}
  try { closeSync(state.childLogReadFd) } catch {}
  slog(`shutdown complete; exiting`)
  process.exit(0)
}

/* ────── env -> config ────── */

function parseExtraArgs(raw: string | undefined): string[] {
  if (!raw || raw === '') return []
  try {
    const v = JSON.parse(raw)
    if (!Array.isArray(v)) throw new Error('not an array')
    if (!v.every((x) => typeof x === 'string')) throw new Error('not all strings')
    return v
  } catch (e) {
    throw new Error(
      `CHAT_CHILD_EXTRA_ARGS (or AICOLLAB_CHAT_CHILD_EXTRA_ARGS) must be a JSON string array: ${(e as Error).message}`,
    )
  }
}

/** Read an env var, preferring the AICOLLAB_-prefixed name (fork) over the bare
 *  CHAT_* name (main repo). See "Fork prefix policy" comment at top of file. */
function envForkAware(aicollabKey: string, chatKey: string): string | undefined {
  const a = process.env[aicollabKey]
  if (a !== undefined && a !== '') return a
  const c = process.env[chatKey]
  if (c !== undefined && c !== '') return c
  return undefined
}

function configFromEnv(): SupervisorConfig {
  // AIC fork sync: ai-collab's ClaudeProvider stamps AICOLLAB_SUPERVISOR_DIR
  // (no CHAT_ infix) but AICOLLAB_CHAT_CHILD_* for the child-spawn vars. Match
  // both shapes explicitly rather than mechanically prefixing.
  const runtimeDir = envForkAware('AICOLLAB_SUPERVISOR_DIR', 'CHAT_SUPERVISOR_DIR')
  if (!runtimeDir) {
    throw new Error('CHAT_SUPERVISOR_DIR (or AICOLLAB_SUPERVISOR_DIR) env required')
  }
  const claudeCwd = envForkAware('AICOLLAB_CHAT_CHILD_CWD', 'CHAT_CHILD_CWD')
  if (!claudeCwd) {
    throw new Error('CHAT_CHILD_CWD (or AICOLLAB_CHAT_CHILD_CWD) env required')
  }
  const claudeBin = envForkAware('AICOLLAB_CHAT_CHILD_BIN', 'CHAT_CHILD_BIN') || 'claude'
  const extraArgs = parseExtraArgs(
    envForkAware('AICOLLAB_CHAT_CHILD_EXTRA_ARGS', 'CHAT_CHILD_EXTRA_ARGS'),
  )
  const model = envForkAware('AICOLLAB_CHAT_CHILD_MODEL', 'CHAT_CHILD_MODEL')
  // Compose canonical claude stream-json args; merge env extras on top.
  // Same flag set as chat-process.ts default spawn.
  const claudeArgs: string[] = [
    '--input-format', 'stream-json',
    '--output-format', 'stream-json',
    '--include-partial-messages',
    '--permission-mode', 'bypassPermissions',
    '--verbose',
  ]
  if (model) claudeArgs.push('--model', model)
  claudeArgs.push(...extraArgs)
  return {
    provider: envForkAware('AICOLLAB_CHAT_PROVIDER', 'CHAT_PROVIDER') || 'claude',
    runtimeDir,
    claudeBin,
    claudeArgs,
    claudeCwd,
    preResumeMarkerPath: envForkAware('AICOLLAB_CHAT_PRE_RESUME_MARKER', 'CHAT_PRE_RESUME_MARKER') || undefined,
    stateFilePath: envForkAware('AICOLLAB_CHAT_STATE_FILE', 'CHAT_STATE_FILE') || undefined,
    catchupRingLines: parseInt(envForkAware('AICOLLAB_CHAT_CATCHUP_RING', 'CHAT_CATCHUP_RING') || '200', 10) || 200,
    maxImageBytes: parseInt(
      envForkAware('AICOLLAB_CHAT_MAX_IMAGE_BYTES', 'CHAT_MAX_IMAGE_BYTES') || String(5 * 1024 * 1024),
      10,
    ) || 5 * 1024 * 1024,
  }
}

/* ────── main ────── */

export async function main(cfg: SupervisorConfig): Promise<void> {
  // 1. Ensure runtime dir exists + log path before we can slog properly.
  try {
    mkdirSync(cfg.runtimeDir, { recursive: true })
  } catch (e) {
    throw new Error(`mkdir ${cfg.runtimeDir} failed: ${(e as Error).message}`)
  }
  const paths = derivePaths(cfg.runtimeDir)
  _supLogPathForLogger = paths.supLogPath
  slog(`starting; provider=${cfg.provider} runtimeDir=${cfg.runtimeDir}`)

  // 2. Reclaim pidfile (collision/stale handling). cycle 5: async for socket probe.
  await reclaimRuntimeFiles(paths)

  // 3. If previous child.pid points to an alive process, SIGTERM it before we
  //    spawn a new one. Old child belongs to the dead supervisor, not us.
  try {
    if (existsSync(paths.childPidPath)) {
      const raw = readFileSync(paths.childPidPath, 'utf8').trim()
      const oldChild = parseInt(raw, 10)
      if (!Number.isNaN(oldChild) && oldChild > 0) {
        let alive = false
        try { process.kill(oldChild, 0); alive = true } catch {}
        if (alive) {
          slog(`prior child pid=${oldChild} still alive; sending SIGTERM`)
          try { process.kill(oldChild, 'SIGTERM') } catch {}
          // Give it 500ms.
          await new Promise((r) => setTimeout(r, 500))
          try { process.kill(oldChild, 0); process.kill(oldChild, 'SIGKILL') } catch {}
        }
      }
      try { unlinkSync(paths.childPidPath) } catch {}
    }
  } catch (e) {
    slog(`prior child reclaim threw (continuing): ${(e as Error).message}`)
  }

  // 4. Open child.log fds (append for write, read for tailer).
  let childLogFd: number
  let childLogReadFd: number
  try {
    childLogFd = openSync(paths.childLogPath, 'a')
  } catch (e) {
    throw new Error(`open child.log (append) failed: ${(e as Error).message}`)
  }
  try {
    childLogReadFd = openSync(paths.childLogPath, 'r')
  } catch (e) {
    try { closeSync(childLogFd) } catch {}
    throw new Error(`open child.log (read) failed: ${(e as Error).message}`)
  }
  // Start tailer at END of file — we don't replay history into newly attached
  // sockets unless they explicitly request via since_log_offset.
  const startOffset = (() => {
    try { return fstatSync(childLogReadFd).size } catch { return 0 }
  })()

  // 5. Initialize state.
  const state: SupervisorState = {
    pid: process.pid,
    paths,
    child: null,
    childSpawnId: 'pending',
    childLogFd,
    childLogReadFd,
    childLogOffset: startOffset,
    catchupRing: [],
    catchupRingMax: cfg.catchupRingLines,
    attached: new Map(),
    writer: null,
    writerEpoch: 0,
    activeTurnInFlight: false,
    interruptPending: false,
    interruptBoundary: null,
    server: null,
    heartbeatTimer: null,
    tailerTimer: null,
    tailerDecoder: new StringDecoder('utf8'),
    tailerLineBuf: '',
    shuttingDown: false,
  }

  // 6. Spawn the first child. Failure here is fatal — supervisor without a child
  //    on first boot is useless (lazy respawn only kicks in for *subsequent* deaths).
  try {
    spawnChild(state, cfg)
  } catch (e) {
    slog(`initial spawn failed: ${(e as Error).message}`)
    try { closeSync(childLogFd) } catch {}
    try { closeSync(childLogReadFd) } catch {}
    try { unlinkSync(paths.supPidPath) } catch {}
    process.exit(3)
  }

  // 7. Start unix socket server.
  try {
    state.server = startSocketServer(state, cfg)
  } catch (e) {
    slog(`socket server start failed: ${(e as Error).message}`)
    try { state.child?.kill('SIGTERM') } catch {}
    try { closeSync(childLogFd) } catch {}
    try { closeSync(childLogReadFd) } catch {}
    try { unlinkSync(paths.supPidPath) } catch {}
    try { unlinkSync(paths.childPidPath) } catch {}
    process.exit(1)
  }

  // 8. Start child.log tailer.
  startTailer(state)

  // 9. Heartbeat (every 1s) — useful for ops to confirm daemon still alive.
  state.heartbeatTimer = setInterval(() => {
    let childAlive = false
    if (state.child) {
      try { process.kill(state.child.pid, 0); childAlive = true } catch {}
    }
    slog(`hb attached=${state.attached.size} child_alive=${childAlive} offset=${state.childLogOffset}`)
  }, 1000)

  // 10. Signal handlers.
  const onSig = (sig: string) => {
    void shutdown(state, cfg, sig)
  }
  process.on('SIGTERM', () => onSig('SIGTERM'))
  process.on('SIGINT', () => onSig('SIGINT'))
  // Detached robustness: SIGHUP would otherwise kill us if the launching terminal
  // closes. POC h1 confirmed detached:true + ignore-SIGHUP is what survives.
  process.on('SIGHUP', () => { slog('ignoring SIGHUP') })
  process.on('uncaughtException', (err) => {
    slog(`uncaughtException: ${(err as Error).stack || err}`)
    void shutdown(state, cfg, 'uncaughtException').catch(() => process.exit(4))
  })
  process.on('unhandledRejection', (reason) => {
    slog(`unhandledRejection: ${reason instanceof Error ? reason.stack : String(reason)}`)
    void shutdown(state, cfg, 'unhandledRejection').catch(() => process.exit(4))
  })

  slog(`ready`)
  // Hold the event loop forever (handled by setInterval timers + socket server).
}

// Entry point when invoked as `bun run chat_supervisor.ts`.
if (import.meta.main) {
  try {
    const cfg = configFromEnv()
    void main(cfg).catch((e) => {
      // main() throws on hard init failure — print + exit.
      // (Once main() resolves it stays alive via timers; rejection is fatal.)
      // eslint-disable-next-line no-console
      console.error(`[sup] main rejected: ${(e as Error).stack || e}`)
      process.exit(1)
    })
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error(`[sup] config error: ${(e as Error).message}`)
    process.exit(1)
  }
}
