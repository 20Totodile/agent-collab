// AIC-124 frame.ts — NDJSON line framing + WireMessage envelope.
//
// Shared by chat_supervisor.ts (server side of unix socket) and supervisor_client.ts
// (server.ts side). Pure functions / class, no Bun-specific API — must be reusable by
// both sides and unit-testable under plain `bun run`.
//
// Design notes (from AIC-124 H3 POC):
// - Bun socket `data` callback fires per-chunk, not per-message. One logical NDJSON
//   line can arrive split across N callbacks; multiple lines can arrive concatenated.
//   A per-connection byte buffer (Buffer) is mandatory.
// - 1 MiB hard cap per connection buffer (H3 recommendation). Overrun = drop the
//   connection — caller is responsible for closing the socket after seeing the throw.
// - Empty lines silently skipped (NDJSON convention).
// - Errors are explicit (Wiki rule 1 / 12 rules #12): malformed JSON throws
//   InvalidFrameError with a raw line snippet (first 100 chars) so the caller can log
//   what actually came across the wire instead of guessing.
//
// Cycle 2 (2026-06-26): LineFramer.feed previously did `chunk.toString('utf8')`
// per-chunk and concatenated to a `string` buffer. A multibyte UTF-8 codepoint
// straddling a chunk boundary would be replaced with U+FFFD inside Buffer.toString,
// permanently corrupting the JSON before parse — manifested as InvalidFrameError
// "Expected '}'" on catchup ≥180KB containing CJK. Now: accumulate raw Buffer
// bytes, run them through a per-instance StringDecoder('utf8') which holds back
// partial multibyte sequences across calls, then line-split the resulting string.

import { StringDecoder } from 'node:string_decoder'

/* ────── WireMessage union (mirrors AIC-124 contract §1) ────── */

export type WireMessage =
  | HelloMessage
  | StdinMessage
  | ChildOutMessage
  // CatchupMessage 移除 (cycle 2)：catchup 数据直接走 ChildOutMessage 帧序列，
  // catchup meta 折叠进 WriterGrantedMessage。复用 live 通路、避免大单帧。
  | TakeWriterMessage
  | WriterGrantedMessage
  | WriterRevokedMessage
  | PingMessage
  | ErrorMessage
  | ChildCrashedMessage
  | InterruptMessage
  | InterruptAckMessage

export interface HelloMessage {
  type: 'hello'
  supervisor_pid: number
  child_pid: number
  child_spawn_id: string
  child_log_path: string
  child_log_offset: number
  provider: string
  protocol_version: 1
}

export interface StdinPayload {
  /** User text prompt. Required (may be empty string if image-only turn). */
  prompt: string
  /** Absolute filesystem paths to image attachments. Supervisor reads + base64-encodes. */
  imagePaths?: string[]
}

export interface StdinMessage {
  type: 'stdin'
  data: StdinPayload
  /** Optional client-side correlation id, echoed back in any ErrorMessage. */
  request_id?: string
}

export interface ChildOutMessage {
  type: 'child_out'
  data: string
  log_offset: number
  /** Cycle 2: true 当此帧属于 take_writer 触发的回放序列。
   *  Client 侧无需特殊处理——同条路径转给 onEvent；标志仅用于日志/可观测性。
   *  Live tailer broadcast 不带或带 false。 */
  catchup?: boolean
}

export interface TakeWriterMessage {
  type: 'take_writer'
  client_label: string
  since_log_offset?: number
  force?: boolean
}

export interface WriterGrantedMessage {
  type: 'writer_granted'
  client_label: string
  epoch: number
  /** Cycle 2: supervisor 把 catchup meta 折叠进 writer_granted。
   *  catchup_log_offset = 回放完成后 client 应认为 lastChildLogOffset 已推进到的目标。
   *  catchup_from_disk = 此次回放是否越过了 ring 边界（运维诊断用，client 仅做日志）。
   *  当 take_writer 不带 since_log_offset 时这两字段缺省。 */
  catchup_log_offset?: number
  catchup_from_disk?: boolean
}

export interface WriterRevokedMessage {
  type: 'writer_revoked'
  new_holder_label: string
  epoch: number
}

export interface PingMessage {
  type: 'ping'
}

export type ErrorCode =
  | 'no_writer_lease'
  | 'bad_stdin_payload'
  | 'image_read_failed'
  | 'image_too_large'
  | 'image_unsupported_mime'
  | 'child_stdin_write_failed'
  | 'protocol_violation'
  | 'interrupt_failed'

export interface ErrorMessage {
  type: 'error'
  code: ErrorCode
  message: string
  request_id?: string
}

export interface ChildCrashedMessage {
  type: 'child_crashed'
  child_pid: number
  exit_code: number | null
  signal: string | null
  during_active_turn: boolean
  tail_log: string
}

/** Cycle 2: PM stop 路径——client→server，请求中断当前 turn 而不杀 session。
 *  Supervisor 收到后向 child SIGINT、设 interruptPending gate 抑制残余 broadcast、
 *  同步回 InterruptAckMessage。socket 不断，下次 send 走原 lazy-respawn 路径。 */
export interface InterruptMessage {
  type: 'interrupt'
  /** 与对应 interrupt_ack 关联，可选。 */
  request_id?: string
}

/** Cycle 2: supervisor→client interrupt 同步回执。 */
export interface InterruptAckMessage {
  type: 'interrupt_ack'
  /** SIGINT 后 child 是否仍存活。false → client 应预期下次 send 触发 lazy respawn。 */
  child_alive: boolean
  /** 收到 interrupt 时的 child.log offset；client 用来对齐"中断点之前的事件保留，之后的丢弃"。 */
  log_offset: number
  request_id?: string
}

/* ────── Constants ────── */

/** Hard cap on per-connection NDJSON buffer. H3 rec: 1 MiB. */
export const MAX_FRAME_BYTES = 1024 * 1024

/* ────── Errors ────── */

/** Thrown by LineFramer.feed when buffer exceeds MAX_FRAME_BYTES. Caller must drop conn. */
export class FrameTooLargeError extends Error {
  readonly bytes: number
  constructor(bytes: number) {
    super(`frame buffer exceeded ${MAX_FRAME_BYTES} bytes (got ${bytes})`)
    this.name = 'FrameTooLargeError'
    this.bytes = bytes
  }
}

/** Thrown by LineFramer.feed when a complete line fails JSON.parse. Snippet = first 100 chars. */
export class InvalidFrameError extends Error {
  readonly snippet: string
  readonly cause: Error
  constructor(line: string, cause: Error) {
    const snippet = line.length > 100 ? line.slice(0, 100) + '…' : line
    super(`invalid NDJSON frame: ${cause.message} | raw: ${snippet}`)
    this.name = 'InvalidFrameError'
    this.snippet = snippet
    this.cause = cause
  }
}

/* ────── Encode ────── */

/**
 * Serialize one WireMessage to a single NDJSON line (trailing '\n' included).
 * Used by both supervisor and client for every write to the socket.
 */
export function encodeMessage(msg: WireMessage): string {
  return JSON.stringify(msg) + '\n'
}

/* ────── Decode / framing ────── */

/**
 * Per-connection NDJSON line framer.
 *
 * Usage:
 *   const framer = new LineFramer()
 *   socket.on('data', chunk => {
 *     try {
 *       const msgs = framer.feed(chunk)
 *       for (const m of msgs) handle(m)
 *     } catch (e) {
 *       if (e instanceof FrameTooLargeError) socket.end()
 *       else if (e instanceof InvalidFrameError) log.warn(e.message)
 *       else throw e
 *     }
 *   })
 *
 * Invariants:
 * - feed() is the only state-mutating method; not thread-safe but Bun is single-threaded per socket.
 * - Returns parsed messages in arrival order.
 * - Throws FrameTooLargeError BEFORE returning any messages if cap is exceeded — caller should
 *   close the socket immediately. Any pre-existing buffer content is preserved on the instance
 *   so a debugger can inspect it; callers should drop the framer along with the socket.
 * - Throws InvalidFrameError on the FIRST malformed line in the chunk. Lines already parsed
 *   successfully BEFORE the bad one are NOT returned (caller loses them) — this is the
 *   "fail loud" policy: a peer that sends garbage has violated protocol and the connection
 *   should be reset anyway. If a more lenient policy is ever needed, swap to a parseErrors[]
 *   out-param like the contract sketched.
 */
export class LineFramer {
  /** Accumulator for partial line text (already UTF-8 decoded codepoints). Public
   *  for diagnostic inspection only. Cycle 2: previously the only buffer; we now
   *  also hold partial *bytes* inside `decoder` until they form a complete
   *  codepoint, so this string is always valid UTF-8 (no spurious U+FFFD). */
  buf: string = ''

  /** Cycle 2: UTF-8 stream decoder. `write()` returns only complete codepoints
   *  and internally holds back any trailing partial multibyte sequence until the
   *  next call. Prevents the catchup ≥180KB-with-CJK corruption bug where
   *  per-chunk `Buffer.toString('utf8')` replaced split codepoints with U+FFFD,
   *  poisoning the embedded JSON string and making JSON.parse barf. */
  private decoder: StringDecoder = new StringDecoder('utf8')

  /** Cycle 2: running count of bytes currently buffered (decoder's hidden
   *  partial bytes + the buf string's UTF-8 byte length). Used for MAX_FRAME_BYTES
   *  cap. We measure bytes (not JS chars) because that's what the wire actually
   *  consumes — a single emoji is ~4 bytes but 2 JS chars. */
  private bufBytes: number = 0

  /**
   * Append `chunk`, split on '\n', JSON.parse each complete line.
   * Returns parsed messages in order. Throws FrameTooLargeError or InvalidFrameError.
   */
  feed(chunk: Buffer | Uint8Array | string): WireMessage[] {
    // Normalize input to Buffer so StringDecoder can hold back partial multibyte
    // sequences across calls. A pre-decoded string input is treated as already
    // decoded — we don't re-encode/decode (would be lossy if it's a deliberate
    // test fixture with no multibyte content).
    let text: string
    let addedBytes: number
    if (typeof chunk === 'string') {
      text = chunk
      addedBytes = Buffer.byteLength(chunk, 'utf8')
    } else {
      const buf = chunk instanceof Buffer ? chunk : Buffer.from(chunk)
      addedBytes = buf.length
      text = this.decoder.write(buf)
    }

    this.buf += text
    this.bufBytes += addedBytes

    if (this.bufBytes > MAX_FRAME_BYTES) {
      // Per rule 12: surface failure explicitly. Buffer is preserved on instance for inspection;
      // caller is expected to drop the socket + framer together.
      throw new FrameTooLargeError(this.bufBytes)
    }

    const out: WireMessage[] = []
    let nl: number
    while ((nl = this.buf.indexOf('\n')) >= 0) {
      const line = this.buf.slice(0, nl)
      this.buf = this.buf.slice(nl + 1)
      // Adjust running byte count by what we just removed (line + the '\n').
      this.bufBytes -= Buffer.byteLength(line, 'utf8') + 1
      if (line.length === 0) continue // NDJSON: skip blank lines silently
      let parsed: unknown
      try {
        parsed = JSON.parse(line)
      } catch (e) {
        throw new InvalidFrameError(line, e as Error)
      }
      // Light validation: every WireMessage has a string `type`. Anything else is a protocol
      // violation — surface it the same way as JSON.parse failure so callers have one code path.
      if (parsed === null || typeof parsed !== 'object' || typeof (parsed as any).type !== 'string') {
        throw new InvalidFrameError(line, new Error('frame missing string `type` field'))
      }
      out.push(parsed as WireMessage)
    }
    return out
  }
}

/* ────── Inline self-check ────── */
// Run with `bun run frame.ts`. No assertion library — minimal hand-rolled checks.

if (import.meta.main) {
  let failed = 0
  function ok(name: string, cond: boolean, detail?: string): void {
    if (cond) {
      console.log(`PASS  ${name}`)
    } else {
      failed++
      console.log(`FAIL  ${name}${detail ? ' — ' + detail : ''}`)
    }
  }

  // 1. encode → feed round-trip for every message kind
  const samples: WireMessage[] = [
    {
      type: 'hello',
      supervisor_pid: 1234,
      child_pid: 5678,
      child_spawn_id: '01J0000000000000000000ABCD',
      child_log_path: '/tmp/child.log',
      child_log_offset: 0,
      provider: 'agent',
      protocol_version: 1,
    },
    { type: 'stdin', data: { prompt: 'hello', imagePaths: ['/tmp/a.png'] }, request_id: 'r1' },
    { type: 'child_out', data: 'chunk\n', log_offset: 6 },
    { type: 'child_out', data: 'replay', log_offset: 100, catchup: true },
    { type: 'take_writer', client_label: 'server.ts.agent', since_log_offset: 42, force: true },
    { type: 'writer_granted', client_label: 'server.ts.agent', epoch: 3 },
    {
      type: 'writer_granted',
      client_label: 'server.ts.agent',
      epoch: 4,
      catchup_log_offset: 200,
      catchup_from_disk: false,
    },
    { type: 'writer_revoked', new_holder_label: 'other', epoch: 4 },
    { type: 'ping' },
    { type: 'error', code: 'bad_stdin_payload', message: 'nope', request_id: 'r1' },
    { type: 'error', code: 'interrupt_failed', message: 'EINVAL', request_id: 'r2' },
    {
      type: 'child_crashed',
      child_pid: 9999,
      exit_code: 1,
      signal: null,
      during_active_turn: true,
      tail_log: 'panic: oom',
    },
    { type: 'interrupt', request_id: 'r3' },
    { type: 'interrupt_ack', child_alive: true, log_offset: 1234, request_id: 'r3' },
    { type: 'interrupt_ack', child_alive: false, log_offset: 1234 },
  ]

  const framer1 = new LineFramer()
  let roundTripped: WireMessage[] = []
  for (const m of samples) {
    const wire = encodeMessage(m)
    ok(`encodeMessage ends in \\n (${m.type})`, wire.endsWith('\n'))
    roundTripped = roundTripped.concat(framer1.feed(wire))
  }
  ok(
    'round-trip count matches',
    roundTripped.length === samples.length,
    `got ${roundTripped.length} want ${samples.length}`,
  )
  ok(
    'round-trip JSON deep-equal',
    JSON.stringify(roundTripped) === JSON.stringify(samples),
  )

  // 2. Multi-message in single chunk, no trailing newline on last (= partial)
  const framer2 = new LineFramer()
  const blob =
    encodeMessage({ type: 'ping' }) +
    encodeMessage({ type: 'ping' }) +
    '{"type":"pin' // partial
  const m2 = framer2.feed(blob)
  ok('two complete msgs from blended chunk', m2.length === 2)
  ok('partial line retained in buffer', framer2.buf === '{"type":"pin')

  // Finish the partial in a second feed call
  const m2b = framer2.feed('g"}\n')
  ok('partial completed across feeds', m2b.length === 1 && m2b[0].type === 'ping')
  ok('buffer empty after completion', framer2.buf === '')

  // 3. Single message split byte-by-byte
  const framer3 = new LineFramer()
  const wire3 = encodeMessage({ type: 'ping' })
  let collected: WireMessage[] = []
  for (const ch of wire3) collected = collected.concat(framer3.feed(ch))
  ok('byte-by-byte assembly yields exactly one msg', collected.length === 1)

  // 4. Blank lines skipped
  const framer4 = new LineFramer()
  const m4 = framer4.feed('\n\n' + encodeMessage({ type: 'ping' }) + '\n')
  ok('blank lines skipped, one ping survives', m4.length === 1 && m4[0].type === 'ping')

  // 5. Invalid JSON throws InvalidFrameError with snippet
  const framer5 = new LineFramer()
  let invErr: unknown = null
  try {
    framer5.feed('not-json-at-all\n')
  } catch (e) {
    invErr = e
  }
  ok('InvalidFrameError thrown on garbage', invErr instanceof InvalidFrameError)
  ok(
    'InvalidFrameError carries snippet',
    invErr instanceof InvalidFrameError && invErr.snippet.includes('not-json-at-all'),
  )

  // 5b. Valid JSON without `type` field also rejected
  const framer5b = new LineFramer()
  let typeErr: unknown = null
  try {
    framer5b.feed('{"foo":1}\n')
  } catch (e) {
    typeErr = e
  }
  ok('missing `type` field is a protocol violation', typeErr instanceof InvalidFrameError)

  // 6. Cap exceeded throws FrameTooLargeError
  const framer6 = new LineFramer()
  let capErr: unknown = null
  try {
    // Feed > 1 MiB with no newline. Use repeat to avoid allocating 10 MiB string in source.
    const big = 'a'.repeat(MAX_FRAME_BYTES + 1)
    framer6.feed(big)
  } catch (e) {
    capErr = e
  }
  ok('FrameTooLargeError thrown on > 1 MiB no-newline buffer', capErr instanceof FrameTooLargeError)
  ok(
    'FrameTooLargeError carries bytes',
    capErr instanceof FrameTooLargeError && capErr.bytes > MAX_FRAME_BYTES,
  )

  // 7. Buffer chunk type tolerance (Buffer + Uint8Array + string)
  const framer7 = new LineFramer()
  const out7a = framer7.feed(Buffer.from(encodeMessage({ type: 'ping' }), 'utf8'))
  const out7b = framer7.feed(new TextEncoder().encode(encodeMessage({ type: 'ping' })))
  ok('Buffer input parses', out7a.length === 1 && out7a[0].type === 'ping')
  ok('Uint8Array input parses', out7b.length === 1 && out7b[0].type === 'ping')

  // 8. Cycle 2 regression: UTF-8 multibyte codepoint split across chunk boundary
  //    must NOT poison the JSON. '中' = E4 B8 AD (3 bytes). Split so chunk1 ends
  //    with the first 2 bytes (E4 B8) and chunk2 starts with the trailing byte
  //    (AD). Pre-cycle-2 framer would call toString('utf8') on chunk1 → replace
  //    the partial sequence with U+FFFD inside the JSON string, then prepend the
  //    raw AD onto the next chunk as more garbage. Result: JSON.parse blew up
  //    with "Expected '}'" on catchup ≥180KB containing CJK.
  const framer8 = new LineFramer()
  const fullLine = encodeMessage({ type: 'child_out', data: '你好中文', log_offset: 42 })
  const fullBuf = Buffer.from(fullLine, 'utf8')
  // Find the '中' codepoint's first byte (E4) and split right after the SECOND byte.
  let splitAt = -1
  for (let i = 0; i < fullBuf.length - 2; i++) {
    if (fullBuf[i] === 0xe4 && fullBuf[i + 1] === 0xb8 && fullBuf[i + 2] === 0xad) {
      splitAt = i + 2 // chunk1 includes E4 B8, chunk2 starts at AD
      break
    }
  }
  ok('found 中 codepoint in test fixture', splitAt > 0)
  const chunk1 = fullBuf.slice(0, splitAt)
  const chunk2 = fullBuf.slice(splitAt)
  let utf8Err: unknown = null
  let out8: WireMessage[] = []
  try {
    out8 = out8.concat(framer8.feed(chunk1))
    out8 = out8.concat(framer8.feed(chunk2))
  } catch (e) {
    utf8Err = e
  }
  ok('UTF-8 split-chunk feed does not throw', utf8Err === null,
    utf8Err instanceof Error ? utf8Err.message : String(utf8Err))
  ok('UTF-8 split-chunk yields exactly one msg', out8.length === 1)
  ok(
    'UTF-8 split-chunk preserves multibyte data byte-perfect',
    out8.length === 1 &&
      out8[0].type === 'child_out' &&
      (out8[0] as ChildOutMessage).data === '你好中文',
  )

  // 9. Cycle 2 regression: lots of CJK + emoji across many tiny chunks, simulating
  //    Bun socket fragmentation on a catchup payload. Encode one big line, then feed
  //    one byte at a time and verify exactly one parsed frame at the end.
  const framer9 = new LineFramer()
  const heavy = '🌸中文测试' + 'A'.repeat(50) + '日本語🎉' + 'B'.repeat(50)
  const heavyLine = encodeMessage({ type: 'child_out', data: heavy, log_offset: 999 })
  const heavyBuf = Buffer.from(heavyLine, 'utf8')
  let heavyErr: unknown = null
  let out9: WireMessage[] = []
  try {
    for (let i = 0; i < heavyBuf.length; i++) {
      out9 = out9.concat(framer9.feed(heavyBuf.slice(i, i + 1)))
    }
  } catch (e) {
    heavyErr = e
  }
  ok('byte-by-byte multibyte feed does not throw', heavyErr === null,
    heavyErr instanceof Error ? heavyErr.message : String(heavyErr))
  ok('byte-by-byte multibyte yields exactly one msg', out9.length === 1)
  ok(
    'byte-by-byte multibyte preserves data',
    out9.length === 1 &&
      out9[0].type === 'child_out' &&
      (out9[0] as ChildOutMessage).data === heavy,
  )

  if (failed === 0) {
    console.log('\nALL PASS')
  } else {
    console.log(`\n${failed} FAIL`)
    process.exit(1)
  }
}
