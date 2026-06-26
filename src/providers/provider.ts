// AIC-116: AgentProvider interface — unified abstraction over CLI agent stdio protocols.
//
// Inspired by multica's `server/pkg/agent/agent.go`. Each provider (Claude / Codex /
// ...) implements this interface; the rest of the server routes through `provider.send()`
// and the unified `AgentEvent` union (normalized inside each provider), never touching
// the raw underlying protocol.
//
// Design: long-lived subprocess (operator choice — ai-collab's value proposition is
// persistent agent context, not per-task agent-as-tool). The provider spawns its subprocess
// lazily on the first `send()`, then reuses it across many sends via the captured
// session_id. State that needs to survive across server restarts (session_id) is persisted
// by the provider itself via `stateFilePath`.

/**
 * AgentEvent — unified event union. Providers normalize their native event stream into
 * this shape; the server only branches on `type`. Use `raw` to access provider-native
 * fields when a feature is not yet abstracted.
 */
export type AgentEvent =
  | { type: 'system_init'; session_id?: string; raw: any }
  | { type: 'assistant'; text?: string; tool_uses?: Array<{ name: string; input: any }>; thinking?: string; raw: any }
  | { type: 'result'; usage?: AgentUsage; raw: any }
  | { type: 'error'; message: string; raw?: any }
  | { type: 'other'; raw: any }

export type AgentUsage = {
  input_tokens?: number
  output_tokens?: number
  cache_read_input_tokens?: number
  cache_creation_input_tokens?: number
}

export type AgentSendOpts = {
  /** Future: file/image attachments passed through to providers that support them. */
  attachments?: Array<{ type: 'file' | 'image'; path: string }>
}

export type AgentCapabilities = {
  /** Stable id for switch/case in the server. New providers add new strings. */
  providerName: 'claude' | 'codex' | string
  /** Provider preserves session id across `send()` calls (resume support). */
  supportsSessionResume: boolean
  /** Provider accepts file/image attachments via `AgentSendOpts.attachments`. */
  supportsAttachments: boolean
  /** Provider emits `assistant` events containing tool_use blocks. */
  supportsToolUse: boolean
  /** Provider emits a `thinking` field on `assistant` events (Claude only as of 2026-06). */
  supportsThinking: boolean
  /** Provider emits partial / streaming chunks (e.g. Anthropic content_block_delta). */
  supportsPartialEvents: boolean
}

export type AgentError = {
  kind: 'spawn_failed' | 'process_exited' | 'parse_error' | 'protocol_error' | 'session_expired' | 'timeout'
  message: string
  raw?: any
}

export type AgentProviderConfig = {
  /** Human-readable label for log prefixes. */
  label?: string
  /** Working directory for the spawned subprocess. */
  cwd?: string
  /** Path to persist per-agent state (session_id, latest sid path, etc). Provider-specific shape. */
  stateFilePath?: string
  /** Override binary path. Defaults to PATH lookup of the provider's expected binary. */
  binaryPath?: string
  /** Extra args appended to the spawn command line. */
  extraArgs?: string[]
  /** Event callback (overrides `provider.onEvent()` registration). */
  onEvent?: (ev: AgentEvent) => void | Promise<void>
  /** Error callback (overrides `provider.onError()` registration). */
  onError?: (err: AgentError) => void
}

/**
 * AgentProvider — implement this for each new CLI you want ai-collab to talk to.
 * See `claude.ts` (Anthropic stream-json) and `codex.ts` (Codex JSON-RPC 2.0) for
 * canonical examples.
 */
export interface AgentProvider {
  /**
   * Send a user message into the long-lived subprocess. Lazy-spawns on first call;
   * subsequent calls reuse the same process via the captured `session_id`. Resolves
   * once the message has been accepted by the subprocess (stdin write + handshake
   * complete); downstream agent events stream back asynchronously through `onEvent`.
   *
   * `send()` rejects when the agent CANNOT accept this turn — spawn failure, binary
   * missing, initialize handshake error, thread start/resume protocol error. The
   * dispatch layer uses this to mark delivery as failed and skip optimistic delivered
   * bookkeeping.
   *
   * `send()` resolves (does NOT reject) on mid-turn issues that fire AFTER the agent
   * accepted input: parse errors on downstream events, turn-level protocol errors,
   * session expiry while a turn is in flight. These surface through `onError`.
   */
  send(text: string, opts?: AgentSendOpts): Promise<void>

  /** Hard-interrupt the active turn (SIGKILL). No-op when nothing is running. */
  interrupt(): Promise<boolean>

  /** Graceful shutdown — no-op for per-task spawn providers since each `send()` already cleans up. */
  close(): Promise<void>

  /** True while a `send()` call is in progress (subprocess alive). */
  readonly isAlive: boolean

  /** Most recent session id captured from this provider (null when never set). */
  readonly sessionId: string | null

  /** Provider feature flags — server introspects to gate features. */
  capabilities(): AgentCapabilities

  /** Register / replace the event handler. Single listener; latest wins. */
  onEvent(cb: (ev: AgentEvent) => void | Promise<void>): void

  /** Register / replace the error handler. Single listener; latest wins. */
  onError(cb: (err: AgentError) => void): void
}
