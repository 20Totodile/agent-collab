// AIC-129: TmuxProvider capture-pane sanitizer.
//
// capture-pane reads whatever bytes are currently rendered in the terminal — that
// will include env var dumps, API key paste-overs, ssh-add output, AWS_/GH_TOKEN
// exports from .zshrc reload, anything. Before we forward those bytes into the
// AgentEvent stream (which is persisted to chat.db + broadcast to web/iOS UI),
// the strict filter drops or rewrites the obvious leak patterns.
//
// Modes:
//   strict (default) — drop env-var-assignment lines + token-pattern lines; rewrite
//                      $HOME absolute path → ~ and $USER → <user>.
//   loose            — only rewrite $HOME/$USER, keep all lines.
//   off              — pass through unchanged.
//
// This is best-effort: it WILL miss novel secret formats. Operators who care about
// strict secret hygiene should not run tmux provider against sessions that touch
// secrets at all. README documents that constraint.

import { homedir, userInfo } from 'os'

export type FilterMode = 'strict' | 'loose' | 'off'

export function parseFilterMode(raw: string | undefined, fallback: FilterMode = 'strict'): FilterMode {
  const v = (raw || '').toLowerCase()
  if (v === 'strict' || v === 'loose' || v === 'off') return v
  return fallback
}

// Drop entire line if it matches an env-var assignment at the start (e.g. "FOO=bar",
// "export PATH=...", "my_secret='baz'"). We accept up to one leading "export " prefix.
// Match both upper and lower case names so lowercase shell variable conventions
// (e.g. `db_password=xxx`) get caught — POSIX env is uppercase but shell scripts
// routinely lowercase. Common false-positive (key=value in printed test output) is
// intentional in strict mode; operators who don't want it use AGENT*_TMUX_FILTER_MODE=loose.
const ENV_VAR_ASSIGN = /^\s*(export\s+)?[A-Za-z_][A-Za-z0-9_]*=/

// Drop entire line if it contains any of these token-like substrings anywhere.
// Patterns tuned for common false-negative avoidance (Anthropic / OpenAI / GitHub /
// AWS / generic bearer / JWT / private key blocks / ssh public-key lines).
const TOKEN_PATTERNS: RegExp[] = [
  /\bsk-[A-Za-z0-9_\-]{16,}/,            // Anthropic / OpenAI style
  /\bBearer\s+[A-Za-z0-9_\-\.]{12,}/i,    // generic bearer
  /\beyJ[A-Za-z0-9_\-\.]{10,}/,           // JWT prefix (base64 '{"') — chars + segment dots
  /\bghp_[A-Za-z0-9]{20,}/,                // GitHub PAT
  /\bAKIA[0-9A-Z]{16}/,                    // AWS access key id
  /\bssh-rsa\s+[A-Za-z0-9+/=]+/,           // ssh public key
  /\bssh-ed25519\s+[A-Za-z0-9+/=]+/,
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/,    // PEM block header
  /\bxox[abp]-[A-Za-z0-9-]{20,}/,          // Slack tokens
]

/**
 * Sanitize one frame of pane text. Returns the filtered text (line-aligned).
 *
 * Whole-line drops in strict mode are replaced by a single `[redacted]` marker so
 * the consumer sees that *something* was here without leaking content. We never
 * emit empty output when the input was non-empty (that would mask the fact that
 * the agent printed something).
 */
export function sanitizeFrame(text: string, mode: FilterMode): string {
  if (mode === 'off') return text
  const home = homedir()
  const user = userInfo().username
  const lines = text.split('\n')
  const out: string[] = []
  for (const line of lines) {
    let s = line
    if (mode === 'strict') {
      if (ENV_VAR_ASSIGN.test(s) || TOKEN_PATTERNS.some(re => re.test(s))) {
        out.push('[redacted]')
        continue
      }
    }
    if (home && home !== '/') s = s.split(home).join('~')
    if (user) {
      // Replace bare $USER mention or `/Users/<user>` style remnants that survived
      // homedir replace (e.g. references to other users' homes that share the same
      // base path style). Conservative: only swap when the username appears as a
      // standalone token, not inside another word.
      const re = new RegExp(`\\b${escapeRegExp(user)}\\b`, 'g')
      s = s.replace(re, '<user>')
    }
    out.push(s)
  }
  return out.join('\n')
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}
