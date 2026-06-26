// bun test src/providers/tmux-sanitizer.test.ts
import { expect, test, describe } from 'bun:test'
import { sanitizeFrame, parseFilterMode } from './tmux-sanitizer.ts'
import { diffAppendedLines } from './tmux.ts'
import { homedir, userInfo } from 'os'

describe('parseFilterMode', () => {
  test('accepts strict/loose/off', () => {
    expect(parseFilterMode('strict')).toBe('strict')
    expect(parseFilterMode('loose')).toBe('loose')
    expect(parseFilterMode('off')).toBe('off')
    expect(parseFilterMode('STRICT')).toBe('strict')
  })
  test('falls back on garbage', () => {
    expect(parseFilterMode(undefined)).toBe('strict')
    expect(parseFilterMode('')).toBe('strict')
    expect(parseFilterMode('nonsense')).toBe('strict')
    expect(parseFilterMode('off', 'loose')).toBe('off')
    expect(parseFilterMode(undefined, 'loose')).toBe('loose')
  })
})

describe('sanitizeFrame strict mode', () => {
  test('drops env var assignment lines', () => {
    const input = [
      'normal output',
      'API_KEY=abc123secret',
      'export AWS_SECRET=zzz',
      '  PATH=/usr/bin',
      'more output',
    ].join('\n')
    const out = sanitizeFrame(input, 'strict')
    expect(out).not.toContain('abc123secret')
    expect(out).not.toContain('AWS_SECRET=')
    expect(out).not.toContain('PATH=/usr/bin')
    expect(out).toContain('normal output')
    expect(out).toContain('more output')
    expect(out.split('\n').filter(l => l === '[redacted]').length).toBe(3)
  })

  test('drops sk- token lines', () => {
    const input = 'using sk-ant-api03-AAAAAAAAAAAAAAAA for auth'
    const out = sanitizeFrame(input, 'strict')
    expect(out).toBe('[redacted]')
  })

  test('drops Bearer token lines', () => {
    const input = 'Authorization: Bearer ey9.aaaa.bbbb-cccc'
    const out = sanitizeFrame(input, 'strict')
    expect(out).toBe('[redacted]')
  })

  test('drops JWT eyJ-prefixed token lines', () => {
    const input = 'token=eyJhbGciOiJIUzI1NiJ9.payload.sig'
    const out = sanitizeFrame(input, 'strict')
    expect(out).toBe('[redacted]')
  })

  test('drops GitHub ghp_ token lines', () => {
    const input = 'gh auth login --with-token ghp_AAAAAAAAAAAAAAAAAAAA'
    const out = sanitizeFrame(input, 'strict')
    expect(out).toBe('[redacted]')
  })

  test('drops AWS access key lines', () => {
    const input = 'aws cli AKIAIOSFODNN7EXAMPLE used'
    const out = sanitizeFrame(input, 'strict')
    expect(out).toBe('[redacted]')
  })

  test('drops ssh-rsa key lines', () => {
    const input = 'authorized_keys: ssh-rsa AAAAB3NzaC1yc2EAAAA user@host'
    const out = sanitizeFrame(input, 'strict')
    expect(out).toBe('[redacted]')
  })

  test('drops BEGIN PRIVATE KEY lines', () => {
    const input = '-----BEGIN OPENSSH PRIVATE KEY-----'
    const out = sanitizeFrame(input, 'strict')
    expect(out).toBe('[redacted]')
  })

  test('replaces $HOME with ~', () => {
    const home = homedir()
    if (!home || home === '/') return
    const input = `cwd: ${home}/projects/foo`
    const out = sanitizeFrame(input, 'strict')
    expect(out).toBe('cwd: ~/projects/foo')
  })

  test('replaces username with <user>', () => {
    const user = userInfo().username
    if (!user) return
    const input = `[INFO] currently logged in as ${user}`
    const out = sanitizeFrame(input, 'strict')
    expect(out).toContain('<user>')
    expect(out).not.toContain(user)
  })

  test('preserves benign output unchanged', () => {
    const input = 'Hello world\nThis is fine\nNo secrets here'
    const out = sanitizeFrame(input, 'strict')
    expect(out).toBe(input)
  })

  test('full mixed batch — none of the bad strings survive', () => {
    const input = [
      'OK',
      'API_KEY=zzzz',
      'sk-ant-api03-AAAAAAAAAAAAAAAA',
      'Bearer eyJabcdefghij',
      'ssh-rsa AAAAB user@host',
      '-----BEGIN RSA PRIVATE KEY-----',
      'done',
    ].join('\n')
    const out = sanitizeFrame(input, 'strict')
    for (const bad of ['zzzz', 'sk-ant', 'Bearer eyJ', 'ssh-rsa', 'BEGIN RSA PRIVATE']) {
      expect(out).not.toContain(bad)
    }
    expect(out).toContain('OK')
    expect(out).toContain('done')
  })
})

describe('sanitizeFrame loose mode', () => {
  test('keeps tokens but still rewrites $HOME', () => {
    const home = homedir()
    if (!home || home === '/') return
    const input = `sk-ant-api03-AAAA in ${home}/foo`
    const out = sanitizeFrame(input, 'loose')
    expect(out).toContain('sk-ant-api03-AAAA')
    expect(out).toContain('~/foo')
    expect(out).not.toContain(home)
  })

  test('keeps env var assignments in loose', () => {
    const input = 'FOO=bar'
    const out = sanitizeFrame(input, 'loose')
    expect(out).toBe('FOO=bar')
  })
})

describe('sanitizeFrame off mode', () => {
  test('passes through unchanged even with $HOME', () => {
    const home = homedir() || '/Users/x'
    const input = `secret=abc123 in ${home}`
    const out = sanitizeFrame(input, 'off')
    expect(out).toBe(input)
  })
})

describe('diffAppendedLines (TmuxProvider capture diff)', () => {
  test('baseline empty prev returns full curr', () => {
    const out = diffAppendedLines('', 'a\nb\nc')
    expect(out).toEqual(['a', 'b', 'c'])
  })

  test('simple append surfaces only new lines', () => {
    const prev = 'foo\nbar\nbaz'
    const curr = 'foo\nbar\nbaz\nqux\nquux'
    const out = diffAppendedLines(prev, curr)
    expect(out).toEqual(['qux', 'quux'])
  })

  test('repeated prompt line — slices after the N-th occurrence, not the last', () => {
    // chat CLI pattern: prompt "▷ " repeats every turn. Naive rightmost-match would
    // anchor on curr's newest "▷ " and slice [] — kill silently.
    const prev = 'user: hi\n▷ '
    const curr = 'user: hi\n▷ \nclaude: hello\nuser: ok\n▷ '
    const out = diffAppendedLines(prev, curr)
    expect(out).toEqual(['claude: hello', 'user: ok', '▷ '])
  })

  test('no anchor match anywhere → fallback to full curr', () => {
    const prev = 'lost\nscrollback\nold'
    const curr = 'completely\nfresh\ncontent'
    const out = diffAppendedLines(prev, curr)
    expect(out).toEqual(['completely', 'fresh', 'content'])
  })

  test('anchor matches but appears too few times in curr → fallback to full', () => {
    // prev has 3 occurrences of "▷ ", curr only has 2 → scrollback rotated past
    const prev = '▷ \nstuff\n▷ \nmore\n▷ '
    const curr = 'late\n▷ \nbla\n▷ '
    const out = diffAppendedLines(prev, curr)
    expect(out).toEqual(['late', '▷ ', 'bla', '▷ '])
  })

  test('only whitespace lines in prev → fallback to full curr (no anchor)', () => {
    const prev = '\n   \n\t\n'
    const curr = 'real\noutput'
    const out = diffAppendedLines(prev, curr)
    expect(out).toEqual(['real', 'output'])
  })
})

describe('sanitizeFrame lowercase env var assignment', () => {
  test('strict mode drops lowercase env-style line', () => {
    const input = 'db_password=hunter2\nnormal line'
    const out = sanitizeFrame(input, 'strict')
    expect(out).not.toContain('hunter2')
    expect(out).toContain('normal line')
  })
})
