export function parseCookies(cookieHeader: string | null) {
  return Object.fromEntries(
    (cookieHeader || '')
      .split(';')
      .map(c => c.trim())
      .filter(Boolean)
      .map(c => c.split('=').map(s => s.trim())),
  )
}

// Only true loopback addresses count as local trust; LAN access goes through
// cookie / Bearer auth like anything else.
export function isLocalRequestHost(hostname: string) {
  return hostname === '127.0.0.1' || hostname === 'localhost' || hostname === '::1'
}

export function hasCookieAuth(req: Request, authToken: string) {
  const cookies = parseCookies(req.headers.get('cookie'))
  return cookies['aicollab_auth'] === authToken
}

export type RequestAuthMode = 'header' | 'query' | 'cookie' | 'local'

export function requestAuthMode(
  req: Request,
  url: URL,
  authToken: string,
  opts: { allowLocal?: boolean; allowQueryToken?: boolean } = {},
): RequestAuthMode | null {
  const authHeader = req.headers.get('authorization')
  if (authHeader === `Bearer ${authToken}`) return 'header'
  if (opts.allowQueryToken !== false && url.searchParams.get('token') === authToken) return 'query'
  if (hasCookieAuth(req, authToken)) return 'cookie'
  if (opts.allowLocal === true && isLocalRequestHost(url.hostname)) return 'local'
  return null
}

export function hasRequestAuth(
  req: Request,
  url: URL,
  authToken: string,
  opts: { allowLocal?: boolean; allowQueryToken?: boolean } = {},
) {
  return requestAuthMode(req, url, authToken, opts) !== null
}
