export function jsonOk(payload: Record<string, any> = {}) {
  return Response.json({ ok: true, ...payload })
}

export function jsonError(error: string, status = 400, payload: Record<string, any> = {}) {
  return Response.json({ ok: false, error, ...payload }, { status })
}

export async function readJsonBody(req: Request) {
  const body = await req.text()
  if (!body.trim()) return {}
  return JSON.parse(body)
}
