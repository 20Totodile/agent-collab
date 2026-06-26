export type OperationRisk = 'write' | 'remote_control'

export type DangerousEndpoint = {
  method: string
  path: string
  risk: OperationRisk
  reason: string
}

export const DANGEROUS_ENDPOINTS: DangerousEndpoint[] = [
  { method: 'POST', path: '/api/terminal/send', risk: 'remote_control', reason: 'retired in AIC-116 (always returns 410); legacy key-injection endpoint' },
  { method: 'POST', path: '/group/send', risk: 'write', reason: 'can dispatch messages to agent provider subprocesses' },
]

export function dangerousEndpointFor(method: string, pathname: string) {
  return DANGEROUS_ENDPOINTS.find(endpoint => endpoint.method === method && endpoint.path === pathname) || null
}

export function logDangerousOperation(operation: string, details: Record<string, any> = {}) {
  const record = {
    ts: new Date().toISOString(),
    operation,
    ...details,
  }
  process.stderr.write(`ai-collab_security ${JSON.stringify(record)}\n`)
}

export function hasRemoteControlConfirm(body: any, operation: string) {
  return body?.confirm === true || body?.confirm === operation
}
