export function jsonParseSafe(value: any, fallback: any) {
  if (value == null) return fallback
  if (typeof value !== 'string') return value
  try { return JSON.parse(value) } catch { return fallback }
}

export function formatMessageRow(
  row: any,
  getMessagePreview: (messageId: string) => { sender: string; content: string } | null,
) {
  return {
    ...row,
    tool_uses: jsonParseSafe(row.tool_uses, []),
    images: jsonParseSafe(row.images, []),
    audio: jsonParseSafe(row.audio, []),
    files: jsonParseSafe(row.files, []),
    metrics: jsonParseSafe(row.metrics, null),
    reply_preview: row.reply_to ? getMessagePreview(row.reply_to) : null,
  }
}

