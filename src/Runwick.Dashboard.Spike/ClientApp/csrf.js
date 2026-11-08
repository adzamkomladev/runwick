// The token is in the HTML <meta>, never in page props, so it stays out of history state.
export function mutationHeaders(commandId) {
  const token = document.querySelector('meta[name="runwick-csrf"]')?.content ?? ''
  return { 'X-Runwick-Csrf': token, 'X-Runwick-Command-Id': commandId }
}
