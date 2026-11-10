// Chromium, viewer: no control or reveal buttons, and the server refuses a hand-made control POST (the buttons are only hints).
async page => {
  const name = 'chromium: viewer sees no controls and a direct POST is 403'
  try {
    await page.goto('http://127.0.0.1:5791/ops/login/viewer?returnUrl=/ops/runwick')
    await page.getByTestId('run-link').first().click()
    await page.getByTestId('run-name').waitFor()
    for (const id of ['cancel', 'retry', 'rerun', 'reveal', 'reveal-error']) {
      if (await page.getByTestId(id).count() !== 0) throw new Error(`viewer sees ${id}`)
    }
    const status = await page.evaluate(async () => (await fetch(`${location.pathname}/cancel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Inertia': 'true', 'X-Runwick-Command-Id': crypto.randomUUID(),
        'X-Runwick-Csrf': document.querySelector('meta[name="runwick-csrf"]').content },
      body: JSON.stringify({ expectedGeneration: 1, expectedRevision: 1 }),
    })).status)
    if (status !== 403) throw new Error(`POST answered ${status}`)
    // A page the viewer may not read is a page in the shell, not a JSON body (21 §3.7).
    const visit = await page.goto('http://127.0.0.1:5791/ops/runwick/audit')
    if (visit.status() !== 403) throw new Error(`audit visit answered ${visit.status()}`)
    if (await page.getByTestId('status-page').getAttribute('data-status') !== '403') throw new Error('no 403 status page')
    if (await page.getByTestId('status-permission').textContent() !== 'Audit.Read') throw new Error('permission not named')
    await page.waitForTimeout(6000) // past a poll: the shell's own reloads must not raise Inertia's error modal
    if (await page.locator('iframe, dialog').count() !== 0) throw new Error('error modal over the status page')
    await page.getByTestId('status-home').click()
    await page.getByTestId('kpi-created').waitFor() // the first page a viewer can read: Overview
    return [`pass  ${name}`]
  } catch (e) {
    return [`FAIL  ${name}: ${e.message.split('\n')[0]}`]
  }
}
