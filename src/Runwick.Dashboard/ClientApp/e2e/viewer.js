// Chromium, viewer: no control or reveal buttons, and the server refuses a hand-made control POST (the buttons are only hints).
async page => {
  const name = 'chromium: viewer sees no controls and a direct POST is 403'
  try {
    await page.goto('http://127.0.0.1:5791/ops/login/viewer?returnUrl=/ops/runwick')
    await page.getByTestId('job-link').first().click()
    await page.getByTestId('job-name').waitFor()
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
    return [`pass  ${name}`]
  } catch (e) {
    return [`FAIL  ${name}: ${e.message.split('\n')[0]}`]
  }
}
