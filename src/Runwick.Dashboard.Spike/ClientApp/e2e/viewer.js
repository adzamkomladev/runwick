// Chromium, view-only user.
async page => {
  const name = 'chromium: viewer sees no retry and is denied the reveal'
  try {
    await page.goto('http://127.0.0.1:5791/ops/runwick/jobs/2')
    await page.getByTestId('job-name').waitFor()
    if (await page.getByTestId('retry').count() !== 0) throw new Error('retry button shown')
    await page.getByTestId('reveal').click()
    const text = await page.getByTestId('payload').textContent()
    if (text !== 'denied (403)') throw new Error(`reveal shows ${text}`)
    return [`pass  ${name}`]
  } catch (e) {
    return [`FAIL  ${name}: ${e.message.split('\n')[0]}`]
  }
}
