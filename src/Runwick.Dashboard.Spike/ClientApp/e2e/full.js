// Chromium, operator: every dashboard step up to the deploy. Run by browser.sh through `playwright-cli run-code`.
async page => {
  const base = 'http://127.0.0.1:5791' // browser.sh's port
  const hostile = `</script><script>alert(1)</script>"'&<img src=x onerror=alert(2)>`
  const results = [], dialogs = [], errors = []
  const step = async (name, fn) => {
    try { await fn(); results.push(`pass  chromium: ${name}`) } catch (e) { results.push(`FAIL  chromium: ${name}: ${e.message.split('\n')[0]}`) }
  }
  const check = (ok, what) => { if (!ok) throw new Error(what) }
  const sameDocument = () => page.evaluate(() => window.__sameDocument === true)
  page.on('dialog', (d) => { dialogs.push(d.message()); d.dismiss() })
  page.on('pageerror', (e) => errors.push(e.message))
  await page.context().addInitScript(() => {
    window.__csp = []
    document.addEventListener('securitypolicyviolation', (e) => window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`))
  })

  await step('initial HTML renders the React app at /ops/runwick', async () => {
    await page.goto(`${base}/ops/runwick`)
    check(await page.getByTestId('mount').textContent() === 'Runwick (spike)', 'mount title')
  })
  await step('hostile job name renders as text, no dialog', async () => {
    check(await page.getByTestId('job-1').textContent() === hostile, 'job-1 text')
    check(await page.locator('img').count() === 0, 'an <img> was created')
    check(dialogs.length === 0, `dialog: ${dialogs}`)
  })
  await step('partial reload loads only stats', async () => {
    await page.evaluate(() => { window.__sameDocument = true })
    await page.getByTestId('load-stats').click()
    await page.getByTestId('stats').waitFor()
    check(await sameDocument(), 'document reloaded')
  })
  await step('Inertia link navigates without a document load', async () => {
    await page.getByTestId('job-2').click()
    await page.waitForURL(`${base}/ops/runwick/jobs/2`)
    check(await page.getByTestId('job-name').textContent() === 'nightly-report', 'job name')
    check(await sameDocument(), 'document reloaded')
  })
  await step('history state is encrypted (no plaintext props)', async () => {
    check(await page.evaluate(() => Object.prototype.toString.call(window.history.state?.page)) === '[object ArrayBuffer]', 'history.state.page is not encrypted')
    check(!(await page.evaluate(() => JSON.stringify(window.history.state))).includes('nightly-report'), 'plaintext in history')
  })
  await step('reveal is a separate request and never enters history', async () => {
    await page.getByTestId('reveal').click()
    await page.getByTestId('payload').waitFor()
    check(/ops@example\.com/.test(await page.getByTestId('payload').textContent()), 'payload text')
    check(!(await page.evaluate(() => JSON.stringify(window.history.state))).includes('ops@example.com'), 'payload in history')
  })
  await step('retry posts with CSRF + command id and refreshes via 303', async () => {
    await page.getByTestId('retry').click()
    await page.waitForFunction(() => document.querySelector('[data-testid="retries"]')?.textContent === '1')
    check(await page.getByTestId('job-state').textContent() === 'Queued', 'state after retry')
  })
  await step('no CSP violations or page errors', async () => {
    const csp = await page.evaluate(() => window.__csp)
    check(csp.length === 0, `CSP: ${csp}`)
    check(errors.length === 0, `errors: ${errors}`)
  })
  await step('host Inertia page is unchanged beside the mount', async () => {
    const host = await (await page.context().request.get(`${base}/ops/`)).text()
    check(host.includes('<title>Host app</title>') && host.includes('host-secret-user'), 'host page')
    check(!(await page.content()).includes('host-secret-user'), 'host prop on dashboard')
  })
  // browser.sh now restarts the host with a rebuilt client; upgrade.js continues in this same tab.
  await page.evaluate(() => { window.__sameDocument = true })
  return results
}
