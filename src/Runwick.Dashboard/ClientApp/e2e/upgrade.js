// Chromium, the same tab as full.js, after browser.sh restarted the host with a rebuilt client ("--build v2").
// The next Inertia visit gets 409 + X-Inertia-Location and the client loads the new entry in full.
async page => {
  const name = 'chromium: a new build turns the next visit into a full reload of the new entry'
  const entry = () => page.evaluate(() => document.querySelector('script[type="module"]').getAttribute('src'))
  try {
    const before = await entry()
    await page.getByTestId('nav-queues').click()
    await page.waitForFunction(() => window.__sameDocument !== true && document.querySelector('[data-testid="queues"]'))
    const after = await entry()
    if (after === before || !/-v2\.js$/.test(after)) throw new Error(`entry ${before} -> ${after}`)
    const csp = await page.evaluate(() => window.__csp)
    if (csp.length) throw new Error(`CSP: ${csp}`)
    return [`pass  ${name}`]
  } catch (e) {
    return [`FAIL  ${name}: ${e.message.split('\n')[0]}`]
  }
}
