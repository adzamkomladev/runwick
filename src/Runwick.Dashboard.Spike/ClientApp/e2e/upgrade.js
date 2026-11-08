// Chromium, same tab as full.js, after browser.sh restarted the host with a rebuilt client ("--build v2").
async page => {
  const name = 'chromium: asset upgrade turns the next visit into a full reload'
  const entry = () => page.evaluate(() => document.querySelector('script[type="module"]').getAttribute('src'))
  try {
    const before = await entry()
    await page.getByTestId('back').click()
    await page.waitForFunction(() => window.__sameDocument !== true && document.querySelector('[data-testid="mount"]'))
    const after = await entry()
    if (after === before || !/-v2\.js$/.test(after)) throw new Error(`entry ${before} -> ${after}`)
    const csp = await page.evaluate(() => window.__csp)
    if (csp.length) throw new Error(`CSP: ${csp}`)
    return [`pass  ${name}`]
  } catch (e) {
    return [`FAIL  ${name}: ${e.message.split('\n')[0]}`]
  }
}
