// Firefox and WebKit smoke: log in, load, navigate, one control, one reveal; no CSP violations, dialogs or page errors.
async page => {
  const name = `${page.context().browser().browserType().name()}: load, navigate, cancel and reveal with no CSP violations`
  const dialogs = [], errors = []
  page.on('dialog', (d) => { dialogs.push(d.message()); d.dismiss() })
  page.on('pageerror', (e) => errors.push(e.message))
  try {
    await page.context().addInitScript(() => {
      window.__csp = []
      document.addEventListener('securitypolicyviolation', (e) => window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`))
    })
    await page.goto('http://127.0.0.1:5791/ops/login/admin?returnUrl=/ops/runwick')
    await page.getByTestId('run-row').first().waitFor()
    await page.evaluate(() => { window.__sameDocument = true })
    await page.getByTestId('run-row').filter({ hasText: 'pending' }).getByTestId('run-link').first().click()
    await page.getByTestId('run-name').waitFor()
    if (!(await page.evaluate(() => window.__sameDocument === true))) throw new Error('document reloaded')
    await page.getByTestId('reveal').click()
    await page.getByTestId('payload').waitFor()
    await page.getByTestId('cancel').click()
    await page.getByTestId('confirm').click()
    await page.getByTestId('done').waitFor()
    await page.waitForFunction(() => document.querySelector('[data-testid="run-state"]')?.textContent.startsWith('cancelled'))
    if (await page.locator('main img').count() !== 0) throw new Error('an <img> was created')
    const csp = await page.evaluate(() => window.__csp)
    if (csp.length || dialogs.length || errors.length) throw new Error(`csp=${csp} dialogs=${dialogs} errors=${errors}`)
    return [`pass  ${name}`]
  } catch (e) {
    return [`FAIL  ${name}: ${e.message.split('\n')[0]}`]
  }
}
