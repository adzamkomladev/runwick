// Firefox and WebKit smoke: load, navigate, retry, no CSP violations, dialogs or page errors.
async page => {
  const hostile = `</script><script>alert(1)</script>"'&<img src=x onerror=alert(2)>`
  const name = `${page.context().browser().browserType().name()}: load, navigate, retry with no CSP violations`
  const dialogs = [], errors = []
  page.on('dialog', (d) => { dialogs.push(d.message()); d.dismiss() })
  page.on('pageerror', (e) => errors.push(e.message))
  try {
    await page.context().addInitScript(() => {
      window.__csp = []
      document.addEventListener('securitypolicyviolation', (e) => window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`))
    })
    await page.goto('http://127.0.0.1:5791/ops/runwick')
    if (await page.getByTestId('job-1').textContent() !== hostile) throw new Error('hostile name not rendered as text')
    await page.evaluate(() => { window.__sameDocument = true })
    await page.getByTestId('job-1').click()
    await page.getByTestId('job-name').waitFor()
    if (!(await page.evaluate(() => window.__sameDocument === true))) throw new Error('document reloaded')
    const retries = Number(await page.getByTestId('retries').textContent())
    await page.getByTestId('retry').click()
    await page.waitForFunction((n) => document.querySelector('[data-testid="retries"]')?.textContent === String(n), retries + 1)
    const csp = await page.evaluate(() => window.__csp)
    if (csp.length || dialogs.length || errors.length) throw new Error(`csp=${csp} dialogs=${dialogs} errors=${errors}`)
    return [`pass  ${name}`]
  } catch (e) {
    return [`FAIL  ${name}: ${e.message.split('\n')[0]}`]
  }
}
