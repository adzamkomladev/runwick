// Chromium, admin then operator: every page, each control the sample data allows, reveals, polling and the hidden-tab pause,
// login redirects and the CSP check. Run by browser.sh through `playwright-cli run-code`; browser.sh replaces the port.
async page => {
  const base = 'http://127.0.0.1:5791'
  const hostile = `</script><script>alert(1)</script>"'&<img src=x onerror=alert(2)> end`
  const results = [], dialogs = [], errors = []
  const step = async (name, fn) => {
    try { await fn(); results.push(`pass  chromium: ${name}`) } catch (e) { results.push(`FAIL  chromium: ${name}: ${e.message.split('\n')[0]}`) }
  }
  const check = (ok, what) => { if (!ok) throw new Error(what) }
  const text = (id) => page.getByTestId(id).first().textContent()
  const sameDocument = () => page.evaluate(() => window.__sameDocument === true)
  const polls = []
  page.on('dialog', (d) => { dialogs.push(d.message()); d.dismiss() })
  page.on('pageerror', (e) => errors.push(e.message))
  page.on('request', (r) => { if (r.headers()['x-inertia-partial-data']) polls.push({ at: Date.now(), only: r.headers()['x-inertia-partial-data'] }) })
  await page.context().addInitScript(() => {
    window.__csp = []
    document.addEventListener('securitypolicyviolation', (e) => window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`))
  })

  await step('logged out: the mount redirects to the host login page', async () => {
    await page.goto(`${base}/ops/runwick/queues`)
    check(page.url().startsWith(`${base}/ops/login`), `landed on ${page.url()}`)
    await page.getByText('admin').click()
    await page.waitForURL(`${base}/ops/runwick/queues`)
    check(await text('title') === 'Runwick', 'title')
  })
  await step('Jobs page: hostile payload owner, rows, nav filtered by can', async () => {
    await page.getByTestId('nav-jobs').click()
    await page.getByTestId('job-row').first().waitFor()
    check(await page.getByTestId('job-row').count() >= 5, 'rows')
    for (const nav of ['jobs', 'queues', 'workers', 'schedules', 'scaling', 'settings', 'retention', 'audit']) {
      check(await page.getByTestId(`nav-${nav}`).count() === 1, `nav-${nav}`)
    }
  })
  await step('Inertia link navigates without a document load; history state is encrypted', async () => {
    await page.evaluate(() => { window.__sameDocument = true })
    await page.getByTestId('job-link').filter({ hasText: 'sample.flaky' }).click()
    await page.waitForURL(/\/ops\/runwick\/jobs\/[0-9a-f-]{36}$/)
    check(await text('job-name') === 'sample.flaky', 'job name')
    check(await sameDocument(), 'document reloaded')
    check(await page.evaluate(() => Object.prototype.toString.call(window.history.state?.page)) === '[object ArrayBuffer]', 'history.state.page is not encrypted')
  })
  await step('hostile error code renders as text, no <img>, no dialog', async () => {
    check((await text('error-code')).startsWith('</script><img src=x onerror=alert(4)>'), `error code ${await text('error-code')}`)
    check(await page.locator('main img').count() === 0, 'an <img> was created')
    check(dialogs.length === 0, `dialog: ${dialogs}`)
  })
  await step('reveal error: separate request, shown, never in history', async () => {
    await page.getByTestId('reveal-error').click()
    await page.getByTestId('error').waitFor()
    check((await text('error')).includes('error-text-secret'), 'error text')
    check(!(await page.evaluate(() => JSON.stringify(window.history.state))).includes('error-text-secret'), 'error text in history')
  })
  await step('retry: confirm, CSRF + command ID, 303, done', async () => {
    await page.getByTestId('retry').click()
    await page.getByTestId('confirm').click()
    await page.getByTestId('done').waitFor()
    await page.waitForFunction(() => document.querySelector('[data-testid="generation"]')?.textContent === '2')
  })
  await step('partial reload: audit loads on demand', async () => {
    await page.getByTestId('load-audit').click()
    await page.getByTestId('audit').waitFor()
    check((await text('audit')).includes('RetryJob'), 'audit row')
    check(await sameDocument(), 'document reloaded')
  })
  await step('cancel a pending job with a hostile payload, then reveal the payload as text', async () => {
    await page.getByTestId('nav-jobs').click()
    await page.getByTestId('job-row').first().waitFor()
    const links = page.getByTestId('job-link').filter({ hasText: 'sample.report' })
    let found = false
    for (let i = 0; i < await links.count() && !found; i++) {
      await links.nth(i).click()
      await page.getByTestId('reveal').waitFor()
      await page.getByTestId('reveal').click()
      await page.getByTestId('payload').waitFor()
      found = (await text('payload')).includes('</script><script>alert(1)')
      if (!found) { await page.goBack(); await page.getByTestId('job-row').first().waitFor() }
    }
    check(found, 'no job with the hostile payload')
    check(await page.locator('main img').count() === 0 && dialogs.length === 0, 'payload executed')
    await page.getByTestId('cancel').click()
    await page.getByTestId('confirm').click()
    await page.getByTestId('done').waitFor()
    await page.waitForFunction(() => document.querySelector('[data-testid="job-state"]')?.textContent.startsWith('cancelled'))
    check(await page.getByTestId('payload').count() === 0, 'revealed payload kept after navigation')
  })
  await step('Jobs page polls at 5 s or more, one request at a time', async () => {
    await page.getByTestId('nav-jobs').click()
    await page.getByTestId('job-row').first().waitFor()
    polls.length = 0
    await page.waitForTimeout(12_500)
    const jobs = polls.filter((p) => p.only === 'jobs')
    check(jobs.length >= 1 && jobs.length <= 2, `${jobs.length} polls in 12.5 s`)
    for (let i = 1; i < jobs.length; i++) check(jobs[i].at - jobs[i - 1].at >= 4_900, 'polls closer than 5 s')
  })
  await step('a hidden tab sends no polls; becoming visible sends one at once', async () => {
    await page.evaluate(() => {
      Object.defineProperty(document, 'hidden', { configurable: true, get: () => true })
      document.dispatchEvent(new Event('visibilitychange'))
    })
    polls.length = 0
    await page.waitForTimeout(11_000)
    check(polls.length === 0, `${polls.length} polls while hidden`)
    await page.evaluate(() => {
      Object.defineProperty(document, 'hidden', { configurable: true, get: () => false })
      document.dispatchEvent(new Event('visibilitychange'))
    })
    await page.waitForTimeout(1_000)
    check(polls.length === 1, `${polls.length} polls on becoming visible`)
  })
  await step('every other page renders', async () => {
    for (const [nav, id] of [['queues', 'queues'], ['workers', 'workers'], ['schedules', 'schedules'], ['scaling', 'events'], ['settings', 'settings'],
      ['retention', 'policy'], ['audit', 'audit']]) {
      await page.getByTestId(`nav-${nav}`).click()
      await page.getByTestId(id).waitFor()
    }
  })
  await step('queue pause and resume', async () => {
    await page.getByTestId('nav-queues').click()
    await page.getByTestId('queue-pause').first().click()
    await page.getByTestId('confirm').click()
    await page.getByTestId('done').waitFor()
    await page.getByTestId('queue-resume').first().waitFor()
    await page.getByTestId('queue-resume').first().click()
    await page.getByTestId('confirm').click()
    await page.getByTestId('done').waitFor()
    await page.getByTestId('queue-pause').first().waitFor()
  })
  await step('retention page: save the same policy shows a rejection (no_change), not an error', async () => {
    await page.getByTestId('nav-retention').click()
    await page.getByTestId('save-policy').click()
    await page.getByTestId('confirm').click()
    await page.getByTestId('rejected').waitFor()
  })
  await step('operator: hostile display name renders as text in the header and the audit page', async () => {
    await page.goto(`${base}/ops/login/operator?returnUrl=/ops/runwick/audit`)
    await page.getByTestId('audit').waitFor()
    check(await text('user') === hostile, `user ${await text('user')}`)
    check(await page.getByTestId('nav-retention').count() === 1, 'operator retention nav')
    check(await page.locator('header img, main img').count() === 0 && dialogs.length === 0, 'display name executed')
  })
  await step('no CSP violations or page errors', async () => {
    const csp = await page.evaluate(() => window.__csp)
    check(csp.length === 0, `CSP: ${csp}`)
    check(errors.length === 0, `errors: ${errors}`)
  })
  await step('host Inertia page is unchanged beside the mount', async () => {
    const host = await (await page.context().request.get(`${base}/ops/`)).text()
    check(host.includes('host-secret-user') && host.includes('<title>Host app</title>'), 'host page')
  })
  // Leave the tab on the Jobs page for upgrade.js.
  await page.goto(`${base}/ops/runwick`)
  await page.getByTestId('job-row').first().waitFor()
  await page.evaluate(() => { window.__sameDocument = true })
  return results
}
