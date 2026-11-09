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
  // Inertia adds a reload's reset props to the partial data ("jobs,jobs" for a poll that resets jobs); count each name once.
  page.on('request', (r) => { const d = r.headers()['x-inertia-partial-data']; if (d) polls.push({ at: Date.now(), only: [...new Set(d.split(','))].join(',') }) })
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
    for (const nav of ['overview', 'jobs', 'queues', 'workers', 'schedules', 'scaling', 'settings', 'retention', 'audit']) {
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
  await step('job page: status timeline shows each step, the retry and its actor', async () => {
    await page.waitForFunction(() => document.querySelectorAll('[data-testid="timeline"] > li').length >= 5)
    const t = await text('timeline')
    check(t.includes('Created') && t.includes('Attempt 1') && t.includes('admin'), `timeline: ${t.slice(0, 200)}`)
  })
  await step('job page: resources used shows recorded process figures per attempt', async () => {
    await page.getByTestId('usage-cpu').waitFor()
    const cpu = await text('usage-cpu')
    check(/\d+(\.\d+)? (µs|ms|s)$/.test(cpu), `cpu: ${cpu}`)
    check(/\d+(\.\d+)? (B|KiB|MiB|GiB)$/.test(await text('usage-peak')), 'peak working set missing')
    check(await page.locator('[data-testid="usage-rows"] tbody tr').count() >= 1, 'no usage rows')
  })
  await step('sidebar collapses with Ctrl+B, stays collapsed after reload, and expands again', async () => {
    const width = () => page.locator('aside').first().evaluate((el) => el.getBoundingClientRect().width)
    await page.keyboard.press('Control+b')
    await page.waitForFunction(() => document.querySelector('aside').getBoundingClientRect().width < 80)
    await page.reload()
    await page.getByTestId('job-name').waitFor()
    check(await width() < 80, 'not remembered')
    check(await page.getByTestId('nav-jobs').count() === 1, 'nav lost when collapsed')
    await page.keyboard.press('Control+b')
    await page.waitForFunction(() => document.querySelector('aside').getBoundingClientRect().width > 200)
    await page.evaluate(() => { window.__sameDocument = true })
  })
  await step('partial reload: audit loads on demand', async () => {
    await page.getByTestId('load-audit').click()
    await page.getByTestId('audit').waitFor()
    check((await text('audit')).includes('ManualRetry'), 'audit row')
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
    await page.getByTestId('payload').waitFor({ state: 'detached', timeout: 5_000 }).catch(() => {}) // the 'navigate' event clears it just after the new props render
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
    for (const [nav, id] of [['overview', 'kpi-created'], ['queues', 'queues'], ['workers', 'workers'], ['schedules', 'schedules'], ['scaling', 'events'], ['settings', 'settings'],
      ['retention', 'policy'], ['audit', 'audit']]) {
      await page.getByTestId(`nav-${nav}`).click()
      await page.getByTestId(id).waitFor()
    }
  })
  await step('skeletons: each page draws its own skeleton, and the loaded page replaces it within 2 px and under 0.02 layout shift', async () => {
    // Every page has loaded once above, so each skeleton is the page's own layout with its last props (21 §4.5).
    let slow = false
    await page.route('**/*', async (route) => {
      if (slow && route.request().headers()['x-inertia']) await new Promise((r) => setTimeout(r, 1200)) // page visits and deferred props
      await route.continue()
    })
    await page.evaluate(() => {
      window.__cls = 0
      new PerformanceObserver((list) => { for (const e of list.getEntries()) window.__cls += e.value }).observe({ type: 'layout-shift' })
    })
    const boxes = () => page.evaluate(() => [...document.querySelectorAll('main h1, main [data-slot=card]')]
      .map((e) => { const r = e.getBoundingClientRect(); return [r.x, r.y, r.width, r.height].map((v) => Math.round(v)) }))
    const problems = []
    const pages = [['overview', 'kpi-created'], ['jobs', 'job-row'], ['job', 'job-name'], ['queues', 'queues'], ['workers', 'workers'], ['schedules', 'schedules'],
      ['scaling', 'events'], ['audit', 'audit'], ['retention', 'policy'], ['settings', 'settings']]
    try {
      for (const [name, ready] of pages) {
        const [from, fromReady] = name === 'job' ? ['jobs', 'job-row'] : name === 'settings' ? ['audit', 'audit'] : ['settings', 'settings']
        const go = () => (name === 'job' ? page.getByTestId('job-link').filter({ hasText: 'sample.flaky' }).first().click() : page.getByTestId(`nav-${name}`).click())
        if (name === 'job') { await page.getByTestId('nav-jobs').click(); await page.getByTestId('job-row').first().waitFor() }
        await go() // a fresh visit first, so the skeleton compares layout rather than data that changed since the page was last seen
        await page.getByTestId(ready).first().waitFor()
        await page.waitForFunction(() => !document.querySelector('main [data-skeleton]'), null, { timeout: 15_000 })
        await page.getByTestId(`nav-${from}`).click()
        await page.getByTestId(fromReady).first().waitFor()
        await page.waitForTimeout(300)
        slow = true
        await go()
        await page.locator('main [data-skeleton]').first().waitFor()
        const before = await boxes()
        await page.evaluate(() => { window.__cls = 0 })
        await page.waitForFunction(() => !document.querySelector('main [data-skeleton]'), null, { timeout: 15_000 })
        await page.getByTestId(ready).first().waitFor()
        await page.waitForTimeout(400) // past the 150 ms fade
        const after = await boxes()
        const cls = await page.evaluate(() => window.__cls)
        slow = false
        if (cls >= 0.02) problems.push(`${name}: layout shift ${cls.toFixed(3)}`)
        if (before.length !== after.length) problems.push(`${name}: ${before.length} sections in the skeleton, ${after.length} loaded`)
        else before.forEach((b, i) => { if (b.some((v, k) => Math.abs(v - after[i][k]) > 2)) problems.push(`${name}: section ${i} ${b} -> ${after[i]}`) })
      }
    } finally {
      slow = false
      await page.unroute('**/*')
    }
    check(problems.length === 0, problems.join('; '))
  })
  await step('date range preset and the filter sheet keep each other in the URL query', async () => {
    await page.getByTestId('nav-jobs').click()
    await page.getByTestId('range').click()
    await page.getByTestId('range-15m').click()
    await page.waitForURL(/range=15m/)
    await page.getByTestId('filters').click()
    await page.getByTestId('filter-sheet').waitFor()
    await page.getByTestId('filter-jobName').fill('sample.flaky')
    await page.getByTestId('apply-filters').click()
    await page.waitForURL((u) => u.searchParams.get('jobName') === 'sample.flaky' && u.searchParams.get('range') === '15m')
    await page.getByTestId('clear-filters').click()
    await page.waitForURL((u) => !u.searchParams.has('jobName') && u.searchParams.get('range') === '15m')
  })
  await step('account menu theme switch goes to dark and back to the system setting, keeping the menu open', async () => {
    await page.getByTestId('account').click()
    await page.getByTestId('theme-dark').click()
    check(await page.evaluate(() => document.documentElement.classList.contains('dark')), 'dark not applied')
    await page.getByTestId('theme-system').click()
    check(await page.getByTestId('account-menu').isVisible(), 'menu closed on theme change')
    await page.keyboard.press('Escape')
  })
  await step('notifications open from the account menu and clear the unread count', async () => {
    await page.getByTestId('account').click()
    await page.getByTestId('notifications').click()
    await page.getByTestId('notifications-panel').waitFor()
    await page.keyboard.press('Escape')
    await page.waitForFunction(() => !document.querySelector('[data-testid="unread"]'))
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
  await step('retention page: saving the same policy from the sheet shows a rejection toast (no_change), not an error', async () => {
    await page.getByTestId('nav-retention').click()
    await page.getByTestId('edit-policy').click()
    await page.getByTestId('policy-sheet').waitFor()
    await page.getByTestId('save-policy').click()
    await page.getByTestId('rejected').waitFor()
  })
  await step('operator: hostile display name renders as text in the header and the audit page', async () => {
    await page.goto(`${base}/ops/login/operator?returnUrl=/ops/runwick/audit`)
    await page.getByTestId('audit').waitFor()
    check(await text('user') === hostile, `user ${await text('user')}`)
    check(await page.getByTestId('nav-retention').count() === 1, 'operator retention nav')
    check(await page.locator('header img, main img').count() === 0 && dialogs.length === 0, 'display name executed')
  })
  await step('reduced motion: presses do not scale and nothing animates or transitions', async () => {
    const press = async () => { // hold the press, read the scale, then slide off so it never clicks
      const box = await page.getByTestId('nav-jobs').boundingBox()
      await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2)
      await page.mouse.down()
      await page.waitForTimeout(250)
      const scale = await page.getByTestId('nav-jobs').evaluate((el) => getComputedStyle(el).scale)
      await page.mouse.move(box.x + box.width / 2, box.y + 400)
      await page.mouse.up()
      return scale
    }
    await page.goto(`${base}/ops/runwick/queues`)
    await page.getByTestId('queue-row').first().waitFor()
    const normal = await press()
    check(normal === '0.97', `press scale with motion: ${normal}`)
    await page.emulateMedia({ reducedMotion: 'reduce' })
    try {
      await page.goto(`${base}/ops/runwick/queues`)
      await page.getByTestId('queue-row').first().waitFor()
      const reduced = await press()
      check(reduced === 'none', `press scale under reduced motion: ${reduced}`)
      const moving = await page.evaluate(() => [...document.querySelectorAll('body *')].filter((el) => {
        const cs = getComputedStyle(el), most = (v) => Math.max(...v.split(',').map(parseFloat))
        return most(cs.transitionDuration) > 0.001 || (cs.animationName !== 'none' && most(cs.animationDuration) > 0.001)
      }).map((el) => el.dataset.testid ?? el.tagName).slice(0, 5))
      check(moving.length === 0, `still moving: ${moving}`)
    } finally { await page.emulateMedia({ reducedMotion: null }) }
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
