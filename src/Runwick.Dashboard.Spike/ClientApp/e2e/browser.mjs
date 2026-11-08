// SP-023 browser coverage: Chromium runs every step; Firefox and WebKit run the smoke steps.
// Usage (after `dotnet build samples/Runwick.Dashboard.SpikeHost`):
//   PLAYWRIGHT_MODULE=/path/to/playwright/index.mjs [SPIKE_BROWSERS=firefox,webkit] node e2e/browser.mjs
import { spawn } from 'node:child_process'
import path from 'node:path'
import assert from 'node:assert/strict'

const { chromium, firefox, webkit } = await import(process.env.PLAYWRIGHT_MODULE ?? 'playwright')
const repo = path.resolve(import.meta.dirname, '../../../..')
const hostDll = path.join(repo, 'samples/Runwick.Dashboard.SpikeHost/bin/Debug/net10.0/Runwick.Dashboard.SpikeHost.dll')
const port = Number(process.env.SPIKE_PORT ?? 5791)
const origin = `http://127.0.0.1:${port}`
const hostile = `</script><script>alert(1)</script>"'&<img src=x onerror=alert(2)>`
const results = []

async function startHost(...args) {
  const child = spawn('dotnet', [hostDll, ...args], { env: { ...process.env, ASPNETCORE_URLS: origin }, stdio: 'ignore' })
  for (let i = 0; i < 100; i++) {
    try { if ((await fetch(`${origin}/ops/healthz`)).ok) return child } catch {}
    await new Promise((r) => setTimeout(r, 100))
  }
  child.kill()
  throw new Error('host did not start')
}

const stopHost = (child) => new Promise((resolve) => { child.once('exit', resolve); child.kill() })

async function open(browserType, user) {
  // channel 'chromium' is the full build in new headless mode (no separate headless shell needed)
  const browser = await browserType.launch(browserType === chromium ? { channel: 'chromium' } : {})
  const context = await browser.newContext({ httpCredentials: { username: user, password: `pw-${user}` } })
  // Record CSP violations and any dialog (a script injected through a hostile string would raise one).
  await context.addInitScript(() => {
    window.__csp = []
    document.addEventListener('securitypolicyviolation', (e) => window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`))
  })
  const page = await context.newPage()
  page.on('dialog', (d) => { throw new Error(`unexpected dialog: ${d.message()}`) })
  const errors = []
  page.on('pageerror', (e) => errors.push(e.message))
  return { browser, page, errors }
}

async function step(name, fn) {
  try { await fn(); results.push(['pass', name]) } catch (e) { results.push(['FAIL', `${name}: ${e.message.split('\n')[0]}`]) }
}

const markReload = (page) => page.evaluate(() => { window.__sameDocument = true })
const sameDocument = (page) => page.evaluate(() => window.__sameDocument === true)

let host = await startHost()
try {
  // Chromium: the full path.
  {
    const { browser, page, errors } = await open(chromium, 'alice')
    await step('chromium: initial HTML renders the React app at /ops/runwick', async () => {
      await page.goto(`${origin}/ops/runwick`)
      await page.getByTestId('mount').waitFor()
      assert.equal(await page.getByTestId('mount').textContent(), 'Runwick (spike)')
    })
    await step('chromium: hostile job name renders as text, no dialog', async () => {
      assert.equal(await page.getByTestId('job-1').textContent(), hostile)
      assert.equal(await page.locator('img').count(), 0)
    })
    await step('chromium: partial reload loads only stats', async () => {
      await markReload(page)
      await page.getByTestId('load-stats').click()
      await page.getByTestId('stats').waitFor()
      assert.ok(await sameDocument(page))
    })
    await step('chromium: Inertia link navigates without a document load', async () => {
      await page.getByTestId('job-2').click()
      await page.waitForURL(`${origin}/ops/runwick/jobs/2`)
      assert.equal(await page.getByTestId('job-name').textContent(), 'nightly-report')
      assert.ok(await sameDocument(page))
    })
    await step('chromium: history state is encrypted (no plaintext props)', async () => {
      assert.equal(await page.evaluate(() => Object.prototype.toString.call(window.history.state?.page)), '[object ArrayBuffer]')
      assert.ok(!(await page.evaluate(() => JSON.stringify(window.history.state))).includes('nightly-report'))
    })
    await step('chromium: reveal is a separate request and never enters history', async () => {
      await page.getByTestId('reveal').click()
      await page.getByTestId('payload').waitFor()
      assert.match(await page.getByTestId('payload').textContent(), /ops@example\.com/)
      assert.ok(!(await page.evaluate(() => JSON.stringify(window.history.state ?? {}))).includes('ops@example.com'))
    })
    await step('chromium: retry posts with CSRF + command id and refreshes via 303', async () => {
      await page.getByTestId('retry').click()
      await page.waitForFunction(() => document.querySelector('[data-testid="retries"]')?.textContent === '1')
      assert.equal(await page.getByTestId('job-state').textContent(), 'Queued')
    })
    await step('chromium: no CSP violations or page errors', async () => {
      assert.deepEqual(await page.evaluate(() => window.__csp), [])
      assert.deepEqual(errors, [])
    })
    await step('chromium: host Inertia page is unchanged beside the mount', async () => {
      const host = await (await page.context().request.get(`${origin}/ops/`)).text()
      assert.match(host, /<title>Host app<\/title>/)
      assert.match(host, /host-secret-user/)
      assert.ok(!(await page.content()).includes('host-secret-user'))
    })
    await step('chromium: asset upgrade turns the next visit into a full reload', async () => {
      const entry = () => page.evaluate(() => document.querySelector('script[type="module"]').getAttribute('src'))
      const before = await entry()
      await stopHost(host)
      host = await startHost('--build', 'v2')
      await markReload(page)
      await page.getByTestId('back').click()
      await page.getByTestId('mount').waitFor()
      await page.waitForFunction(() => window.__sameDocument !== true)
      const after = await entry()
      assert.notEqual(after, before)
      assert.match(after, /-v2\.js$/)
      assert.deepEqual(await page.evaluate(() => window.__csp), [])
    })
    await browser.close()
  }
  {
    const { browser, page } = await open(chromium, 'victor')
    await step('chromium: viewer sees no retry and is denied the reveal', async () => {
      await page.goto(`${origin}/ops/runwick/jobs/2`)
      await page.getByTestId('job-name').waitFor()
      assert.equal(await page.getByTestId('retry').count(), 0)
      await page.getByTestId('reveal').click()
      assert.equal(await page.getByTestId('payload').textContent(), 'denied (403)')
    })
    await browser.close()
  }
  // Firefox and WebKit: smoke.
  const smoke = (process.env.SPIKE_BROWSERS ?? 'firefox,webkit').split(',')
  for (const [name, type] of [['firefox', firefox], ['webkit', webkit]].filter(([n]) => smoke.includes(n))) {
    let launched
    await step(`${name}: load, navigate, retry with no CSP violations`, async () => {
      launched = await open(type, 'alice')
      const { page, errors } = launched
      await page.goto(`${origin}/ops/runwick`)
      assert.equal(await page.getByTestId('job-1').textContent(), hostile)
      await markReload(page)
      await page.getByTestId('job-1').click()
      await page.getByTestId('job-name').waitFor()
      assert.ok(await sameDocument(page))
      const retries = Number(await page.getByTestId('retries').textContent())
      await page.getByTestId('retry').click()
      await page.waitForFunction((n) => document.querySelector('[data-testid="retries"]')?.textContent === String(n), retries + 1)
      assert.deepEqual(await page.evaluate(() => window.__csp), [])
      assert.deepEqual(errors, [])
    })
    await launched?.browser.close()
  }
} finally {
  await stopHost(host)
}

for (const [status, name] of results) console.log(`${status}  ${name}`)
process.exit(results.some(([s]) => s !== 'pass') ? 1 : 0)
