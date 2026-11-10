// Generates the small navigation manifest and the lazy search index from MDX frontmatter.
// Runs before `vite` and `vite build`; no MDX module is imported just to render the sidebar.
import fs from 'node:fs/promises'
import path from 'node:path'
import GithubSlugger from 'github-slugger'
import YAML from 'yaml'

const root = path.resolve(import.meta.dirname, '..')
const docsRoot = path.join(root, 'content', 'docs')
const output = path.join(root, 'ClientApp', 'lib')

const sectionOrder = [
  'Get started',
  'Jobs',
  'Stores',
  'Operations',
  'Reference',
]

const stripMarkdown = (value) =>
  value
    .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
    .replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/[`*_~]/g, '')
    .replace(/<[^>]+>/g, '')
    .replace(/\s+/g, ' ')
    .trim()

const cleanBody = (source) =>
  source
    .replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n/, '')
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/[#>*_`|]/g, ' ')
    .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
    .replace(/\s+/g, ' ')
    .trim()

function headingsOf(source) {
  const body = source.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n/, '')
  const lines = body.split(/\r?\n/)
  const headings = []
  const slugger = new GithubSlugger()
  let fence

  for (const line of lines) {
    const fenceMatch = line.match(/^\s*(`{3,}|~{3,})/)
    if (fenceMatch) {
      if (!fence) fence = fenceMatch[1][0]
      else if (line.trimStart().startsWith(fence.repeat(3))) fence = undefined
      continue
    }
    if (fence) continue

    const match = line.match(/^(##|###)\s+(.+?)\s*#*\s*$/)
    if (!match) continue

    const text = stripMarkdown(match[2])
    headings.push({ id: slugger.slug(text), text, depth: match[1].length })
  }

  return headings
}

async function walk(directory) {
  const entries = await fs.readdir(directory, { withFileTypes: true })
  const files = []
  for (const entry of entries) {
    const full = path.join(directory, entry.name)
    if (entry.isDirectory()) files.push(...(await walk(full)))
    else if (entry.name.endsWith('.mdx')) files.push(full)
  }
  return files
}

const files = await walk(docsRoot)
const pages = []
const search = []
const sourceBySlug = new Map()

for (const file of files) {
  const source = await fs.readFile(file, 'utf8')
  const match = source.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n/)
  const frontmatter = match ? YAML.parse(match[1]) : {}
  const slug = path.relative(docsRoot, file).replaceAll('\\', '/').replace(/\.mdx$/, '')
  const headings = headingsOf(source)

  if (!frontmatter.title || !frontmatter.section || !Number.isFinite(frontmatter.order)) {
    throw new Error(`${path.relative(root, file)} must define title, section and a numeric order in frontmatter`)
  }

  const page = {
    slug,
    url: `/docs/${slug}`,
    title: frontmatter.title,
    description: frontmatter.description ?? '',
    section: frontmatter.section,
    order: frontmatter.order,
    headings,
  }

  pages.push(page)
  sourceBySlug.set(slug, source.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n/, '').trim())
  search.push({
    id: slug,
    title: page.title,
    description: page.description,
    section: page.section,
    url: page.url,
    headings: headings.map((heading) => heading.text),
    body: cleanBody(source),
  })
}

pages.sort((a, b) => {
  const section = sectionOrder.indexOf(a.section) - sectionOrder.indexOf(b.section)
  return section || a.order - b.order || a.title.localeCompare(b.title)
})

await fs.mkdir(output, { recursive: true })
await fs.writeFile(path.join(output, 'generated-nav.json'), `${JSON.stringify({ pages }, null, 2)}\n`)
await fs.writeFile(path.join(output, 'generated-search.json'), `${JSON.stringify(search, null, 2)}\n`)

// llms.txt (https://llmstxt.org) and the full plain-text docs for coding agents.
const siteUrl = 'https://runwick.temtem.dev'
const summary = 'Runwick is a background-job library for .NET 10. An app writes a job to a store (PostgreSQL, SQLite or Redis) and a worker, in the same process or another, runs its handler. Delivery is at-least-once; schedules, retries, rate limits, uniqueness, a transactional outbox, a management API, a dashboard and a CLI are included.'
const llmsIndex = [
  '# Runwick',
  '',
  `> ${summary}`,
  '',
  `Full documentation as one file: ${siteUrl}/llms-full.txt. Agent conventions: ${siteUrl}/docs/ai-agents.`,
  '',
  ...sectionOrder.flatMap((section) => {
    const inSection = pages.filter((page) => page.section === section)
    if (inSection.length === 0) return []
    return [`## ${section}`, '', ...inSection.map((page) => `- [${page.title}](${siteUrl}/docs/${page.slug}): ${page.description}`), '']
  }),
].join('\n')
const llmsFull = [
  '# Runwick documentation',
  '',
  `> ${summary}`,
  '',
  ...pages.flatMap((page) => ['---', '', `# ${page.title}`, '', `URL: ${siteUrl}/docs/${page.slug}`, '', page.description ? `> ${page.description}\n` : '', sourceBySlug.get(page.slug), '']),
].join('\n')
const publicRoot = path.join(root, 'wwwroot')
await fs.mkdir(publicRoot, { recursive: true })
await fs.writeFile(path.join(publicRoot, 'llms.txt'), `${llmsIndex.trimEnd()}\n`)
await fs.writeFile(path.join(publicRoot, 'llms-full.txt'), `${llmsFull.trimEnd()}\n`)

console.log(`docs: ${pages.length} pages`)
