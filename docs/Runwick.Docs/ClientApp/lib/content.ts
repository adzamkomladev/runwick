import navData from './generated-nav.json'

export interface DocFrontmatter {
  title: string
  description?: string
  section: string
  order: number
}

export interface DocPage extends DocFrontmatter {
  slug: string
  url: string
  headings: Heading[]
}

export interface NavSection {
  title: string
  pages: DocPage[]
}

export interface Heading {
  id: string
  text: string
  depth: 2 | 3
}

const sectionOrder = ['Get started', 'Jobs', 'Stores', 'Operations', 'Reference']

export const pages: DocPage[] = navData.pages as DocPage[]

export const navigation: NavSection[] = sectionOrder
  .map((title) => ({ title, pages: pages.filter((page) => page.section === title) }))
  .filter((section) => section.pages.length > 0)

const bySlug = new Map(pages.map((page) => [page.slug, page]))

export const getPage = (slug: string) => bySlug.get(slug.replace(/^\/|\/$/g, ''))

export function getAdjacent(slug: string) {
  const index = pages.findIndex((page) => page.slug === slug)
  return {
    previous: index > 0 ? pages[index - 1] : undefined,
    next: index >= 0 && index < pages.length - 1 ? pages[index + 1] : undefined,
  }
}

export const getHeadings = (slug: string) => getPage(slug)?.headings ?? []

export interface SearchEntry {
  id: string
  title: string
  description: string
  section: string
  url: string
  headings: string[]
  body: string
}

let searchIndex: Promise<SearchEntry[]> | undefined

export function getSearchIndex() {
  searchIndex ??= import('./generated-search.json').then((module) => module.default as SearchEntry[])
  return searchIndex
}
