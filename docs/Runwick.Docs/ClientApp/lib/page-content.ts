import type { ComponentType } from 'react'

export interface DocModule {
  default: ComponentType<{ components?: Record<string, unknown> }>
}

const modules = import.meta.glob<DocModule>('/content/docs/**/*.mdx', { eager: true })

export const getDocModule = (slug: string) => modules[`/content/docs/${slug.replace(/^\/|\/$/g, '')}.mdx`]
