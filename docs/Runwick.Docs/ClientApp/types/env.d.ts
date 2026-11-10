/// <reference types="vite/client" />

declare module '*.mdx' {
  import type { ComponentType } from 'react'
  import type { DocFrontmatter } from '../lib/content'

  export const frontmatter: DocFrontmatter
  const MDXContent: ComponentType<{ components?: Record<string, unknown> }>
  export default MDXContent
}
