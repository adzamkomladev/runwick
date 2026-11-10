import { Head, usePage } from '@inertiajs/react'
import ogImage from '../assets/og.png?url'

interface SeoProps {
  title: string
  description: string
  type?: 'website' | 'article'
  section?: string
  noIndex?: boolean
}

const siteName = 'Runwick'

export function Seo({ title, description, type = 'website', section, noIndex = false }: SeoProps) {
  const { props, url } = usePage()
  const baseUrl = String(props.baseUrl ?? '').replace(/\/$/, '')
  const path = url.split('?')[0].split('#')[0]
  const canonical = `${baseUrl}${path}`
  const image = ogImage.startsWith('http') ? ogImage : `${baseUrl}${ogImage}`

  return (
    <Head>
      <title head-key="title">{title}</title>
      <meta head-key="description" name="description" content={description} />
      <link head-key="canonical" rel="canonical" href={canonical} />
      <meta head-key="robots" name="robots" content={noIndex ? 'noindex, nofollow' : 'index, follow'} />

      <meta head-key="og:site_name" property="og:site_name" content={siteName} />
      <meta head-key="og:type" property="og:type" content={type} />
      <meta head-key="og:title" property="og:title" content={title} />
      <meta head-key="og:description" property="og:description" content={description} />
      <meta head-key="og:url" property="og:url" content={canonical} />
      <meta head-key="og:image" property="og:image" content={image} />
      <meta head-key="og:image:type" property="og:image:type" content="image/png" />
      <meta head-key="og:image:width" property="og:image:width" content="1200" />
      <meta head-key="og:image:height" property="og:image:height" content="630" />
      <meta head-key="og:image:alt" property="og:image:alt" content={`${siteName}: ${description}`} />
      {type === 'article' && section && <meta head-key="article:section" property="article:section" content={section} />}

      <meta head-key="twitter:card" name="twitter:card" content="summary_large_image" />
      <meta head-key="twitter:title" name="twitter:title" content={title} />
      <meta head-key="twitter:description" name="twitter:description" content={description} />
      <meta head-key="twitter:image" name="twitter:image" content={image} />
    </Head>
  )
}
