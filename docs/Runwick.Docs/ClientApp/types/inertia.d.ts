export interface InertiaPages {
  Home: Record<string, never>
  'Docs/Show': DocsShowPage
  NotFound: Record<string, never>
}

export interface DocsShowPage {
  slug: string
}

export interface SharedProps {
  appName: string
  version: string
  baseUrl: string
}

export interface FlashData extends Flash {}

export interface Flash {}

declare module '@inertiajs/core' {
  export interface InertiaConfig {
    sharedPageProps: SharedProps
    flashDataType: FlashData
    errorValueType: string
  }
}
