// Syntax colours from the Runwick palette (pine keywords, violet types, sage strings, chocolate numbers).
// Every token colour holds at least 4.5:1 on the code background in its theme; comments use the muted ink.
type Palette = Record<"bg" | "fg" | "comment" | "keyword" | "type" | "fn" | "string" | "number" | "attr" | "punct", string>

const theme = (name: string, type: "light" | "dark", c: Palette) => ({
  name,
  type,
  colors: { 'editor.background': c.bg, 'editor.foreground': c.fg },
  tokenColors: [
    { settings: { foreground: c.fg } },
    { scope: ['comment', 'punctuation.definition.comment'], settings: { foreground: c.comment, fontStyle: 'italic' } },
    { scope: ['keyword', 'storage', 'storage.type', 'storage.modifier', 'keyword.operator.new', 'variable.language'], settings: { foreground: c.keyword } },
    { scope: ['entity.name.type', 'entity.name.class', 'entity.name.interface', 'support.type', 'support.class', 'entity.other.inherited-class', 'entity.name.tag'], settings: { foreground: c.type } },
    { scope: ['entity.name.function', 'support.function', 'meta.function-call'], settings: { foreground: c.fn } },
    { scope: ['string', 'string.quoted', 'string.template', 'markup.inline.raw'], settings: { foreground: c.string } },
    { scope: ['constant.numeric', 'constant.language', 'constant.character', 'support.constant'], settings: { foreground: c.number } },
    { scope: ['entity.other.attribute-name', 'meta.attribute', 'variable.parameter', 'support.type.property-name', 'meta.object-literal.key'], settings: { foreground: c.attr } },
    { scope: ['punctuation', 'keyword.operator'], settings: { foreground: c.punct } },
    { scope: ['markup.deleted'], settings: { foreground: c.number } },
    { scope: ['markup.inserted'], settings: { foreground: c.string } },
  ],
})

export const runwickLight = theme('runwick-light', 'light', {
  bg: '#f4f4ea', fg: '#241623', comment: '#6b5468', keyword: '#2c6166', type: '#6b3a66',
  fn: '#38283a', string: '#59591f', number: '#8c4520', attr: '#2c6166', punct: '#5e4d5c',
})

export const runwickDark = theme('runwick-dark', 'dark', {
  bg: '#1f141f', fg: '#ecebdf', comment: '#bba8b9', keyword: '#86c6cc', type: '#d8d49b',
  fn: '#f1efe4', string: '#bcd96f', number: '#eaa881', attr: '#86c6cc', punct: '#c9bcc8',
})
