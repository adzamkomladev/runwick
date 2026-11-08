import { Layout } from '../lib'

export default function Settings({ settings }) {
  return (
    <Layout heading="Settings">
      <dl data-testid="settings">
        <dt>Application</dt><dd>{settings.application}</dd>
        <dt>Store</dt><dd>{settings.storeKind} (schema revision {settings.schemaRevision})</dd>
        <dt>Persistence</dt><dd>{settings.persistenceProfile}</dd>
        <dt>Roles in this process</dt><dd data-testid="roles">{settings.roles.join(', ')}</dd>
        <dt>Retention policy revision</dt><dd>{settings.retentionPolicyRevision}</dd>
        {Object.entries(settings.limits).map(([k, v]) => <FragmentRow key={k} name={k} value={v} />)}
      </dl>
    </Layout>
  )
}

const FragmentRow = ({ name, value }) => <><dt>{name}</dt><dd>{value}</dd></>
