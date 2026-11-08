import { Link, router } from '@inertiajs/react'
import { useState } from 'react'
import { mutationHeaders } from '../csrf'

export default function Job({ base, job, canOperate, outcome }) {
  const [payload, setPayload] = useState(null)
  const retry = () => router.post(`${base}/jobs/${job.id}/retry`, { revision: job.revision }, { headers: mutationHeaders(crypto.randomUUID()) })
  // Sensitive detail is a separate no-store request, never an Inertia prop, so it never enters history.
  const reveal = async () => {
    const response = await fetch(`${base}/jobs/${job.id}/payload`, { headers: { Accept: 'application/json' } })
    setPayload(response.ok ? JSON.stringify(await response.json()) : `denied (${response.status})`)
  }
  return (
    <main>
      <Link href={base} data-testid="back">All jobs</Link>
      <h1 data-testid="job-name">{job.name}</h1>
      <p>State: <span data-testid="job-state">{job.state}</span>, retries: <span data-testid="retries">{job.retries}</span></p>
      {outcome && <p className="notice" data-testid="outcome">{outcome.status} {outcome.commandId}</p>}
      {canOperate && <button data-testid="retry" onClick={retry}>Retry</button>}
      <button data-testid="reveal" onClick={reveal}>Reveal payload</button>
      {payload && <pre data-testid="payload">{payload}</pre>}
    </main>
  )
}
