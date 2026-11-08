import { Link, router } from '@inertiajs/react'

export default function Overview({ base, mount, user, jobs, stats }) {
  return (
    <main>
      <h1 data-testid="mount">{mount}</h1>
      <p>Signed in as <span data-testid="user">{user}</span></p>
      <table>
        <thead><tr><th>Job</th><th>State</th></tr></thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.id}>
              <td><Link href={`${base}/jobs/${job.id}`} data-testid={`job-${job.id}`}>{job.name}</Link></td>
              <td>{job.state}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <button data-testid="load-stats" onClick={() => router.reload({ only: ['stats'] })}>Load stats</button>
      {stats && <p data-testid="stats">{stats.total} jobs</p>}
    </main>
  )
}
