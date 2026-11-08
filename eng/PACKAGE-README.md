# Runwick

Runwick is a background-job library for .NET 10. Jobs are stored in PostgreSQL, SQLite or Redis and run by
workers inside your application or in separate processes.

- Delivery is at-least-once. Make handlers safe to run twice, or use idempotency keys.
- Cancellation is cooperative: a running handler is asked to stop through its cancellation token.
- Exactly-once effects exist only for work done in the same database as the job (transactional completion).

Documentation: the `docs/guide` folder of the source repository. Licence: MIT.
