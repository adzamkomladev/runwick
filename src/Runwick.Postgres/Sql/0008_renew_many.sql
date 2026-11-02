-- 0008_renew_many (PF-06, 03/04 Changes 2026-11-02): no catalog change. The revision adds RenewMany/1, batched lease
-- renewal over the existing jobs and concurrency_permits rows, to the compatibility manifest.
SET LOCAL lock_timeout = '5s';
