## Outbox D: grouped delivery under mixed outcomes, stalled owners and crashes

**What this round was for.** The original Outbox D item asks for grouped destination delivery where every message keeps its own transport identity and receipt, where source confirmation stays guarded by each message's lease token, and where mixed outcomes, lost replies and reclaims are tested. Grouped delivery itself was already built (merged 2026-11-15) and has been on by default with groups of four since 2026-11-20. On a PostgreSQL destination, one relay slot sends up to `DeliveryGroupSize` plain messages in one call (`receive_outbox_group_v1`). Each message still gets its own command ID, deadline, receipt and delivery mapping. The relay then marks each message delivered under its own lease token. This round adds no code to the relay or the stores. It adds tests for the three failure shapes that were not yet tested for groups, run on both the PostgreSQL and SQLite sources.

**What is proven, and how.**

- **Mixed outcomes in one group (OUTBOX-08).** Four messages go out in one group call, with no fallback. Two are new. One was already delivered earlier, as if its reply had been lost; the destination answers it from the existing mapping, with the same job. One message's identity already maps to a different job, so the destination reports a conflict. Result: three messages are delivered with their own job IDs, and the conflicting one is blocked with `delivery_conflict` while its mapping still points at the other job. The bad item does not hold back the others. This test passed on both sources in a smoke run.
- **A stalled relay cannot confirm after a takeover (OUTBOX-09).** Relay 1's group hangs at the destination. Its lease renewals report success but never reach the source, which is what a frozen process looks like to its own code. Relay 2 takes over the expired leases and delivers all four messages. When relay 1's group finally returns, each delivered mark relay 1 tries is refused because its lease token is stale. Relay 1 confirms nothing, every row keeps relay 2's result, and there is one job per message.
- **A group that commits after its relay crashed (OUTBOX-10).** Relay 1 dies with a group in flight. The group then commits at the destination with no one listening for the reply, so four jobs exist while the source rows are still leased. A new relay reclaims the leases once they expire. The destination recognises each message from its receipt or mapping, every message is delivered with its proposed job ID, and there are still four jobs.
- **Lost group replies** were already tested (`A_lost_group_reply_sends_the_same_commands_alone_and_creates_no_second_job`). If a group reply is lost, the relay resends the same commands one at a time instead of resending the group, and the per-command receipts replay what the group already applied.

**Not changed.** Grouping still happens only on a PostgreSQL destination. A SQLite destination already shares commits through its producer group, and a Redis destination sends each message alone. Redis grouping is still open.

**Test results.** On the loaded host, all 17 grouped-delivery tests passed when run on their own. The whole outbox suite passed 78 of 79, with 2 bench tests skipped by design. The one failure was OUTBOX-09 on PostgreSQL: it timed out after 30 s waiting for relay 1's stale marks to be refused. It passed 3 of 3 times alone at load 21 to 30, so its wait was raised to 90 s.

**P-09 qualification (2026-11-23).** The host was at load 17 to 37, so the timings are informational. Each run offered 100 messages/s and measured 3 minutes.

| | confirmed/s | ready-to-confirm p50 | p99 | max |
|---|---|---|---|---|
| 4 slots, groups of 4 | 99.5 | 0.30 s | 2.18 s | 2.48 s |
| 16 slots, groups of 4 | 101.6 | 0.15 s | 1.29 s | 3.77 s |
| 4 slots, no grouping | 45.7 | 127 s | 147 s | 148 s |

In every run nothing was lost or delivered twice, and no message was left unconfirmed.

At four slots the relay kept up with the offered rate, but p99 was 0.18 s over the 2 s limit. That matches the 2.1 to 2.3 s seen at similar load on 2026-11-20, against a pass at load 7 to 13. Sixteen slots met both targets. Without grouping, four slots could not keep up at all and the backlog grew for the whole run. So grouping is what makes four slots viable, and the four-slot p99 target is met only on a quieter host.
