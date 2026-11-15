## Grouped outbox delivery (D4)

**What changed.** The relay can now send several messages it claimed together to the destination as one transaction. Each message keeps everything that made single delivery safe: its own transport command ID and deadline, its own receipt, its own delivery mapping and its own reply. After the destination answers, the relay marks each message delivered at the source under that message's own lease token, as before. Only the connection, the transaction and the commit are shared.

It is off by default. `OutboxRelayOptions.DeliveryGroupSize` is 1 (each message alone, exactly the old behavior); 2 to 16 turns grouping on. Only plain messages are grouped (no uniqueness key, no concurrency scope, no rate limit), so a group never takes a row lock that another group might also want. Every other message in the same claim is sent alone at the same time.

**PostgreSQL destination.** Migration `0023_outbox_group` (schema revision 23) adds one driver-only function, `receive_outbox_group_v1`. It checks the shape of the whole group first (2 to 16 items, no repeated command or message identity, no keyed item, at most 2 MiB of payload) before it creates any receipt. It then runs each item through the existing `receive_outbox_v3` in its own subtransaction, in command-ID order. An item that is refused (conflict, expired command, passed horizon, invalid) rolls back with its receipt placeholder and reports its own reply, and the others commit. No protocol operation was added, and the manifest changes only its schema revision. The migration file may need a different number when it is merged with the other PostgreSQL migration in this round.

**SQLite destination.** No relay change was needed. SQLite already commits concurrent producer calls together (up to 8 calls or 1 MiB queued at the same moment, each in its own savepoint). `ReceiveOutbox/1` now goes through that same path, so relay slots that send at the same time share one commit.

**Redis destination.** Not changed. It does not implement the grouped call, so the option is ignored and every message is sent alone. A grouped Redis script would be a separate change.

**If a group fails.** Any answer that is not a complete, applied group reply (a lost reply, an unreachable destination, a timeout, a refusal of the whole group) makes the relay send each command alone with its original command ID. The per-command receipts and the delivery mapping answer the ones the group already applied, so no second job can appear. This is covered by a test that applies the group and then loses the reply.

**Tests.** Seven new test methods in `GroupedDeliveryTests` (11 runs, counting both source adapters), all passing:

- a protocol-level group with fresh, replayed, conflicting and expired commands, each getting its own reply and receipt, with no leftover receipt for the refused ones;
- groups the store refuses before any effect (one command, a repeated command, a repeated message identity);
- the relay delivering twelve messages as groups, one job and one mapping each, on both source adapters;
- a lost group reply followed by single resends (one receipt per command, no second job);
- one message that blocks at the relay while its claim mates are delivered and confirmed;
- delivered marks lost after a group (written but reply lost, and never written), repaired by the mappings;
- the option having no effect on a SQLite destination.

The whole Outbox project passed 57 of 57 before the last test was added (the last test passed on its own, 2 of 2), the SQLite project passes (54 of 54), and the PostgreSQL migration, operation-version, round-trip-budget, wake-notification and batch-cap cases pass (83 of 83) with the revision 23 golden manifest. The outbox and relay contract cases in the Contract project pass (14 of 14).

**What is not done.**

- P-09 numbers: see the measurement section below; they are informational because the host is shared.
- Batched source marks (D2) are not part of this change; the mark of each message is still its own source call.
- The earlier isolated destination-wake candidate (`p09-d4/receive-wake.patch`) was not adopted here. It adds its own migration and entry point that would have to be reconciled with `0023_outbox_group`.
- Grouping on Redis.

## P-09 measurement (informational, shared host)

Setup: the P-09 workload (`--rate 100`, 8 business writers, 1 KiB intents, 30 s warm-up, 60 s measurement, 2 min drain), PostgreSQL 18 source and destination in a fresh pinned container with fsync, synchronous commit and full-page writes on. The same build runs both arms: group size 1 against group size 4 at 4 relay slots. The host was shared with the SP-029 qualification run and several other agents' builds and benchmarks, so absolute rates are far below earlier quiet-host numbers and only the paired comparison means anything.

**Pair 1 at 4 slots (the only completed pair when this was written):**

| Arm | Confirmed deliveries/s | Destination commit, mean per call | Destination function, mean per call | Lease losses | Fallbacks |
|---|---|---|---|---|---|
| Group size 1 | 19.45 | 72.8 ms | 35.9 ms | 13 | 0 |
| Group size 4 | 25.27 | 44.0 ms | 29.2 ms | 28 | 0 |

Grouped delivery confirmed 1.30× as many messages per second in this pair. The 1,170 group calls carried 2,658 commands, an average group of 2.3, because a claim only returns as many messages as there are free slots. Neither arm drained its backlog inside the 2-minute bound (both stayed far under the 100/s offered rate), so the run reports a run failure and no ready-to-confirm p99. Every message that was delivered had exactly one mapping and one job and no mark mismatches, in both arms. **This does not meet P-09** (100 deliveries/s, p99 at most 2 s). One pair is a direction, not a result.

The rest of the plan (three pairs at 4 slots and group 4, three pairs at 16 slots and group 8) was started as a detached script; it waits its turn on the shared benchmark lock. Results land in `artifacts/perf/round-two/outbox-d4/slots4-g4/` and `slots16-g8/` in the worktree, and `python3 summarize.py <dir>` in `artifacts/perf/round-two/outbox-d4/` prints the paired medians. The 16-slot run is the separate 16-slot measurement the plan asks for.
