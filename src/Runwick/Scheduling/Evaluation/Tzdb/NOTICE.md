# Time-zone data artifact

`tzdb2026f.nzd` is the Noda Time compilation of IANA time zone database release 2026f, downloaded from
`https://nodatime.org/tzdb/tzdb2026f.nzd` on 2026-10-31.

- Size: 132,773 bytes
- SHA-256: `b17c31650e9163761b50645b4ab3806649deb24be55c790857fdd004b5611be3`

The IANA time zone database is in the public domain (https://www.iana.org/time-zones). The NZD format and the
compiler that produced this file are part of Noda Time, licensed under the Apache License 2.0
(https://github.com/nodatime/nodatime/blob/main/LICENSE.txt).

The evaluator refuses any other bytes (12 §6.1). Replacing this file is a data upgrade (SCHED-13), never an edit in
place: add the new release beside it, update the artifact identity, and record its hash here.
