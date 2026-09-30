# Phase 0 spike results

Sep 30, 2026 · delta-rs (`deltalake`) 1.6.6 · Python 3.14.3 · M4, 16 GB · script: [delta_spikes.py](delta_spikes.py)

| Spike | Result | What it means for the build |
| --- | --- | --- |
| **B1** Reproduce the MERGE hang (40 runs of a fresh table + first MERGE, 30 s timeout) | 0/40 hung | Not reproduced. The one hang seen earlier is still unexplained, so the Phase 3 heartbeat watchdog stays. |
| **Watchdog premise:** `kill -9` during a 5,000-row MERGE, 30 runs | 8 killed mid-write, **0 inconsistent tables** (version was either old or old+1, row count exact), and the next MERGE succeeded | Killing a stuck worker is safe. Delta commits are atomic, so a killed MERGE either landed completely or not at all. |
| **B2** OPTIMIZE running at the same time as MERGEs (4 writers; compacting other partitions *and* the partitions being written) | 0 retries, **0 duplicate keys, 0 lost updates** | Owner-only compaction is safe, and even cross-process compaction didn't break anything in this run. We still keep owner-only compaction (ADR-0001), since the sample is small. |
| **B2** OPTIMIZE of the whole Landing log while 2 processes append | 0 retries, all 5,184 rows present | A maintenance job can compact the Landing log while the API keeps appending. |
| **B3** Per-partition change-feed read: `load_cdf(starting_version, predicate="partition IN (...)")` over 100 appends | Exactly 2,000 of 32,000 rows, **0.036 s vs 0.293 s** for a full read | Workers read only their own partitions, about 8× faster. |
| **B3** Change feed after OPTIMIZE | Same 32,000 rows, no duplicates | Compaction commits (`dataChange=false`) are invisible to readers of the change feed. |
| **Group commit sizing:** append latency | ~14 ms/commit for 64 rows, ~15 ms for 960 rows | Commit cost is almost all fixed overhead. One appender tops out around 65 commits/s, so a 100 ms group-commit window (10 commits/s) has plenty of headroom, and big batches are nearly free. |
| **Worker MERGE sizing:** 1,000-row MERGE into a 1M-row, 64-partition store | 13 ms fresh, 11 ms after 50 MERGEs, 11 ms after compaction | Not a bottleneck: the 1,000-change batch cap costs about 11 ms per MERGE. Classification will dominate worker time. |

Earlier checks (Sep 29, same versions): the conditional MERGE (`s.sv > t.sv`) ignores stale writes; MERGE writes the change feed (`insert`, `update_preimage`, `update_postimage`); 8 processes × 20 MERGEs on disjoint partitions had 0 conflicts; a checkpoint is written automatically at version 99; and DuckDB queries `DeltaTable.to_pyarrow_dataset()` directly.

**No fallback needed.** The "64 separate Listing Store tables" fallback from the plan isn't triggered.

**Caveats:** these are small samples on one machine. Phase 7's chaos scenarios re-test the same properties at volume, with the oracles as the judge.
