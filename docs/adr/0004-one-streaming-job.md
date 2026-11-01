---
status: proposed
---

# Apply every Change in one streaming job, not in partitioned workers

One Databricks streaming job reads the Landing log's change feed in landing order and is the Listing Store's only writer: its Ingestion job applies every Change, and its maintenance thread runs `OPTIMIZE` and `VACUUM`. ADR-0001's 64 partitions then protect nothing: per-key order comes from the feed's order and the conditional MERGE, not from partition ownership. The Listing Store is unpartitioned and liquid-clustered on the Listing key, with deletion vectors. This supersedes [ADR-0001](0001-partition-by-listing-key.md).

## Considered Options

- **Keep the 64 partitions.** Rejected: the stream is the only writer, so partition ownership protects nothing.
- **Run the Backfill outside the job, in Python on Container Apps.** Rejected: its gate needs the stream's checkpoint, which only Spark reads.
- **Send reclassifies through the API.** Rejected: it needs an internal route and key.
- **One query per Category.** Rejected: reclassification would split one Listing across queries. Per-Category logic, once it exists, becomes rules inside the job, not separate routing.

## Consequences

ADR-0001's rules that still hold:

- No Category routing: one job serves every Category.
- One writer: only the streaming job writes the Listing Store, and only its Ingestion job changes Listings. Anything else that changes a Listing appends to the Landing log or runs inside the job, as the Backfill does at the tail of each micro-batch.
- Order: Changes apply in landing order, `(_commit_version, seq)` from the change feed, and the highest source version per key wins. The stream's first start pins `startingVersion` to the earliest retained version, so every row keeps its own commit version.
- A bad Change never blocks: it gets its Outcome and is skipped. A storage failure fails the batch without advancing the checkpoint.
