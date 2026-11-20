"""Change Export: the Listing Store's change feed as Parquet files, one per micro-batch
(docs/specs/step-a7.md).

A file is a directory that Spark writes, named by its batch's `_commit_version` range, zero-padded
so name order is version order. It holds the latest row per Listing key. The query's checkpoint is
the watermark: Spark commits it after the batch returns, so a crash replays the batch, with the
same rows and the same name, over what the crash left. A stream with no checkpoint starts with the
store's current state as one file, which is also the recovery after a lost checkpoint.
"""

from functools import partial

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ingestion import entry

MAX_FILES = 2**31 - 1  # no cap that binds: a batch takes each waiting commit whole (decision 6)
_KEY = ("merchant_id", "merchant_product_id")
_FEED = ("_change_type", "_commit_version", "_commit_timestamp")


def apply_batch(data: str, batch: DataFrame, batch_id: int) -> None:
    rows = batch.filter("_change_type != 'update_preimage'")
    version = F.col("_commit_version")
    first, last, count = rows.agg(F.min(version), F.max(version), F.count("*")).first()
    if not count:  # the first batch over an empty store: Spark still calls it
        return
    # One MERGE holds one row per key, so the version alone orders a key's rows.
    latest = rows.groupBy(*_KEY).agg(F.max_by(F.struct("*"), version).alias("row")).select("row.*")
    # Tombstones are kept, so a physical delete is unexpected; export it as a delete anyway.
    deleted = F.col("is_tombstone") | (F.col("_change_type") == "delete")
    out = latest.withColumn("op", F.when(deleted, "delete").otherwise("upsert")).drop(*_FEED)
    # ponytail: coalesce(1) runs the collapse in one task; repartition(1) if a 1M batch is slow.
    path = f"{data}/export/{first:012}-{last:012}.parquet"
    out.coalesce(1).write.mode("overwrite").parquet(path)  # a replay writes it again (question 2)
    # A diagnostic (rule 6): a crash loses it, and a replay prints its own.
    entry.say({"type": "export", "batch_id": batch_id, "from": first, "to": last, "rows": count})


def start(spark: SparkSession, data: str, *, trigger: dict) -> StreamingQuery:
    """The export query. No `startingVersion`: the first start and a start after a lost checkpoint
    both export the store's current state (decision 5)."""
    reader = spark.readStream.format("delta").option("readChangeFeed", "true")
    feed = reader.option("maxFilesPerTrigger", MAX_FILES).load(f"{data}/listing_store")
    writer = feed.writeStream.queryName("export").foreachBatch(partial(apply_batch, data))
    checkpoint = f"{data}/checkpoints/export"
    return writer.option("checkpointLocation", checkpoint).trigger(**trigger).start()


def files(spark: SparkSession, data: str) -> list[list[dict]]:
    """Each export file's rows, sorted by Listing key, in name order: what a consumer replays."""
    rows = spark.read.parquet(f"{data}/export/*.parquet")
    rows = rows.withColumn("_file", F.col("_metadata.file_path")).orderBy("_file", *_KEY).collect()
    by_file: dict[str, list[dict]] = {}
    for row in rows:
        fields = row.asDict(recursive=True)
        by_file.setdefault(fields.pop("_file").rsplit("/", 2)[-2], []).append(fields)
    return list(by_file.values())
