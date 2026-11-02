"""Delta on local Spark: a write, a MERGE and a change-feed read (docs/specs/step-g5.md)."""

from delta.tables import DeltaTable


def test_a_merge_lands_in_the_change_feed(spark, tmp_path):
    path = str(tmp_path / "listings")
    rows = spark.createDataFrame([("a", 1), ("b", 2)], "key string, price long")
    rows.write.format("delta").option("delta.enableChangeDataFeed", "true").save(path)
    changes = spark.createDataFrame([("b", 3), ("c", 4)], "key string, price long")
    table = DeltaTable.forPath(spark, path).alias("t")
    merge = table.merge(changes.alias("c"), "t.key = c.key")
    merge.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    feed = spark.read.format("delta").option("readChangeFeed", "true")
    seen = feed.option("startingVersion", 1).load(path).select("key", "price", "_change_type")
    assert {tuple(r) for r in seen.collect()} == {
        ("b", 2, "update_preimage"),
        ("b", 3, "update_postimage"),
        ("c", 4, "insert"),
    }
