# ruff: noqa
from pathlib import Path

from support import up
from test_ingest import commit, land, run


def show(label, value):
    print(f"\nPROBE {label}: {value}")


def attempt(label, f):
    try:
        show(label, f())
    except Exception as e:
        show(label, "ERROR " + str(e)[:260].replace("\n", " "))


def test_probe(spark, tmp_path):
    store = str(tmp_path / "listing_store")
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1)]))
    run(spark, tmp_path)
    loc = str(tmp_path / "snapshots" / "snapshot_v1")
    make = f"TABLE IF NOT EXISTS default.snapshot_v1 SHALLOW CLONE delta.`{store}` VERSION AS OF 1 LOCATION '{loc}'"
    attempt("named clone with LOCATION", lambda: spark.sql("CREATE " + make).collect())
    attempt("again, IF NOT EXISTS", lambda: spark.sql("CREATE " + make).collect())
    attempt(
        "show tables",
        lambda: [
            (r.namespace, r.tableName)
            for r in spark.sql("SHOW TABLES IN default LIKE 'snapshot_v*'").collect()
        ],
    )
    attempt(
        "rows by name",
        lambda: sorted(r.merchant_product_id for r in spark.table("default.snapshot_v1").collect()),
    )
    attempt(
        "history",
        lambda: [
            (r.version, r.timestamp, r.operation, r.operationParameters.get("sourceVersion"))
            for r in spark.sql("DESCRIBE HISTORY default.snapshot_v1").collect()
        ],
    )
    attempt(
        "detail",
        lambda: (
            spark.sql("DESCRIBE DETAIL default.snapshot_v1").select("createdAt", "location").first()
        ),
    )
    attempt("drop", lambda: spark.sql("DROP TABLE default.snapshot_v1").collect())
    show(
        "files left after drop",
        sorted(p.name for p in Path(loc).rglob("*") if p.is_file() and not p.name.endswith(".crc")),
    )
    attempt(
        "show tables after drop",
        lambda: [
            r.tableName for r in spark.sql("SHOW TABLES IN default LIKE 'snapshot_v*'").collect()
        ],
    )
    attempt("recreate over the left files", lambda: spark.sql("CREATE " + make).collect())
    attempt(
        "catalog impl",
        lambda: (
            spark.conf.get("spark.sql.catalogImplementation"),
            spark.conf.get("spark.sql.warehouse.dir"),
        ),
    )
    spark.sql("DROP TABLE IF EXISTS default.snapshot_v1")
