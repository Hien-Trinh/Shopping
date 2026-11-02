worker-0: python -m ingestion.worker --index 0 --workers 4 --classifier student
worker-1: python -m ingestion.worker --index 1 --workers 4 --classifier student
worker-2: python -m ingestion.worker --index 2 --workers 4 --classifier student
worker-3: python -m ingestion.worker --index 3 --workers 4 --classifier student
api: python -m ingestion.api
export: python -m ingestion.export
maintenance: python -m ingestion.maintenance
snapshots: python -m ingestion.snapshots
backfill: python -m ingestion.backfill --classifier student
