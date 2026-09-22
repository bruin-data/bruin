# Data-quality checks

Column checks live with their assets so failures appear in lineage. They cover primary keys, nulls, accepted values, freshness inputs and foreign-key relationships. Run `bruin validate .` before a backfill and query `marts.mart_pipeline_health` after it.
