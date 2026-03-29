"""Delta Lake writer with MERGE upsert/delete logic for CDC events."""

import os
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, coalesce, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


def make_foreach_batch_handler(spark: SparkSession, delta_path: str, primary_key: str):
    """Factory function to create a foreachBatch handler with proper closure.

    Avoids the Python lambda-in-loop closure pitfall by capturing
    delta_path and primary_key as bound arguments.
    """

    def upsert_to_delta(micro_batch_df: DataFrame, batch_id: int) -> None:
        """Process a micro-batch of CDC events and merge into Delta Lake.

        Strategy:
        1. For deletes (op='d'), the 'after' columns are null -- use __before_pk
        2. Deduplicate within batch: keep latest event per PK by ts_ms
        3. Single atomic MERGE:
           - whenMatchedDelete for op='d'
           - whenMatchedUpdateAll for op='c'/'u'/'r'
           - whenNotMatchedInsertAll for new rows (skip deletes)
        """
        if micro_batch_df.isEmpty():
            return

        count = micro_batch_df.count()
        logger.info(f"Processing batch {batch_id}: {count} events for {delta_path}")

        # For delete events, the PK comes from __before_pk since 'after' is null
        # Coalesce to get PK regardless of operation type
        df = micro_batch_df.withColumn(
            primary_key,
            coalesce(col(primary_key), col("__before_pk"))
        )

        # Deduplicate: keep only the latest event per primary key within this batch
        window = Window.partitionBy(primary_key).orderBy(col("__ts_ms").desc())
        deduped = (
            df.withColumn("_rn", row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn", "__before_pk")
        )

        # Data columns = everything except CDC metadata
        data_columns = [c for c in deduped.columns if not c.startswith("__")]

        if DeltaTable.isDeltaTable(spark, delta_path):
            delta_table = DeltaTable.forPath(spark, delta_path)

            merge_condition = f"target.{primary_key} = source.{primary_key}"

            (
                delta_table.alias("target")
                .merge(deduped.alias("source"), merge_condition)
                .whenMatchedDelete(condition="source.__op = 'd'")
                .whenMatchedUpdate(
                    condition="source.__op != 'd'",
                    set={c: f"source.{c}" for c in data_columns},
                )
                .whenNotMatchedInsert(
                    condition="source.__op != 'd'",
                    values={c: f"source.{c}" for c in data_columns},
                )
                .execute()
            )

            logger.info(f"Batch {batch_id}: merged into {delta_path}")
        else:
            # First write -- create the Delta table (skip deletes)
            initial = deduped.filter("__op != 'd'").select(data_columns)
            if initial.count() > 0:
                (
                    initial.write
                    .format("delta")
                    .mode("overwrite")
                    .save(delta_path)
                )
                logger.info(f"Batch {batch_id}: created Delta table at {delta_path}")

    return upsert_to_delta
