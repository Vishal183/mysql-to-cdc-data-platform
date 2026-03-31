"""Delta Lake writer with MERGE upsert/delete logic for CDC events."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, coalesce, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


def make_foreach_batch_handler(spark: SparkSession, delta_path: str, primary_key: str):
    """Factory function to create a foreachBatch handler."""

    def upsert_to_delta(micro_batch_df: DataFrame, batch_id: int) -> None:
        if micro_batch_df.isEmpty():
            return

        count = micro_batch_df.count()
        logger.info(f"Processing batch {batch_id}: {count} events for {delta_path}")

        df = micro_batch_df.withColumn(
            primary_key,
            coalesce(col(primary_key), col("__before_pk"))
        )

        window = Window.partitionBy(primary_key).orderBy(col("__ts_ms").desc())
        deduped = (
            df.withColumn("_rn", row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn", "__before_pk")
        )

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
            initial = deduped.filter("__op != 'd'").select(data_columns)
            if initial.count() > 0:
                initial.write.format("delta").mode("overwrite").save(delta_path)
                logger.info(f"Batch {batch_id}: created Delta table at {delta_path}")

    return upsert_to_delta
