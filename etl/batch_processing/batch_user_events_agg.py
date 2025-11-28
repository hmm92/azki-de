import os
import logging
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from spark_session import create_spark
from clickhouse_utils import (
    get_clickhouse_credentials,
    read_clickhouse_table,
    write_clickhouse_table,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("backfill_user_events_agg")

logger.info("Job started")

end_date_str = "2025-11-27"
end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
start_date = end_date - timedelta(days=100)

start_date_str = start_date.isoformat()
end_date_str = end_date.isoformat()

logger.info(f"Window: {start_date_str} → {end_date_str}")

spark = create_spark(app_name="backfill_user_events_agg")
conn = get_clickhouse_credentials()

try:
    logger.info("Reading users")
    df_users: DataFrame = read_clickhouse_table(
        spark=spark,
        conn=conn,
        table="users",
        date_column="signup_date",
        start_date="2000-01-01",
        end_date=end_date_str,
    )
    logger.info(f"Users count: {df_users.count()}")

    logger.info("Reading user_events")
    df_user_events: DataFrame = read_clickhouse_table(
        spark=spark,
        conn=conn,
        table="user_events",
        date_column="event_time",
        start_date=start_date_str,
        end_date=end_date_str,
    )
    logger.info(f"User events count: {df_user_events.count()}")

    df_user_events_with_date = df_user_events.withColumn(
        "event_date", F.to_date("event_time")
    )
    df_users_small = df_users.select("user_id", "city")

    df_joined = df_user_events_with_date.join(
        df_users_small, on="user_id", how="left"
    )

    df_user_events_agg: DataFrame = (
        df_joined
        .groupBy("event_date", "city", "channel")
        .agg(
            F.sum("premium_amount").alias("total_premium"),
            F.count(F.lit(1)).alias("events_count"),
        )
    )

    logger.info(f"Aggregated rows: {df_user_events_agg.count()}")
    df_user_events_agg.show(10, truncate=False)

    write_clickhouse_table(
        spark=spark,
        df = df_user_events_agg,
        conn=conn,
        table="daily_premium_by_city_channel",
        mode="append"
    )
finally:
    logger.info("Stopping Spark session")
    spark.stop()

logger.info("Job completed")
