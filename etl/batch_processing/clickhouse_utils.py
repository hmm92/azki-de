import os
from typing import Dict
from pyspark.sql import DataFrame, SparkSession

def get_clickhouse_credentials() -> Dict[str, str]:
    host = os.getenv("CLICKHOUSE_HOST")
    port = os.getenv("CLICKHOUSE_PORT", "8123")
    database = os.getenv("CLICKHOUSE_DB")
    user = os.getenv("CLICKHOUSE_USER")
    password = os.getenv("CLICKHOUSE_PASSWORD")

    if not host or not database or not user or not password:
        raise ValueError("Missing ClickHouse DB env vars")

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }


def build_clickhouse_jdbc_url(host: str, port: str, database: str) -> str:
    return f"jdbc:clickhouse://{host}:{port}/{database}"


def build_clickhouse_query(
    table: str,
    date_column: str,
    start_date: str,
    end_date: str
) -> str:
    return (
        f"(SELECT *, toDate({date_column}) AS event_date "
        f"FROM {table} "
        f"WHERE toDate({date_column}) >= '{start_date}' "
        f"AND toDate({date_column}) <= '{end_date}') AS t"
    )

def read_clickhouse_table(
    spark: SparkSession,
    conn: Dict[str, str],
    table: str,
    date_column: str,
    start_date: str,
    end_date: str,
) -> DataFrame:

    jdbc_url = build_clickhouse_jdbc_url(
        conn["host"], conn["port"], conn["database"]
    )

    query = build_clickhouse_query(
        table=table,
        date_column=date_column,
        start_date=start_date,
        end_date=end_date,
    )

    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()
    )

    return df

def write_clickhouse_table(
    spark: SparkSession,
    df: DataFrame,
    conn: Dict[str, str],
    table: str,
    mode: str = "append",
) -> None:

    jdbc_url = build_clickhouse_jdbc_url(
        conn["host"], conn["port"], conn["database"]
    )

    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode(mode)
        .save()
    )
