from pyspark.sql import SparkSession


def create_spark(app_name: str = "spark-job") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)  
        .config("spark.jars", "./jars/clickhouse-jdbc.jar")
        .getOrCreate()
    )
    return spark
