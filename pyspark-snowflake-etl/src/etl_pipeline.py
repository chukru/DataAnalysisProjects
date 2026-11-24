"""Basic batch ETL: CSV extraction -> PySpark transformations -> Snowflake load."""
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pyspark_snowflake_etl")


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def extract_csv(spark: SparkSession, file_path: str):
    """Extract source records from CSV."""
    logger.info("Extracting %s", file_path)
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(file_path)
    )


def transform_customers(source_df):
    """Clean, type-cast, validate, and deduplicate customer records."""
    df = source_df.toDF(
        *[c.strip().lower().replace(" ", "_") for c in source_df.columns]
    )
    df = (
        df.withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("customer_name", F.trim(F.col("customer_name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
        .withColumn("amount", F.col("amount").cast(DecimalType(12, 2)))
        .filter(
            F.col("customer_id").isNotNull()
            & F.col("email").isNotNull()
            & (F.length("email") > 0)
        )
    )
    window = Window.partitionBy("customer_id").orderBy(
        F.col("signup_date").desc_nulls_last(),
        F.col("amount").desc_nulls_last(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("etl_loaded_at", F.current_timestamp())
    )


def load_to_snowflake(df):
    """Write curated records with the Snowflake Spark Connector."""
    account = required_env("SNOWFLAKE_ACCOUNT")
    options = {
        "sfURL": f"{account}.snowflakecomputing.com",
        "sfUser": required_env("SNOWFLAKE_USER"),
        "sfPassword": required_env("SNOWFLAKE_PASSWORD"),
        "sfWarehouse": required_env("SNOWFLAKE_WAREHOUSE"),
        "sfDatabase": required_env("SNOWFLAKE_DATABASE"),
        "sfSchema": required_env("SNOWFLAKE_SCHEMA"),
        "dbtable": "CUSTOMERS_CLEAN",
    }
    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        options["sfRole"] = role

    logger.info("Writing %s rows to Snowflake", df.count())
    (
        df.write.format("net.snowflake.spark.snowflake")
        .options(**options)
        .option("sfSaveMode", "OVERWRITE")
        .mode("overwrite")
        .save()
    )
    logger.info("Snowflake load completed")


def main():
    project_root = Path(__file__).resolve().parents[1]
    source_path = project_root / "data" / "customers.csv"
    spark = SparkSession.builder.appName("PySpark-CSV-to-Snowflake").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = extract_csv(spark, str(source_path))
        logger.info("Extracted %s rows", source.count())
        curated = transform_customers(source)
        curated.show(truncate=False)
        load_to_snowflake(curated)
    except Exception:
        logger.exception("ETL pipeline failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
