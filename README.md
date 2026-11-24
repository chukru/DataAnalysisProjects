# DataAnalysisProjects

Beginner_Projects

## PySpark to Snowflake ETL

A batch ETL starter project demonstrating CSV extraction, PySpark transformations, and loading curated records into Snowflake.

### Project structure

```text
pyspark-snowflake-etl/
├── data/customers.csv
├── src/etl_pipeline.py
├── .env.example
├── .gitignore
└── requirements.txt
```

### Setup

- Python 3.10+ and Java 11 or 17
- Create and activate a virtual environment, then run `pip install -r requirements.txt`
- Copy `.env.example` to `.env` and set your Snowflake account, user, password, warehouse, database, and schema.
- Never commit `.env` or real credentials.

Run from the repository root:

```bash
spark-submit --packages net.snowflake:spark-snowflake_2.12:3.1.2-spark_3.5 src/etl_pipeline.py
```

The example reads `data/customers.csv`, standardizes column names, trims strings, lowercases email addresses, casts dates and amounts, filters invalid records, deduplicates by customer ID, adds an ETL timestamp, and overwrites `CUSTOMERS_CLEAN` in Snowflake.

The Snowflake Spark Connector artifact must match your Spark and Scala versions. This is a learning sample; add logging, validation, retries, incremental processing, and a MERGE-based load before production use.
