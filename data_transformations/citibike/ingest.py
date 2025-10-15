import logging
from typing import List

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)     #added by shweta , to get log info

def sanitize_columns(columns: List[str]) -> List[str]:
    return [column.replace(" ", "_") for column in columns]      #DQ check


def run(spark: SparkSession, ingest_path: str, transformation_path: str) -> None:
    logging.info("Reading text file from: %s", ingest_path)
    input_df = spark.read.format("org.apache.spark.csv").option("header", True).csv(ingest_path)
    input_df = spark.read.format("csv").option("header", True).load(ingest_path)     #added by shweta
    renamed_columns = sanitize_columns(input_df.columns)
    ref_df = input_df.toDF(*renamed_columns)
    ref_df.printSchema()
    ref_df.show()

    ref_df.write.parquet(transformation_path)
    ref_df.write.mode("overwrite").parquet(transformation_path) #changes done by shweta
    logging.info("Parquet written to : %s", transformation_path)   #changes done by shweta
