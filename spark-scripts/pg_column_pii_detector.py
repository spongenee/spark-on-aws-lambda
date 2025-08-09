import argparse
import json
import logging
import os
import sys
import boto3
from botocore.exceptions import ClientError

from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from pyspark.context import SparkContext
from glue_functions import extract_jdbc_conf
import random

randint = random.getrandbits(128)
randstr = f'{randint:032x}'
print(randstr)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info("------> Building spark session...")
spark = SparkSession.builder.appName("SourceDBSession") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "5g") \
    .getOrCreate()
sc = spark.sparkContext
analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()
broadcasted_analyzer = sc.broadcast(analyzer)
broadcasted_anonymizer = sc.broadcast(anonymizer)

def anonymize_text(text: str) -> str:
    analyzer = broadcasted_analyzer.value
    anonymizer = broadcasted_anonymizer.value
    analyzer_results = analyzer.analyze(text=text, language="en", allow_list = ['"',"'",":","{","}",",", "=>"])
    anonymized_results = anonymizer.anonymize(
        text=text,
        analyzer_results=analyzer_results,
        operators={"DEFAULT": OperatorConfig("encrypt", {"key": randstr})},
    )
    return anonymized_results.text


def anonymize_dataframe_multiple_columns(df, column_names: list):
    """
    Anonymize multiple columns in a PySpark DataFrame.

    Args:
        df: Input PySpark DataFrame
        column_names: List of column names to anonymize

    Returns:
        DataFrame with specified columns anonymized
    """
    anonymize_udf = udf(anonymize_text, StringType())

    # Apply anonymization to each specified column
    for column_name in column_names:
        column_name = column_name.strip()
        if column_name in df.columns:
            df = df.withColumn(column_name, anonymize_udf(col(column_name)))
        else:
            print(f"Warning: Column '{column_name}' not found in DataFrame")

    return df


"""
 Function that gets triggered when AWS Lambda is running.
 We are using the example from Redshift documentation
 https://docs.aws.amazon.com/redshift/latest/dg/spatial-tutorial.html#spatial-tutorial-test-data

  Add the below parameters in the labmda function
  SCRIPT_BUCKET       BUCKER WHER YOU SAVE THIS SCRIPT
  SPARK_SCRIPT        THE SCRIPT NAME AND PATH
  INPUT_PATH          s3a://redshift-downloads/spatial-data/accommodations.csv
  OUTPUT_PATH         s3a://YOUR_BUCKET/YOUR_PATH
  DATABASE_NAME       AWS Glue Database name
  TABLE_NAME          AWS Glue Table name

  Create the below table in Athena

  CREATE EXTERNAL TABLE accommodations_delta
  LOCATION 's3://YOUR_BUCKET/YOUR_PATH' 
  TBLPROPERTIES (
      'table_type'='DELTA'
  );

"""

AWS_REGION = 'us-east-1'


def get_secret(secret_id):
    secrets_client = boto3.client(service_name='secretsmanager', region_name=AWS_REGION)
    try:
        secret_response = secrets_client.get_secret_value(SecretId=secret_id)
        secret_data = json.loads(secret_response['SecretString'])
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_id}: {e}")
    return secret_data


def main(db_name, table_name, columns):
    jdbc_conf = extract_jdbc_conf(db_name, AWS_REGION)
    logger.info(f"------> Retrieving Dataframe of {table_name} from {db_name}")
    secret_data = get_secret(jdbc_conf['secretId'])
    username = secret_data.get('username', '')
    password = secret_data.get('password', '')
    df = spark.read.jdbc(
        url = jdbc_conf['fullUrl'],
        table=table_name,
        properties={"user": username, "password": password, "driver": jdbc_conf['driver']}
    )
    # apply the udf
    anonymized_df = anonymize_dataframe_multiple_columns(df, columns)

    anonymized_df.show(n=2)
    target_jdbc_conf = extract_jdbc_conf("staging3-target-vfadb", AWS_REGION)

    anonymized_df.write.jdbc(
        url=target_jdbc_conf["fullUrl"],
        table=table_name,
        mode="append",
        properties={"user": target_jdbc_conf["user"], "password": target_jdbc_conf["password"], "driver": target_jdbc_conf["driver"], "stringtype":"unspecified"}
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--event",
                        help="event data from lambda")
    args = parser.parse_args()
    params = json.loads(args.event)
    main(db_name=params["DATABASE_NAME"], table_name=params["TABLE_NAME"], columns=params["COLUMNS"].split(','))
