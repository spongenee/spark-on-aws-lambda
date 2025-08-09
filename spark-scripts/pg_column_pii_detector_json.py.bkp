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
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
from pyspark.context import SparkContext
from glue_functions import extract_jdbc_conf
import random
from presidio_structured import StructuredEngine, JsonDataProcessor, StructuredAnalysis, PandasDataProcessor
import tldextract

custom_cache_extract = tldextract.TLDExtract(cache_dir='/var/lang/lib/python3.10/site-packages/tldextract/.suffix_cache')

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


def anonymize_text(text: str, is_json: bool = False, entity_mapping: dict = None) -> str:
    """
    Anonymize text using Presidio analyzer and anonymizer.
    Handles both regular text and JSON-formatted text.

    Args:
        text: Input text to anonymize
        is_json: Whether the text is in JSON format
        entity_mapping: Dictionary mapping JSON keys to entity types (for JSON anonymization)

    Returns:
        Anonymized text
    """
    if text is None:
        return None

    if is_json:
        return anonymize_json_text(text, entity_mapping)
    else:
        return anonymize_regular_text(text)


def anonymize_regular_text(text: str) -> str:
    """Anonymize regular text using standard Presidio approach."""
    analyzer = broadcasted_analyzer.value
    anonymizer = broadcasted_anonymizer.value
    analyzer_results = analyzer.analyze(text=text, language="en")
    anonymized_results = anonymizer.anonymize(
        text=text,
        analyzer_results=analyzer_results,
        operators={"DEFAULT": OperatorConfig("encrypt", {"key": randstr})},
    )
    return anonymized_results.text


def anonymize_json_text(json_text: str, entity_mapping: dict = None) -> str:
    """
    Anonymize JSON-formatted text using presidio_structured.

    Args:
        json_text: JSON string to anonymize

    Returns:
        Anonymized JSON string
    """
    if entity_mapping is None:
        entity_mapping = {}
    try:

        # Use StructuredEngine for JSON anonymization
        json_data = json.loads(json_text)


    except (json.JSONDecodeError, Exception) as e:
        # If JSON parsing fails or other error, fall back to regular text anonymization
        print(f"Warning: Failed to parse JSON, using regular text anonymization: {e}")
        return anonymize_regular_text(json_text)
    json_analysis =  StructuredAnalysis(entity_mapping=entity_mapping)


    structured_engine = StructuredEngine(data_processor=JsonDataProcessor())

    # Anonymize the JSON structure
    anonymized_result = structured_engine.anonymize(
        json_data,
        json_analysis,
        operators={"DEFAULT": OperatorConfig("encrypt", {"key": randstr})},
    )

    return json.dumps(anonymized_result)


def anonymize_dataframe_as_tabular(df, column_name, entity_mapping):
    """
    Anonymize entire PySpark DataFrame by converting to pandas and using StructuredEngine.
    This approach treats the DataFrame as tabular data for structured anonymization.

    Args:
        df: Input PySpark DataFrame
    Returns:
        Anonymized PySpark DataFrame
    """
    # Convert PySpark DataFrame to Pandas
    pandas_df = df.select(["email",column_name]).toPandas()
    print(pandas_df.columns)
    # Initialize structured engine
    structured_engine = StructuredEngine(data_processor=PandasDataProcessor())
    tabular_analysis =  StructuredAnalysis(entity_mapping=entity_mapping)

    # Anonymize the DF column
    anonymized_df = structured_engine.anonymize(
        pandas_df.dropna(subset=[column_name]),
        tabular_analysis,
        operators={"DEFAULT": OperatorConfig("encrypt", {"key": randstr}),
    "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "000-000-0000"}),
    # TypeError: object of type 'NoneType' has no len()
    # "PHONE_NUMBER": OperatorConfig(
    #     "mask",
    #     {
    #         "type": "mask",
    #         "masking_char": "*",
    #         "chars_to_mask": 12,
    #         "from_end": True,
    #     },
    # ),
    "IP_ADDRESS": OperatorConfig("replace", {"new_value": "1.1.1.1"})}
    )
    columns = df.columns
    # df = df.drop(column_name)
    print("anonymized_df COLUMNS")
    print(anonymized_df.columns)
    print("df COLUMNS")
    print(df.columns)
    # Convert back to PySpark DataFrame

    # return spark.createDataFrame(anonymized_df)
    # return df.unionByName(spark.createDataFrame(anonymized_df, StringType()), allowMissingColumns=True)
    return df.drop(column_name).join(spark.createDataFrame(anonymized_df, ["email", column_name]), "email","leftouter").select(columns)


def is_valid_json(text: str) -> bool:
    """Check if text is valid JSON format."""
    if not text or not isinstance(text, str):
        return False
    try:
        json.loads(text.strip())
        return True
    except (json.JSONDecodeError, ValueError):
        return False
def anonymize_dataframe_auto_detect_all_json(df, entity_mappings: dict = None):
    print(entity_mappings)
    if entity_mappings is None:
        entity_mappings = {}

    # Apply anonymization to each specified column
    for column_name in entity_mappings.keys():
        if column_name in df.columns:
            entity_mapping = entity_mappings.get(column_name, None)
            if type(entity_mapping) == str:
                entity_mapping = {column_name: entity_mapping}
                print("PRINT df.columns before filter")
                print(df.columns)
                # df = df.where(col(column_name).cast("int") < 1)
                print("PRINT df.columns after filter")
                print(df.columns)
                df = anonymize_dataframe_as_tabular(df, column_name, entity_mapping)
            elif type(entity_mapping) == dict:

                def anonymize_with_auto_detect(text: str) -> str:
                    if text is None:
                        return None

                    # Auto-detect if text is JSON
                    is_json = is_valid_json(text)
                    return anonymize_text(text, is_json=is_json, entity_mapping=entity_mapping if is_json else None)
                anonymize_udf = udf(anonymize_with_auto_detect, StringType())
                df = df.withColumn(column_name, anonymize_udf(col(column_name)))
            else:
                print(f"Warning: Verify entity_mapping definition of '{column_name}'")
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


def main(db_name, table_name, entity_mappings):
    try: 
        entity_mappings = json.loads(entity_mappings.replace("'","\""))
    except Exception as e:
        logger.error("Failed to load Entity_Mappings to dict")
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
    anonymized_df = anonymize_dataframe_auto_detect_all_json(df, entity_mappings)

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
    main(db_name=params["DATABASE_NAME"], table_name=params["TABLE_NAME"], entity_mappings=params["ENTITY_MAPPINGS"])
