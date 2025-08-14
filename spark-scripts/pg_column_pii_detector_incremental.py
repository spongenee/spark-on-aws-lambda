import argparse
import json
import logging
import os
import sys
import boto3
from botocore.exceptions import ClientError


from presidio_anonymizer.entities import OperatorConfig
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
from pyspark.context import SparkContext
from glue_functions import extract_jdbc_conf
import random
from presidio_structured import StructuredEngine, JsonDataProcessor, StructuredAnalysis, PandasDataProcessor
from pyspark.conf import SparkConf

conf = SparkConf()
conf.setMaster("local[6]")
conf.set("spark.driver.bindAddress", "127.0.0.1")
conf.set("spark.driver.memory", "6g")
conf.set("spark.driver.maxResultSize", "2g")
conf.set("spark.ui.enabled", "false")
conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
conf.set("spark.memory.fraction", "0.4")
conf.set("spark.network.maxRemoteBlockSizeFetchToMem", "1g")
conf.set("spark.task.maxDirectResultSize", "1g")
conf.set('spark.worker.cleanup.enabled', 'True')
conf.set('spark.sql.shuffle.partitions', 130)
conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
conf.set("spark.hadoop.fs.s3a.session.token", os.getenv("AWS_SESSION_TOKEN"))
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
conf.set("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
conf.set("fs.s3a.committer.name", "magic")
conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
conf.set("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")


AWS_REGION = "us-east-1"
INCREMENTAL_STEP = 1000000
CHECKPOINT_BUCKET_NAME = "aws-glue-atvenu-spark-checkpoints"
PRIMARY_KEY = "id"
randint = random.getrandbits(128)
randstr = f'{randint:032x}'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info("------> Building spark session...")


spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

print("Driver is running as:", spark.sparkContext.appName)
print("Running in mode:", spark.sparkContext.master)
print("Driver memory:", spark.conf.get("spark.driver.memory"))
print("Default parallelism:", spark.sparkContext.defaultParallelism)

class Parameters:
    def __init__(self):
        self.first_time_read_flag = False
        self.processed_rows_count = 0
        self.prev_processed_rows_count = 0
        self.processing_rows_count = 0
        self.insert_data_exists_flag = False
        self.update_data_exists_flag = False


class Checkpoints(Parameters):
    def __init__(self, directory, table_name):
        self.directory = directory
        self.table_name = table_name
        Parameters.__init__(self)

    def read(self):
        if self.is_exists():
            self.prev_processed_rows_count, self.table_name = spark.read.csv(
                f"s3a://{CHECKPOINT_BUCKET_NAME}/{self.checkpoint_file}").collect()[0]
            return True
        else:
            self.first_time_read_flag = True
            return False

    def write(self):
        spark_df = spark.createDataFrame(data=[(int(self.processed_rows_count), self.table_name)],
                                         schema=["processed_rows_count", "table_name"])
        spark_df.write.mode("overwrite").csv(self.directory)
        spark_df.unpersist()
        return True

    
    def is_exists(self):
        s3client = boto3.client('s3')
        PREFIX = self.directory.removeprefix(f"s3a://{CHECKPOINT_BUCKET_NAME}/")
        s3_paginator = s3client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': CHECKPOINT_BUCKET_NAME,
                                'Prefix': PREFIX}
        s3_iterator = s3_paginator.paginate(**operation_parameters)

        filtered_iterator = s3_iterator.search(
            "Contents[?ends_with(Key, '.csv') && Size!=`0`]"
            # " | reverse(sort_by(@, &to_string(LastModified)))"
            # " | @[].Key"
            # " | [:1]"
        )
        #{'Key': 'staging3-vfadb/public.users/9ca68050-0e74-44c5-a2ec-947290fa30f2/', 'LastModified': datetime.datetime(2025, 8, 7, 6, 9, 12, tzinfo=tzlocal()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'ChecksumAlgorithm': ['CRC64NVME'], 'ChecksumType': 'FULL_OBJECT', 'Size': 0, 'StorageClass': 'STANDARD'}
        checkpoint_files = []
        for key_data in filtered_iterator:
            if key_data != None:
                checkpoint_files.append(key_data)
        exists=False
        if len(checkpoint_files) > 0:
            exists=True
            latest = max(checkpoint_files, key = lambda x: x["LastModified"])
            self.checkpoint_file = latest["Key"]
        return exists


class QuerySource(object):
    def __init__(self, primary_key_column_name, checkpoint_instance, jdbc_conf):
        self.primary_key_column_name = primary_key_column_name
        self.checkpoint_instance = checkpoint_instance
        self.jdbc_conf = jdbc_conf
    def get_inc_update(self):
        query = f"SELECT * FROM {self.checkpoint_instance.table_name} OFFSET {self.checkpoint_instance.prev_processed_rows_count} ROWS FETCH NEXT {INCREMENTAL_STEP} ROWS ONLY"
        logger.info(f"Read query performed on source db: {query}")
        df = spark.read.format('jdbc').options(
            url=self.jdbc_conf['fullUrl'],
            query=query,
            user=self.jdbc_conf['username'],
            password=self.jdbc_conf['password'],
            driver=self.jdbc_conf['driver']
        ).load()

        self.processing_rows_count = df.count()
        if self.processing_rows_count >= 0: self.checkpoint_instance.update_data_exists_flag = True
        return df




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
        logger.error(f"ERROR: Failed to parse JSON, using regular text anonymization: {e}")
        raise e
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
    pandas_df = df.select([PRIMARY_KEY,column_name]).toPandas()
    # Initialize structured engine
    structured_engine = StructuredEngine(data_processor=PandasDataProcessor())
    tabular_analysis =  StructuredAnalysis(entity_mapping=entity_mapping)

    # Anonymize the DF column
    anonymized_df = structured_engine.anonymize(
        pandas_df.dropna(subset=[column_name]),
        tabular_analysis,
        operators={"DEFAULT": OperatorConfig("encrypt", {"key": randstr}),
    # TypeError: object of type 'NoneType' has no len()
    "PHONE_NUMBER": OperatorConfig(
        "mask",
        {
            "type": "mask",
            "masking_char": "*",
            "chars_to_mask": 5,
            "from_end": True,
        },
    ),
    "IP_ADDRESS": OperatorConfig("replace", {"new_value": "1.1.1.1"})}
    )
    columns = df.columns
    if anonymized_df.empty:
        return df
    else: 
        return df.drop(column_name).join(spark.createDataFrame(anonymized_df, [PRIMARY_KEY, column_name]), PRIMARY_KEY,"leftouter").select(columns)

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
    if entity_mappings is None:
        entity_mappings = {}

    # Apply anonymization to each specified column
    for column_name in entity_mappings.keys():
        if column_name in df.columns:
            entity_mapping = entity_mappings.get(column_name, None)
            if type(entity_mapping) == str:
                entity_mapping = {column_name: entity_mapping}
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
                logger.warning(f"Warning: Verify entity_mapping definition of '{column_name}'")
        else:
            logger.warning(f"Warning: Column '{column_name}' not found in DataFrame")
    return df

def put_event(params):
    eventbridge_client = boto3.client(service_name='events', region_name=AWS_REGION)
    try:
        putEvent_response = eventbridge_client.put_events(
            Entries=[
                {
                    'Source': 'custom.atvenuObfuscator',
                    'DetailType': 'obfuscate',
                    'Detail': json.dumps(params)
                }
            ]
        )
        print(putEvent_response)
    except Exception as e:
        logger.error(f"Error putting event: {e}")

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
    except Exception:
        logger.error("Failed to load Entity_Mappings to dict")
    jdbc_conf = extract_jdbc_conf(db_name, AWS_REGION)
    logger.info(f"------> Retrieving Dataframe of {table_name} from {db_name}")
    secret_data = get_secret(jdbc_conf['secretId'])
    jdbc_conf['username'] = secret_data.get('username', '')
    jdbc_conf['password'] = secret_data.get('password', '')
    jdbc_conf['table'] = table_name
    helper_checkpoints = Checkpoints(
        directory=f"s3a://{CHECKPOINT_BUCKET_NAME}/{db_name}/{table_name}",
        table_name=table_name
    )
    checkpoints_flag = helper_checkpoints.read()
    logger.info(f"checkpoints_flag: {checkpoints_flag}")
    query_instance = QuerySource(
        primary_key_column_name = PRIMARY_KEY,
        checkpoint_instance = helper_checkpoints,
        jdbc_conf = jdbc_conf
    )
    df = query_instance.get_inc_update()
    sc.setCheckpointDir(f"s3a://{CHECKPOINT_BUCKET_NAME}/{db_name}/{table_name}")
    

    if helper_checkpoints.update_data_exists_flag:
        helper_checkpoints.processed_rows_count = int(helper_checkpoints.processing_rows_count) +  int(helper_checkpoints.prev_processed_rows_count)
        logger.info(f"helper_checkpoints.processed_rows_count: {helper_checkpoints.processed_rows_count}")
        try: 
            # apply the udf
            anonymized_df = anonymize_dataframe_auto_detect_all_json(df, entity_mappings)
            target_jdbc_conf = extract_jdbc_conf("staging3-target-mmsdb", AWS_REGION)        
            anonymized_df.write.jdbc(
                url=target_jdbc_conf["fullUrl"],
                table=table_name,
                mode="append",
                properties={"user": target_jdbc_conf["user"], "password": target_jdbc_conf["password"], "driver": target_jdbc_conf["driver"], "stringtype":"unspecified"}
            )
            anonymized_df.unpersist()
            helper_checkpoints.write()
            put_event(params)
        except Exception as e:
            logger.error(f"Failed to apply UDF on {db_name}/{table_name} {INCREMENTAL_STEP} ROWS from ROW {str(helper_checkpoints.prev_processed_rows_count)}")
            logger.error(e)
    else:
        logger.info(f"------> No more rows to process for {db_name}/{table_name}!")
    df.unpersist()    
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--event",
                        help="event data from lambda")
    args = parser.parse_args()
    event = json.loads(args.event)
    params = event['detail']
    main(db_name=params["DATABASE_NAME"], table_name=params["TABLE_NAME"], entity_mappings=params["ENTITY_MAPPINGS"])
