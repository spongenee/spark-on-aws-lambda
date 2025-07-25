import logging
import sys

import boto3

from pyspark.sql.types import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def extract_jdbc_conf(connection_name, aws_region):
    """
    Extract JDBC configuration from AWS Glue connection.

    Args:
        connection_name (str): Name of the Glue connection
        aws_region (str): AWS region (default: 'us-east-1')

    Returns:
        dict: Dictionary containing fullUrl, user, password, and driver
    """
    try:
        glue = boto3.client('glue', region_name=aws_region)
        response = glue.get_connection(
            Name=connection_name,
            HidePassword=False
        )
        connection_properties = response['Connection']['ConnectionProperties']

        # Get credentials from Secrets Manager if SECRETID is present
        user = ''
        password = ''

        secret_id = connection_properties.get('SECRET_ID')
        if secret_id:
            secrets_client = boto3.client('secretsmanager', region_name=aws_region)
            try:
                secret_response = secrets_client.get_secret_value(SecretId=secret_id)
                secret_data = json.loads(secret_response['SecretString'])
                user = secret_data.get('username', '')
                password = secret_data.get('password', '')
            except Exception as e:
                logger.error(f"Error retrieving secret {secret_id}: {e}")
                # Fall back to connection properties if secret retrieval fails
                user = connection_properties.get('USERNAME', '')
                password = connection_properties.get('PASSWORD', '')
        else:
            # Fall back to connection properties if no SECRETID
            user = connection_properties.get('USERNAME', '')
            password = connection_properties.get('PASSWORD', '')

        # Build the configuration dictionary
        jdbc_conf = {
            'fullUrl': connection_properties.get('JDBC_CONNECTION_URL', ''),
            'user': user,
            'password': password,
            'driver': connection_properties.get('JDBC_DRIVER_CLASS_NAME', '')
        }

        return jdbc_conf
    except Exception as e:
        logger.error(f"Error fetching connection properties from glue connection {connection_name}: {e}")
        return None

def get_table(db_name, table_name, aws_region):
    """
    Fetches table metadata from AWS Glue Catalog.

    Parameters:
    - db_name (str): The name of the database in Glue Catalog.
    - table_name (str): The name of the table in Glue Database.

    Returns:
    - dict: The response from the Glue `get_table` API call.
    """
    try:
        glue = boto3.client('glue', region_name=aws_region)
        response = glue.get_table(DatabaseName=db_name, Name=table_name)
        return response
    except Exception as e:
        logger.error(f"Error fetching table {table_name} from database {db_name}: {e}")
        return None


def build_schema_for_table(glue_table):
    """
    Converts AWS Glue table schema to PySpark schema.

    Parameters:
    - glue_table (dict): The table metadata from AWS Glue.

    Returns:
    - StructType: The corresponding PySpark schema.
    """
    try:
        # Check if glue_table is valid and contains necessary keys
        if not glue_table \
                or 'Table' not in glue_table \
                or 'StorageDescriptor' not in glue_table['Table'] \
                or 'Columns' not in glue_table['Table']['StorageDescriptor']:
            return StructType()  # Return an empty schema

        # Extract columns and data types from the response
        columns = glue_table['Table']['StorageDescriptor']['Columns']

        # Mapping dictionary for Glue to PySpark data types
        dtype_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'bigint': LongType(),
            'double': DoubleType(),
            'float': FloatType(),
            'boolean': BooleanType(),
            'timestamp': TimestampType(),
            'date': DateType(),
            'binary': BinaryType(),
            'decimal': DecimalType(),
            'array': ArrayType(StringType()),
            'map': MapType(StringType(), StringType()),
            'struct': StructType()
        }

        # Convert Glue schema to PySpark schema
        schema = [
            StructField(
                column['Name'],
                dtype_mapping.get(column.get('Type', 'string'), StringType())
            ) for column in columns
        ]
        return StructType(schema)
    except Exception as e:
        logger.error(f"Error building schema: {e}")
        return StructType()


def query_table(spark_session, s3_location, schema):
    """
    Queries a table stored in S3 using PySpark with the provided schema.

    Parameters:
    - spark_session (SparkSession): The active SparkSession.
    - s3_location (str): The S3 path to the table data.
    - schema (StructType): The PySpark schema for the table.

    Returns:
    - None: Prints the schema.
    """
    s3_location = _convert_s3_uri_to_s3a(s3_location)

    try:
        df = spark_session.read.schema(schema).format("delta").parquet(s3_location)
        df.printSchema()
    except Exception as e:
        logger.error(f"Error querying table from {s3_location}: {e}")


def _convert_s3_uri_to_s3a(s3_location):
    """
    Spark needs S3 location specified with the "s3a" protocol.
    This replaces "s3://" with "s3a://" in s3_location.

    Parameters:
    - s3_location (str): An S3 path.

    Returns:
    - str
    """
    if s3_location.startswith("s3://"):
        s3_location = s3_location.replace("s3://", "s3a://")

    return s3_location
