# Multi-stage build for optimal size
FROM public.ecr.aws/lambda/python:3.10 as builder

# Build arguments - consolidated at top
ARG HADOOP_VERSION=3.2.4
ARG AWS_SDK_VERSION=1.11.901
ARG PYSPARK_VERSION=3.3.0
ARG FRAMEWORK
ARG DELTA_FRAMEWORK_VERSION=2.2.0
ARG HUDI_FRAMEWORK_VERSION=0.12.2
ARG ICEBERG_FRAMEWORK_VERSION=3.3_2.12
ARG ICEBERG_FRAMEWORK_SUB_VERSION=1.0.0
ARG DEEQU_FRAMEWORK_VERSION=2.0.3-spark-3.3

# Single consolidated RUN layer for all build operations
COPY download_jars.sh /tmp/
RUN set -ex && \
    # System updates and package installation
    yum update -y && \
    yum install -y java-11-amazon-corretto-headless wget unzip && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    # Python package installation
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir pyspark==$PYSPARK_VERSION boto3 && \
    pip install presidio_analyzer presidio_anonymizer && \
    # Conditional DEEQU installation
    (echo "$FRAMEWORK" | grep -q "DEEQU" && \
     pip install --no-cache-dir --no-deps pydeequ && \
     pip install --no-cache-dir pandas || \
     echo "DEEQU not found in FRAMEWORK") && \
    # JAR download and cleanup
    chmod +x /tmp/download_jars.sh && \
    SPARK_HOME="/var/lang/lib/python3.10/site-packages/pyspark" && \
    /tmp/download_jars.sh $FRAMEWORK $SPARK_HOME $HADOOP_VERSION $AWS_SDK_VERSION $DELTA_FRAMEWORK_VERSION $HUDI_FRAMEWORK_VERSION $ICEBERG_FRAMEWORK_VERSION $ICEBERG_FRAMEWORK_SUB_VERSION $DEEQU_FRAMEWORK_VERSION && \
    rm -rf /tmp/* /var/tmp/*

# Final optimized stage
FROM public.ecr.aws/lambda/python:3.10

# Single consolidated RUN layer for runtime setup
COPY --from=builder /var/lang/lib/python3.10/site-packages/ /var/lang/lib/python3.10/site-packages/
COPY --from=builder /var/runtime/ /var/runtime/
COPY libs /home/libs
COPY spark-class /var/lang/lib/python3.10/site-packages/pyspark/bin/
COPY sparkLambdaHandler.py ${LAMBDA_TASK_ROOT}

RUN set -ex && \
    # Install runtime Java and cleanup
    yum update -y && \
    yum install -y java-11-amazon-corretto-headless && \
    yum clean all && \
    rm -rf /var/cache/yum /tmp/* /var/tmp/* && \
    # Set permissions in single operation
    chmod -R 755 /home/libs /var/lang/lib/python3.10/site-packages/pyspark

# Consolidated environment variables
ENV SPARK_HOME="/var/lang/lib/python3.10/site-packages/pyspark" \
    SPARK_VERSION=3.3.0 \
    JAVA_HOME="/usr/lib/jvm/java-11-amazon-corretto" \
    PATH="$PATH:/var/lang/lib/python3.10/site-packages/pyspark/bin:/var/lang/lib/python3.10/site-packages/pyspark/sbin:/usr/lib/jvm/java-11-amazon-corretto/bin" \
    PYTHONPATH="/var/lang/lib/python3.10/site-packages/pyspark/python:/var/lang/lib/python3.10/site-packages/pyspark/python/lib/py4j-0.10.9-src.zip:/home/libs" \
    INPUT_PATH="" \
    OUTPUT_PATH="" \
    AWS_ACCESS_KEY_ID="" \
    AWS_SECRET_ACCESS_KEY="" \
    AWS_REGION="" \
    AWS_SESSION_TOKEN="" \
    CUSTOM_SQL=""

CMD [ "sparkLambdaHandler.lambda_handler" ]
