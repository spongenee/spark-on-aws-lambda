# Ubuntu Spark Container Guide

## Overview

The `Dockerfile.ubuntu` creates a Ubuntu-based container with the same Spark functionality as the AWS Lambda version, but optimized for general Ubuntu deployment scenarios.

## Key Differences from Lambda Version

### Base Image
- **Lambda**: `public.ecr.aws/lambda/python:3.10`
- **Ubuntu**: `ubuntu:22.04`

### Package Manager
- **Lambda**: `yum` (Amazon Linux)
- **Ubuntu**: `apt-get` (Debian/Ubuntu)

### Java Runtime
- **Lambda**: Amazon Corretto 11
- **Ubuntu**: OpenJDK 11 (headless)

### Python Paths
- **Lambda**: `/var/lang/lib/python3.10/site-packages/`
- **Ubuntu**: `/usr/local/lib/python3.10/dist-packages/`

### Security
- **Ubuntu**: Runs as non-root user `spark` for better security
- **Lambda**: Runs as Lambda runtime user

## Build Instructions

### Basic Build
```bash
# Build with Delta framework
docker build -f Dockerfile.ubuntu --build-arg FRAMEWORK=DELTA -t spark-ubuntu .

# Build with multiple frameworks
docker build -f Dockerfile.ubuntu --build-arg FRAMEWORK=DELTA,DEEQU -t spark-ubuntu .

# Build with Iceberg
docker build -f Dockerfile.ubuntu --build-arg FRAMEWORK=ICEBERG -t spark-ubuntu .

# Build with Hudi
docker build -f Dockerfile.ubuntu --build-arg FRAMEWORK=HUDI -t spark-ubuntu .
```

### Advanced Build Options
```bash
# Custom PySpark version
docker build -f Dockerfile.ubuntu \
  --build-arg FRAMEWORK=DELTA \
  --build-arg PYSPARK_VERSION=3.3.0 \
  -t spark-ubuntu .
```

## Usage Examples

### 1. Run Interactive PySpark Shell
```bash
docker run -it spark-ubuntu python3 -c "
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
print('Spark version:', spark.version)
spark.stop()
"
```

### 2. Run Local Spark Script
```bash
# Mount local script and run
docker run -v /path/to/your/script.py:/opt/spark/script.py spark-ubuntu \
  python3 /opt/spark/run_spark.py /opt/spark/script.py
```

### 3. Run Spark Script from S3
```bash
# Set AWS credentials and run S3 script
docker run -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  -e AWS_REGION=us-east-1 \
  spark-ubuntu \
  python3 /opt/spark/run_spark.py s3://your-bucket/your-script.py
```

### 4. Run with Custom Environment Variables
```bash
docker run -e INPUT_PATH=s3a://input-bucket/data/ \
  -e OUTPUT_PATH=s3a://output-bucket/results/ \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  spark-ubuntu \
  python3 /opt/spark/run_spark.py s3://scripts/process_data.py
```

### 5. Run as Background Service
```bash
# Run container as daemon
docker run -d --name spark-worker \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  spark-ubuntu \
  tail -f /dev/null

# Execute Spark jobs in running container
docker exec spark-worker python3 /opt/spark/run_spark.py s3://bucket/script.py
```

## Built-in Spark Runner

The container includes a generic Spark runner (`/opt/spark/run_spark.py`) that can:

1. **Run local scripts**: `python3 run_spark.py /path/to/script.py`
2. **Download and run S3 scripts**: `python3 run_spark.py s3://bucket/script.py`
3. **Pass arguments**: `python3 run_spark.py script.py arg1 arg2`

## Environment Variables

### Required for S3 Access
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region
```

### Optional Configuration
```bash
INPUT_PATH=s3a://input-bucket/
OUTPUT_PATH=s3a://output-bucket/
CUSTOM_SQL="SELECT * FROM table"
```

## Volume Mounts

### Mount Local Scripts
```bash
-v /local/scripts:/opt/spark/scripts
```

### Mount Data Directory
```bash
-v /local/data:/opt/spark/data
```

### Mount Configuration
```bash
-v /local/spark-defaults.conf:/usr/local/lib/python3.10/dist-packages/pyspark/conf/spark-defaults.conf
```

## Networking

### Expose Spark UI (if needed)
```bash
docker run -p 4040:4040 spark-ubuntu
```

### Connect to External Services
```bash
docker run --network host spark-ubuntu
```

## Use Cases

### 1. Development Environment
```bash
# Interactive development with mounted code
docker run -it -v $(pwd):/opt/spark/workspace spark-ubuntu bash
```

### 2. CI/CD Pipeline
```bash
# Run tests in pipeline
docker run --rm spark-ubuntu python3 /opt/spark/run_spark.py s3://tests/test_suite.py
```

### 3. Batch Processing
```bash
# Process files in batch
docker run --rm \
  -e INPUT_PATH=s3a://data/batch-$(date +%Y%m%d)/ \
  -e OUTPUT_PATH=s3a://results/batch-$(date +%Y%m%d)/ \
  spark-ubuntu \
  python3 /opt/spark/run_spark.py s3://scripts/batch_processor.py
```

### 4. Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-worker
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-worker
  template:
    metadata:
      labels:
        app: spark-worker
    spec:
      containers:
      - name: spark
        image: spark-ubuntu
        env:
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: aws-credentials
              key: access-key-id
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: aws-credentials
              key: secret-access-key
```

## Security Considerations

1. **Non-root user**: Container runs as `spark` user
2. **Minimal packages**: Only essential runtime packages installed
3. **No SSH/shell access**: Secure by default
4. **Environment variables**: Use secrets management for credentials

## Troubleshooting

### Check Spark Installation
```bash
docker run spark-ubuntu python3 -c "import pyspark; print(pyspark.__version__)"
```

### Check Java Installation
```bash
docker run spark-ubuntu java -version
```

### Debug Container
```bash
docker run -it spark-ubuntu bash
```

### View Logs
```bash
docker logs container_name
```

## Performance Tuning

### Memory Settings
```bash
# Increase container memory
docker run -m 4g spark-ubuntu

# Set Spark driver memory
docker run -e SPARK_DRIVER_MEMORY=2g spark-ubuntu
```

### CPU Settings
```bash
# Limit CPU usage
docker run --cpus="2.0" spark-ubuntu
```
