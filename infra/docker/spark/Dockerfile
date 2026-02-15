FROM apache/spark:3.5.3-scala2.12-java17-python3-r-ubuntu

USER root

# Downloads S3A connector JARs so spark can read from minio
RUN curl -fL --progress-bar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fL --progress-bar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Download Iceberg Spark runtime JAR
RUN curl -fL --progress-bar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar

# Install pip and the pipeline package
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml /opt/pipeline/
COPY src/ /opt/pipeline/src/
RUN pip3 install --no-cache-dir /opt/pipeline/

# Copy spark jobs
COPY spark_jobs/ /opt/spark/jobs/
