FROM apache/spark:3.5.3-scala2.12-java17-python3-r-ubuntu

USER root

# Downloads S3A connector JARs so spark can read from minio
RUN curl -fL --progress-bar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fL --progress-bar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
