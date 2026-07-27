ARG SPARK_VERSION=4.0.3
FROM apache/spark:${SPARK_VERSION}

ARG AWS_SDK_VERSION=2.24.6
ARG HADOOP_VERSION=3.4.1
ARG ICEBERG_VERSION=1.11.0

USER root

# These are the same Spark, Iceberg, Hadoop AWS, and AWS SDK versions used by
# the ADBC Spark driver's own Spark 4 integration environment.
RUN wget -q \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar" \
        -P /opt/spark/jars/ \
    && wget -q \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
        -P /opt/spark/jars/ \
    && wget -q \
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" \
        -P /opt/spark/jars/ \
    && mkdir -p /opt/spark-data/warehouse \
    && chown -R spark:spark /opt/spark-data

USER spark

EXPOSE 15002

CMD ["/opt/spark/sbin/start-connect-server.sh", \
     "--wait", \
     "--master", "local[2]", \
     "--conf", "spark.connect.grpc.binding.address=0.0.0.0", \
     "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions", \
     "--conf", "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog", \
     "--conf", "spark.sql.catalog.local.type=hadoop", \
     "--conf", "spark.sql.catalog.local.warehouse=/opt/spark-data/warehouse", \
     "--conf", "spark.hadoop.fs.s3a.access.key=admin", \
     "--conf", "spark.hadoop.fs.s3a.secret.key=password", \
     "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000", \
     "--conf", "spark.hadoop.fs.s3a.endpoint.region=us-east-1", \
     "--conf", "spark.hadoop.fs.s3a.path.style.access=true", \
     "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false", \
     "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"]
