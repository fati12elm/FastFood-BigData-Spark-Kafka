
# Recuperer Spark depuis l'image officielle Apache
FROM apache/spark:3.5.7-java17-python3 AS spark-source

# Conserver Python 3.11 pour notre projet
FROM python:3.11-slim-bookworm

# Installer Java et les outils systeme
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    ca-certificates \
    curl \
    procps \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Copier Spark 3.5.7 deja installe
COPY --from=spark-source /opt/spark /opt/spark

# Configurer Java, Spark et PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYTHONPATH="/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

# Installer uniquement les bibliotheques complementaires
RUN pip install --no-cache-dir \
    jupyterlab==4.4.10 \
    boto3==1.37.14

# Connecteur Hadoop AWS pour MinIO
RUN curl -fL --retry 5 \
    -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
    https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# SDK AWS
RUN curl -fL --retry 5 \
    -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /workspace
