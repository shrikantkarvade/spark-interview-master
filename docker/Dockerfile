# ==============================================================================
# Module 6: Production Spark 3.5.1 & Delta Lake Container Image on Java 17
# ==============================================================================
FROM eclipse-temurin:17-jammy

ARG spark_uid=185
RUN groupadd --system --gid=${spark_uid} spark && \
    useradd --system --uid=${spark_uid} --gid=spark spark

# Install essential runtime tools
RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        gnupg2 wget bash tini libc6 libpam-modules procps net-tools gosu; \
    rm -rf /var/lib/apt/lists/*

# Environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Download and install Apache Spark 3.5.1
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
RUN set -ex; \
    wget -nv -O /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz; \
    tar -xf /tmp/spark.tgz -C /opt/; \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark; \
    rm /tmp/spark.tgz; \
    chown -R spark:spark /opt/spark

# Set up work directory & security contexts
WORKDIR /opt/spark/work-dir
RUN mkdir -p /opt/spark/work-dir && chown -R spark:spark /opt/spark/work-dir

# Copy application fat JAR built via Gradle into the container image
COPY build/libs/spark-interview-master-all.jar /opt/spark/apps/spark-interview-app.jar

USER spark
ENTRYPOINT ["/opt/spark/bin/spark-class"]