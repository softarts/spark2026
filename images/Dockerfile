# Spark 4.1.0 official multi-arch image, pinned to the digest verified for
# linux/arm64 on 2026-11-20. It provides Java 21 LTS, Python 3.10, and Hadoop
# client 3.4.2. The same image is used by the Notebook, Driver, and Executors.
FROM apache/spark@sha256:370ada682945240b41cc2785aeb0b12c1b276b5bb46fc3930ba6732726ed6836

USER root

# ---------- Hadoop 3.4.2 S3A dependency chain ----------
# Hadoop 3.4.2 migrated from AWS SDK v1 to v2 (HADOOP-18073).
# The correct JARs are determined by hadoop-project-3.4.2.pom:
#   hadoop-aws            3.4.2         S3A filesystem implementation
#   awssdk bundle         2.29.52       AWS SDK v2 (replaces aws-java-sdk-bundle)
#   wildfly-openssl       2.1.4.Final   Native TLS (runtime scope in hadoop-aws)
#   analyticsaccelerator  1.2.1         S3 analytics accelerator (compile scope)
#
# Note: aws-java-sdk-bundle (SDK v1) is NOT used with Hadoop 3.4.2.
ARG HADOOP_VERSION=3.4.2
ARG AWS_SDK_V2_BUNDLE_VERSION=2.29.52
ARG WILDFLY_OPENSSL_VERSION=2.1.4.Final
ARG S3_ANALYTICS_VERSION=1.2.1
ARG JUPYTERHUB_VERSION=5.5.5

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl git \
    && rm -rf /var/lib/apt/lists/* \
    # --- S3A JARs ---
    && curl --fail --location --retry 3 \
      --output /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    && curl --fail --location --retry 3 \
      --output /opt/spark/jars/bundle-${AWS_SDK_V2_BUNDLE_VERSION}.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_BUNDLE_VERSION}/bundle-${AWS_SDK_V2_BUNDLE_VERSION}.jar \
    && curl --fail --location --retry 3 \
      --output /opt/spark/jars/wildfly-openssl-${WILDFLY_OPENSSL_VERSION}.jar \
      https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/${WILDFLY_OPENSSL_VERSION}/wildfly-openssl-${WILDFLY_OPENSSL_VERSION}.jar \
    && curl --fail --location --retry 3 \
      --output /opt/spark/jars/analyticsaccelerator-s3-${S3_ANALYTICS_VERSION}.jar \
      https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${S3_ANALYTICS_VERSION}/analyticsaccelerator-s3-${S3_ANALYTICS_VERSION}.jar \
    # --- Python ---
    && pip3 install --no-cache-dir \
      jupyterhub==${JUPYTERHUB_VERSION} \
      jupyterlab==4.4.7 \
    && useradd --create-home --uid 1000 --shell /bin/bash jovyan \
    && mkdir -p /home/jovyan/work \
    && chown -R jovyan:jovyan /home/jovyan

ENV HOME=/home/jovyan \
    SPARK_HOME=/opt/spark \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# Lab examples. The Notebook's init container copies them into
# ~/work/examples on every server start; Cluster-Mode Driver Pods run the job
# scripts directly from here, e.g. local:///opt/spark-lab/examples/jobs/wordcount.py
COPY --chown=1000:1000 jobs/ /opt/spark-lab/examples/jobs/
COPY --chown=1000:1000 notebooks/ /opt/spark-lab/examples/notebooks/

# Spark UI behind the Ingress sub-paths (see the file for details).
COPY images/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# /home/jovyan always exists (image directory or the Notebook PVC mount). A
# WORKDIR that does not exist on a fresh PVC is created by the container
# runtime as root:root, which breaks file creation in JupyterLab.
WORKDIR /home/jovyan
USER 1000

# Hash of the build inputs (images/, jobs/, notebooks/), set by startcluster.sh.
# It lets the script skip the build and the node import when nothing changed.
ARG LAB_INPUTS_HASH=unset
LABEL org.sparklab.inputs-hash=${LAB_INPUTS_HASH}
