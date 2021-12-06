FROM ubuntu:xenial
MAINTAINER Segundo Cruz <segundo.cruz@scalac.io>

ENV DEVUSER scalac
RUN useradd -ms /bin/bash $DEVUSER

# System packages
RUN apt-get update -y  \
    && apt-get install -y curl wget unzip procps openjdk-8-jdk \
    && apt-get install -f  \
    && apt-get clean  \
    && apt-get update

# Install Spark
ENV SPARK_VERSION=3.2.0
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/opt/spark

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p $SPARK_HOME \
    && tar -xf apache-spark.tgz -C $SPARK_HOME --strip-components=1 \
    && rm apache-spark.tgz \
    && chown -R root:root $SPARK_HOME

ENV PATH=$SPARK_HOME/bin:$PATH
ENV SHARED_WORKSPACE /opt/workspace

USER $DEVUSER

WORKDIR ${SHARED_WORKSPACE}

#default command
CMD ["/opt/spark/bin/spark-shell"]