FROM java:8-jre

MAINTAINER Yiannis Mouchakis <gmouchakis@iit.demokritos.gr>

# define hadoop version
ENV HADOOP_VERSION 2.7.1

# Hadoop env variables
ENV HADOOP_PREFIX /opt/hadoop
ENV HADOOP_CONF_DIR $HADOOP_PREFIX/conf
ENV PATH $PATH:$HADOOP_PREFIX/bin
ENV PATH $PATH:$HADOOP_PREFIX/sbin

RUN apt-get update && apt-get install -y \
  wget \
  tar \
  && apt-get clean \ 
  && rm -rf /var/lib/apt/lists/*

# deploy hadoop
RUN wget http://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
RUN tar -zxf /hadoop-$HADOOP_VERSION.tar.gz 
RUN rm /hadoop-$HADOOP_VERSION.tar.gz 
RUN mv hadoop-$HADOOP_VERSION $HADOOP_PREFIX 

# add configuration files
ADD core-site.xml $HADOOP_CONF_DIR/core-site.xml
ADD hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
