
# Start with ubuntu image
FROM ubuntu:22.04

ENV ARQ amd64

# Update and install curl
RUN apt-get update && apt-get install -y 
RUN apt-get install curl -y

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Bahia

# Install python 3.11
RUN apt-get install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt-get update && apt-get install python3.11 -y
RUN apt-get install python3-pip -y

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-${ARQ}/
RUN export JAVA_HOME

# Install Kafka
RUN curl https://mirrors.estointernet.in/apache/kafka/3.2.0/kafka_2.12-3.2.0.tgz -o kafka_2.12-3.2.0.tgz
RUN tar -xf kafka_*.tgz 
RUN rm kafka_*.tgz
RUN mv kafka_* /kafka

WORKDIR /app
COPY . /app

# Install requirements
RUN pip3 install -r requirements.txt

# Expose Kafka ports
EXPOSE 9092 2181