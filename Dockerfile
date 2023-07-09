
# Start with ubuntu image
FROM ubuntu:22.04

# windows
# ENV ARQ amd64 

# apple
ENV ARQ arm64

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

# Install required dependencies for Gradle
RUN apt-get update && \
    apt-get install -y wget unzip

# Download and install Gradle
ENV GRADLE_VERSION=7.0.2
ENV GRADLE_HOME=/opt/gradle
ENV PATH=$PATH:$GRADLE_HOME/bin

RUN wget -q https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip \
    && unzip -q gradle-${GRADLE_VERSION}-bin.zip -d /opt \
    && ln -s /opt/gradle-${GRADLE_VERSION} ${GRADLE_HOME} \
    && rm gradle-${GRADLE_VERSION}-bin.zip


# Install Kafka
RUN curl https://mirrors.estointernet.in/apache/kafka/3.2.0/kafka_2.12-3.2.0.tgz -o kafka_2.12-3.2.0.tgz
RUN tar -xf kafka_*.tgz 
RUN rm kafka_*.tgz
RUN mv kafka_* /kafka

ENV KAFKA_HOME=/kafka \
    PATH=$PATH:/kafka/bin
    
WORKDIR /app
COPY . /app

# Install requirements
RUN pip3 install -r requirements.txt


# Expose Kafka ports
EXPOSE 9092 2181 8000