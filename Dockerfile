
#FROM bitnami/kafka:latest
FROM openjdk:21-oraclelinux8l

WORKDIR /app/

RUN rpm -qa | grep curl
RUN curl http://archive.apache.org/dist/kafka/3.3.1/kafka_2.13-3.3.1.tgz --output kafka.tgz
RUN tar -xvzf /kafka.tgz

COPY . /app/
#ENTRYPOINT ["/bin/bash/"]