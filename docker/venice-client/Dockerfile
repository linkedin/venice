FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ENV VENICE_DIR=/opt/venice

RUN apt-get update
RUN apt-get install netcat tree wget python3 -y
RUN mkdir -p "${VENICE_DIR}/bin"
RUN wget -O ${VENICE_DIR}/bin/avro-tools.jar https://downloads.apache.org/avro/stable/java/avro-tools-1.11.2.jar
RUN wget -O ${VENICE_DIR}/bin/hadoop-mapreduce-client-core.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/2.3.0/hadoop-mapreduce-client-core-2.3.0.jar
RUN wget -O ${VENICE_DIR}/bin/hadoop-mapreduce-client-common.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-common/2.3.0/hadoop-mapreduce-client-common-2.3.0.jar
RUN wget -O ${VENICE_DIR}/bin/hadoop-common.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/2.3.0/hadoop-common-2.3.0.jar
WORKDIR ${VENICE_DIR}

COPY venice-push-job-all.jar bin/venice-push-job-all.jar
COPY venice-thin-client-all.jar bin/venice-thin-client-all.jar
COPY venice-admin-tool-all.jar bin/venice-admin-tool-all.jar
COPY sample-data sample-data
COPY run-vpj.sh .
COPY fetch.sh .
COPY create-store.sh .
COPY avro-to-json.sh .
RUN chmod +x *.sh

CMD /bin/sh -c bash
