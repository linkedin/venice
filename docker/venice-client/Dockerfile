FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu as base

RUN apt-get update
RUN apt-get install git -y

RUN mkdir -p /opt/venice/src
COPY / /opt/venice/src

WORKDIR /opt/venice/src

RUN ./gradlew :clients:venice-admin-tool:shadowJar
RUN ./gradlew :clients:venice-thin-client:shadowJar
RUN ./gradlew :clients:venice-push-job:shadowJar

FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ENV VENICE_DIR=/opt/venice

RUN apt-get update
RUN apt-get install netcat tree wget -y
RUN mkdir -p "${VENICE_DIR}/bin"
RUN wget -O ${VENICE_DIR}/bin/avro-tools.jar https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.11.2/avro-tools-1.11.2.jar
RUN wget -O ${VENICE_DIR}/bin/hadoop-mapreduce-client-core.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/2.10.2/hadoop-mapreduce-client-core-2.10.2.jar
RUN wget -O ${VENICE_DIR}/bin/hadoop-mapreduce-client-common.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-common/2.10.2/hadoop-mapreduce-client-common-2.10.2.jar
RUN wget -O ${VENICE_DIR}/bin/hadoop-common.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/2.10.2/hadoop-common-2.10.2.jar
WORKDIR ${VENICE_DIR}

COPY --from=base /opt/venice/src/clients/venice-push-job/build/libs/venice-push-job-all.jar bin/venice-push-job-all.jar
COPY --from=base /opt/venice/src/clients/venice-thin-client/build/libs/venice-thin-client-all.jar bin/venice-thin-client-all.jar
COPY --from=base /opt/venice/src/clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar bin/venice-admin-tool-all.jar
COPY docker/venice-client/sample-data sample-data
COPY docker/venice-client/run-vpj.sh .
COPY docker/venice-client/fetch.sh .
COPY docker/venice-client/create-store.sh .
COPY docker/venice-client/avro-to-json.sh .
RUN chmod +x *.sh

CMD /bin/sh -c bash
