FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ENV VENICE_DIR=/opt/venice

RUN apt-get update
RUN apt-get install netcat -y
RUN apt-get install tree -y
RUN apt-get install wget -y
RUN mkdir -p "${VENICE_DIR}/bin"
RUN wget -O ${VENICE_DIR}/bin/avro-tools.jar https://downloads.apache.org/avro/stable/java/avro-tools-1.11.1.jar

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
