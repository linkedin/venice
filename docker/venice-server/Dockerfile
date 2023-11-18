FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu as base

RUN apt-get update
RUN apt-get install git -y

RUN mkdir -p /opt/venice/src
COPY / /opt/venice/src

WORKDIR /opt/venice/src

RUN ./gradlew :service:venice-server:shadowJar

FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ENV VENICE_DIR=/opt/venice

RUN apt-get update
RUN apt-get install tree netcat -y
RUN mkdir -p ${VENICE_DIR}/bin && mkdir -p ${VENICE_DIR}/configs && mkdir -p ${VENICE_DIR}/rocksdb

WORKDIR ${VENICE_DIR}

COPY --from=base /opt/venice/src/services/venice-server/build/libs bin/venice-server-all.jar
# COPY *py bin/
COPY docker/venice-server/single-dc-configs configs/single-dc

COPY docker/venice-server/multi-dc-configs configs/multi-dc

CMD java -jar bin/venice-server-all.jar configs/single-dc
