FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu as base

RUN apt-get update
RUN apt-get install git -y

RUN mkdir -p /opt/venice/src
COPY / /opt/venice/src

WORKDIR /opt/venice/src

RUN ./gradlew :services:venice-controller:shadowJar

FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ENV VENICE_DIR=/opt/venice

RUN apt-get update
RUN apt-get install tree netcat -y
RUN mkdir -p ${VENICE_DIR}/bin && mkdir -p ${VENICE_DIR}/configs

WORKDIR ${VENICE_DIR}

COPY --from=base /opt/venice/src/services/venice-controller/build/libs/venice-controller-all.jar bin/venice-controller-all.jar
# COPY *py bin/
COPY docker/venice-controller/single-dc-configs configs/single-dc
COPY docker/venice-controller/multi-dc-configs configs/multi-dc

CMD java -jar bin/venice-controller-all.jar configs/single-dc/cluster.properties configs/single-dc/controller.properties
