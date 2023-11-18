FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu as base

RUN apt-get update
RUN apt-get install git -y

RUN mkdir -p /opt/venice/src
COPY / /opt/venice/src

WORKDIR /opt/venice/src

RUN ./gradlew :services:venice-router:shadowJar

FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ENV VENICE_DIR=/opt/venice

RUN apt-get update
RUN apt-get install tree netcat -y
RUN mkdir -p ${VENICE_DIR}/bin && mkdir -p ${VENICE_DIR}/configs

WORKDIR ${VENICE_DIR}

COPY --from=base /opt/venice/src/services/venice-router/build/libs/venice-router-all.jar bin/venice-router-all.jar
COPY docker/venice-router/single-dc-configs configs/single-dc
COPY docker/venice-router/multi-dc-configs configs/multi-dc

CMD java -jar bin/venice-router-all.jar configs/single-dc/router.properties
