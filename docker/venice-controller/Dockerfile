FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ENV VENICE_DIR=/opt/venice

RUN apt-get update
RUN apt-get install tree python3 -y
RUN mkdir -p ${VENICE_DIR}/bin && mkdir -p ${VENICE_DIR}/configs

WORKDIR ${VENICE_DIR}

COPY venice-controller-all.jar bin/venice-controller-all.jar
COPY *py bin/
COPY single-dc-configs configs/single-dc
COPY multi-dc-configs configs/multi-dc

CMD java -jar bin/venice-controller-all.jar configs/single-dc/cluster.properties configs/single-dc/controller.properties
