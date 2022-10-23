FROM openjdk:11.0.6-jre-buster

ARG SRC_ZK_CFG="zoo.cfg"

ENV ZK_DIR=/opt/zookeeper
ENV ZK_CFG="${ZK_DIR}/zoo.cfg"

RUN apt-get update
RUN apt-get install netcat -y
RUN apt-get install wget -y
RUN mkdir -p ${ZK_DIR}
RUN mkdir -p "${ZK_DIR}/data"
WORKDIR ${ZK_DIR}
RUN wget https://sushantmane.jfrog.io/artifactory/venicedb/zookeeper-3.9.0-SNAPSHOT-all.jar
COPY ${SRC_ZK_CFG} ${ZK_CFG}

CMD java -jar zookeeper-3.9.0-SNAPSHOT-all.jar server ${ZK_CFG}
