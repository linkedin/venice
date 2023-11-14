FROM openjdk:11.0.6-jre-buster

# build time
ARG ZOOKEEPER_ADDRESS=zookeeper:2181

# runtime
ENV KAFKA_TAR="kafka_2.13-3.3.1.tgz"
ENV KAFKA_DIR=/opt/kafka
ENV ZOOKEEPER_ADDRESS="${ZOOKEEPER_ADDRESS}"
RUN mkdir -p ${KAFKA_DIR}
RUN apt-get update
RUN apt-get install wget -y
RUN wget https://downloads.apache.org/kafka/3.3.1/"${KAFKA_TAR}"
RUN tar -xzf ${KAFKA_TAR} -C /opt/kafka --strip-components=1
RUN rm -f ${KAFKA_TAR}
WORKDIR ${KAFKA_DIR}
COPY kafka-startup.sh ${KAFKA_DIR}
RUN chmod +x kafka-startup.sh

CMD bash kafka-startup.sh
