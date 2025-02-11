FROM jupyter/pyspark-notebook:spark-3.3.0

ENV LANG=en_US.UTF-8
ENV JAVA_HOME=/usr/lib/jvm/msopenjdk-11
ENV PATH="${JAVA_HOME}/bin:${PATH}"
COPY --from=mcr.microsoft.com/openjdk/jdk:11-ubuntu $JAVA_HOME $JAVA_HOME

ENV APACHE_SPARK_VERSION=3.3.3
ENV PYSPARK_SUBMIT_ARGS='--packages org.apache.spark:spark-avro_2.12:3.3.3 --master local[2] pyspark-shell'

USER root
RUN apt clean
RUN apt-get -y update
RUN apt-get install iputils-ping netcat tree wget python3 python3-pip -y
RUN pip install avro

USER $NB_UID

# Install java kernel (not used for now, but would like to support this for future UDF demo's of java API's)
RUN mkdir -p "./bin"
RUN wget -O ./bin/ijava-1.3.0.zip https://github.com/SpencerPark/IJava/releases/download/v1.3.0/ijava-1.3.0.zip
RUN unzip ./bin/ijava-1.3.0.zip -d ./bin/
RUN python3 ./bin/install.py --sys-prefix

EXPOSE 8888
EXPOSE 4444

COPY venice-push-job-all.jar bin/venice-push-job-all.jar
COPY venice-thin-client-all.jar bin/venice-thin-client-all.jar
COPY venice-admin-tool-all.jar bin/venice-admin-tool-all.jar
COPY fetch.sh .
COPY create-store.sh .
COPY venice-logo-lion.png .
COPY Venice_Demo.ipynb .
COPY wineKeySchema.avsc .
COPY wineValueSchema.avsc .
COPY batch-push-job.properties .
USER root
RUN chmod +x *.sh
USER $NB_UID

#CMD /bin/sh -c bash
