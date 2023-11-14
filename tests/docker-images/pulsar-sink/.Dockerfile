
FROM apachepulsar/pulsar:2.10.3 as pulsar-venice

COPY clients/venice-pulsar/build/libs/pulsar-venice-sink.nar /pulsar/connectors/

CMD bash
