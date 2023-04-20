Build:

  ./gradlew clean assemble
  cd docker
  ./build-venice-docker-images.sh
  cd ..

Start Pulsar + Venice:

  docker compose -f clients/venice-pulsar/src/test/resources/docker-compose-pulsar-venice.yaml up -d

Init store:

... TBD ...

Start Sink (from pulsar's folder):

 bin/pulsar-admin sinks create --tenant t1 --namespace n1 --name venice --archive ~/src/venice/clients/venice-pulsar/build/libs/pulsar-venice-sink.nar -i t1/n1/input --sink-config '{"veniceDiscoveryUrl":"http://venice-controller:5555","storeName":"t1_n1_s1","kafkaSaslMechanism":"PLAIN","kafkaSecurityProtocol":"SASL_PLAINTEXT","kafkaSaslConfig":"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"public\" password=\"token:eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzdXBlcnVzZXIifQ.JQpDWJ9oHD743ZyuIw55Qp0bb8xzP6gK0KIWRniF2WnJB1m3v5MsrpfMlmRIlFc3-htWRAFHCc4E0ipj7JU8HjBqLIvVErRseRG-UTM1EprVkj0mk37jXV3ef7gER0KHn9CUKEQPfmTACeKlQ2oV4_qPAZ6HiEt51vzANfZH24vLCIjiOG77Z4s_w2sfgpiodRmhBLFOg_qnQTfGs7TBDWgu4DRoJ6CYZSEcp8q7j8xp_zNVIFGTRjWskocUvedHS9ZsCGZjzuPvRPp19B0VvAjEjtwpa6j7Khvjf4imjp2QHDnZwpCIEp4DSicwM48F5q4k722IdiyTTsVBWy8Cyg\"\";\");"}'

Delete Sink (from pulsar's folder):

  bin/pulsar-admin sinks delete --tenant t1 --namespace n1 --name venice

Stop Pulsar + Venice:

  docker compose -f clients/venice-pulsar/src/test/resources/docker-compose-pulsar-venice.yaml down

