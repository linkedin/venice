## Build:

```shell
cd ~/src/venice
./gradlew clean assemble

# build Venice's latest-dev docker images
cd ~/src/venice/docker
./build-venice-docker-images.sh

cd ~/src/venice
```

## Start Pulsar + Venice:

```shell
  docker compose -f tests/docker-images/docker-compose-pulsar-venice.yaml up -d
```

## Init Pulsar (from Pulsar's folder):

```shell
  bin/pulsar-admin tenants create t1
  bin/pulsar-admin namespaces create t1/n1
  bin/pulsar-admin topics create-partitioned-topic -p 1 public/default/venice_admin_venice-cluster0
  bin/pulsar-admin topics create-partitioned-topic -p 1 t1/n1/input
```

## Init Store:

```shell
  java -jar bin/venice-admin-tool-all.jar --new-store --url http://venice-controller:5555 --cluster venice-cluster0 --store t1_n1_s1 --key-schema-file /tmp/key.asvc --value-schema-file /tmp/value.asvc

  java -jar bin/venice-admin-tool-all.jar --update-store --url http://venice-controller:5555 --cluster venice-cluster0 --store t1_n1_s1 --storage-quota -1 --incremental-push-enabled true
  java -jar bin/venice-admin-tool-all.jar --update-store --url http://venice-controller:5555 --cluster venice-cluster0 --store t1_n1_s1 --read-quota 1000000

  java -jar bin/venice-admin-tool-all.jar --empty-push --url http://venice-controller:5555 --cluster venice-cluster0 --store t1_n1_s1 --push-id init --store-size 1000
```

## Start Sink (from pulsar's folder):

```shell
 bin/pulsar-admin sinks create --tenant t1 --namespace n1 --name venice --archive ~/src/venice/clients/venice-pulsar/build/libs/pulsar-venice-sink.nar -i t1/n1/input --sink-config '{"veniceDiscoveryUrl":"http://venice-controller:5555","storeName":"t1_n1_s1","kafkaSaslMechanism":"PLAIN","kafkaSecurityProtocol":"SASL_PLAINTEXT","kafkaSaslConfig":"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"public\" password=\"token:eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzdXBlcnVzZXIifQ.JQpDWJ9oHD743ZyuIw55Qp0bb8xzP6gK0KIWRniF2WnJB1m3v5MsrpfMlmRIlFc3-htWRAFHCc4E0ipj7JU8HjBqLIvVErRseRG-UTM1EprVkj0mk37jXV3ef7gER0KHn9CUKEQPfmTACeKlQ2oV4_qPAZ6HiEt51vzANfZH24vLCIjiOG77Z4s_w2sfgpiodRmhBLFOg_qnQTfGs7TBDWgu4DRoJ6CYZSEcp8q7j8xp_zNVIFGTRjWskocUvedHS9ZsCGZjzuPvRPp19B0VvAjEjtwpa6j7Khvjf4imjp2QHDnZwpCIEp4DSicwM48F5q4k722IdiyTTsVBWy8Cyg\"\";\");"}'
```

  If the sink is packaged in the Pulsar image (e.g. integration tests):

```shell
  bin/pulsar-admin sinks create --tenant t1 --namespace n1 --name venice -t venice -i t1/n1/input --sink-config '{"veniceDiscoveryUrl":"http://venice-controller:5555","storeName":"t1_n1_s1","kafkaSaslMechanism":"PLAIN","kafkaSecurityProtocol":"SASL_PLAINTEXT","kafkaSaslConfig":"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"public\" password=\"token:eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzdXBlcnVzZXIifQ.JQpDWJ9oHD743ZyuIw55Qp0bb8xzP6gK0KIWRniF2WnJB1m3v5MsrpfMlmRIlFc3-htWRAFHCc4E0ipj7JU8HjBqLIvVErRseRG-UTM1EprVkj0mk37jXV3ef7gER0KHn9CUKEQPfmTACeKlQ2oV4_qPAZ6HiEt51vzANfZH24vLCIjiOG77Z4s_w2sfgpiodRmhBLFOg_qnQTfGs7TBDWgu4DRoJ6CYZSEcp8q7j8xp_zNVIFGTRjWskocUvedHS9ZsCGZjzuPvRPp19B0VvAjEjtwpa6j7Khvjf4imjp2QHDnZwpCIEp4DSicwM48F5q4k722IdiyTTsVBWy8Cyg\"\";\");"}'
```

## Delete Sink (from pulsar's folder):

```shell
  bin/pulsar-admin sinks delete --tenant t1 --namespace n1 --name venice
```

## Stop Pulsar + Venice:

```shell
  docker compose -f clients/venice-pulsar/src/test/resources/docker-compose-pulsar-venice.yaml down
```

## Run integration tests 

```shell
# build Venice and sink
cd ~/src/venice
./gradlew clean assemble

# build Venice's latest-dev docker images
cd ~/src/venice/docker
./build-venice-docker-images.sh

# build latest-dev docker image for Pulsar to use in tests
cd ~/src/venice/tests/docker-images/pulsar-sink
docker build --tag=pulsar/venice-test:latest-dev ~/src/venice -f ./Dockerfile

# run the test
cd ~/src/venice
./gradlew :tests:venice-pulsar-test:pulsarIntegrationTest -i
```