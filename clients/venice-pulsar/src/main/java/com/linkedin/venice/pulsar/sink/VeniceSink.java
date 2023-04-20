package com.linkedin.venice.pulsar.sink;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.DOT;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CONTROLLER_DISCOVERY_URL;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_STORE;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.samza.config.MapConfig;


public class VeniceSink implements Sink<GenericObject> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceSink.class);

  VeniceSinkConfig config;
  VeniceSystemProducer producer;

  AtomicInteger count = new AtomicInteger();

  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    this.config = VeniceSinkConfig.load(config, sinkContext);
    LOGGER.info("Starting, config {}", this.config);
    VeniceSystemFactory factory = new VeniceSystemFactory();
    final String systemName = "venice";
    this.producer =
        factory.getClosableProducer(systemName, new MapConfig(getConfig(this.config.getStoreName(), systemName)), null);
    this.producer.start();
    String kafkaBootstrapServers = this.producer.getKafkaBootstrapServers();
    LOGGER.info("Connected to Kafka {}", kafkaBootstrapServers);

    VeniceWriter<byte[], byte[], byte[]> veniceWriter = this.producer.getInternalProducer();
    String topicName = veniceWriter.getTopicName();
    LOGGER.info("Kafka topic name is {}", topicName);
  }

  @Override
  public void write(Record<GenericObject> record) throws Exception {
    Object nativeObject = record.getValue().getNativeObject();
    Object key;
    Object value;
    if (nativeObject instanceof KeyValue) {
      KeyValue keyValue = (KeyValue) nativeObject;
      key = extract(keyValue.getKey());
      value = extract(keyValue.getValue());
    } else {
      // this is a string
      key = record.getKey();
      value = extract(record.getValue());
    }
    LOGGER.info("Processing {}", nativeObject);
    // dumpSchema("key", key);
    // dumpSchema("value", value);
    if (value == null) {
      // here we are making it explicit, but "put(key, null) means DELETE in the API"
      // LOGGER.info("Deleting key: {}", key);
      producer.delete(key).whenComplete((___, error) -> {
        if (error != null) {
          record.fail();
        } else {
          record.ack();
        }
      });
    } else {
      // LOGGER.info("Writing key: {} value {}", key, value);
      producer.put(key, value).whenComplete((___, error) -> {
        if (count.incrementAndGet() % 1000 == 0) {
          LOGGER.info("written {} records", count);
        }
        if (error != null) {
          LOGGER.error("error", error);
          record.fail();
        } else {
          record.ack();
        }
      });
    }
  }

  private static void dumpSchema(String prefix, Object key) {
    if (key instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) key;
      Schema schema = record.getSchema();
      LOGGER.info("Schema for {}: {}", prefix, schema.toString());
    }
  }

  private static Object extract(Object o) {
    if (o instanceof GenericRecord) {
      return (GenericRecord) o;
    }
    // Pulsar GenericRecord is a wrapper over AVRO GenericRecord
    if (o instanceof org.apache.pulsar.client.api.schema.GenericRecord) {
      return ((org.apache.pulsar.client.api.schema.GenericRecord) o).getNativeObject();
    }

    return o;
  }

  @Override
  public void close() throws Exception {
    if (producer != null) {
      producer.close();
    }
  }

  private Map<String, String> getConfig(String storeName, String systemName) {
    Map<String, String> config = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + systemName + DOT;
    config.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.INCREMENTAL.toString());
    config.put(configPrefix + VENICE_STORE, storeName);
    config.put(configPrefix + VENICE_AGGREGATE, "false");
    config.put("venice.discover.urls", this.config.getVeniceDiscoveryUrl());
    config.put(VENICE_CONTROLLER_DISCOVERY_URL, this.config.getVeniceDiscoveryUrl());
    config.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id-pulsar-sink"));
    config.put(SSL_ENABLED, "false");
    if (this.config.getKafkaSaslConfig() != null && !this.config.getKafkaSaslConfig().isEmpty()) {
      config.put("kafka.sasl.jaas.config", this.config.getKafkaSaslConfig());
    }
    config.put("kafka.sasl.mechanism", this.config.getKafkaSaslMechanism());
    config.put("kafka.security.protocol", this.config.getKafkaSecurityProtocol());
    LOGGER.info("CONFIG: {}", config);
    return config;
  }

}
