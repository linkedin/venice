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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.samza.config.MapConfig;


/**
 * A Pulsar Sink that sends messages to Venice.
 *
 * Please refer to Apache Pulsar's documentation for more information on Pulsar connectors:
 * https://pulsar.apache.org/docs/2.10.x/io-use/
 *
 * Available configuration parameters: see VeniceSinkConfig class.
 */
public class VeniceSink implements Sink<GenericObject> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceSink.class);

  VeniceSinkConfig config;
  VeniceSystemProducer producer;

  /** thread safe, fast access to count() **/
  private ArrayBlockingQueue<Record<GenericObject>> pendingFlushQueue;
  private final ScheduledExecutorService scheduledExecutor =
      Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "pulsar-venice-sink-flush-thread"));

  private int maxNumberUnflushedRecords = 10;
  private long flushIntervalMs = 500L;
  private volatile long lastFlush = 0L;

  private volatile Throwable flushException = null;
  private volatile boolean doThrottle = false;

  @Override
  public void open(Map<String, Object> cfg, SinkContext sinkContext) throws Exception {
    this.config = VeniceSinkConfig.load(cfg, sinkContext);
    LOGGER.info("Starting, config {}", this.config);

    VeniceSystemFactory factory = new VeniceSystemFactory();
    final String systemName = "venice";
    this.producer =
        factory.getClosableProducer(systemName, new MapConfig(getConfig(this.config.getStoreName(), systemName)), null);
    this.producer.start();
    String kafkaBootstrapServers = this.producer.getKafkaBootstrapServers();
    LOGGER.info("Kafka bootstrap for Venice producer {}", kafkaBootstrapServers);

    LOGGER.info("Kafka topic name is {}", this.producer.getTopicName());

    maxNumberUnflushedRecords = this.config.getMaxNumberUnflushedRecords();
    final int capacityMutliplier = 3;
    int queueSize = Integer.MAX_VALUE / capacityMutliplier < maxNumberUnflushedRecords
        ? Integer.MAX_VALUE
        : maxNumberUnflushedRecords * capacityMutliplier;
    pendingFlushQueue = new ArrayBlockingQueue<>(queueSize, false);
    flushIntervalMs = this.config.getFlushIntervalMs();

    scheduledExecutor.scheduleAtFixedRate(() -> flush(false), flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void write(Record<GenericObject> record) throws Exception {

    if (flushException != null) {
      LOGGER.error("Error while flushing records, stopping processing", flushException);
      throw new RuntimeException("Error while flushing records", flushException);
    }

    if (doThrottle) {
      LOGGER.warn("Throttling, not accepting new records; {} records pending", pendingFlushQueue.size());

      while (pendingFlushQueue.size() > maxNumberUnflushedRecords) {
        Thread.sleep(1);
      }
      doThrottle = false;
      LOGGER.info("done throttling");
    }

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

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Processing key: {}, value: {}", key, value);
    }
    if (value == null) {
      // here we are making it explicit, but "put(key, null) means DELETE in the API"
      producer.delete(key).whenComplete((___, error) -> {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Deleted key: {}", key);
        }

        if (error != null) {
          LOGGER.error("Error deleting record with key {}", key, error);
          record.fail();
        } else {
          if (safePutToQueue(record)) {
            maybeSubmitFlush();
          }
        }
      });
    } else {
      producer.put(key, value).whenComplete((___, error) -> {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Processed key: {}, value: {}", key, value);
        }

        if (error != null) {
          LOGGER.error("Error handling record with key {}", key, error);
          record.fail();
        } else {
          if (safePutToQueue(record)) {
            maybeSubmitFlush();
          }
        }
      });
    }
  }

  private boolean safePutToQueue(Record<GenericObject> record) {
    if (pendingFlushQueue.offer(record)) {
      return true;
    }

    doThrottle = true;
    scheduledExecutor.submit(() -> safePutToQueue(record));
    return false;
  }

  private void maybeSubmitFlush() {
    if (pendingFlushQueue.size() >= maxNumberUnflushedRecords) {
      scheduledExecutor.submit(() -> flush(false));
    }
  }

  // flush should happen on the same thread
  private void flush(boolean force) {
    long startTimeMillis = System.currentTimeMillis();
    int sz = pendingFlushQueue.size();
    if (force || sz >= maxNumberUnflushedRecords || startTimeMillis - lastFlush > flushIntervalMs) {
      lastFlush = System.currentTimeMillis();
      if (sz == 0) {
        LOGGER.debug("Nothing to flush");
        return;
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Flushing {} records", sz);
      }

      try {
        // there are no checked exceptions but unchecked ones can be thrown
        producer.flush("not used");
      } catch (Throwable t) {
        LOGGER.error("Error flushing", t);
        flushException = t;
        failAllPendingRecords();
        throw new RuntimeException("Error while flushing records", flushException);
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Acking the records");
      }
      for (int i = 0; i < sz; i++) {
        Record<GenericObject> rec = pendingFlushQueue.poll();
        if (rec != null) {
          rec.ack();
        } else {
          // should not happen as long as the removal from queue happens in the single thread
          RuntimeException err =
              new IllegalStateException("Error while flushing records: Record is null, expected actual record");
          flushException = err;
          failAllPendingRecords();
          throw err;
        }
      }

      LOGGER.info("Flush of {} records took {} ms", sz, System.currentTimeMillis() - startTimeMillis);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Skipping flush of {} records", sz);
      }
    }
  }

  private void failAllPendingRecords() {
    for (Record<GenericObject> rec: pendingFlushQueue) {
      if (rec != null) {
        rec.fail();
      } else {
        break;
      }
    }
  }

  private static Object extract(Object o) {
    // Pulsar GenericRecord is a wrapper over AVRO GenericRecord
    if (o instanceof org.apache.pulsar.client.api.schema.GenericRecord) {
      return ((org.apache.pulsar.client.api.schema.GenericRecord) o).getNativeObject();
    }

    return o;
  }

  @Override
  public void close() throws Exception {
    if (producer != null) {
      scheduledExecutor.submit(() -> flush(true)).get();
      scheduledExecutor.shutdown();
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
