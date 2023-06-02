package com.linkedin.venice.pulsar.sink;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.DOT;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CONTROLLER_DISCOVERY_URL;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_ROUTER_URL;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_STORE;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
public class VenicePulsarSink implements Sink<GenericObject> {
  private static final Logger LOGGER = LogManager.getLogger(VenicePulsarSink.class);

  VenicePulsarSinkConfig config;
  VeniceSystemProducer producer;

  private final AtomicInteger pendingRecordsCount = new AtomicInteger(0);
  private final ScheduledExecutorService scheduledExecutor =
      Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "pulsar-venice-sink-flush-thread"));

  private int maxNumberUnflushedRecords = 10;
  private long flushIntervalMs = 500L;
  private volatile long lastFlush = 0L;

  private volatile Throwable flushException = null;
  private volatile boolean doThrottle = false;

  @Override
  public void open(Map<String, Object> cfg, SinkContext sinkContext) throws Exception {
    VenicePulsarSinkConfig veniceCfg = VenicePulsarSinkConfig.load(cfg, sinkContext);
    LOGGER.info("Starting, Venice config {}", veniceCfg);

    VeniceSystemFactory factory = new VeniceSystemFactory();
    final String systemName = "venice";
    VeniceSystemProducer p =
        factory.getClosableProducer(systemName, new MapConfig(getConfig(veniceCfg, systemName)), null);
    p.start();
    String kafkaBootstrapServers = p.getKafkaBootstrapServers();
    LOGGER.info("Kafka bootstrap for Venice producer {}", kafkaBootstrapServers);
    LOGGER.info("Kafka topic name is {}", p.getTopicName());

    open(veniceCfg, p, sinkContext);
  }

  /** to simplify unit testing **/
  public void open(VenicePulsarSinkConfig config, VeniceSystemProducer startedProducer, SinkContext sinkContext)
      throws Exception {
    this.config = config;
    this.producer = startedProducer;

    maxNumberUnflushedRecords = this.config.getMaxNumberUnflushedRecords();
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
      LOGGER.warn("Throttling, not accepting new records; {} records pending", pendingRecordsCount.get());
      long start = System.currentTimeMillis();
      throttle();
      doThrottle = false;
      LOGGER.warn("Throttling is done, took {} ms", System.currentTimeMillis() - start);
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
        pendingRecordsCount.decrementAndGet();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Deleted key: {}", key);
        }

        if (error != null) {
          LOGGER.error("Error deleting record with key {}", key, error);
          flushException = error;
          record.fail();
        } else {
          record.ack();
        }
      });
    } else {
      producer.put(key, value).whenComplete((___, error) -> {
        pendingRecordsCount.decrementAndGet();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Processed key: {}, value: {}", key, value);
        }

        if (error != null) {
          LOGGER.error("Error handling record with key {}", key, error);
          flushException = error;
          record.fail();
        } else {
          record.ack();
        }
      });
    }

    pendingRecordsCount.incrementAndGet();
    maybeSubmitFlush();
  }

  protected void throttle() throws InterruptedException {
    while (pendingRecordsCount.get() > maxNumberUnflushedRecords) {
      Thread.sleep(1);
    }
  }

  private void maybeSubmitFlush() {
    int sz = pendingRecordsCount.get();
    if (sz >= maxNumberUnflushedRecords) {
      scheduledExecutor.submit(() -> flush(false));
      if (sz > 2 * maxNumberUnflushedRecords) {
        LOGGER.info("Too many records pending: {}. Will throttle.", sz);
        doThrottle = true;
      }
    }
  }

  // flush should happen on the same thread
  private void flush(boolean force) {
    long startTimeMillis = System.currentTimeMillis();

    int sz = pendingRecordsCount.get();
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
        LOGGER.error("Error while flushing records", t);
        throw new RuntimeException("Error while flushing records", flushException);
      }

      LOGGER.info("Flush of {} records took {} ms", sz, System.currentTimeMillis() - startTimeMillis);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Skipping flush of {} records", sz);
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
      Future<?> f = scheduledExecutor.submit(() -> flush(true));
      scheduledExecutor.shutdown();
      try {
        f.get();
      } finally {
        if (!scheduledExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
          LOGGER.error("Failed to shutdown scheduledExecutor");
          scheduledExecutor.shutdownNow();
        }
        producer.close();
      }
    }
  }

  public static Map<String, String> getConfig(VenicePulsarSinkConfig veniceCfg, String systemName) {
    Map<String, String> config = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + systemName + DOT;
    config.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.INCREMENTAL.toString());
    config.put(configPrefix + VENICE_STORE, veniceCfg.getStoreName());
    config.put(configPrefix + VENICE_AGGREGATE, "false");

    config.put("venice.discover.urls", veniceCfg.getVeniceDiscoveryUrl());
    config.put(VENICE_CONTROLLER_DISCOVERY_URL, veniceCfg.getVeniceDiscoveryUrl());
    config.put(VENICE_ROUTER_URL, veniceCfg.getVeniceRouterUrl());
    config.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id-pulsar-sink"));
    config.put(SSL_ENABLED, "false");
    if (veniceCfg.getKafkaSaslConfig() != null && !veniceCfg.getKafkaSaslConfig().isEmpty()) {
      config.put("kafka.sasl.jaas.config", veniceCfg.getKafkaSaslConfig());
    }
    config.put("kafka.sasl.mechanism", veniceCfg.getKafkaSaslMechanism());
    config.put("kafka.security.protocol", veniceCfg.getKafkaSecurityProtocol());

    if (veniceCfg.getWriterConfig() != null && !veniceCfg.getWriterConfig().isEmpty()) {
      LOGGER.info("Additional WriterConfig: {}", veniceCfg.getWriterConfig());
      config.putAll(veniceCfg.getWriterConfig());
    }

    LOGGER.info("CONFIG: {}", config);
    return config;
  }

}
