package com.linkedin.venice.writer;

import static com.linkedin.venice.writer.ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX;
import static com.linkedin.venice.writer.VeniceWriter.CLOSE_TIMEOUT_MS;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service maintains a pool of kafka producer. Ingestion task can acquire or release a producer on demand basis.
 * It does lazy initialization of producers. Also producers are assigned based on least loaded manner.
 */
public class SharedKafkaProducerService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(SharedKafkaProducerService.class);

  // This helps override kafka config for shared producer seperately than dedicated producer.
  public static final String SHARED_KAFKA_PRODUCER_CONFIG_PREFIX = "shared.producer.";

  private final int numOfProducersPerKafkaCluster;
  private final Properties producerProperties;
  private final String localKafkaBootstrapServers;
  private final int kafkaProducerCloseTimeout;

  private final SharedKafkaProducer[] producers;
  private final Map<String, SharedKafkaProducer> producerTaskToProducerMap = new VeniceConcurrentHashMap<>();
  private final KafkaProducerSupplier kafkaProducerSupplier;
  private volatile boolean isRunning = true;

  // stats
  private final MetricsRepository metricsRepository;
  private final Set<String> producerMetricsToBeReported;
  final AtomicLong activeSharedProducerTasksCount = new AtomicLong(0);
  final AtomicLong activeSharedProducerCount = new AtomicLong(0);

  /**
   *
   * @param properties -- List of properties to construct a kafka producer
   * @param sharedProducerPoolCount  -- producer pool sizes
   * @param kafkaProducerSupplier -- function to create a KafkaProducer object
   * @param metricsRepository -- metric repository
   * @param producerMetricsToBeReported -- a comma seperated list of KafkaProducer metrics that will exported as ingraph metrics
   */
  public SharedKafkaProducerService(
      Properties properties,
      int sharedProducerPoolCount,
      KafkaProducerSupplier kafkaProducerSupplier,
      MetricsRepository metricsRepository,
      Set<String> producerMetricsToBeReported) {
    this.kafkaProducerSupplier = kafkaProducerSupplier;
    boolean sslToKafka = Boolean.parseBoolean(properties.getProperty(ConfigKeys.SSL_TO_KAFKA, "false"));
    if (!sslToKafka) {
      localKafkaBootstrapServers = properties.getProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
    } else {
      localKafkaBootstrapServers = properties.getProperty(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS);
    }

    producerProperties = new Properties();
    producerProperties.putAll(properties);
    producerProperties.put(PROPERTIES_KAFKA_PREFIX + BOOTSTRAP_SERVERS_CONFIG, localKafkaBootstrapServers);

    VeniceProperties veniceWriterProperties = new VeniceProperties(producerProperties);
    kafkaProducerCloseTimeout = veniceWriterProperties.getInt(CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS);

    // replace all properties starting with SHARED_KAFKA_PRODUCER_CONFIG_PREFIX with PROPERTIES_KAFKA_PREFIX.
    Properties sharedProducerProperties =
        veniceWriterProperties.clipAndFilterNamespace(SHARED_KAFKA_PRODUCER_CONFIG_PREFIX).toProperties();
    for (Map.Entry<Object, Object> entry: sharedProducerProperties.entrySet()) {
      String key = PROPERTIES_KAFKA_PREFIX + (String) entry.getKey();
      producerProperties.put(key, entry.getValue());
    }

    this.numOfProducersPerKafkaCluster = sharedProducerPoolCount;
    this.producers = new SharedKafkaProducer[numOfProducersPerKafkaCluster];

    this.metricsRepository = metricsRepository;
    this.producerMetricsToBeReported = producerMetricsToBeReported;
    LOGGER.info("SharedKafkaProducer: is initialized");
  }

  @Override
  public boolean startInner() throws Exception {
    return true;
  }

  @Override
  public synchronized void stopInner() throws Exception {
    isRunning = false;
    LOGGER.info("SharedKafkaProducer: is being closed");
    // This map should be empty when this is called.
    if (!producerTaskToProducerMap.isEmpty()) {
      LOGGER.error(
          "SharedKafkaProducer: following producerTasks are still using the shared producers. [{}]",
          producerTaskToProducerMap.keySet().stream().collect(Collectors.joining(",")));
    }

    Set<SharedKafkaProducer> producerInstanceSet = new HashSet<>(Arrays.asList(producers));
    producerInstanceSet.parallelStream().filter(Objects::nonNull).forEach(sharedKafkaProducer -> {
      try {
        // Force close all the producer even if there are active producerTask assigned to it.
        LOGGER.info(
            "SharedKafkaProducer: Closing producer: {}, Currently assigned task: {}",
            sharedKafkaProducer,
            sharedKafkaProducer.getProducerTaskCount());
        sharedKafkaProducer.close(kafkaProducerCloseTimeout, false);
        producers[sharedKafkaProducer.getId()] = null;
        decrActiveSharedProducerCount();
      } catch (Exception e) {
        LOGGER.warn("SharedKafkaProducer: Error in closing kafka producer", e);
      }
    });
  }

  public boolean isRunning() {
    return isRunning;
  }

  public synchronized KafkaProducerWrapper acquireKafkaProducer(String producerTaskName) {
    if (!isRunning) {
      throw new VeniceException(
          "SharedKafkaProducer: is already closed, can't assign new producer for task:" + producerTaskName);
    }

    SharedKafkaProducer sharedKafkaProducer = null;

    if (producerTaskToProducerMap.containsKey(producerTaskName)) {
      sharedKafkaProducer = producerTaskToProducerMap.get(producerTaskName);
      LOGGER
          .info("SharedKafkaProducer: {} already has a producer id: {}", producerTaskName, sharedKafkaProducer.getId());
      return sharedKafkaProducer;
    }

    // Do lazy creation of producers
    for (int i = 0; i < producers.length; i++) {
      if (producers[i] == null) {
        LOGGER.info("SharedKafkaProducer: Creating Producer id: {}", i);
        producerProperties.put(PROPERTIES_KAFKA_PREFIX + CLIENT_ID_CONFIG, "shared-producer-" + String.valueOf(i));
        KafkaProducerWrapper kafkaProducerWrapper =
            kafkaProducerSupplier.getNewProducer(new VeniceProperties(producerProperties));
        sharedKafkaProducer =
            new SharedKafkaProducer(this, i, kafkaProducerWrapper, metricsRepository, producerMetricsToBeReported);
        producers[i] = sharedKafkaProducer;
        LOGGER.info("SharedKafkaProducer: Created Shared Producer instance: {}", sharedKafkaProducer);
        incrActiveSharedProducerCount();
        break;
      }
    }

    // Find the least used producer instance
    if (sharedKafkaProducer == null) {
      int minProducerTaskCount = Integer.MAX_VALUE;
      for (int i = 0; i < producers.length; i++) {
        if (producers[i].getProducerTaskCount() < minProducerTaskCount) {
          minProducerTaskCount = producers[i].getProducerTaskCount();
          sharedKafkaProducer = producers[i];
        }
      }
    }

    sharedKafkaProducer.addProducerTask(producerTaskName);
    producerTaskToProducerMap.put(producerTaskName, sharedKafkaProducer);
    LOGGER.info("SharedKafkaProducer: {} acquired the producer id: {}", producerTaskName, sharedKafkaProducer.getId());
    logProducerInstanceAssignments();
    incrActiveSharedProducerTasksCount();
    return sharedKafkaProducer;
  }

  public synchronized void releaseKafkaProducer(String producerTaskName) {
    if (!isRunning) {
      throw new VeniceException(
          "SharedKafkaProducer: is already closed, can't release the producer for task:" + producerTaskName);
    }

    if (!producerTaskToProducerMap.containsKey(producerTaskName)) {
      LOGGER.error("SharedKafkaProducer: {} does not have a producer", producerTaskName);
      return;
    }
    SharedKafkaProducer sharedKafkaProducer = producerTaskToProducerMap.get(producerTaskName);
    sharedKafkaProducer.removeProducerTask(producerTaskName);
    producerTaskToProducerMap.remove(producerTaskName, sharedKafkaProducer);
    LOGGER.info("SharedKafkaProducer: {} released the producer id: {}", producerTaskName, sharedKafkaProducer.getId());
    logProducerInstanceAssignments();
    decrActiveSharedProducerTasksCount();
  }

  /**
   * This will print a log line consisting of how each producer instances are shared among producerTasks. It prints
   * the following tuple for each ProducerInstance
   * {producerId : count of producerTask using this producer}
   *
   * An example is following.
   * Current Assignments: [{Id: 0, Task Count: 1},{Id: 1, Task Count: 1},{Id: 2, Task Count: 1},{Id: 3, Task Count: 1},{Id: 4, Task Count: 1},]
   *
   * This is purely for debugging purpose to check the producers are evenly loaded.
   */
  private void logProducerInstanceAssignments() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < producers.length; i++) {
      if (producers[i] != null) {
        sb.append(producers[i].toString()).append(",");
      }
    }
    sb.append("]");
    LOGGER.info("SharedKafkaProducer: Current Assignments: {}", sb);
  }

  public interface KafkaProducerSupplier {
    KafkaProducerWrapper getNewProducer(VeniceProperties props);
  }

  public long getActiveSharedProducerTasksCount() {
    return activeSharedProducerTasksCount.get();
  }

  public long getActiveSharedProducerCount() {
    return activeSharedProducerCount.get();
  }

  private void incrActiveSharedProducerTasksCount() {
    activeSharedProducerTasksCount.incrementAndGet();
  }

  private void decrActiveSharedProducerTasksCount() {
    activeSharedProducerTasksCount.decrementAndGet();
  }

  private void incrActiveSharedProducerCount() {
    activeSharedProducerCount.incrementAndGet();
  }

  private void decrActiveSharedProducerCount() {
    activeSharedProducerCount.decrementAndGet();
  }

}
