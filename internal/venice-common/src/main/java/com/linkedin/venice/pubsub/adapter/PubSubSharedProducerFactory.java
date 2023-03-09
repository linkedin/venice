package com.linkedin.venice.pubsub.adapter;

import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_CLOSE_TIMEOUT_MS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterFactory;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This abstract class provides common functionality required to create and track shared producers.
 *
 * This service maintains a pool of shared producers. Ingestion task can acquire or release a producer on demand basis.
 * It does lazy initialization of producers. Also, producers are assigned based on the least loaded manner.
 */
public abstract class PubSubSharedProducerFactory implements PubSubProducerAdapterFactory<PubSubSharedProducerAdapter> {
  private static final Logger LOGGER = LogManager.getLogger(PubSubSharedProducerFactory.class);
  private final PubSubSharedProducerAdapter[] producers;
  private final Map<String, PubSubSharedProducerAdapter> producerTaskToProducerMap = new VeniceConcurrentHashMap<>();
  private volatile boolean isRunning = true;

  // stats
  private final MetricsRepository metricsRepository;
  private final PubSubSharedProducerStats pubSubSharedProducerStats;
  private final AtomicLong activeSharedProducerTasksCount = new AtomicLong(0);
  private final AtomicLong activeSharedProducerCount = new AtomicLong(0);

  protected final Properties producerProperties = new Properties();
  protected int producerCloseTimeout = DEFAULT_CLOSE_TIMEOUT_MS;

  /**
   * @param sharedProducerPoolCount  -- producer pool sizes
   * @param properties -- List of properties to construct a producer
   * @param metricsRepository -- metric repository
   */
  public PubSubSharedProducerFactory(
      int sharedProducerPoolCount,
      Properties properties,
      MetricsRepository metricsRepository) {
    this.producerProperties.putAll(properties);
    this.producers = new PubSubSharedProducerAdapter[sharedProducerPoolCount];
    this.metricsRepository = metricsRepository;
    this.pubSubSharedProducerStats =
        metricsRepository != null ? new PubSubSharedProducerStats(metricsRepository, this) : null;
  }

  @Override
  public synchronized void close() {
    isRunning = false;
    LOGGER.info("Closing shared producer factory");
    // This map should be empty when this is called.
    if (!producerTaskToProducerMap.isEmpty()) {
      LOGGER.warn(
          "Some producerTasks are still using the shared producers. [{}]",
          String.join(",", producerTaskToProducerMap.keySet()));
    }

    Set<PubSubSharedProducerAdapter> producerInstanceSet = new HashSet<>(Arrays.asList(producers));
    producerInstanceSet.parallelStream().filter(Objects::nonNull).forEach(sharedProducerAdapter -> {
      try {
        // Force close all the producer even if there are active producerTask assigned to it.
        LOGGER.info(
            "Closing producer: {}, Currently assigned task count: {}",
            sharedProducerAdapter,
            sharedProducerAdapter.getProducerTaskCount());
        sharedProducerAdapter.close(producerCloseTimeout, false);
        producers[sharedProducerAdapter.getId()] = null;
        decrActiveSharedProducerCount();
      } catch (Exception e) {
        LOGGER.warn("Error in closing shared producer", e);
      }
    });
  }

  public boolean isRunning() {
    return isRunning;
  }

  public synchronized PubSubSharedProducerAdapter acquireSharedProducer(String producerTaskName) {
    if (!isRunning) {
      throw new VeniceException(
          "Shared producer factory is already closed, can't assign new producer for task:" + producerTaskName);
    }

    PubSubSharedProducerAdapter sharedProducerAdapter = null;

    // Check if producer is assigned already
    if (producerTaskToProducerMap.containsKey(producerTaskName)) {
      sharedProducerAdapter = producerTaskToProducerMap.get(producerTaskName);
      LOGGER.info("{} already has a shared producer: {}", producerTaskName, sharedProducerAdapter.getId());
      return sharedProducerAdapter;
    }

    PubSubSharedProducerAdapter leastLoadedSharedProducer = null;
    int minProducerTaskCount = Integer.MAX_VALUE;

    // Do lazy creation of producers
    for (int i = 0; i < producers.length; i++) {
      if (producers[i] != null) {
        // keep track of the least used producer
        if (producers[i].getProducerTaskCount() < minProducerTaskCount) {
          minProducerTaskCount = producers[i].getProducerTaskCount();
          leastLoadedSharedProducer = producers[i];
        }
        continue;
      }
      LOGGER.info("Creating shared producer with id: {}", i);
      sharedProducerAdapter = createSharedProducer(i);
      producers[i] = sharedProducerAdapter;
      LOGGER.info("Created shared producer instance: {}", sharedProducerAdapter);
      incrActiveSharedProducerCount();
      break;
    }

    // Use the least used producer instance
    if (sharedProducerAdapter == null) {
      if (leastLoadedSharedProducer == null) {
        throw new VeniceException("No shared producer available");
      }
      sharedProducerAdapter = leastLoadedSharedProducer;
    }

    sharedProducerAdapter.addProducerTask(producerTaskName);
    producerTaskToProducerMap.put(producerTaskName, sharedProducerAdapter);
    LOGGER.info("Acquired the shared producer: {} for:{}", sharedProducerAdapter.getId(), producerTaskName);
    incrActiveSharedProducerTasksCount();
    return sharedProducerAdapter;
  }

  public synchronized void releaseSharedProducer(String producerTaskName) {
    if (!isRunning) {
      throw new VeniceException(
          "Shared producer factory has been already closed, can't release the producer for task:" + producerTaskName);
    }

    if (!producerTaskToProducerMap.containsKey(producerTaskName)) {
      LOGGER.error("Shared producer factory does not have a producer for:{}", producerTaskName);
      return;
    }
    PubSubSharedProducerAdapter sharedProducerAdapter = producerTaskToProducerMap.get(producerTaskName);
    sharedProducerAdapter.removeProducerTask(producerTaskName);
    producerTaskToProducerMap.remove(producerTaskName, sharedProducerAdapter);
    LOGGER.info("{} released the producer id: {}", producerTaskName, sharedProducerAdapter.getId());
    decrActiveSharedProducerTasksCount();
  }

  /**
   * N.B. This shared producer does not support brokerAddressToOverride. Do not use this in controllers.
   *
   * @param veniceProperties
   * @return An instance of shared producer adapter
   */
  @Override
  public PubSubSharedProducerAdapter create(
      VeniceProperties veniceProperties,
      String topicName,
      String brokerAddressToOverride) {
    return acquireSharedProducer(topicName);
  }

  /**
   * @param id Used as unique identifier to construct a new shared producer
   * @return An instance of shared producer adapter
   */
  public abstract PubSubSharedProducerAdapter createSharedProducer(int id);

  /**
   * @return returns the name of shared producer factory
   */
  public abstract String getName();

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

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }
}
