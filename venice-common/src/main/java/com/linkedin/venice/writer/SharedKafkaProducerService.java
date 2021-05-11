package com.linkedin.venice.writer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.KafkaClientStats;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

import static com.linkedin.venice.writer.ApacheKafkaProducer.*;
import static com.linkedin.venice.writer.VeniceWriter.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;


/**
 * This service maintains a pool of kafka producer. Ingestion task can acquire or release a producer on demand basis.
 * It does lazy initialization of producers. Also producers are assigned based on least loaded manner.
 */
public class SharedKafkaProducerService extends AbstractVeniceService {
  private static final Logger LOGGER = Logger.getLogger(SharedKafkaProducerService.class);

  //This helps override kafka config for shared producer seperately than dedicated producer.
  public static final String SHARED_KAFKA_PRODUCER_CONFIG_PREFIX = "shared.producer.";
  private final Optional<KafkaClientStats> kafkaClientStats;

  private final int numOfProducersPerKafkaCluster;
  private Properties producerProperties;
  private final String localKafkaBootstrapServers;
  private final int kafkaProducerCloseTimeout;

  private final SharedKafkaProducer[] producers;
  private final Map<String, SharedKafkaProducer> producerTaskToProducerMap = new VeniceConcurrentHashMap<>();
  private final KafkaProducerSupplier kafkaProducerSupplier;
  private boolean closed = false;

  public SharedKafkaProducerService(Properties properties, int sharedProducerPoolCount,
      KafkaProducerSupplier kafkaProducerSupplier, Optional<KafkaClientStats> kafkaClientStats) {
    this.kafkaProducerSupplier = kafkaProducerSupplier;
    boolean sslToKafka = Boolean.valueOf(properties.getProperty(ConfigKeys.SSL_TO_KAFKA, "false"));
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

    //replace all properties starting with SHARED_KAFKA_PRODUCER_CONFIG_PREFIX with PROPERTIES_KAFKA_PREFIX.
    Properties sharedProducerProperties = veniceWriterProperties.clipAndFilterNamespace(SHARED_KAFKA_PRODUCER_CONFIG_PREFIX).toProperties();
    for (Map.Entry<Object,Object> entry: sharedProducerProperties.entrySet()) {
      String key = PROPERTIES_KAFKA_PREFIX + (String)entry.getKey();
      producerProperties.put(key, entry.getValue());
    }

    this.numOfProducersPerKafkaCluster = sharedProducerPoolCount;
    producers = new SharedKafkaProducer[numOfProducersPerKafkaCluster];

    this.kafkaClientStats = kafkaClientStats;
    logger.info("SharedKafkaProducer: is initialized");
  }

  @Override
  public boolean startInner() throws Exception {
    return true;
  }

  @Override
  public synchronized void stopInner() throws Exception {
    //This map should be empty when this is called.
    if (!producerTaskToProducerMap.isEmpty()) {
      logger.error("SharedKafkaProducer: is being closed, but following producerTasks are still using the shared producers. {"
          + producerTaskToProducerMap.keySet().stream().collect(Collectors.joining(",")) + "}");
    }

    Set<SharedKafkaProducer> producerInstanceSet = new HashSet<>(Arrays.asList(producers));
    producerInstanceSet.parallelStream().filter(Objects::nonNull).forEach(sharedKafkaProducer -> {
      try {
        //Force close all the producer even if there are active producerTask assigned to it.
        logger.info("SharedKafkaProducer: Closing producer: " + sharedKafkaProducer + ", Currently assigned task: "
            + sharedKafkaProducer.getProducerTaskCount());
        sharedKafkaProducer.close(kafkaProducerCloseTimeout);
        producers[sharedKafkaProducer.getId()] = null;
        if (kafkaClientStats.isPresent()) {
          kafkaClientStats.get().decrActiveSharedProducerCount();
        }
      } catch (Exception e) {
        logger.warn("SharedKafkaProducer: Error in closing kafka producer", e);
      }
    });
    closed = true;
  }

  public synchronized KafkaProducerWrapper acquireKafkaProducer(String producerTaskName) {
    SharedKafkaProducer sharedKafkaProducer = null;

    if (producerTaskToProducerMap.containsKey(producerTaskName)) {
      sharedKafkaProducer = producerTaskToProducerMap.get(producerTaskName);
      LOGGER.info(
          "SharedKafkaProducer: " + producerTaskName + " already has a producer id: " + sharedKafkaProducer.getId());
      return sharedKafkaProducer;
    }

    //Do lazy creation of producers
    for (int i = 0; i < producers.length; i++) {
      if (producers[i] == null) {
        LOGGER.info("SharedKafkaProducer: Creating Producer id: " + i);
        producerProperties.put(PROPERTIES_KAFKA_PREFIX + CLIENT_ID_CONFIG,
            "shared-producer-" + String.valueOf(i));
        KafkaProducerWrapper kafkaProducerWrapper =
            kafkaProducerSupplier.getNewProducer(new VeniceProperties(producerProperties));
        sharedKafkaProducer = new SharedKafkaProducer(this, i, kafkaProducerWrapper);
        producers[i] = sharedKafkaProducer;
        LOGGER.info("SharedKafkaProducer: Created Shared Producer instance: " + sharedKafkaProducer);
        if (kafkaClientStats.isPresent()) {
          kafkaClientStats.get().incrActiveSharedProducerCount();
        }
        break;
      }
    }

    //Find the least used producer instance
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
    LOGGER.info(
        "SharedKafkaProducer: " + producerTaskName + " acquired the producer id: " + sharedKafkaProducer.getId());
    logProducerInstanceAssignments();
    if (kafkaClientStats.isPresent()) {
      kafkaClientStats.get().incrActiveSharedProducerTasksCount();
    }
    return sharedKafkaProducer;
  }

  public synchronized void releaseKafkaProducer(String producerTaskName) {
    if (!producerTaskToProducerMap.containsKey(producerTaskName)) {
      LOGGER.error("SharedKafkaProducer: " + producerTaskName + " does not have a producer");
      return;
    }
    SharedKafkaProducer sharedKafkaProducer = producerTaskToProducerMap.get(producerTaskName);
    sharedKafkaProducer.removeProducerTask(producerTaskName);
    producerTaskToProducerMap.remove(producerTaskName, sharedKafkaProducer);
    LOGGER.info(
        "SharedKafkaProducer: " + producerTaskName + " released the producer id: " + sharedKafkaProducer.getId());
    logProducerInstanceAssignments();
    if (kafkaClientStats.isPresent()) {
      kafkaClientStats.get().decrActiveSharedProducerTasksCount();
    }
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
    LOGGER.info("SharedKafkaProducer: Current Assignments: " + sb.toString());
  }

  public interface KafkaProducerSupplier {
    KafkaProducerWrapper getNewProducer(VeniceProperties props);
  }

}
