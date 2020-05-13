package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;


/**
 * {@link KafkaConsumerService} is used to manage a shared consumer pool for all the ingestion tasks running in the same instance.
 * The reasons to have the shared consumer pool:
 * 1. Reduce the unnecessary overhead to have one consumer per store version, which includes the internal IO threads/connections
 *    to brokers and internal buffers;
 * 2. This shared consumer pool is expected to reduce the GC overhead when there are a lot of store versions bootstrapping/ingesting at the same;
 * 3. With the shared consumer pool, the total resources occupied by the consumers become configurable no matter how many
 *    store versions are being hosted in the same instances;
 *
 * TODO: So far, the current implementation is assuming all the topics are in the same Kafka cluster, and it won't be this case
 * when Active/Active replication is available, and we need to make the following changes to support it:
 * 1. This class needs to maintain a consumer pool/consumer executors per Kafka cluster;
 * 2. Functions: {@link #getConsumer}, {@link #attach} and {@link #detach} need to have a 'kafkaUrl' to decide which
 *    consumer pool to use;
 * 3. {@link StoreIngestionTask} needs to have something like ConsumerManager since it will be using consumers for
 *    multiple different Kafka clusters;
 *
 * The main idea of shared consumer pool:
 * 1. This class will be mostly in charge of managing subscriptions/unsubscriptions;
 * 2. The main function is to poll messages and delegate the processing logic to the existing {@link StoreIngestionTask}
 *    to make the logic in both classes isolated;
 *
 */
public class KafkaConsumerService extends AbstractVeniceService {
  private static final Logger LOGGER = Logger.getLogger(KafkaConsumerService.class);

  private final int numOfConsumersPerKafkaCluster;
  private final long readCycleDelayMs;

  private final List<SharedKafkaConsumer> consumers = new ArrayList<>();
  /**
   * This field is used to maintain the mapping between version topic and the corresponding ingestion task.
   * In theory, One version topic should only be mapped to one ingestion task, and if this assumption is violated
   * in the future, we need to change the design of this service.
   */
  private final Map<String, SharedKafkaConsumer> versionTopicToConsumerMap = new VeniceConcurrentHashMap<>();
  private final ExecutorService consumerExecutor;
  private final KafkaConsumerServiceStats stats;

  private EventThrottler bandwidthThrottler;
  private EventThrottler recordsThrottler;
  private boolean stopped = false;


  public KafkaConsumerService(final KafkaClientFactory consumerFactory, final Properties consumerProperties,
      final VeniceServerConfig serverConfig, final EventThrottler bandwidthThrottler, final EventThrottler recordsThrottler,
      final MetricsRepository metricsRepository) {
    this.readCycleDelayMs = serverConfig.getKafkaReadCycleDelayMs();
    this.numOfConsumersPerKafkaCluster = serverConfig.getConsumerPoolSizePerKafkaCluster();
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.stats = new KafkaConsumerServiceStats(metricsRepository);

    String kafkaUrl = consumerProperties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    // Initialize consumers and consumerExecutor
    consumerExecutor = Executors.newFixedThreadPool(numOfConsumersPerKafkaCluster, new DaemonThreadFactory("venice-shared-consumer-for-" + kafkaUrl));
    for (int i = 0; i < numOfConsumersPerKafkaCluster; ++i) {
      /**
       * The consumer properties doesn't specify `client.id` explicitly, and internally Kafka client will assign an unique
       * client id for each consumer.
       */
      SharedKafkaConsumer newConsumer = new SharedKafkaConsumer(consumerFactory.getConsumer(consumerProperties));
      consumerExecutor.submit(new ConsumptionTask(newConsumer));
      consumers.add(newConsumer);
    }
    consumerExecutor.shutdown();

    LOGGER.info("KafkaConsumerService was initialized with " + numOfConsumersPerKafkaCluster + " consumers.");
  }

  /**
   * This function is used to check whether the passed {@param consumer} has already subscribed the topics
   * belonging to the same store, which owns the passed {@param versionTopic} or not.
   * @param consumer
   * @param versionTopic
   * @return
   */
  private boolean checkWhetherConsumerHasSubscribedSameStore(SharedKafkaConsumer consumer, String versionTopic)  {
    String storeName = Version.parseStoreFromKafkaTopicName(versionTopic);

    for (Map.Entry<String, SharedKafkaConsumer> entry : versionTopicToConsumerMap.entrySet()) {
      String vt = entry.getKey();
      SharedKafkaConsumer c = entry.getValue();
      if (c != consumer) {
        continue;
      }
      if (Version.parseStoreFromKafkaTopicName(vt).equals(storeName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This function will return a consumer for the passed {@link StoreIngestionTask}.
   * If the version topic of the passed {@link StoreIngestionTask} has been attached before, the previously assigned
   * consumer will be returned.
   *
   * This function will also try to avoid assigning the same consumer to the version topics, which are belonging to
   * the same store since for Hybrid store, the ingestion tasks for different store versions will subscribe the same
   * Real-time topic, which won't work if they are using the same shared consumer.
   * @param ingestionTask
   * @return
   */
  public synchronized KafkaConsumerWrapper getConsumer(StoreIngestionTask ingestionTask) {
    String versionTopic = ingestionTask.getVersionTopic();
    // Check whether this version topic has been subscribed before or not.
    SharedKafkaConsumer chosenConsumer = null;
    chosenConsumer = versionTopicToConsumerMap.get(versionTopic);
    if (null != chosenConsumer) {
      LOGGER.info("The version topic: " + versionTopic + " has been subscribed previously,"
          + " so this function will return the previously assigned shared consumer directly");
      return chosenConsumer;
    }
    // Find the least loaded consumer
    int maxAssignmentPerConsumer = Integer.MAX_VALUE;
    for (SharedKafkaConsumer consumer : consumers) {
      if (ingestionTask.isHybridMode()) {
        /**
         * Firstly, we need to make sure multiple store versions won't share the same consumer since for Hybrid stores,
         * all the store versions will consume the same RT topic with different offset.
         */
        if (checkWhetherConsumerHasSubscribedSameStore(consumer, versionTopic)) {
          LOGGER.info("Current consumer has already subscribed the same store as the new topic: " + versionTopic + ", will skip it and try next consumer in consumer pool");
          continue;
        }
      }

      int assignedPartitions = consumer.getAssignment().size();
      if (assignedPartitions < maxAssignmentPerConsumer) {
        maxAssignmentPerConsumer = assignedPartitions;
        chosenConsumer = consumer;
      }
    }
    if (null == chosenConsumer) {
      throw new VeniceException("Failed to find consumer for topic: " + versionTopic + ", and it might be caused by that all"
          + " the existing consumers have subscribed the same store, and that might be caused by a bug or resource leaking");
    }
    versionTopicToConsumerMap.put(versionTopic, chosenConsumer);
    LOGGER.info("Assigned a shared consumer for topic: " + ingestionTask.topic);
    return chosenConsumer;
  }

  /**
   * Attach the messages belonging to {@param topic} to the passed {@param ingestionTask}
   */
  public synchronized void attach(KafkaConsumerWrapper consumer, String topic, StoreIngestionTask ingestionTask) {
    if (!(consumer instanceof SharedKafkaConsumer)) {
      throw new VeniceException("The `consumer` passed must be a `SharedKafkaConsumer`");
    }
    SharedKafkaConsumer sharedConsumer = (SharedKafkaConsumer)consumer;
    if (! consumers.contains(sharedConsumer)) {
      throw new VeniceException("Unknown shared consumer passed");
    }
    sharedConsumer.attach(topic, ingestionTask);
  }

  /**
   * Detach the messages processing belonging to the topics of the passed {@param ingestionTask}
   */
  public synchronized void detach(StoreIngestionTask ingestionTask) {
    String versionTopic = ingestionTask.getVersionTopic();
    SharedKafkaConsumer sharedKafkaConsumer = versionTopicToConsumerMap.get(versionTopic);
    if (null == sharedKafkaConsumer) {
      LOGGER.warn("No assigned shared consumer found for this version topic: " + versionTopic);
      return;
    }
    versionTopicToConsumerMap.remove(versionTopic);
    sharedKafkaConsumer.detach(ingestionTask);
  }

  @Override
  public boolean startInner() throws Exception {
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stopped = true;
    consumerExecutor.shutdownNow();
    consumerExecutor.awaitTermination(30, TimeUnit.SECONDS);
    consumers.forEach( consumer -> consumer.close());
  }

  private class ConsumptionTask implements Runnable {
    private final SharedKafkaConsumer consumer;

    public ConsumptionTask(final SharedKafkaConsumer consumer) {
      this.consumer = consumer;
    }

    @Override
    public void run() {
      boolean addSomeDelay = false;
      Map<String, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> topicRecordsMap = new HashMap<>();
      while (! stopped) {
        try {
          if (!consumer.hasSubscription()) {
            Thread.sleep(readCycleDelayMs);
            continue;
          }
          if (addSomeDelay) {
            Thread.sleep(readCycleDelayMs);
            addSomeDelay = false;
          }
          long beforePollingTimeStamp = System.currentTimeMillis();
          ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records = consumer.poll(readCycleDelayMs);
          stats.recordPollRequestLatency(LatencyUtils.getElapsedTimeInMs(beforePollingTimeStamp));
          stats.recordPollResultNum(records.count());
          long totalBytes = 0;
          if (!records.isEmpty()) {
            topicRecordsMap.clear();
            for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
              totalBytes += record.serializedKeySize() + record.serializedValueSize();
              String topic = record.topic();
              List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> recordsOfCurrentTopic = topicRecordsMap.computeIfAbsent(topic, k -> new LinkedList<>());
              recordsOfCurrentTopic.add(record);
            }
            for (Map.Entry<String, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> entry : topicRecordsMap.entrySet()) {
              String topic = entry.getKey();
              List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> topicRecords = entry.getValue();
              StoreIngestionTask ingestionTask = consumer.getIngestionTaskForTopic(topic);
              if (null == ingestionTask) {
                // defensive code
                LOGGER.error("Couldn't find IngestionTask for topic: " + topic + " after receiving records from `poll` request");
                continue;
              }
              try {
                /**
                 * This function could be blocked by the following reasons:
                 * 1. The pre-condition is not satisfied before producing to the shared StoreBufferService, such as value schema is not available;
                 * 2. The producing is blocked by the throttling of the shared StoreBufferService;
                 *
                 * For #1, it is acceptable since there is a timeout for the blocking logic, and it doesn't happen very often
                 * based on the operational experience;
                 * For #2, the blocking caused by throttling is expected since all the ingestion tasks are sharing the
                 * same StoreBufferService;
                 *
                 * If there are changes with the above assumptions or new blocking behaviors, we need to evaluate whether
                 * we need to do some kind of isolation here, otherwise the consumptions for other store versions with the
                 * same shared consumer will be affected.
                 * The potential isolation strategy is:
                 * 1. When detecting such kind of prolonged or infinite blocking, the following function should expose a
                 * param to decide whether it should return early in those conditions;
                 * 2. Once this function realizes this behavior, it could choose to temporarily {@link KafkaConsumerWrapper#pause}
                 * the blocked consumptions;
                 * 3. This runnable could {@link KafkaConsumerWrapper#resume} the subscriptions after some delays or
                 * condition change, and there are at least two ways to make the subscription resumption without missing messages:
                 * a. Keep the previous message leftover in this class and retry, and once the messages can be processed
                 * without blocking, then resume the paused subscriptions;
                 * b. Don't keep the message leftover in this class, but every time, rewind the offset to the checkpointed offset
                 * of the corresponding {@link StoreIngestionTask} and resume subscriptions;
                 *
                 * For option #a, the logic is simpler and but the concern is that
                 * the buffered messages inside the shared consumer and the message leftover could potentially cause
                 * some GC issue, and option #b won't have this problem since {@link KafkaConsumerWrapper#pause} will drop
                 * all the buffered messages for the paused partitions, but just slightly more complicate.
                 *
                 */
                ingestionTask.produceToStoreBufferService(topicRecords, false);
              } catch (Exception e) {
                LOGGER.error("Received exception when StoreIngestionTask is processing the polled consumer record for topic: " + topic, e);
                ingestionTask.setLastConsumerException(e);
              }
            }

            bandwidthThrottler.maybeThrottle(totalBytes);
            recordsThrottler.maybeThrottle(records.count());
          } else {
            // No result came back, here will add some delay
            addSomeDelay = true;
          }
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException, will exit");
          break;
        } catch (Exception e) {
          LOGGER.error("Received exception while polling, will retry", e);
          addSomeDelay = true;
          stats.recordPollError();
        }
      }
      LOGGER.info("Shared consumer thread: " + Thread.currentThread().getName() + " exited");
    }
  }
}
