package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link KafkaConsumerService} is used to manage a shared consumer pool for all the ingestion tasks running in the same instance.
 * The reasons to have the shared consumer pool:
 * 1. Reduce the unnecessary overhead to have one consumer per store version, which includes the internal IO threads/connections
 *    to brokers and internal buffers;
 * 2. This shared consumer pool is expected to reduce the GC overhead when there are a lot of store versions bootstrapping/ingesting at the same;
 * 3. With the shared consumer pool, the total resources occupied by the consumers become configurable no matter how many
 *    store versions are being hosted in the same instances;
 *
 * The main idea of shared consumer pool:
 * 1. This class will be mostly in charge of managing subscriptions/unsubscriptions;
 * 2. The main function is to poll messages and delegate the processing logic to the existing {@link StoreIngestionTask}
 *    to make the logic in both classes isolated;
 *
 */

public abstract class KafkaConsumerService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerService.class);

  private final long readCycleDelayMs;
  private final IntList consumerPartitionsNumSubscribed;
  private final ExecutorService consumerExecutor;
  private final EventThrottler bandwidthThrottler;
  private final EventThrottler recordsThrottler;
  private final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;
  private final String kafkaUrl;
  private final boolean liveConfigBasedKafkaThrottlingEnabled;

  protected final KafkaConsumerServiceStats stats;
  protected final List<SharedKafkaConsumer> readOnlyConsumersList;

  private boolean stopped = false;

  public KafkaConsumerService(final KafkaClientFactory consumerFactory, final Properties consumerProperties,
      final long readCycleDelayMs, final int numOfConsumersPerKafkaCluster, final EventThrottler bandwidthThrottler,
      final EventThrottler recordsThrottler, final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final KafkaConsumerServiceStats stats, final long sharedConsumerNonExistingTopicCleanupDelayMS,
      final TopicExistenceChecker topicExistenceChecker, final boolean liveConfigBasedKafkaThrottlingEnabled) {
    this.readCycleDelayMs = readCycleDelayMs;
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.liveConfigBasedKafkaThrottlingEnabled = liveConfigBasedKafkaThrottlingEnabled;
    this.kafkaClusterBasedRecordThrottler = kafkaClusterBasedRecordThrottler;
    this.stats = stats;

    this.kafkaUrl = consumerProperties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    // Initialize consumers and consumerExecutor
    consumerExecutor = Executors.newFixedThreadPool(numOfConsumersPerKafkaCluster, new DaemonThreadFactory("venice-shared-consumer-for-" + kafkaUrl));
    consumerPartitionsNumSubscribed = new IntArrayList(numOfConsumersPerKafkaCluster);
    ArrayList<SharedKafkaConsumer> consumers = new ArrayList<>(numOfConsumersPerKafkaCluster);
    for (int i = 0; i < numOfConsumersPerKafkaCluster; ++i) {
      /**
       * We need to assign an unique client id across all the storage nodes, otherwise, they will fail into the same throttling bucket.
       */
      consumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, getUniqueClientId(kafkaUrl, i));
      SharedKafkaConsumer newConsumer = createSharedKafkaConsumer(consumerFactory.getConsumer(consumerProperties),sharedConsumerNonExistingTopicCleanupDelayMS, topicExistenceChecker);
      consumerExecutor.submit(new ConsumptionTask(kafkaUrl, newConsumer));
      consumers.add(newConsumer);
      consumerPartitionsNumSubscribed.add(0);
    }
    readOnlyConsumersList = Collections.unmodifiableList(consumers);
    consumerExecutor.shutdown();

    LOGGER.info("KafkaConsumerService was initialized with " + numOfConsumersPerKafkaCluster + " consumers.");
  }

  protected abstract SharedKafkaConsumer createSharedKafkaConsumer(final KafkaConsumerWrapper kafkaConsumerWrapper, final long nonExistingTopicCleanupDelayMS,
      TopicExistenceChecker topicExistenceChecker);

  private String getUniqueClientId(String kafkaUrl, int suffix) {
    return Utils.getHostName() + "_" + kafkaUrl + "_" + suffix;
  }

  KafkaConsumerServiceStats getStats() {
    return stats;
  }

  /**
   * @return a consumer that was previously assigned to a version topic {@link StoreIngestionTask#getVersionTopic()}. In
   *         other words, if {@link this#assignConsumerFor} is never called, there is no assigned consumer to given version
   *         topic. Hence, {@link Optional#empty()} is returned.
   */
  public abstract Optional<KafkaConsumerWrapper> getConsumerAssignedToVersionTopic(String versionTopic);

  /**
   * This function assigns a consumer for the given {@link StoreIngestionTask} and returns the assigned consumer.
   */
  public abstract KafkaConsumerWrapper assignConsumerFor(StoreIngestionTask ingestionTask);
  /**
   * Attach the messages belonging to {@param topic} to the passed {@param ingestionTask}
   */
  public abstract void attach(KafkaConsumerWrapper consumer, String topic, StoreIngestionTask ingestionTask);

  /**
   * Stop all subscription associated with the given version topic.
   */
  public abstract void unsubscribeAll(String versionTopic);

  @Override
  public boolean startInner() throws Exception {
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stopped = true;
    consumerExecutor.shutdownNow();
    consumerExecutor.awaitTermination(30, TimeUnit.SECONDS);
    readOnlyConsumersList.forEach( consumer -> consumer.close());
  }

  private class ConsumptionTask implements Runnable {
    private final String kafkaUrl;
    private final SharedKafkaConsumer consumer;

    public ConsumptionTask(final String kafkaUrl, final SharedKafkaConsumer consumer) {
      this.kafkaUrl = kafkaUrl;
      this.consumer = consumer;
    }

    @Override
    public void run() {
      boolean addSomeDelay = false;

      // Pre-allocate some variables to clobber in the loop
      long beforePollingTimeStamp;
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records;
      long beforeProducingToWriteBufferTimestamp;
      StoreIngestionTask ingestionTask;
      List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> partitionRecords;
      long totalBytes;
      while (!stopped) {
        try {
          if (addSomeDelay) {
            Thread.sleep(readCycleDelayMs);
            addSomeDelay = false;
          }
          beforePollingTimeStamp = System.currentTimeMillis();
          if (liveConfigBasedKafkaThrottlingEnabled) {
            records = kafkaClusterBasedRecordThrottler.poll(consumer, kafkaUrl, readCycleDelayMs);
          } else {
            records = consumer.poll(readCycleDelayMs);
          }
          stats.recordPollRequestLatency(LatencyUtils.getElapsedTimeInMs(beforePollingTimeStamp));
          stats.recordPollResultNum(records.count());
          if (!records.isEmpty()) {
            beforeProducingToWriteBufferTimestamp = System.currentTimeMillis();
            for (TopicPartition topicPartition : records.partitions()) {
              ingestionTask = consumer.getIngestionTaskForTopicPartition(topicPartition);
              if (ingestionTask == null) {
                // defensive code
                LOGGER.error("Couldn't find IngestionTask for topic partition : " + topicPartition + " after receiving records from `poll` request");
                continue;
              }
              partitionRecords = records.records(topicPartition);
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
                ingestionTask.produceToStoreBufferServiceOrKafka(
                    partitionRecords,
                    false,
                    topicPartition,
                    kafkaUrl);
              } catch (Exception e) {
                LOGGER.error("Received exception when StoreIngestionTask is processing the polled consumer record for topic: " + topicPartition, e);
                ingestionTask.setLastConsumerException(e);
              }
            }
            stats.recordConsumerRecordsProducingToWriterBufferLatency(LatencyUtils.getElapsedTimeInMs(beforeProducingToWriteBufferTimestamp));
            if (bandwidthThrottler.getMaxRatePerSecond() > 0) {
              // Bandwidth throttling requires doing an O(N) operation proportional to the number of records
              // consumed, so we will do it only if it's enabled, and avoid it otherwise.
              totalBytes = 0;
              for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
                totalBytes += record.serializedKeySize() + record.serializedValueSize();
              }
              bandwidthThrottler.maybeThrottle(totalBytes);
            }
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

  private void recordPartitionsPerConsumerSensor() {
    int totalPartitions = 0;
    int minPartitionsPerConsumer = Integer.MAX_VALUE;
    int maxPartitionsPerConsumer = Integer.MIN_VALUE;
    int avgPartitionsPerConsumer = -1;

    if (consumerPartitionsNumSubscribed.isEmpty()) {
      minPartitionsPerConsumer = -1;
      maxPartitionsPerConsumer = -1;
    } else {
      for (int partitionsNum : consumerPartitionsNumSubscribed) {
        totalPartitions += partitionsNum;
        minPartitionsPerConsumer = Math.min(minPartitionsPerConsumer, partitionsNum);
        maxPartitionsPerConsumer = Math.max(maxPartitionsPerConsumer, partitionsNum);
      }
      avgPartitionsPerConsumer = totalPartitions / consumerPartitionsNumSubscribed.size();
    }

    stats.recordAvgPartitionsPerConsumer(avgPartitionsPerConsumer);
    stats.recordMaxPartitionsPerConsumer(maxPartitionsPerConsumer);
    stats.recordMinPartitionsPerConsumer(minPartitionsPerConsumer);
  }

  public void setPartitionsNumSubscribed(SharedKafkaConsumer consumer, int assignedPartitions) {
    if (readOnlyConsumersList.contains(consumer)) {
      consumerPartitionsNumSubscribed.set(readOnlyConsumersList.indexOf(consumer), assignedPartitions);
      recordPartitionsPerConsumerSensor();
    } else {
      throw new VeniceException("Shared consumer cannot be found in KafkaConsumerService.");
    }
  }

  /**
   * This consumer assignment strategy specify how consumers from consumer pool are allocated. Now we support two basic
   * strategies with topic-wise and partition-wise for supporting consumer shared in topic and topic-partition granularity,
   * respectively. Each strategy will have a specific extension of {@link KafkaConsumerService}.
   */
  public enum ConsumerAssignmentStrategy {
    TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY,
    PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY
  }

}
