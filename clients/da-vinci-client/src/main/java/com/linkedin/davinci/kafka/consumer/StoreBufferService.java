package com.linkedin.davinci.kafka.consumer;

import static java.util.Collections.reverseOrder;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

import com.linkedin.venice.common.Measurable;
import com.linkedin.venice.exceptions.VeniceChecksumException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is serving as a {@link ConsumerRecord} buffer with an accompanying pool of drainer threads. The drainers
 * pull records out of the buffer and delegate the persistence and validation to the appropriate {@link StoreIngestionTask}.
 *
 * High-level idea:
 * 1. {@link StoreBufferService} will be maintaining a fixed number (configurable) of {@link StoreBufferDrainer} pool;
 * 2. For each {@link StoreBufferDrainer}, there is a corresponding {@link BlockingQueue}, which will buffer {@link QueueNode};
 * 3. All the records belonging to the same topic+partition will be allocated to the same drainer thread, otherwise DIV will fail;
 * 4. The logic to assign topic+partition to drainer, please check {@link #getDrainerIndexForConsumerRecord(ConsumerRecord, int)};
 * 5. There is still a thread executing {@link StoreIngestionTask} for each topic, which will handle admin actions, such
 * as subscribe, unsubscribe, kill and so on, and also poll consumer records from Kafka and put them into {@link #blockingQueueArr}
 * maintained by {@link StoreBufferService};
 *
 * For now, the assumption is that one-consumer-polling-thread should be fast enough to catch up with Kafka MM replication,
 * and data processing is the slowest part. If we find that polling is also slow later on, we may consider to adopt a consumer
 * thread pool to speed up polling from local Kafka brokers.
 */
public class StoreBufferService extends AbstractStoreBufferService {
  /**
   * Queue node type in {@link BlockingQueue} of each drainer thread.
   */
  private static class QueueNode implements Measurable {
    /**
     * Considering the overhead of {@link ConsumerRecord} and its internal structures.
     */
    private static final int QUEUE_NODE_OVERHEAD_IN_BYTE = 256;
    private final ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord;
    private final StoreIngestionTask ingestionTask;
    private final String kafkaUrl;
    private final long beforeProcessingRecordTimestamp;

    public QueueNode(
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
        StoreIngestionTask ingestionTask,
        String kafkaUrl,
        long beforeProcessingRecordTimestamp) {
      this.consumerRecord = consumerRecord;
      this.ingestionTask = ingestionTask;
      this.kafkaUrl = kafkaUrl;
      this.beforeProcessingRecordTimestamp = beforeProcessingRecordTimestamp;
    }

    public ConsumerRecord<KafkaKey, KafkaMessageEnvelope> getConsumerRecord() {
      return this.consumerRecord;
    }

    public StoreIngestionTask getIngestionTask() {
      return this.ingestionTask;
    }

    public LeaderProducedRecordContext getLeaderProducedRecordContext() {
      return null;
    }

    public CompletableFuture<Void> getQueuedRecordPersistedFuture() {
      return null;
    }

    public String getKafkaUrl() {
      return this.kafkaUrl;
    }

    public long getBeforeProcessingRecordTimestamp() {
      return this.beforeProcessingRecordTimestamp;
    }

    /**
     * This function is being used by {@link BlockingQueue#contains(Object)}.
     * The goal is to find out whether the buffered queue still has any records belonging to the specified topic+partition.
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null)
        return false;
      if (getClass() != o.getClass())
        return false;

      QueueNode node = (QueueNode) o;
      return this.consumerRecord.topic().equals(node.consumerRecord.topic())
          && this.consumerRecord.partition() == node.consumerRecord.partition();

    }

    @Override
    public int getSize() {
      // TODO:This should not be a big issue but ideally it should calculate the size from leaderProducedRecordContext
      // if present.
      return this.consumerRecord.serializedKeySize() + this.consumerRecord.serializedValueSize()
          + this.consumerRecord.topic().length() + QUEUE_NODE_OVERHEAD_IN_BYTE;
    }

    @Override
    public String toString() {
      return this.consumerRecord.toString();
    }
  }

  private static class FollowerQueueNode extends QueueNode {
    private final CompletableFuture<Void> queuedRecordPersistedFuture;

    public FollowerQueueNode(
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
        StoreIngestionTask ingestionTask,
        String kafkaUrl,
        long beforeProcessingRecordTimestamp,
        CompletableFuture<Void> queuedRecordPersistedFuture) {
      super(consumerRecord, ingestionTask, kafkaUrl, beforeProcessingRecordTimestamp);
      this.queuedRecordPersistedFuture = queuedRecordPersistedFuture;
    }

    @Override
    public CompletableFuture<Void> getQueuedRecordPersistedFuture() {
      return queuedRecordPersistedFuture;
    }
  }

  private static class LeaderQueueNode extends QueueNode {
    private final LeaderProducedRecordContext leaderProducedRecordContext;

    public LeaderQueueNode(
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
        StoreIngestionTask ingestionTask,
        String kafkaUrl,
        long beforeProcessingRecordTimestamp,
        LeaderProducedRecordContext leaderProducedRecordContext) {
      super(consumerRecord, ingestionTask, kafkaUrl, beforeProcessingRecordTimestamp);
      this.leaderProducedRecordContext = leaderProducedRecordContext;
    }

    @Override
    public LeaderProducedRecordContext getLeaderProducedRecordContext() {
      return this.leaderProducedRecordContext;
    }
  }

  /**
   * Worker thread, which will invoke {@link StoreIngestionTask#processConsumerRecord(ConsumerRecord, LeaderProducedRecordContext, String, long)} to process
   * each {@link ConsumerRecord} buffered in {@link BlockingQueue}.
   */
  private static class StoreBufferDrainer implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(StoreBufferDrainer.class);
    private final BlockingQueue<QueueNode> blockingQueue;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final int drainerIndex;
    private final ConcurrentMap<TopicPartition, Long> topicToTimeSpent = new ConcurrentHashMap<>();

    public StoreBufferDrainer(BlockingQueue<QueueNode> blockingQueue, int drainerIndex) {
      this.blockingQueue = blockingQueue;
      this.drainerIndex = drainerIndex;
    }

    public void stop() {
      isRunning.set(false);
    }

    @Override
    public void run() {
      LOGGER.info("Starting StoreBufferDrainer Thread for drainer: {}....", drainerIndex);
      QueueNode node = null;
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = null;
      LeaderProducedRecordContext leaderProducedRecordContext = null;
      StoreIngestionTask ingestionTask = null;
      CompletableFuture<Void> recordPersistedFuture = null;
      long beforeProcessingRecordTimestamp = -1L;
      while (isRunning.get()) {
        try {
          node = blockingQueue.take();

          consumerRecord = node.getConsumerRecord();
          leaderProducedRecordContext = node.getLeaderProducedRecordContext();
          ingestionTask = node.getIngestionTask();
          recordPersistedFuture = node.getQueuedRecordPersistedFuture();
          beforeProcessingRecordTimestamp = node.getBeforeProcessingRecordTimestamp();

          long startTime = System.currentTimeMillis();

          ingestionTask.processConsumerRecord(
              consumerRecord,
              leaderProducedRecordContext,
              node.getKafkaUrl(),
              beforeProcessingRecordTimestamp);

          // complete the leaderProducedRecordContext future as processing for this leaderProducedRecordContext is done
          // here.
          if (leaderProducedRecordContext != null) {
            leaderProducedRecordContext.completePersistedToDBFuture(null);
          }

          /**
           * Complete {@link QueueNode#queuedRecordPersistedFuture} since the processing for the current record is done.
           */
          if (recordPersistedFuture != null) {
            recordPersistedFuture.complete(null);
          }

          TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
          topicToTimeSpent
              .compute(topicPartition, (K, V) -> (V == null ? 0 : V) + System.currentTimeMillis() - startTime);
        } catch (Throwable e) {
          if (e instanceof InterruptedException) {
            LOGGER.error("Drainer {} received InterruptedException, will exit", drainerIndex);
            break;
          }
          StringBuilder logBuilder = new StringBuilder().append("Drainer ").append(drainerIndex);
          if (consumerRecord == null) {
            logBuilder.append(" received throwable: ");
          } else {
            String consumerRecordString = consumerRecord.toString();
            if (consumerRecordString.length() > 1024) {
              // Careful not to flood the logs with too much content...
              consumerRecordString = consumerRecordString.substring(0, 1024);
              logBuilder
                  .append(" received throwable while processing consumer record (truncated at 1024 characters): ");
            } else {
              logBuilder.append(" received throwable while processing consumer record: ");
            }
            logBuilder.append(consumerRecordString);
          }
          LOGGER.error(logBuilder.toString(), e);

          /**
           * Catch all the thrown exception and store it in {@link StoreIngestionTask#lastWorkerException}.
           */
          if (e instanceof Exception) {
            Exception processConsumerRecordException = (Exception) e;
            if (ingestionTask != null) {
              try {
                ingestionTask.offerDrainerException(processConsumerRecordException, consumerRecord.partition());
              } catch (VeniceException offerToQueueException) {
                ingestionTask.setLastStoreIngestionException(offerToQueueException);
              }
              if (e instanceof VeniceChecksumException) {
                ingestionTask.recordChecksumVerificationFailure();
              }
            }
            if (leaderProducedRecordContext != null) {
              leaderProducedRecordContext.completePersistedToDBFuture(processConsumerRecordException);
            }
            if (recordPersistedFuture != null) {
              recordPersistedFuture.completeExceptionally(processConsumerRecordException);
            }
          } else {
            break;
          }
        }
      }
      LOGGER.info("Current StoreBufferDrainer {} stopped", drainerIndex);
    }
  }

  private static final Logger LOGGER = LogManager.getLogger(StoreBufferService.class);
  private final int drainerNum;
  private final ArrayList<MemoryBoundBlockingQueue<QueueNode>> blockingQueueArr;
  private ExecutorService executorService;
  private final List<StoreBufferDrainer> drainerList = new ArrayList<>();
  private final long bufferCapacityPerDrainer;

  public StoreBufferService(int drainerNum, long bufferCapacityPerDrainer, long bufferNotifyDelta) {
    this.drainerNum = drainerNum;
    this.blockingQueueArr = new ArrayList<>();
    this.bufferCapacityPerDrainer = bufferCapacityPerDrainer;
    for (int cur = 0; cur < drainerNum; ++cur) {
      this.blockingQueueArr.add(new MemoryBoundBlockingQueue<>(bufferCapacityPerDrainer, bufferNotifyDelta));
    }
  }

  protected int getDrainerIndexForConsumerRecord(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      int subPartition) {
    String topic = consumerRecord.topic();
    /**
     * This will guarantee that 'topicHash' will be a positive integer, whose maximum value is
     * {@link Integer.MAX_VALUE} / 2 + 1, which could make sure 'topicHash + consumerRecord.partition()' should be
     * positive for most of time to guarantee even partition assignment.
     */
    int topicHash = Math.abs(topic.hashCode() / 2);
    return Math.abs((topicHash + subPartition) % this.drainerNum);
  }

  @Override
  public void putConsumerRecord(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestamp) throws InterruptedException {
    int drainerIndex = getDrainerIndexForConsumerRecord(consumerRecord, subPartition);
    MemoryBoundBlockingQueue<QueueNode> queue = blockingQueueArr.get(drainerIndex);
    if (leaderProducedRecordContext == null) {
      /**
       * The last queued record persisted future will only be setup when {@param leaderProducedRecordContext} is 'null',
       * since {@link LeaderProducedRecordContext#persistedToDBFuture} is a superset of this, which is tracking the
       * end-to-end completeness when producing to local Kafka is needed.
       */
      CompletableFuture<Void> recordFuture = new CompletableFuture<>();
      queue.put(
          new FollowerQueueNode(
              consumerRecord,
              ingestionTask,
              kafkaUrl,
              beforeProcessingRecordTimestamp,
              recordFuture));

      // Setup the last queued record's future
      PartitionConsumptionState partitionConsumptionState =
          ingestionTask.getPartitionConsumptionState(consumerRecord.partition());
      if (partitionConsumptionState != null) {
        partitionConsumptionState.setLastQueuedRecordPersistedFuture(recordFuture);
      }
    } else {
      queue.put(
          new LeaderQueueNode(
              consumerRecord,
              ingestionTask,
              kafkaUrl,
              beforeProcessingRecordTimestamp,
              leaderProducedRecordContext));
    }
  }

  /**
   * This function is used to drain all the records for the specified topic + partition.
   * The reason is that we don't want overlap Kafka messages between two different subscriptions,
   * which could introduce complicate dependencies in {@link StoreIngestionTask}.
   * @param topic
   * @param partition
   * @throws InterruptedException
   */
  public void drainBufferedRecordsFromTopicPartition(String topic, int partition) throws InterruptedException {
    int retryNum = 1000;
    int sleepIntervalInMS = 50;
    internalDrainBufferedRecordsFromTopicPartition(topic, partition, retryNum, sleepIntervalInMS);
  }

  protected void internalDrainBufferedRecordsFromTopicPartition(
      String topic,
      int partition,
      int retryNum,
      int sleepIntervalInMS) throws InterruptedException {
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> fakeRecord = new ConsumerRecord<>(topic, partition, -1, null, null);
    int workerIndex = getDrainerIndexForConsumerRecord(fakeRecord, partition);
    BlockingQueue<QueueNode> blockingQueue = blockingQueueArr.get(workerIndex);
    if (!drainerList.get(workerIndex).isRunning.get()) {
      throw new VeniceException(
          "Drainer thread " + workerIndex + " has stopped running, cannot drain the topic " + topic);
    }

    QueueNode fakeNode = new QueueNode(fakeRecord, null, "dummyKafkaUrl", 0);

    int cur = 0;
    while (cur++ < retryNum) {
      if (!blockingQueue.contains(fakeNode)) {
        LOGGER.info(
            "The blocking queue of store writer thread: {} doesn't contain any record for topic: {} partition: {}",
            workerIndex,
            topic,
            partition);
        return;
      }
      Thread.sleep(sleepIntervalInMS);
    }
    String errorMessage = "There are still some records left in the blocking queue of store writer thread: "
        + workerIndex + " for topic: " + topic + " partition after retry for " + retryNum + " times";
    LOGGER.error(errorMessage);
    throw new VeniceException(errorMessage);
  }

  @Override
  public boolean startInner() {
    this.executorService = Executors.newFixedThreadPool(drainerNum, new DaemonThreadFactory("Store-writer"));

    // Submit all the buffer drainers
    for (int cur = 0; cur < drainerNum; ++cur) {
      StoreBufferDrainer drainer = new StoreBufferDrainer(this.blockingQueueArr.get(cur), cur);
      this.executorService.submit(drainer);
      drainerList.add(drainer);
    }
    this.executorService.shutdown();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    // Graceful shutdown
    drainerList.forEach(drainer -> drainer.stop());
    if (this.executorService != null) {
      this.executorService.shutdownNow();
      this.executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Override
  public int getDrainerCount() {
    return blockingQueueArr.size();
  }

  @Override
  public long getDrainerQueueMemoryUsage(int index) {
    return blockingQueueArr.get(index).getMemoryUsage();
  }

  @Override
  public long getTotalMemoryUsage() {
    long totalUsage = 0;
    for (MemoryBoundBlockingQueue<QueueNode> queue: blockingQueueArr) {
      totalUsage += queue.getMemoryUsage();
    }
    return totalUsage;
  }

  @Override
  public long getTotalRemainingMemory() {
    long totalRemaining = 0;
    for (MemoryBoundBlockingQueue<QueueNode> queue: blockingQueueArr) {
      totalRemaining += queue.remainingMemoryCapacityInByte();
    }
    return totalRemaining;
  }

  @Override
  public long getMaxMemoryUsagePerDrainer() {
    long maxUsage = 0;
    boolean slowDrainerExists = false;

    for (MemoryBoundBlockingQueue<QueueNode> queue: blockingQueueArr) {
      maxUsage = Math.max(maxUsage, queue.getMemoryUsage());
      if (queue.getMemoryUsage() > 0.8 * bufferCapacityPerDrainer) {
        slowDrainerExists = true;
      }
    }

    for (int index = 0; index < blockingQueueArr.size(); index++) {
      StoreBufferDrainer drainer = drainerList.get(index);
      // print drainer info when there is a slow drainer.
      if (slowDrainerExists) {
        MemoryBoundBlockingQueue<QueueNode> queue = blockingQueueArr.get(index);
        int count = queue.getMemoryUsage() > 0.8 * bufferCapacityPerDrainer ? 5 : 1;
        List<Map.Entry<TopicPartition, Long>> slowestEntries = drainer.topicToTimeSpent.entrySet()
            .stream()
            .sorted(comparing(Map.Entry::getValue, reverseOrder()))
            .limit(count)
            .collect(toList());

        int finalIndex = index;
        slowestEntries.forEach(
            entry -> LOGGER.info(
                "In drainer number {}, time spent on topic {}, partition {} : {} ms",
                finalIndex,
                entry.getKey().topic(),
                entry.getKey().partition(),
                entry.getValue()));
        LOGGER.info(
            "Drainer number {} hosting {} partitions, has memory usage of {}",
            index,
            drainer.topicToTimeSpent.size(),
            queue.getMemoryUsage());
      }
      drainer.topicToTimeSpent.clear();
    }

    return maxUsage;
  }

  /** Used for testing */
  Map<TopicPartition, Long> getTopicToTimeSpentMap(int i) {
    return drainerList.get(i).topicToTimeSpent;
  }

  @Override
  public long getMinMemoryUsagePerDrainer() {
    long minUsage = Long.MAX_VALUE;
    for (MemoryBoundBlockingQueue<QueueNode> queue: blockingQueueArr) {
      minUsage = Math.min(minUsage, queue.getMemoryUsage());
    }
    return minUsage;
  }
}
