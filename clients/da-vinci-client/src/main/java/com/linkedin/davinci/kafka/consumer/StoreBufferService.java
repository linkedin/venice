package com.linkedin.davinci.kafka.consumer;

import static java.util.Collections.reverseOrder;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

import com.linkedin.venice.common.Measurable;
import com.linkedin.venice.exceptions.VeniceChecksumException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.PartitionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * 4. The logic to assign topic+partition to drainer, please check {@link #getDrainerIndexForConsumerRecord(PubSubMessage, int)};
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
    private final PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord;
    private final StoreIngestionTask ingestionTask;
    private final String kafkaUrl;
    private final long beforeProcessingRecordTimestampNs;

    public QueueNode(
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
        StoreIngestionTask ingestionTask,
        String kafkaUrl,
        long beforeProcessingRecordTimestampNs) {
      this.consumerRecord = consumerRecord;
      this.ingestionTask = ingestionTask;
      this.kafkaUrl = kafkaUrl;
      this.beforeProcessingRecordTimestampNs = beforeProcessingRecordTimestampNs;
    }

    public PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> getConsumerRecord() {
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

    public long getBeforeProcessingRecordTimestampNs() {
      return this.beforeProcessingRecordTimestampNs;
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
      return this.consumerRecord.getTopicPartition().equals(node.consumerRecord.getTopicPartition());
    }

    @Override
    public int hashCode() {
      return consumerRecord.hashCode();
    }

    @Override
    public int getSize() {
      // N.B.: This is just an estimate. TODO: Consider if it is really useful, and whether to get rid of it.
      return this.consumerRecord.getKey().getEstimatedObjectSizeOnHeap()
          + getEstimateOfMessageEnvelopeSizeOnHeap(this.consumerRecord.getValue()) + QUEUE_NODE_OVERHEAD_IN_BYTE;
    }

    private int getEstimateOfMessageEnvelopeSizeOnHeap(KafkaMessageEnvelope messageEnvelope) {
      int kmeBaseOverhead = 100; // Super rough estimate. TODO: Measure with a more precise library and store statically
      switch (MessageType.valueOf(messageEnvelope)) {
        case PUT:
          Put put = (Put) messageEnvelope.payloadUnion;
          int size = put.putValue.capacity();
          if (put.replicationMetadataPayload != null
              /**
               * N.B.: When using the {@link org.apache.avro.io.OptimizedBinaryDecoder}, the {@link put.putValue} and the
               *       {@link put.replicationMetadataPayload} will be backed by the same underlying array. If that is the
               *       case, then we don't want to account for the capacity twice.
               */
              && put.replicationMetadataPayload.array() != put.putValue.array()) {
            size += put.replicationMetadataPayload.capacity();
          }
          return size + kmeBaseOverhead;
        case UPDATE:
          Update update = (Update) messageEnvelope.payloadUnion;
          return update.updateValue.capacity() + kmeBaseOverhead;
        default:
          return kmeBaseOverhead;
      }
    }

    @Override
    public String toString() {
      return this.consumerRecord.toString();
    }
  }

  private static class FollowerQueueNode extends QueueNode {
    private final CompletableFuture<Void> queuedRecordPersistedFuture;

    public FollowerQueueNode(
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
        StoreIngestionTask ingestionTask,
        String kafkaUrl,
        long beforeProcessingRecordTimestampNs,
        CompletableFuture<Void> queuedRecordPersistedFuture) {
      super(consumerRecord, ingestionTask, kafkaUrl, beforeProcessingRecordTimestampNs);
      this.queuedRecordPersistedFuture = queuedRecordPersistedFuture;
    }

    @Override
    public CompletableFuture<Void> getQueuedRecordPersistedFuture() {
      return queuedRecordPersistedFuture;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }
  }

  private static class LeaderQueueNode extends QueueNode {
    private final LeaderProducedRecordContext leaderProducedRecordContext;

    public LeaderQueueNode(
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
        StoreIngestionTask ingestionTask,
        String kafkaUrl,
        long beforeProcessingRecordTimestampNs,
        LeaderProducedRecordContext leaderProducedRecordContext) {
      super(consumerRecord, ingestionTask, kafkaUrl, beforeProcessingRecordTimestampNs);
      this.leaderProducedRecordContext = leaderProducedRecordContext;
    }

    @Override
    public LeaderProducedRecordContext getLeaderProducedRecordContext() {
      return this.leaderProducedRecordContext;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }
  }

  /**
   * Worker thread, which will invoke {@link StoreIngestionTask#processConsumerRecord}
   * to process each {@link PubSubMessage} buffered in {@link BlockingQueue}.
   */
  private static class StoreBufferDrainer implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(StoreBufferDrainer.class);
    private final BlockingQueue<QueueNode> blockingQueue;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final int drainerIndex;
    private final ConcurrentMap<PubSubTopicPartition, Long> topicToTimeSpent = new ConcurrentHashMap<>();

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
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord = null;
      LeaderProducedRecordContext leaderProducedRecordContext = null;
      StoreIngestionTask ingestionTask = null;
      CompletableFuture<Void> recordPersistedFuture = null;
      while (isRunning.get()) {
        try {
          node = blockingQueue.take();

          consumerRecord = node.getConsumerRecord();
          leaderProducedRecordContext = node.getLeaderProducedRecordContext();
          ingestionTask = node.getIngestionTask();
          recordPersistedFuture = node.getQueuedRecordPersistedFuture();

          long startTime = System.currentTimeMillis();

          int subPartition = PartitionUtils
              .getSubPartition(consumerRecord.getTopicPartition(), ingestionTask.getAmplificationFactor());

          processRecord(
              consumerRecord,
              ingestionTask,
              leaderProducedRecordContext,
              subPartition,
              node.getKafkaUrl(),
              node.getBeforeProcessingRecordTimestampNs());

          /**
           * Complete {@link QueueNode#queuedRecordPersistedFuture} since the processing for the current record is done.
           */
          if (recordPersistedFuture != null) {
            recordPersistedFuture.complete(null);
          }

          topicToTimeSpent.compute(
              consumerRecord.getTopicPartition(),
              (K, V) -> (V == null ? 0 : V) + System.currentTimeMillis() - startTime);
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
                ingestionTask.setIngestionException(
                    consumerRecord.getTopicPartition().getPartitionNumber(),
                    processConsumerRecordException);
              } catch (VeniceException ingestionException) {
                ingestionTask.setLastStoreIngestionException(ingestionException);
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

  private final RecordHandler leaderRecordHandler;

  public StoreBufferService(
      int drainerNum,
      long bufferCapacityPerDrainer,
      long bufferNotifyDelta,
      boolean queueLeaderWrites) {
    this.drainerNum = drainerNum;
    this.blockingQueueArr = new ArrayList<>();
    this.bufferCapacityPerDrainer = bufferCapacityPerDrainer;
    for (int cur = 0; cur < drainerNum; ++cur) {
      this.blockingQueueArr.add(new MemoryBoundBlockingQueue<>(bufferCapacityPerDrainer, bufferNotifyDelta));
    }
    this.leaderRecordHandler = queueLeaderWrites ? this::queueLeaderRecord : StoreBufferService::processRecord;
  }

  protected MemoryBoundBlockingQueue<QueueNode> getDrainerForConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      int subPartition) {
    int drainerIndex = getDrainerIndexForConsumerRecord(consumerRecord, subPartition);
    return blockingQueueArr.get(drainerIndex);
  }

  protected int getDrainerIndexForConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      int subPartition) {
    /**
     * This will guarantee that 'topicHash' will be a positive integer, whose maximum value is
     * {@link Integer.MAX_VALUE} / 2 + 1, which could make sure 'topicHash + consumerRecord.partition()' should be
     * positive for most of time to guarantee even partition assignment.
     */
    int topicHash = Math.abs(consumerRecord.getTopicPartition().getPubSubTopic().hashCode() / 2);
    return Math.abs((topicHash + subPartition) % this.drainerNum);
  }

  @Override
  public void putConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) throws InterruptedException {
    if (leaderProducedRecordContext == null) {
      /**
       * The last queued record persisted future will only be setup when {@param leaderProducedRecordContext} is 'null',
       * since {@link LeaderProducedRecordContext#persistedToDBFuture} is a superset of this, which is tracking the
       * end-to-end completeness when producing to local Kafka is needed.
       */
      CompletableFuture<Void> recordFuture = new CompletableFuture<>();
      getDrainerForConsumerRecord(consumerRecord, subPartition).put(
          new FollowerQueueNode(
              consumerRecord,
              ingestionTask,
              kafkaUrl,
              beforeProcessingRecordTimestampNs,
              recordFuture));

      // Setup the last queued record's future
      PartitionConsumptionState partitionConsumptionState =
          ingestionTask.getPartitionConsumptionState(consumerRecord.getTopicPartition().getPartitionNumber());
      if (partitionConsumptionState != null) {
        partitionConsumptionState.setLastQueuedRecordPersistedFuture(recordFuture);
      }
    } else {
      leaderRecordHandler.handle(
          consumerRecord,
          ingestionTask,
          leaderProducedRecordContext,
          subPartition,
          kafkaUrl,
          beforeProcessingRecordTimestampNs);
    }
  }

  private interface RecordHandler {
    void handle(
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
        StoreIngestionTask ingestionTask,
        LeaderProducedRecordContext leaderProducedRecordContext,
        int subPartition,
        String kafkaUrl,
        long beforeProcessingRecordTimestamp) throws InterruptedException;
  }

  private void queueLeaderRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestamp) throws InterruptedException {
    getDrainerForConsumerRecord(consumerRecord, subPartition).put(
        new LeaderQueueNode(
            consumerRecord,
            ingestionTask,
            kafkaUrl,
            beforeProcessingRecordTimestamp,
            leaderProducedRecordContext));
  }

  private static void processRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) throws InterruptedException {
    ingestionTask.processConsumerRecord(
        consumerRecord,
        leaderProducedRecordContext,
        subPartition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);

    // complete the leaderProducedRecordContext future as processing for this leaderProducedRecordContext is done here.
    if (leaderProducedRecordContext != null) {
      leaderProducedRecordContext.completePersistedToDBFuture(null);
    }
  }

  /**
   * This function is used to drain all the records for the specified topic + partition.
   * The reason is that we don't want overlap Kafka messages between two different subscriptions,
   * which could introduce complicate dependencies in {@link StoreIngestionTask}.
   * @param topicPartition for which to drain buffer
   * @throws InterruptedException
   */
  public void drainBufferedRecordsFromTopicPartition(PubSubTopicPartition topicPartition) throws InterruptedException {
    int retryNum = 1000;
    int sleepIntervalInMS = 50;
    internalDrainBufferedRecordsFromTopicPartition(topicPartition, retryNum, sleepIntervalInMS);
  }

  protected void internalDrainBufferedRecordsFromTopicPartition(
      PubSubTopicPartition topicPartition,
      int retryNum,
      int sleepIntervalInMS) throws InterruptedException {
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> fakeRecord = new FakePubSubMessage(topicPartition);
    int workerIndex = getDrainerIndexForConsumerRecord(fakeRecord, topicPartition.getPartitionNumber());
    BlockingQueue<QueueNode> blockingQueue = blockingQueueArr.get(workerIndex);
    if (!drainerList.get(workerIndex).isRunning.get()) {
      throw new VeniceException(
          "Drainer thread " + workerIndex + " has stopped running, cannot drain the topic "
              + topicPartition.getPubSubTopic().getName());
    }

    QueueNode fakeNode = new QueueNode(fakeRecord, null, "dummyKafkaUrl", 0);

    int cur = 0;
    while (cur++ < retryNum) {
      if (!blockingQueue.contains(fakeNode)) {
        LOGGER.info(
            "The blocking queue of store writer thread: {} doesn't contain any record for: {}",
            workerIndex,
            topicPartition);
        return;
      }
      Thread.sleep(sleepIntervalInMS);
    }
    String errorMessage = "There are still some records left in the blocking queue of store writer thread: "
        + workerIndex + " for topic: " + topicPartition.getPubSubTopic().getName() + " partition after retry for "
        + retryNum + " times";
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
        List<Map.Entry<PubSubTopicPartition, Long>> slowestEntries = drainer.topicToTimeSpent.entrySet()
            .stream()
            .sorted(comparing(Map.Entry::getValue, reverseOrder()))
            .limit(count)
            .collect(toList());

        int finalIndex = index;
        slowestEntries.forEach(
            entry -> LOGGER
                .info("In drainer number {}, time spent on {} : {} ms", finalIndex, entry.getKey(), entry.getValue()));
      }
      drainer.topicToTimeSpent.clear();
    }

    return maxUsage;
  }

  /** Used for testing */
  Map<PubSubTopicPartition, Long> getTopicToTimeSpentMap(int i) {
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

  private static class FakePubSubMessage implements PubSubMessage {
    private final PubSubTopicPartition topicPartition;

    FakePubSubMessage(PubSubTopicPartition topicPartition) {
      this.topicPartition = Objects.requireNonNull(topicPartition);
    }

    @Override
    public Object getKey() {
      return null;
    }

    @Override
    public Object getValue() {
      return null;
    }

    @Override
    public PubSubTopicPartition getTopicPartition() {
      return topicPartition;
    }

    @Override
    public Object getOffset() {
      return null;
    }

    @Override
    public long getPubSubMessageTime() {
      return 0;
    }

    @Override
    public int getPayloadSize() {
      return 0;
    }
  }
}
