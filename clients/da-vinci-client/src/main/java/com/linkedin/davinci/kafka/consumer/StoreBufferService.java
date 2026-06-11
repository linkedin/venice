package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;
import static java.util.Collections.reverseOrder;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.linkedin.davinci.stats.StoreBufferServiceStats;
import com.linkedin.davinci.utils.LockAssistedCompletableFuture;
import com.linkedin.davinci.validation.PartitionTracker;
import com.linkedin.venice.exceptions.VeniceChecksumException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.collections.MemoryBoundBlockingQueue;
import io.tehuti.metrics.MetricsRepository;
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
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is serving as a {@link PubSubMessage} buffer with an accompanying pool of drainer threads. The drainers
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
  private static final Logger LOGGER = LogManager.getLogger(StoreBufferService.class);
  private final int drainerNum;
  private final ArrayList<MemoryBoundBlockingQueue<QueueNode>> blockingQueueArr;
  private ExecutorService executorService;
  private final List<StoreBufferDrainer> drainerList = new ArrayList<>();
  private final long bufferCapacityPerDrainer;

  private final RecordHandler leaderRecordHandler;
  private final StoreBufferServiceStats storeBufferServiceStats;
  private final LoadingCache<PubSubTopic, Integer> hashCodeCache;

  private final boolean isSorted;

  private volatile boolean isStarted = false;
  private final LogContext logContext;

  public StoreBufferService(
      int drainerNum,
      long bufferCapacityPerDrainer,
      long bufferNotifyDelta,
      boolean queueLeaderWrites,
      LogContext logContext,
      MetricsRepository metricsRepository,
      boolean sorted,
      String clusterName) {
    this(
        drainerNum,
        bufferCapacityPerDrainer,
        bufferNotifyDelta,
        queueLeaderWrites,
        null,
        logContext,
        metricsRepository,
        sorted,
        clusterName);
  }

  /**
   * Package-private constructor for testing
   */
  StoreBufferService(
      int drainerNum,
      long bufferCapacityPerDrainer,
      long bufferNotifyDelta,
      boolean queueLeaderWrites,
      StoreBufferServiceStats stats,
      LogContext logContext) {
    this(
        drainerNum,
        bufferCapacityPerDrainer,
        bufferNotifyDelta,
        queueLeaderWrites,
        stats,
        logContext,
        null,
        true,
        null);
  }

  /**
   * Shared code for the main and test constructors.
   *
   * N.B.: Either {@param stats} or {@param metricsRepository} should be null, but not both. If neither are null, then
   * we default to the main code's expected path, meaning that the metric repo will be used to construct a
   * {@link StoreBufferServiceStats} instance, and the passed in stats object will be ignored.
   */
  private StoreBufferService(
      int drainerNum,
      long bufferCapacityPerDrainer,
      long bufferNotifyDelta,
      boolean queueLeaderWrites,
      StoreBufferServiceStats stats,
      LogContext logContext,
      MetricsRepository metricsRepository,
      boolean sorted,
      String clusterName) {
    this.logContext = logContext;
    this.drainerNum = drainerNum;
    this.blockingQueueArr = new ArrayList<>();
    this.bufferCapacityPerDrainer = bufferCapacityPerDrainer;
    for (int cur = 0; cur < drainerNum; ++cur) {
      this.blockingQueueArr.add(new MemoryBoundBlockingQueue<>(bufferCapacityPerDrainer, bufferNotifyDelta));
    }
    this.isSorted = sorted;
    this.leaderRecordHandler = queueLeaderWrites ? this::queueLeaderRecord : StoreBufferService::processRecord;
    this.storeBufferServiceStats = metricsRepository == null
        ? Objects.requireNonNull(stats)
        : new StoreBufferServiceStats(
            Objects.requireNonNull(metricsRepository),
            sorted ? "StoreBufferServiceSorted" : "StoreBufferServiceUnsorted",
            clusterName,
            sorted,
            this::getTotalMemoryUsage,
            this::getTotalRemainingMemory,
            this::getMaxMemoryUsagePerDrainer,
            this::getMinMemoryUsagePerDrainer);
    /*
     * {@link #getDrainerIndexForConsumerRecord} hashes the topic name and partition to determine a drainer. Due to the
     * different naming conventions for RT (_rt) and Separate RT (_rt_sep), different drainers might be assigned while
     * the same drainer handling both topics would help with concurrency. Normalizing the topic name fixes this issue.
     */
    this.hashCodeCache = Caffeine.newBuilder().maximumSize(2000).build(Utils::calculateTopicHashCode);
  }

  protected MemoryBoundBlockingQueue<QueueNode> getDrainerForConsumerRecord(
      DefaultPubSubMessage consumerRecord,
      int partition) {
    int drainerIndex = getDrainerIndexForConsumerRecord(consumerRecord, partition);
    return blockingQueueArr.get(drainerIndex);
  }

  protected int getDrainerIndexForConsumerRecord(DefaultPubSubMessage consumerRecord, int partition) {
    /**
     * This will guarantee that 'topicHash' will be a positive integer, whose maximum value is
     * {@link Integer.MAX_VALUE} / 2 + 1, which could make sure 'topicHash + consumerRecord.partition()' should be
     * positive for most time to guarantee even partition assignment.
     */
    Integer topicHashCode = hashCodeCache.get(consumerRecord.getTopicPartition().getPubSubTopic());
    if (topicHashCode == null) { // this should never happen, but FindBugs linting needs to be soothed
      topicHashCode = Utils.calculateTopicHashCode(consumerRecord.getTopicPartition().getPubSubTopic());
    }
    int topicHash = Math.abs(topicHashCode / 2);
    return Math.abs((topicHash + partition) % this.drainerNum);
  }

  @Override
  public void putConsumerRecord(
      DefaultPubSubMessage consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) throws InterruptedException {
    if (leaderProducedRecordContext == null) {
      /**
       * The last queued record persisted future will only be setup when {@param leaderProducedRecordContext} is 'null',
       * since {@link LeaderProducedRecordContext#persistedToDBFuture} is a superset of this, which is tracking the
       * end-to-end completeness when producing to local Kafka is needed.
       */
      CompletableFuture<Void> recordFuture = new CompletableFuture<>();
      getDrainerForConsumerRecord(consumerRecord, partition).put(
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
          partition,
          kafkaUrl,
          beforeProcessingRecordTimestampNs);
    }
  }

  private interface RecordHandler {
    void handle(
        DefaultPubSubMessage consumerRecord,
        StoreIngestionTask ingestionTask,
        LeaderProducedRecordContext leaderProducedRecordContext,
        int partition,
        String kafkaUrl,
        long beforeProcessingRecordTimestamp) throws InterruptedException;
  }

  private void queueLeaderRecord(
      DefaultPubSubMessage consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestamp) throws InterruptedException {
    getDrainerForConsumerRecord(consumerRecord, partition).put(
        new LeaderQueueNode(
            consumerRecord,
            ingestionTask,
            kafkaUrl,
            beforeProcessingRecordTimestamp,
            leaderProducedRecordContext));
  }

  private static void processRecord(
      DefaultPubSubMessage consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    ingestionTask.processConsumerRecord(
        consumerRecord,
        leaderProducedRecordContext,
        partition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);

    // complete the leaderProducedRecordContext future as processing for this leaderProducedRecordContext is done here.
    if (leaderProducedRecordContext != null) {
      leaderProducedRecordContext.completePersistedToDBFuture(null);
    }
  }

  private static void processCommand(
      CommandQueueNode cmd,
      StoreIngestionTask ingestionTask,
      PartitionConsumptionState pcs) {
    // We only support SYNC_OFFSET command for now.
    if (cmd.getCommandType() != CommandQueueNode.CommandType.SYNC_OFFSET) {
      throw new VeniceException("Unsupported command type: " + cmd.getCommandType());
    }

    cmd.executeGuarded(() -> {
      if (pcs == null) {
        LOGGER.warn(
            "PCS for topic-partition: {} is null. Skipping {} command in StoreBufferDrainer.",
            cmd.getConsumerRecord().getTopicPartition(),
            cmd.getCommandType());
      } else {
        ingestionTask.updateOffsetMetadataAndSyncOffset(pcs);
      }
    });
  }

  /**
   * This function is used to drain all the records for the specified topic + partition.
   * The reason is that we don't want overlap Kafka messages between two different subscriptions,
   * which could introduce complicate dependencies in {@link StoreIngestionTask}.
   * @param topicPartition for which to drain buffer
   * @throws InterruptedException
   */
  @Override
  public void drainBufferedRecordsFromTopicPartition(PubSubTopicPartition topicPartition, long timeoutMs)
      throws InterruptedException {
    DefaultPubSubMessage fakeRecord = new FakePubSubMessage(topicPartition);
    int workerIndex = getDrainerIndexForConsumerRecord(fakeRecord, topicPartition.getPartitionNumber());
    BlockingQueue<QueueNode> blockingQueue = blockingQueueArr.get(workerIndex);
    if (!drainerList.get(workerIndex).isRunning.get()) {
      throw new VeniceException(
          "Drainer thread " + workerIndex + " has stopped running, cannot drain the topic "
              + topicPartition.getPubSubTopic().getName());
    }

    QueueNode fakeNode = new QueueNode(fakeRecord, null, "dummyKafkaUrl", 0);
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (!blockingQueue.contains(fakeNode)) {
        LOGGER.info(
            "The blocking queue of store writer thread: {} doesn't contain any record for: {}",
            workerIndex,
            topicPartition);
        return;
      }
      Thread.sleep(50);
    }
    String errorMessage = "Drainer queue for " + topicPartition + " on writer thread " + workerIndex
        + " not fully drained after " + timeoutMs + "ms";
    LOGGER.error(errorMessage);
    throw new VeniceException(errorMessage);
  }

  @Override
  public CompletableFuture<Void> execSyncOffsetCommandAsync(
      PubSubTopicPartition topicPartition,
      StoreIngestionTask ingestionTask) throws InterruptedException {
    DefaultPubSubMessage fakeRecord = new FakePubSubMessage(topicPartition);
    CommandQueueNode syncOffsetCmd =
        new CommandQueueNode(CommandQueueNode.CommandType.SYNC_OFFSET, fakeRecord, ingestionTask);
    getDrainerForConsumerRecord(fakeRecord, topicPartition.getPartitionNumber()).put(syncOffsetCmd);
    return syncOffsetCmd.getExecutedFuture();
  }

  /**
   * Enqueues a waitable {@link SyncGlobalRtDivNode}. The drainer snapshots the VT DIV and syncs it to the OffsetRecord
   * (see {@link StoreIngestionTask#syncGlobalRtDivFromSnapshot}). Unlike the fire-and-forget {@link SyncVtDivNode}, the
   * returned future lets the graceful-shutdown path await completion deterministically.
   */
  @Override
  public CompletableFuture<Void> execSyncGlobalRtDivAsync(
      PubSubTopicPartition topicPartition,
      StoreIngestionTask ingestionTask) throws InterruptedException {
    DefaultPubSubMessage fakeRecord = new FakePubSubMessage(topicPartition);
    SyncGlobalRtDivNode syncGlobalRtDivNode = new SyncGlobalRtDivNode(fakeRecord, ingestionTask);
    getDrainerForConsumerRecord(fakeRecord, topicPartition.getPartitionNumber()).put(syncGlobalRtDivNode);
    return syncGlobalRtDivNode.getExecutedFuture();
  }

  /**
   * lastRecordPersistedFuture indicates whether the last record was persisted successfully and will have two sources.
   * 1. From the follower code path, it is lastQueuedRecordPersistedFuture from the PCS set in putConsumeRecord().
   * 2. From the leader side, it is the persistedToDBFuture from the LeaderProducedRecordContext from when the
   *    LeaderProducerCallback was created.
   *
   * <p>Returns the node's completion future. Steady-state callers ignore it (fire-and-forget); the graceful-shutdown
   * leader path awaits it so the aggregate shutdown future deterministically covers this VT DIV sync without enqueuing a
   * second redundant {@link SyncGlobalRtDivNode}.
   */
  @Override
  public CompletableFuture<Void> execSyncOffsetFromSnapshotAsync(
      PubSubTopicPartition topicPartition,
      PartitionTracker vtDivSnapshot,
      CompletableFuture<Void> lastRecordPersistedFuture,
      StoreIngestionTask ingestionTask) throws InterruptedException {
    DefaultPubSubMessage fakeRecord = new FakePubSubMessage(topicPartition);
    SyncVtDivNode syncDivNode = new SyncVtDivNode(fakeRecord, vtDivSnapshot, lastRecordPersistedFuture, ingestionTask);
    getDrainerForConsumerRecord(fakeRecord, topicPartition.getPartitionNumber()).put(syncDivNode);
    return syncDivNode.getExecutedFuture();
  }

  @Override
  public boolean startInner() {
    this.executorService = Executors.newFixedThreadPool(
        drainerNum,
        new DaemonThreadFactory(isSorted ? "Store-writer-sorted" : "Store-writer-hybrid", logContext));

    // Submit all the buffer drainers
    for (int cur = 0; cur < drainerNum; ++cur) {
      StoreBufferDrainer drainer = new StoreBufferDrainer(this.blockingQueueArr.get(cur), cur, storeBufferServiceStats);
      this.executorService.submit(drainer);
      drainerList.add(drainer);
    }
    this.executorService.shutdown();
    isStarted = true;
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    // Graceful shutdown
    isStarted = false;
    drainerList.forEach(drainer -> drainer.stop());
    if (this.executorService != null) {
      this.executorService.shutdownNow();
      this.executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
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

    if (!isStarted) {
      return maxUsage;
    }

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

  /**
   * Queue node type in {@link BlockingQueue} of each drainer thread.
   */
  static class QueueNode implements Measurable {
    private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(QueueNode.class);
    private final DefaultPubSubMessage consumerRecord;
    private final StoreIngestionTask ingestionTask;
    private final String kafkaUrl;
    private final long beforeProcessingRecordTimestampNs;

    public QueueNode(
        DefaultPubSubMessage consumerRecord,
        StoreIngestionTask ingestionTask,
        String kafkaUrl,
        long beforeProcessingRecordTimestampNs) {
      this.consumerRecord = consumerRecord;
      this.ingestionTask = ingestionTask;
      this.kafkaUrl = kafkaUrl;
      this.beforeProcessingRecordTimestampNs = beforeProcessingRecordTimestampNs;
    }

    public DefaultPubSubMessage getConsumerRecord() {
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

    protected int getBaseClassOverhead() {
      return SHALLOW_CLASS_OVERHEAD;
    }

    @Override
    public int getHeapSize() {
      /** The other non-primitive fields point to shared instances and are therefore ignored. */
      return getBaseClassOverhead() + consumerRecord.getHeapSize();
    }

    @Override
    public String toString() {
      return this.consumerRecord.toString();
    }
  }

  private static class FollowerQueueNode extends QueueNode {
    /**
     * N.B.: We don't want to recurse fully into the {@link CompletableFuture}, but we do want to take into account an
     * "empty" one.
     */
    private static final int PARTIAL_CLASS_OVERHEAD =
        getClassOverhead(FollowerQueueNode.class) + getClassOverhead(CompletableFuture.class);

    private final CompletableFuture<Void> queuedRecordPersistedFuture;

    public FollowerQueueNode(
        DefaultPubSubMessage consumerRecord,
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

    @Override
    protected int getBaseClassOverhead() {
      return PARTIAL_CLASS_OVERHEAD;
    }
  }

  static class LeaderQueueNode extends QueueNode {
    private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(LeaderQueueNode.class);

    private final LeaderProducedRecordContext leaderProducedRecordContext;

    public LeaderQueueNode(
        DefaultPubSubMessage consumerRecord,
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

    @Override
    protected int getBaseClassOverhead() {
      return SHALLOW_CLASS_OVERHEAD + leaderProducedRecordContext.getHeapSize();
    }
  }

  /**
   * A {@link QueueNode} whose drainer-side execution is awaitable. It exposes a {@link LockAssistedCompletableFuture}
   * that completes once the drainer has run the node's action, guaranteeing that a {@link CompletableFuture#cancel}
   * cannot abort an action that is already executing (cancel and execution synchronize on the same lock).
   */
  private abstract static class WaitableQueueNode extends QueueNode {
    private final LockAssistedCompletableFuture<Void> executedFuture;

    WaitableQueueNode(DefaultPubSubMessage consumerRecord, StoreIngestionTask ingestionTask) {
      super(consumerRecord, ingestionTask, StringUtils.EMPTY, 0);
      this.executedFuture = new LockAssistedCompletableFuture<>(this);
    }

    public CompletableFuture<Void> getExecutedFuture() {
      return executedFuture;
    }

    /**
     * Runs {@code action} under the future's lock (so it cannot race a {@link CompletableFuture#cancel}), completing the
     * future on success or failure. A no-op if the future was already cancelled or completed.
     */
    protected void executeGuarded(Runnable action) {
      synchronized (executedFuture.getLock()) {
        if (executedFuture.isDone() || executedFuture.isCancelled()) {
          LOGGER.warn(
              "Drainer node {} for {} is already done or cancelled",
              getClass().getSimpleName(),
              getConsumerRecord().getTopicPartition());
          return;
        }
        try {
          action.run();
          executedFuture.complete(null);
        } catch (Exception e) {
          executedFuture.completeExceptionally(e);
          LOGGER.error(
              "Drainer node {} for {} failed",
              getClass().getSimpleName(),
              getConsumerRecord().getTopicPartition(),
              e);
        }
      }
    }
  }

  /**
   * Issues a command to the drainer thread. Today the only command is {@link CommandType#SYNC_OFFSET}, the
   * non-Global-RT-DIV graceful-shutdown OffsetRecord sync.
   */
  private static class CommandQueueNode extends WaitableQueueNode {
    /**
     * N.B.: We don't want to recurse fully into the {@link CompletableFuture}, but we do want to take into account an
     * "empty" one.
     */
    private static final int PARTIAL_CLASS_OVERHEAD =
        getClassOverhead(CommandQueueNode.class) + getClassOverhead(LockAssistedCompletableFuture.class);

    enum CommandType {
      // only supports SYNC_OFFSET command today.
      SYNC_OFFSET
    }

    private final CommandType commandType;

    public CommandQueueNode(
        CommandType commandType,
        DefaultPubSubMessage consumerRecord,
        StoreIngestionTask ingestionTask) {
      super(consumerRecord, ingestionTask);
      this.commandType = commandType;
    }

    public CommandType getCommandType() {
      return commandType;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    protected int getBaseClassOverhead() {
      return PARTIAL_CLASS_OVERHEAD;
    }
  }

  /**
   * Waitable drainer node that snapshots the VT DIV in the drainer thread and syncs it to the OffsetRecord (see
   * {@link StoreIngestionTask#syncGlobalRtDivFromSnapshot}). Used by the Global-RT-DIV graceful-shutdown path for
   * followers / leaders with no RT progress. Mirrors the fire-and-forget {@link SyncVtDivNode} but exposes a completion
   * future so the shutdown path can await it.
   */
  private static class SyncGlobalRtDivNode extends WaitableQueueNode {
    private static final int PARTIAL_CLASS_OVERHEAD =
        getClassOverhead(SyncGlobalRtDivNode.class) + getClassOverhead(LockAssistedCompletableFuture.class);

    public SyncGlobalRtDivNode(DefaultPubSubMessage consumerRecord, StoreIngestionTask ingestionTask) {
      super(consumerRecord, ingestionTask);
    }

    public void execute() {
      executeGuarded(() -> getIngestionTask().syncGlobalRtDivFromSnapshot(getConsumerRecord().getTopicPartition()));
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    protected int getBaseClassOverhead() {
      return PARTIAL_CLASS_OVERHEAD;
    }
  }

  /**
   * Allows the ConsumptionTask to command the Drainer to sync the VT DIV to the OffsetRecord. Waitable: the leader
   * graceful-shutdown path ({@link StoreIngestionTask#forceGlobalRtDivSync}) awaits {@link #getExecutedFuture()} on the
   * node enqueued by the leader-produce completion callback, so it does not need to enqueue a second redundant
   * {@link SyncGlobalRtDivNode}. Steady-state callers enqueue it fire-and-forget and ignore the future.
   */
  static class SyncVtDivNode extends WaitableQueueNode {
    private static final int PARTIAL_CLASS_OVERHEAD =
        getClassOverhead(SyncVtDivNode.class) + getClassOverhead(LockAssistedCompletableFuture.class);

    private final PartitionTracker vtDivSnapshot;
    private final CompletableFuture<Void> lastRecordPersistedFuture;

    public SyncVtDivNode(
        DefaultPubSubMessage consumerRecord,
        PartitionTracker vtDivSnapshot,
        CompletableFuture<Void> lastRecordPersistedFuture,
        StoreIngestionTask ingestionTask) {
      super(consumerRecord, ingestionTask);
      this.vtDivSnapshot = vtDivSnapshot;
      this.lastRecordPersistedFuture = lastRecordPersistedFuture;
    }

    public void execute() {
      executeGuarded(() -> {
        if (!lastRecordPersistedFuture.isDone() || lastRecordPersistedFuture.isCompletedExceptionally()) {
          LOGGER.warn(
              "event=globalRtDiv Skipping SyncVtDivNode for {} because preceding record failed (done={} exception={})",
              getConsumerRecord().getTopicPartition(),
              lastRecordPersistedFuture.isDone(),
              lastRecordPersistedFuture.isCompletedExceptionally());
          return;
        }
        getIngestionTask().updateAndSyncOffsetFromSnapshot(vtDivSnapshot, getConsumerRecord().getTopicPartition());
      });
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    protected int getBaseClassOverhead() {
      return PARTIAL_CLASS_OVERHEAD;
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
    private final StoreBufferServiceStats stats;

    public StoreBufferDrainer(BlockingQueue<QueueNode> blockingQueue, int drainerIndex, StoreBufferServiceStats stats) {
      this.blockingQueue = blockingQueue;
      this.drainerIndex = drainerIndex;
      this.stats = stats;
    }

    public void stop() {
      isRunning.set(false);
    }

    @Override
    public void run() {
      LOGGER.info("Starting StoreBufferDrainer Thread for drainer: {}....", drainerIndex);
      QueueNode node = null;
      DefaultPubSubMessage consumerRecord = null;
      LeaderProducedRecordContext leaderProducedRecordContext = null;
      StoreIngestionTask ingestionTask = null;
      CompletableFuture<Void> recordPersistedFuture = null;
      String storeName = OpenTelemetryMetricsSetup.UNKNOWN_STORE_NAME;
      while (isRunning.get()) {
        try {
          node = blockingQueue.take();

          consumerRecord = node.getConsumerRecord();
          int partitionNum = consumerRecord.getTopicPartition().getPartitionNumber();
          leaderProducedRecordContext = node.getLeaderProducedRecordContext();
          ingestionTask = node.getIngestionTask();
          recordPersistedFuture = node.getQueuedRecordPersistedFuture();
          storeName =
              OpenTelemetryMetricsSetup.sanitizeStoreName(ingestionTask != null ? ingestionTask.getStoreName() : null);

          long startTime = System.currentTimeMillis();

          if (node instanceof CommandQueueNode) {
            processCommand(
                (CommandQueueNode) node,
                ingestionTask,
                ingestionTask.getPartitionConsumptionState(partitionNum));
            continue;
          } else if (node instanceof SyncVtDivNode) {
            ((SyncVtDivNode) node).execute();
            continue;
          } else if (node instanceof SyncGlobalRtDivNode) {
            ((SyncGlobalRtDivNode) node).execute();
            continue;
          }

          processRecord(
              consumerRecord,
              ingestionTask,
              leaderProducedRecordContext,
              consumerRecord.getTopicPartition().getPartitionNumber(),
              node.getKafkaUrl(),
              node.getBeforeProcessingRecordTimestampNs());

          /**
           * Complete {@link QueueNode#queuedRecordPersistedFuture} since the processing for the current record is done.
           */
          if (recordPersistedFuture != null) {
            recordPersistedFuture.complete(null);
          }
          long latencyInMS = System.currentTimeMillis() - startTime;
          this.stats.recordInternalProcessingLatency(latencyInMS, storeName);
          topicToTimeSpent.compute(consumerRecord.getTopicPartition(), (K, V) -> (V == null ? 0 : V) + latencyInMS);
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
          stats.recordInternalProcessingError(storeName);

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

  static class FakePubSubMessage implements DefaultPubSubMessage {
    private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(FakePubSubMessage.class);
    private final PubSubTopicPartition topicPartition;

    FakePubSubMessage(PubSubTopicPartition topicPartition) {
      this.topicPartition = Objects.requireNonNull(topicPartition);
    }

    @Override
    public KafkaKey getKey() {
      return null;
    }

    @Override
    public KafkaMessageEnvelope getValue() {
      return null;
    }

    @Override
    public PubSubTopicPartition getTopicPartition() {
      return topicPartition;
    }

    @Override
    public PubSubPosition getPosition() {
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

    @Override
    public boolean isEndOfBootstrap() {
      return false;
    }

    @Override
    public int getHeapSize() {
      /** We assume that {@link #topicPartition} is a singleton instance, and therefore we're not counting it. */
      return SHALLOW_CLASS_OVERHEAD;
    }
  }
}
