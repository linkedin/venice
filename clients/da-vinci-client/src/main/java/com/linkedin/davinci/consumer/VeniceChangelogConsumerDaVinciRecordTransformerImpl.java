package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;

import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.SeekableDaVinciClient;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.MaterializedView;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceChangelogConsumerDaVinciRecordTransformerImpl<K, V>
    implements StatefulVeniceChangelogConsumer<K, V>, VeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceChangelogConsumerDaVinciRecordTransformerImpl.class);
  private long START_TIMEOUT_IN_SECONDS = 60;

  private final ChangelogClientConfig changelogClientConfig;
  private final String storeName;

  // A buffer of messages that will be returned to the user
  private final BlockingQueue<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubMessages;
  // Determines what version per partition is currently serving
  private final Map<Integer, Integer> partitionToVersionToServe;
  private final DaVinciRecordTransformerConfig recordTransformerConfig;
  // CachingDaVinciClientFactory used instead of DaVinciClientFactory, so we have the ability to close down the client
  private final CachingDaVinciClientFactory daVinciClientFactory;
  private final SeekableDaVinciClient<K, V> daVinciClient;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final CountDownLatch startLatch = new CountDownLatch(1);
  // Using a dedicated thread pool for CompletableFutures created by this class to avoid potential thread starvation
  // issues in the default ForkJoinPool
  private final ExecutorService completableFutureThreadPool =
      Executors.newFixedThreadPool(1, new DaemonThreadFactory("VeniceChangelogConsumerDaVinciRecordTransformerImpl"));

  private final Set<Integer> subscribedPartitions = VeniceConcurrentHashMap.newKeySet();
  private final ReentrantLock bufferLock = new ReentrantLock();
  private final Condition bufferIsFullCondition = bufferLock.newCondition();
  private BackgroundReporterThread backgroundReporterThread;
  private long backgroundReporterThreadSleepIntervalSeconds = 60L;
  private final BasicConsumerStats changeCaptureStats;
  private final AtomicBoolean isCaughtUp = new AtomicBoolean(false);
  private final ConcurrentHashMap<Integer, Long> currentVersionLastHeartbeat = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, AtomicLong> consumerSequenceIdGeneratorMap;
  private final long consumerSequenceIdStartingValue;
  private final boolean isVersionSpecificClient;
  private final VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory;
  private final boolean includeControlMessages;
  private final boolean includeDeserializedReplicationMetadata;
  private final ReplicationMetadataSchemaRepository replicationMetadataSchemaRepository;
  private final StoreDeserializerCache<GenericRecord> rmdDeserializerCache;
  private final String viewName;

  public VeniceChangelogConsumerDaVinciRecordTransformerImpl(
      ChangelogClientConfig changelogClientConfig,
      VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory) {
    this(changelogClientConfig, System.nanoTime(), veniceChangelogConsumerClientFactory);
  }

  VeniceChangelogConsumerDaVinciRecordTransformerImpl(
      ChangelogClientConfig changelogClientConfig,
      long consumerSequenceIdStartingValue,
      VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory) {
    this.changelogClientConfig = changelogClientConfig;
    this.storeName = changelogClientConfig.getStoreName();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setStorageClass(StorageClass.DISK);
    ClientConfig innerClientConfig = changelogClientConfig.getInnerClientConfig();
    this.pubSubMessages = new ArrayBlockingQueue<>(changelogClientConfig.getMaxBufferSize());
    this.partitionToVersionToServe = new VeniceConcurrentHashMap<>();
    this.isVersionSpecificClient = changelogClientConfig.getStoreVersion() != null;
    this.veniceChangelogConsumerClientFactory = veniceChangelogConsumerClientFactory;
    this.viewName = changelogClientConfig.getViewName();

    recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(DaVinciRecordTransformerChangelogConsumer::new)
        /*
         * Setting this to false, otherwise records won't be able to be deserialized from on disk scan if
         * schema evolution occurred.
         */
        .setRecordTransformationEnabled(false)
        .setKeyClass(innerClientConfig.getSpecificKeyClass())
        .setOutputValueClass(innerClientConfig.getSpecificValueClass())
        .setOutputValueSchema(innerClientConfig.getSpecificValueSchema())
        // For now, stateless client will always seek to the beginning of the topic
        .setAlwaysBootstrapFromVersionTopic(!changelogClientConfig.isStateful())
        .setStoreRecordsInDaVinci(changelogClientConfig.isStateful())
        .setRecordMetadataEnabled(true)
        .build();
    daVinciConfig.setRecordTransformerConfig(recordTransformerConfig);

    this.daVinciClientFactory = new CachingDaVinciClientFactory(
        changelogClientConfig.getD2Client(),
        changelogClientConfig.getD2ServiceName(),
        innerClientConfig.getMetricsRepository(),
        buildVeniceConfig());

    if (isVersionSpecificClient) {
      LOGGER.info(
          "Version specific CDC client is in use. Subscribing to version: {} for store: {}",
          changelogClientConfig.getStoreVersion(),
          storeName);
      this.daVinciClient = this.daVinciClientFactory.getVersionSpecificGenericAvroClient(
          this.storeName,
          changelogClientConfig.getStoreVersion(),
          this.viewName,
          daVinciConfig);
    } else {
      if (innerClientConfig.isSpecificClient()) {
        this.daVinciClient = this.daVinciClientFactory.getSpecificSeekableAvroClient(
            this.storeName,
            this.viewName,
            daVinciConfig,
            innerClientConfig.getSpecificValueClass());
      } else {
        this.daVinciClient =
            this.daVinciClientFactory.getGenericSeekableAvroClient(this.storeName, this.viewName, daVinciConfig);
      }
    }

    if (changelogClientConfig.getInnerClientConfig().getMetricsRepository() != null) {
      this.changeCaptureStats = new BasicConsumerStats(
          changelogClientConfig.getInnerClientConfig().getMetricsRepository(),
          "vcc-" + changelogClientConfig.getConsumerName(),
          storeName);
    } else {
      changeCaptureStats = null;
    }
    this.consumerSequenceIdGeneratorMap = new VeniceConcurrentHashMap<>();
    this.consumerSequenceIdStartingValue = consumerSequenceIdStartingValue;
    this.includeControlMessages = changelogClientConfig.shouldIncludeControlMessages();
    this.includeDeserializedReplicationMetadata = changelogClientConfig.shouldDeserializeReplicationMetadata();
    if (includeDeserializedReplicationMetadata) {
      this.replicationMetadataSchemaRepository =
          new ReplicationMetadataSchemaRepository(changelogClientConfig.getD2ControllerClient());
      this.rmdDeserializerCache = new RmdDeserializerCache<>(replicationMetadataSchemaRepository, storeName, 1, false);
    } else {
      this.replicationMetadataSchemaRepository = null;
      this.rmdDeserializerCache = null;
    }
  }

  private void startDaVinciClient() {
    // Start daVinci client if not already started
    if (!isStarted.get()) {
      daVinciClient.start();
      isStarted.set(true);
    }
  }

  /**
   * Helper method to initialize client, update subscribed partitions, and execute subscription.
   * This consolidates common logic across start, seekToCheckpoint, and seekToTimestamps.
   *
   * @param partitions Partitions to subscribe to (empty set means all partitions)
   * @param subscriptionCall Function that takes subscribedPartitions and returns subscription future
   * @return CompletableFuture that represents the async initialization work
   */
  private CompletableFuture<Void> initializeAndSubscribe(
      Set<Integer> partitions,
      Function<Set<Integer>, CompletableFuture<Void>> subscriptionCall) {
    startDaVinciClient();

    // If a user passes in empty partitions set, we subscribe to all partitions
    Set<Integer> targetPartitions = new HashSet<>();
    if (partitions.isEmpty()) {
      for (int i = 0; i < daVinciClient.getPartitionCount(); i++) {
        targetPartitions.add(i);
      }
    } else {
      targetPartitions.addAll(partitions);
    }

    // Explicitly don't allow seeking to an already subscribed partition, as DaVinci doesn't support it
    Set<Integer> intersection = new HashSet<>(subscribedPartitions);
    intersection.retainAll(targetPartitions);
    if (!intersection.isEmpty()) {
      throw new VeniceClientException(
          "Cannot subscribe to partitions: " + intersection + " as they are already subscribed");
    }

    subscribedPartitions.addAll(targetPartitions);

    CompletableFuture<Void> startFuture = CompletableFuture.supplyAsync(() -> {
      try {
        /*
         * When this latch gets released, this means there's at least one message in pubSubMessages. So when the user
         * calls poll, they don't get an empty response. This also signals that blob transfer was completed
         * for at least one partition.
         */
        if (!startLatch.await(START_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
          LOGGER.warn(
              "Unable to consume a message after {} seconds for store: {}. Moving on to unblock start.",
              START_TIMEOUT_IN_SECONDS,
              storeName);
        }

        if (changeCaptureStats != null) {
          backgroundReporterThread = new BackgroundReporterThread();
          backgroundReporterThread.start();
        }
      } catch (InterruptedException e) {
        LOGGER.info("Thread was interrupted", e);
        // Restore the interrupt status
        Thread.currentThread().interrupt();
      }
      return null;
    }, completableFutureThreadPool);

    /*
     * Avoid waiting on the CompletableFuture to prevent a circular dependency.
     * When subscribe is called, DVRT scans the entire storage engine and fills pubSubMessages.
     * Because pubSubMessages has limited capacity, blocking on the CompletableFuture
     * prevents the user from calling poll to drain pubSubMessages, so the threads populating pubSubMessages
     * will wait forever for capacity to become available. This leads to a deadlock.
    */
    subscriptionCall.apply(subscribedPartitions).whenComplete((result, error) -> {
      if (error != null) {
        LOGGER.error("Failed to subscribe to partitions: {} for store: {}", subscribedPartitions, storeName, error);
        startFuture.completeExceptionally(new VeniceClientException(error));
        return;
      }

      isCaughtUp.set(true);
      LOGGER.info(
          "VeniceChangelogConsumer is caught up for store: {} for partitions: {}",
          storeName,
          subscribedPartitions);
    });

    return startFuture;
  }

  // StatefulVeniceChangelogConsumer methods below

  @Override
  public synchronized CompletableFuture<Void> start(Set<Integer> partitions) {
    return initializeAndSubscribe(partitions, daVinciClient::subscribe);
  }

  @Override
  public CompletableFuture<Void> start() {
    return this.start(Collections.emptySet());
  }

  @Override
  public void stop() throws Exception {
    LOGGER.info("Closing Changelog Consumer with name: {}", changelogClientConfig.getConsumerName());

    if (backgroundReporterThread != null) {
      backgroundReporterThread.interrupt();
    }
    daVinciClient.close();
    isStarted.set(false);
    veniceChangelogConsumerClientFactory.deregisterClient(changelogClientConfig.getConsumerName());
    clearPartitionState(Collections.emptySet());

    LOGGER.info("Closed Changelog Consumer with name: {}", changelogClientConfig.getConsumerName());
  }

  // VeniceChangelogConsumer methods below

  public int getPartitionCount() {
    startDaVinciClient();
    return this.daVinciClient.getPartitionCount();
  }

  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    // ToDo: Start at beginning of topic
    return this.start(partitions);
  }

  public CompletableFuture<Void> subscribeAll() {
    return this.subscribe(Collections.emptySet());
  }

  public void unsubscribe(Set<Integer> partitions) {
    this.daVinciClient.unsubscribe(partitions);
    clearPartitionState(partitions);
  }

  public void unsubscribeAll() {
    this.daVinciClient.unsubscribeAll();
    clearPartitionState(Collections.emptySet());
  }

  public CompletableFuture<Void> seekToBeginningOfPush(Set<Integer> partitions) {
    return this.subscribe(partitions);
  }

  public CompletableFuture<Void> seekToBeginningOfPush() {
    return this.subscribe(Collections.emptySet());
  }

  public CompletableFuture<Void> seekToEndOfPush(Set<Integer> partitions) {
    throw new VeniceClientException("seekToEndOfPush will not be supported");
  }

  public CompletableFuture<Void> seekToEndOfPush() {
    return this.seekToEndOfPush(Collections.emptySet());
  }

  public CompletableFuture<Void> seekToTail(Set<Integer> partitions) {
    return daVinciClient.seekToTail(partitions);
  }

  public CompletableFuture<Void> seekToTail() {
    return daVinciClient.seekToTail();
  }

  public CompletableFuture<Void> seekToCheckpoint(Set<VeniceChangeCoordinate> checkpoints) {
    // Extract partitions from checkpoints
    Set<Integer> partitions = new HashSet<>();
    for (VeniceChangeCoordinate coordinate: checkpoints) {
      partitions.add(coordinate.getPartition());
    }

    return initializeAndSubscribe(partitions, ignore -> daVinciClient.seekToCheckpoint(checkpoints));
  }

  public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps) {
    return initializeAndSubscribe(timestamps.keySet(), ignore -> daVinciClient.seekToTimestamps(timestamps));
  }

  public CompletableFuture<Void> seekToTimestamp(Long timestamp) {
    // Start client first, so we can call getPartitionCount()
    startDaVinciClient();

    Map<Integer, Long> timestamps = new HashMap<>();
    for (int i = 0; i < daVinciClient.getPartitionCount(); i++) {
      timestamps.put(i, timestamp);
    }

    return seekToTimestamps(timestamps);
  }

  public void pause(Set<Integer> partitions) {
    throw new VeniceClientException("pause will not be supported");
  }

  public void pause() {
    this.pause(Collections.emptySet());
  }

  public void resume(Set<Integer> partitions) {
    throw new VeniceClientException("resume will not be supported");
  }

  public void resume() {
    this.resume(Collections.emptySet());
  }

  public void close() {
    try {
      this.stop();
    } catch (Exception e) {
      LOGGER.error("Close failed for VeniceChangelogConsumer", e);
      throw new RuntimeException(e);
    }
  }

  // Common methods below

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs) {
    try {
      try {
        bufferLock.lock();

        // Wait until pubSubMessages becomes full, or until the timeout is reached
        if (pubSubMessages.remainingCapacity() > 0) {
          bufferIsFullCondition.await(timeoutInMs, TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException exception) {
        LOGGER.info("Thread was interrupted", exception);
        // Restore the interrupt status
        Thread.currentThread().interrupt();
      } finally {
        bufferLock.unlock();
      }

      Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> drainedPubSubMessages = new ArrayList<>();
      pubSubMessages.drainTo(drainedPubSubMessages);
      int messagesPolled = drainedPubSubMessages.size();

      if (changelogClientConfig.shouldCompactMessages()) {
        if (includeControlMessages) {
          drainedPubSubMessages = compactPubSubMessagesWithControlMessage(drainedPubSubMessages);
        } else {
          drainedPubSubMessages = compactPubSubMessages(drainedPubSubMessages);
        }
      }

      if (changeCaptureStats != null) {
        changeCaptureStats.emitRecordsConsumedCountMetrics(messagesPolled);
        changeCaptureStats.emitPollCountMetrics(SUCCESS);
      }

      return drainedPubSubMessages;
    } catch (Exception exception) {
      if (changeCaptureStats != null) {
        changeCaptureStats.emitPollCountMetrics(FAIL);
      }

      LOGGER.error("Encountered an exception when polling records for store: {}", storeName, exception);
      throw exception;
    }
  }

  @Override
  public boolean isCaughtUp() {
    return isCaughtUp.get();
  }

  /**
   * Compacts the given collection of PubSubMessages by retaining only the latest message for each unique key.
   * This method assumes that there is no control message (with key=null and value=null) in the input collection.
   */
  private Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> compactPubSubMessages(
      Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> messages) {
    Map<K, PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> tempMap = new LinkedHashMap<>();
    /*
     * The behavior of LinkedHashMap is such that it maintains the order of insertion, but for values which are
     * replaced, it's put in at the position of the first insertion. This isn't quite what we want, we want to keep
     * only a single key (just as a map would), but we want to keep the position of the last insertion as well. So in
     * order to do that, we remove the entry before inserting it.
     */
    for (PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> message: messages) {
      tempMap.remove(message.getKey());
      tempMap.put(message.getKey(), message);
    }
    return tempMap.values();
  }

  /**
   * Compacts the given collection of PubSubMessages by retaining only the latest message for each unique key,
   * while preserving control messages (with key=null and value=null).
   * This method will use more memory compared to compactPubSubMessages since it duplicates the input collection.
   */
  private Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> compactPubSubMessagesWithControlMessage(
      Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> messages) {
    Set<K> seenKeys = new HashSet<>();
    LinkedList<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> compactedMessageList = new LinkedList<>();
    // Iterate in reverse order to keep the latest message for each key
    ArrayList<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> messageList = new ArrayList<>(messages);
    for (int i = messageList.size() - 1; i >= 0; i--) {
      PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> message = messageList.get(i);
      K key = message.getKey();
      if (key == null || !seenKeys.contains(key)) {
        compactedMessageList.addFirst(message);
        if (key != null) {
          seenKeys.add(key);
        }
      }
    }
    return compactedMessageList;
  }

  private VeniceProperties buildVeniceConfig() {
    return new PropertyBuilder().put(changelogClientConfig.getConsumerProperties())
        // We don't need the block cache, since we only read each key once from disk
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 0)
        .put(DATA_BASE_PATH, changelogClientConfig.getBootstrapFileSystemPath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(PUSH_STATUS_STORE_ENABLED, !isVersionSpecificClient)
        .build();
  }

  @VisibleForTesting
  public DaVinciRecordTransformerConfig getRecordTransformerConfig() {
    return recordTransformerConfig;
  }

  class BackgroundReporterThread extends Thread {
    private BackgroundReporterThread() {
      super("Change-Data-CaptureBackground-Reporter-Thread");
      // To not block JVM shutdown
      setDaemon(true);
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          recordStats();
          TimeUnit.SECONDS.sleep(backgroundReporterThreadSleepIntervalSeconds);
        } catch (InterruptedException e) {
          LOGGER.warn("BackgroundReporterThread interrupted!  Shutting down...", e);
          Thread.currentThread().interrupt(); // Restore the interrupt status
          break;
        }
      }
    }

    private void recordStats() {
      // Emit heartbeat delay metrics based on last heartbeat per partition
      long now = System.currentTimeMillis();
      long maxLag = Long.MIN_VALUE;
      for (Long heartBeatTimestamp: getLastHeartbeatPerPartition().values()) {
        if (heartBeatTimestamp != null) {
          maxLag = Math.max(maxLag, now - heartBeatTimestamp);
        }
      }
      if (maxLag != Long.MIN_VALUE) {
        changeCaptureStats.emitHeartBeatDelayMetrics(maxLag);
      }

      int minVersion = Integer.MAX_VALUE;
      int maxVersion = -1;

      Map<Integer, Integer> partitionToVersionToServeCopy = new HashMap<>(partitionToVersionToServe);
      for (Integer version: partitionToVersionToServeCopy.values()) {
        minVersion = Math.min(minVersion, version);
        maxVersion = Math.max(maxVersion, version);
      }
      if (minVersion == Integer.MAX_VALUE) {
        minVersion = -1;
      }

      // Record max and min consumed versions
      changeCaptureStats.emitCurrentConsumingVersionMetrics(minVersion, maxVersion);
    }
  }

  @Override
  public Map<Integer, Long> getLastHeartbeatPerPartition() {
    // Snapshot the heartbeat map to avoid iterating while it is being updated concurrently
    return new HashMap<>(currentVersionLastHeartbeat);
  }

  @VisibleForTesting
  protected void setBackgroundReporterThreadSleepIntervalSeconds(long interval) {
    backgroundReporterThreadSleepIntervalSeconds = interval;
  }

  public class DaVinciRecordTransformerChangelogConsumer extends DaVinciRecordTransformer<K, V, V> {
    private final String topicName;
    private final Map<Integer, PubSubTopicPartition> pubSubTopicPartitionMap = new VeniceConcurrentHashMap<>();

    public DaVinciRecordTransformerChangelogConsumer(
        String storeName,
        int storeVersion,
        Schema keySchema,
        Schema inputValueSchema,
        Schema outputValueSchema,
        DaVinciRecordTransformerConfig recordTransformerConfig) {
      super(storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);

      // Determine the topic name based on whether a view name is provided
      if (viewName != null && !viewName.isEmpty() && getViewClass().equals(MaterializedView.class.getCanonicalName())) {
        this.topicName =
            MaterializedView.composeTopicName(changelogClientConfig.getStoreName(), getStoreVersion(), viewName);
      } else {
        this.topicName = Version.composeKafkaTopic(changelogClientConfig.getStoreName(), getStoreVersion());
      }
    }

    @Override
    public void onStartVersionIngestion(int partitionId, boolean isCurrentVersion) {
      if (isCurrentVersion || isVersionSpecificClient) {
        /*
         * This condition can occur when the application is starting up (due to a restart/deployment) or when the
         * next current version begins ingestion while the previous version is still serving.
         * In the first case, it is acceptable to immediately serve the current version.
         * In the second case, we should not immediately serve the next current version. Doing so would result in
         * sending all messages from SOP to nearline events that the user has already received.
         * To avoid this, we should only serve the current version if no other version is currently being served.
         * Once the next current version has consumed the version swap message, then it has caught up enough to be
         * ready to serve.
         *
         * This can also occur when it is running in version specific mode, and no version swap will happen.
         * In this case, just serve the currently subscribed version.
         */
        partitionToVersionToServe.computeIfAbsent(partitionId, v -> getStoreVersion());
      }

      // Caching these objects, so we don't need to recreate them on every single message received
      pubSubTopicPartitionMap
          .putIfAbsent(partitionId, new PubSubTopicPartitionImpl(new PubSubTopicImpl(topicName), partitionId));

      // Initialize heartbeat timestamp for the partition to avoid empty lag until first heartbeat arrives
      currentVersionLastHeartbeat.putIfAbsent(partitionId, System.currentTimeMillis());
    }

    @Override
    public DaVinciRecordTransformerResult<V> transform(
        Lazy<K> key,
        Lazy<V> value,
        int partitionId,
        DaVinciRecordTransformerRecordMetadata recordMetadata) {
      // No transformations happening here. How to handle the record will be done in processPut
      return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
    }

    /**
     * Add ChangeCapturePubSubMessage to the buffer based on record metadata.
     */
    public void addMessageToBuffer(
        K key,
        V value,
        int partitionId,
        DaVinciRecordTransformerRecordMetadata recordMetadata) {
      Integer versionToServe = partitionToVersionToServe.get(partitionId);
      if (versionToServe != null && versionToServe == getStoreVersion()) {
        ChangeEvent<V> changeEvent = new ChangeEvent<>(null, value);
        GenericRecord deserializedReplicationMetadata = null;
        if (includeDeserializedReplicationMetadata && recordMetadata.getReplicationMetadataPayload() != null
            && recordMetadata
                .getReplicationMetadataVersionId() != DaVinciRecordTransformerRecordMetadata.UNSPECIFIED_SCHEMA_ID) {
          // provide deserialized replication metadata, recordMetadata.getReplicationMetadataVersionId() will be useful
          // in the future when we evolve our RMD schema.
          RecordDeserializer<GenericRecord> deserializer = rmdDeserializerCache
              .getDeserializer(recordMetadata.getWriterSchemaId(), recordMetadata.getWriterSchemaId());
          deserializedReplicationMetadata = deserializer.deserialize(recordMetadata.getReplicationMetadataPayload());
        }
        ImmutableChangeCapturePubSubMessage<K, ChangeEvent<V>> pubSubMessage =
            new ImmutableChangeCapturePubSubMessage<>(
                key,
                changeEvent,
                pubSubTopicPartitionMap.get(partitionId),
                recordMetadata.getPubSubPosition(),
                recordMetadata.getTimestamp(),
                recordMetadata.getPayloadSize(),
                isCaughtUp(),
                getNextConsumerSequenceId(partitionId),
                recordMetadata.getWriterSchemaId(),
                recordMetadata.getReplicationMetadataPayload(),
                null,
                deserializedReplicationMetadata);
        internalAddMessageToBuffer(partitionId, pubSubMessage);
      }
    }

    /**
     * Add ControlMessage to the buffer.
     * Key and Value are both null. Details for control message are in controlMessage param.
     */
    public void addControlMessageToBuffer(
        int partitionId,
        PubSubPosition offset,
        ControlMessage controlMessage,
        long pubSubMessageTimestamp) {
      Integer versionToServe = partitionToVersionToServe.get(partitionId);
      if (versionToServe != null && versionToServe == getStoreVersion()) {
        ImmutableChangeCapturePubSubMessage<K, ChangeEvent<V>> pubSubMessage =
            new ImmutableChangeCapturePubSubMessage<>(
                null,
                null,
                pubSubTopicPartitionMap.get(partitionId),
                offset,
                pubSubMessageTimestamp,
                0,
                false,
                getNextConsumerSequenceId(partitionId),
                -1,
                null,
                controlMessage,
                null);
        internalAddMessageToBuffer(partitionId, pubSubMessage);
      }
    }

    /**
     * Internal method to add message to buffer.
     */
    private void internalAddMessageToBuffer(
        int partitionId,
        ImmutableChangeCapturePubSubMessage<K, ChangeEvent<V>> pubSubMessage) {
      Integer versionToServe = partitionToVersionToServe.get(partitionId);
      if (versionToServe == null || versionToServe != getStoreVersion()) {
        return;
      }

      try {
        pubSubMessages.put(pubSubMessage);
        /*
         * pubSubMessages is full, signal to a poll thread awaiting on bufferFullCondition.
         * Not signaling to all threads, because if multiple poll threads try to read pubSubMessages at
         * the same time, all other poll threads besides the first reader will get any messages.
         * Also, don't acquire the locker before inserting into pubSubMessages. If we acquire the lock before,
         * and pubSubMessages is full, the put will be blocked, and we won't be able to release the lock. Leading
         * to a deadlock. We also shouldn't signal before insertion either, because when the buffer is full multiple
         * drainer threads will send a signal out at once. This leads to the original issue described at the
         * beginning of this comment block.
         */
        if (pubSubMessages.remainingCapacity() == 0) {
          bufferLock.lock();
          try {
            bufferIsFullCondition.signal();
          } finally {
            bufferLock.unlock();
          }
        }

        startLatch.countDown();
      } catch (InterruptedException e) {
        LOGGER.error("Thread was interrupted while putting a message into pubSubMessages", e);
        Thread.currentThread().interrupt();
      }
    }

    /**
     * Helper method to get the view class based on the view name and store configuration.
     */
    private String getViewClass() {
      return veniceChangelogConsumerClientFactory.viewClassGetter.apply(
          changelogClientConfig.getStoreName(),
          viewName,
          changelogClientConfig.getD2ControllerClient(),
          changelogClientConfig.getControllerRequestRetryCount());
    }

    private long getNextConsumerSequenceId(int partition) {
      AtomicLong consumerSequenceIdGenerator = consumerSequenceIdGeneratorMap
          .computeIfAbsent(partition, p -> new AtomicLong(consumerSequenceIdStartingValue));
      return consumerSequenceIdGenerator.incrementAndGet();
    }

    @Override
    public void processPut(
        Lazy<K> key,
        Lazy<V> value,
        int partitionId,
        DaVinciRecordTransformerRecordMetadata recordMetadata) {
      addMessageToBuffer(key.get(), value.get(), partitionId, recordMetadata);
    }

    @Override
    public void processDelete(Lazy<K> key, int partitionId, DaVinciRecordTransformerRecordMetadata recordMetadata) {
      // When value is null, it indicates it's a delete
      addMessageToBuffer(key.get(), null, partitionId, recordMetadata);
    };

    /**
     * If includeControlMessages is FALSE, or the client is NOT version specific, this method is a no-op.
     * If includeControlMessages is true, AND the client is version specific, it adds the control message to the buffer.
     */
    public void onControlMessage(
        int partitionId,
        PubSubPosition offset,
        ControlMessage controlMessage,
        long pubSubMessageTimestamp) {
      if (!isVersionSpecificClient || !includeControlMessages) {
        return;
      }
      addControlMessageToBuffer(partitionId, offset, controlMessage, pubSubMessageTimestamp);
    }

    public void onVersionSwap(int currentVersion, int futureVersion, int partitionId) {
      /*
       * ToDo: In the event of a version rollback, VSMs will be added to the VT again to the backup version.
       *  This means that when we first encounter a VSM we will swap to the next version too early as we read
       *  an old VSM. What we need to do here is ignore it until we consume the same VSM that the current version
       *  has ingested. This means we need some coordination mechanism between the versions regarding VSM.
       */

      /*
       * ToDo: If the user doesn't call poll frequently enough, it's possible that the future version can get ahead
       *  of the current version. This is because when the user doesn't call poll, the buffer will reach capacity and
       *  the threads trying to insert into it will get blocked until the buffer is drained. When this happens,
       *  we will consume the VSM in the future version before the current version could consume it. Therefore, we
       *  would perform a version swap too early.
       */

      if (isVersionSpecificClient) {
        LOGGER.info(
            "Ignoring version swap for store: {} since it's subscribed using a version specific client",
            storeName);
        return;
      }

      /*
       * Only the futureVersion should act on the version swap message (VSM).
       * The previousVersion will consume the VSM earlier than the futureVersion, leading to an early version swap.
       * This early swap causes the buffer to fill with records before the EOP, which is undesirable.
       * By only allowing the futureVersion to perform the version swap, we ensure that only nearline events are served.
       */
      String replicaId = Utils.getReplicaId(storeName, futureVersion, partitionId);
      try {
        if (futureVersion == getStoreVersion()) {
          partitionToVersionToServe.put(partitionId, futureVersion);
          LOGGER.info("Swapped versions from: {} to: {} for replica: {}", currentVersion, futureVersion, replicaId);

          if (changeCaptureStats != null) {
            changeCaptureStats.emitVersionSwapCountMetrics(SUCCESS);
          }
        }
      } catch (Exception exception) {
        if (changeCaptureStats != null) {
          changeCaptureStats.emitVersionSwapCountMetrics(FAIL);
        }

        LOGGER.error(
            "Encountered an exception when processing Version Swap from version: {} to version: {} for replica: {}",
            currentVersion,
            futureVersion,
            replicaId,
            exception);
        throw exception;
      }
    }

    /**
     * Receive heartbeat timestamp for a partition and update latest seen time.
     */
    public void onHeartbeat(int partitionId, long heartbeatTimestamp) {
      Integer versionToServe = partitionToVersionToServe.get(partitionId);
      if (versionToServe != null && versionToServe == getStoreVersion()) {
        currentVersionLastHeartbeat.put(partitionId, heartbeatTimestamp);
      }
    }

    @Override
    public void close() throws IOException {

    }
  }

  @VisibleForTesting
  public void clearPartitionState(Set<Integer> partitions) {
    if (partitions.isEmpty()) {
      subscribedPartitions.clear();
      partitionToVersionToServe.clear();
      currentVersionLastHeartbeat.clear();
    } else {
      for (int partition: partitions) {
        subscribedPartitions.remove(partition);
        partitionToVersionToServe.remove(partition);
        currentVersionLastHeartbeat.remove(partition);
      }
    }
  }

  @VisibleForTesting
  public boolean isStarted() {
    return isStarted.get();
  }

  @VisibleForTesting
  public Set<Integer> getSubscribedPartitions() {
    return subscribedPartitions;
  }

  @VisibleForTesting
  public void setStartTimeout(long seconds) {
    START_TIMEOUT_IN_SECONDS = seconds;
  }
}
