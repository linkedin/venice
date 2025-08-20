package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@Experimental
public class BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl<K, V>
    implements BootstrappingVeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER =
      LogManager.getLogger(BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class);

  private final ChangelogClientConfig changelogClientConfig;
  private final String storeName;

  // A buffer of messages that will be returned to the user
  private final BlockingQueue<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubMessages;
  // Determines what version per partition is currently serving
  private final Map<Integer, Integer> partitionToVersionToServe;
  private final DaVinciRecordTransformerConfig recordTransformerConfig;
  // CachingDaVinciClientFactory used instead of DaVinciClientFactory, so we have the ability to close down the client
  private final CachingDaVinciClientFactory daVinciClientFactory;
  private final DaVinciClient<Object, Object> daVinciClient;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final CountDownLatch startLatch = new CountDownLatch(1);
  // Using a dedicated thread pool for CompletableFutures created by this class to avoid potential thread starvation
  // issues in the default ForkJoinPool
  private final ExecutorService completableFutureThreadPool = Executors.newFixedThreadPool(1);

  private final Set<Integer> subscribedPartitions = new HashSet<>();
  private final ReentrantLock bufferLock = new ReentrantLock();
  private final Condition bufferIsFullCondition = bufferLock.newCondition();
  private BackgroundReporterThread backgroundReporterThread;
  private long backgroundReporterThreadSleepIntervalSeconds = 60L;
  private final BasicConsumerStats changeCaptureStats;
  private final AtomicBoolean isCaughtUp = new AtomicBoolean(false);
  private final VeniceConcurrentHashMap<Integer, AtomicLong> consumerSequenceIdGeneratorMap;
  private final long consumerSequenceIdStartingValue;

  public BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl(ChangelogClientConfig changelogClientConfig) {
    this(changelogClientConfig, System.nanoTime());
  }

  BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl(
      ChangelogClientConfig changelogClientConfig,
      long consumerSequenceIdStartingValue) {
    this.changelogClientConfig = changelogClientConfig;
    this.storeName = changelogClientConfig.getStoreName();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setStorageClass(StorageClass.DISK);
    ClientConfig innerClientConfig = changelogClientConfig.getInnerClientConfig();
    this.pubSubMessages = new ArrayBlockingQueue<>(changelogClientConfig.getMaxBufferSize());
    this.partitionToVersionToServe = new ConcurrentHashMap<>();

    recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(DaVinciRecordTransformerBootstrappingChangelogConsumer::new)
        /*
         * Setting this to true since we're not transforming records and frequent changes can be made to the
         * DVRT implmentation. This is to prevent the local state from being wiped everytime a change is deployed
         */
        .setSkipCompatibilityChecks(true)
        .setKeyClass(innerClientConfig.getSpecificKeyClass())
        .setOutputValueClass(innerClientConfig.getSpecificValueClass())
        .setOutputValueSchema(innerClientConfig.getSpecificValueSchema())
        .build();
    daVinciConfig.setRecordTransformerConfig(recordTransformerConfig);

    this.daVinciClientFactory = new CachingDaVinciClientFactory(
        changelogClientConfig.getD2Client(),
        changelogClientConfig.getD2ServiceName(),
        innerClientConfig.getMetricsRepository(),
        buildVeniceConfig());

    if (innerClientConfig.isSpecificClient()) {
      this.daVinciClient = this.daVinciClientFactory
          .getSpecificAvroClient(this.storeName, daVinciConfig, innerClientConfig.getSpecificValueClass());
    } else {
      this.daVinciClient = this.daVinciClientFactory.getGenericAvroClient(this.storeName, daVinciConfig);
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
  }

  @Override
  public synchronized CompletableFuture<Void> start(Set<Integer> partitions) {
    if (isStarted.get()) {
      throw new VeniceException("BootstrappingVeniceChangelogConsumer is already started!");
    }

    daVinciClient.start();
    isStarted.set(true);

    // If a user passes in empty partitions set, we subscribe to all partitions
    if (partitions.isEmpty()) {
      for (int i = 0; i < daVinciClient.getPartitionCount(); i++) {
        subscribedPartitions.add(i);
      }
    } else {
      subscribedPartitions.addAll(partitions);
    }

    CompletableFuture<Void> startFuture = CompletableFuture.supplyAsync(() -> {
      try {
        /*
         * When this latch gets released, this means there's at least one message in pubSubMessages. So when the user
         * calls poll, they don't get an empty response. This also signals that blob transfer was completed
         * for at least one partition.
         */
        startLatch.await();

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
    daVinciClient.subscribe(subscribedPartitions).whenComplete((result, error) -> {
      if (error != null) {
        LOGGER.error("Failed to subscribe to partitions: {} for store: {}", subscribedPartitions, storeName, error);
        startFuture.completeExceptionally(new VeniceException(error));
        return;
      }

      isCaughtUp.set(true);
      LOGGER.info(
          "BootstrappingVeniceChangelogConsumer is caught up for store: {} for partitions: {}",
          storeName,
          subscribedPartitions);
    });

    return startFuture;
  }

  @Override
  public CompletableFuture<Void> start() {
    return this.start(Collections.emptySet());
  }

  @Override
  public void stop() throws Exception {
    if (backgroundReporterThread != null) {
      backgroundReporterThread.interrupt();
    }
    daVinciClientFactory.close();
    isStarted.set(false);
  }

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
        Map<K, PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> tempMap = new LinkedHashMap<>();
        /*
         * The behavior of LinkedHashMap is such that it maintains the order of insertion, but for values which are
         * replaced, it's put in at the position of the first insertion. This isn't quite what we want, we want to keep
         * only a single key (just as a map would), but we want to keep the position of the last insertion as well. So in
         * order to do that, we remove the entry before inserting it.
         */
        for (PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> message: drainedPubSubMessages) {
          tempMap.remove(message.getKey());
          tempMap.put(message.getKey(), message);
        }
        drainedPubSubMessages = tempMap.values();
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

      LOGGER.error("Encountered an exception when polling records for store: {}", storeName);
      throw exception;
    }
  }

  @Override
  public boolean isCaughtUp() {
    return isCaughtUp.get();
  }

  private VeniceProperties buildVeniceConfig() {
    return new PropertyBuilder().put(changelogClientConfig.getConsumerProperties())
        // We don't need the block cache, since we only read each key once from disk
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 0)
        .put(DATA_BASE_PATH, changelogClientConfig.getBootstrapFileSystemPath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        // Turning this off, so users don't subscribe to unwanted partitions automatically
        .put(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, false)
        // This is set to true so it's compatible with blob transfer
        .put(PUSH_STATUS_STORE_ENABLED, true)
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

  @VisibleForTesting
  protected void setBackgroundReporterThreadSleepIntervalSeconds(long interval) {
    backgroundReporterThreadSleepIntervalSeconds = interval;
  }

  public class DaVinciRecordTransformerBootstrappingChangelogConsumer extends DaVinciRecordTransformer<K, V, V> {
    private final String topicName;
    private final Map<Integer, PubSubTopicPartition> pubSubTopicPartitionMap = new HashMap<>();

    public DaVinciRecordTransformerBootstrappingChangelogConsumer(
        int storeVersion,
        Schema keySchema,
        Schema inputValueSchema,
        Schema outputValueSchema,
        DaVinciRecordTransformerConfig recordTransformerConfig) {
      super(storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
      this.topicName = Version.composeKafkaTopic(changelogClientConfig.getStoreName(), getStoreVersion());
    }

    @Override
    public void onStartVersionIngestion(boolean isCurrentVersion) {
      for (int partitionId: subscribedPartitions) {
        if (isCurrentVersion) {
          /*
           * This condition can occur when the application is starting up (due to a restart/deployment) or when the
           * next current version begins ingestion while the previous version is still serving.
           * In the first case, it is acceptable to immediately serve the current version.
           * In the second case, we should not immediately serve the next current version. Doing so would result in
           * sending all messages from SOP to nearline events that the user has already received.
           * To avoid this, we should only serve the current version if no other version is currently being served.
           * Once the next current version has consumed the version swap message, then it has caught up enough to be
           * ready to serve.
           */
          partitionToVersionToServe.computeIfAbsent(partitionId, v -> getStoreVersion());
        }

        // Caching these objects, so we don't need to recreate them on every single message received
        pubSubTopicPartitionMap
            .put(partitionId, new PubSubTopicPartitionImpl(new PubSubTopicImpl(topicName), partitionId));
      }
    }

    @Override
    public DaVinciRecordTransformerResult<V> transform(Lazy<K> key, Lazy<V> value, int partitionId) {
      // No transformations happening here. How to handle the record will be done in processPut
      return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
    }

    public void addMessageToBuffer(K key, V value, int partitionId) {
      if (partitionToVersionToServe.get(partitionId) == getStoreVersion()) {
        ChangeEvent<V> changeEvent = new ChangeEvent<>(null, value);
        try {
          /*
           * ToDo: The placeholders used here are fine bootstrapping changelog consumer but we will need to fill them in
           *  for real when creating an implementation for VeniceChangeLogConsumer.
           *
           * ToDo: Add metrics for number of records consumed and heartbeat lag. We might be able to leverage
           *  StoreIngestionTask::recordHeartbeatReceived.
           */
          pubSubMessages.put(
              new ImmutableChangeCapturePubSubMessage<>(
                  key,
                  changeEvent,
                  pubSubTopicPartitionMap.get(partitionId),
                  PubSubSymbolicPosition.EARLIEST,
                  0,
                  0,
                  false,
                  getNextConsumerSequenceId(partitionId)));

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
    }

    private long getNextConsumerSequenceId(int partition) {
      AtomicLong consumerSequenceIdGenerator = consumerSequenceIdGeneratorMap
          .computeIfAbsent(partition, p -> new AtomicLong(consumerSequenceIdStartingValue));
      return consumerSequenceIdGenerator.incrementAndGet();
    }

    @Override
    public void processPut(Lazy<K> key, Lazy<V> value, int partitionId) {
      addMessageToBuffer(key.get(), value.get(), partitionId);
    }

    @Override
    public void processDelete(Lazy<K> key, int partitionId) {
      // When value is null, it indicates it's a delete
      addMessageToBuffer(key.get(), null, partitionId);
    };

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

      /*
       * Only the futureVersion should act on the version swap message (VSM).
       * The previousVersion will consume the VSM earlier than the futureVersion, leading to an early version swap.
       * This early swap causes the buffer to fill with records before the EOP, which is undesirable.
       * By only allowing the futureVersion to perform the version swap, we ensure that only nearline events are served.
       */
      try {
        if (futureVersion == getStoreVersion()) {
          partitionToVersionToServe.put(partitionId, futureVersion);
          LOGGER.info(
              "Swapped from version: {} to version: {} for store: {} for partition: {}",
              currentVersion,
              futureVersion,
              storeName,
              partitionId);

          if (changeCaptureStats != null) {
            changeCaptureStats.emitVersionSwapCountMetrics(SUCCESS);
          }
        }
      } catch (Exception exception) {
        if (changeCaptureStats != null) {
          changeCaptureStats.emitVersionSwapCountMetrics(FAIL);
        }

        LOGGER.error(
            "Encountered an exception when processing Version Swap from version: {} to version: {} for store: {} for partition: {}",
            currentVersion,
            futureVersion,
            storeName,
            partitionId);
        throw exception;
      }
    }

    @Override
    public void close() throws IOException {

    }
  }
}
