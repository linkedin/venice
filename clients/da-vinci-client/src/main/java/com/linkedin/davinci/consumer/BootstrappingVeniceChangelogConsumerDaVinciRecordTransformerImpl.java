package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
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
  private final ConcurrentHashMap<Integer, Integer> partitionToVersionToServe;
  private final DaVinciRecordTransformerConfig recordTransformerConfig;
  // CachingDaVinciClientFactory used instead of DaVinciClientFactory, so we have the ability to close down the client
  private final CachingDaVinciClientFactory daVinciClientFactory;
  private final DaVinciClient<Object, Object> daVinciClient;
  private boolean isStarted = false;
  private final CountDownLatch startLatch = new CountDownLatch(1);
  // Using a dedicated thread pool for CompletableFutures created by this class to avoid potential thread starvation
  // issues in the default ForkJoinPool
  private final ExecutorService completableFutureThreadPool = Executors.newFixedThreadPool(1);

  private final Set<Integer> subscribedPartitions = new HashSet<>();
  private final ApacheKafkaOffsetPosition placeHolderOffset = ApacheKafkaOffsetPosition.of(0);
  private final ReentrantLock bufferLock = new ReentrantLock();
  private final Condition bufferIsFullCondition = bufferLock.newCondition();

  public BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl(ChangelogClientConfig changelogClientConfig) {
    this.changelogClientConfig = changelogClientConfig;
    this.storeName = changelogClientConfig.getStoreName();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
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
  }

  @Override
  public CompletableFuture<Void> start(Set<Integer> partitions) {
    internalStart();
    subscribedPartitions.addAll(partitions);

    /*
     * Avoid waiting on the CompletableFuture to prevent a circular dependency.
     * When subscribe is called, DVRT scans the entire storage engine and fills pubSubMessages.
     * Because pubSubMessages has limited capacity, blocking on the CompletableFuture
     * prevents the user from calling poll to drain pubSubMessages, so the threads populating pubSubMessages
     * will wait forever for capacity to become available. This leads to a deadlock.
     */
    daVinciClient.subscribe(partitions).whenComplete((result, error) -> {
      if (error != null) {
        LOGGER.error("Failed to subscribe to partitions: {} for store: {}", partitions, storeName, error);
        throw new VeniceException(error);
      }
    });

    return CompletableFuture.supplyAsync(() -> {
      try {
        /*
         * When this latch gets released, this means there's at least one message in pubSubMessages. So when the user
         * calls poll, they don't get an empty response. This also signals that blob transfer was completed
         * for at least one partition.
         */
        startLatch.await();
      } catch (InterruptedException e) {
        LOGGER.info("Thread was interrupted", e);
        // Restore the interrupt status
        Thread.currentThread().interrupt();
      }
      return null;
    }, completableFutureThreadPool);
  }

  @Override
  public CompletableFuture<Void> start() {
    internalStart();

    Set<Integer> allPartitions = new HashSet<>();
    for (int i = 0; i < daVinciClient.getPartitionCount(); i++) {
      allPartitions.add(i);
    }

    return this.start(allPartitions);
  }

  @Override
  public void stop() throws Exception {
    daVinciClientFactory.close();
    isStarted = false;
  }

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs) {
    try {
      bufferLock.lock();

      // Wait until pubSubMessages becomes full, or until the timeout is reached
      if (pubSubMessages.remainingCapacity() > 0) {
        bufferIsFullCondition.await(timeoutInMs, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      LOGGER.info("Thread was interrupted", e);
      // Restore the interrupt status
      Thread.currentThread().interrupt();
    } finally {
      bufferLock.unlock();
    }

    List<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> drainedPubSubMessages = new ArrayList<>();
    pubSubMessages.drainTo(drainedPubSubMessages);

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
      return tempMap.values();
    }
    return drainedPubSubMessages;
  }

  private void internalStart() {
    if (isStarted) {
      return;
    }

    daVinciClient.start();
    isStarted = true;
  }

  private VeniceProperties buildVeniceConfig() {
    return new PropertyBuilder()
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, changelogClientConfig.getRocksDBBlockCacheSizeInBytes())
        .put(changelogClientConfig.getConsumerProperties())
        .put(DATA_BASE_PATH, changelogClientConfig.getBootstrapFileSystemPath())
        .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        // Turning this off, so users don't subscribe to unwanted partitions automatically
        .put(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, false)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        // Turning this off, as the CDC client will be throttling ingestion based on calls to poll, which can block
        // version pushes and may delay a version being ready to serve
        .put(PARTICIPANT_MESSAGE_STORE_ENABLED, false)
        .build();
  }

  @VisibleForTesting
  public DaVinciRecordTransformerConfig getRecordTransformerConfig() {
    return recordTransformerConfig;
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
                  placeHolderOffset,
                  0,
                  0,
                  false));

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
      if (futureVersion == getStoreVersion()) {
        partitionToVersionToServe.put(partitionId, futureVersion);
        LOGGER.info(
            "Swapped from version: {} to version: {} for partitionId: {}",
            currentVersion,
            futureVersion,
            partitionId);
      }
    }

    @Override
    public void close() throws IOException {

    }
  }
}
