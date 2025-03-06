package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl<K, V>
    extends VeniceAfterImageConsumerImpl<K, V> implements BootstrappingVeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER =
      LogManager.getLogger(BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class);

  private final ChangelogClientConfig changelogClientConfig;
  private final String storeName;

  // A buffer of messages that will be returned to the user
  private final BlockingQueue<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubMessages;
  // Determines what version per partition is currently serving
  private final ConcurrentHashMap<Integer, Integer> partitionToVersionToServe;
  private final DaVinciConfig daVinciConfig;
  private final CachingDaVinciClientFactory daVinciClientFactory;
  private final DaVinciClient<Object, Object> daVinciClient;
  private boolean isStarted = false;
  private final CountDownLatch startLatch = new CountDownLatch(1);

  private Set<Integer> subscribedPartitions = new HashSet<>();

  public BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter pubSubConsumer,
      String consumerId) {
    super(changelogClientConfig, pubSubConsumer);
    this.changelogClientConfig = changelogClientConfig;
    this.storeName = changelogClientConfig.getStoreName();
    this.daVinciConfig = new DaVinciConfig();
    ClientConfig innerClientConfig = changelogClientConfig.getInnerClientConfig();

    // ToDo: Determine default capacity and make configurable by the user
    this.pubSubMessages = new ArrayBlockingQueue<>(1000);
    this.partitionToVersionToServe = new ConcurrentHashMap<>();

    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(DaVinciRecordTransformerBootstrappingChangelogConsumer::new)
        .build();
    this.daVinciConfig.setRecordTransformerConfig(recordTransformerConfig);

    this.daVinciClientFactory = new CachingDaVinciClientFactory(
        changelogClientConfig.getD2Client(),
        changelogClientConfig.getD2ServiceName(),
        innerClientConfig.getMetricsRepository(),
        buildVeniceConfig(changelogClientConfig.getBootstrapFileSystemPath()));

    if (innerClientConfig.isSpecificClient()) {
      this.daVinciClient = this.daVinciClientFactory
          .getSpecificAvroClient(this.storeName, this.daVinciConfig, innerClientConfig.getSpecificValueClass());
    } else {
      this.daVinciClient = this.daVinciClientFactory.getGenericAvroClient(this.storeName, this.daVinciConfig);
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
         * When this latch gets released, that means there's at least one message in pubSubMessages. So when the user
         * calls poll, they don't get an empty response. This also signals that blob transfer was completed
         * for at least one partition.
         */
        startLatch.await();
      } catch (InterruptedException e) {
        // Restore the interrupt status
        Thread.currentThread().interrupt();
      }
      return null;
    });
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
      Thread.sleep(timeoutInMs);
    } catch (InterruptedException e) {
      // Restore the interrupt status
      Thread.currentThread().interrupt();
    }

    List<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> drainedPubSubMessages = new ArrayList<>();
    pubSubMessages.drainTo(drainedPubSubMessages);
    return drainedPubSubMessages;
  }

  private void internalStart() {
    if (isStarted) {
      // throw new VeniceException("Bootstrapping Changelog client is already started!");
      return;
    }

    daVinciClient.start();
    isStarted = true;
  }

  private VeniceProperties buildVeniceConfig(String bootstrapFileSystemPath) {
    return new PropertyBuilder().put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, 4) // RocksDB
        // default config
        .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER, 20) // RocksDB default config
        .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER, 36) // RocksDB default config
        .put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION, 40)
        .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION, 60)
        .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION, 80)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, changelogClientConfig.getRocksDBBlockCacheSizeInBytes())
        .put(changelogClientConfig.getConsumerProperties())
        .put(DATA_BASE_PATH, bootstrapFileSystemPath)
        .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false)
        // Turning this off, so users don't subscribe to unwanted partitions automatically
        .put(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, false)
        .build();
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
          partitionToVersionToServe.put(partitionId, getStoreVersion());
        }

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
          pubSubMessages.put(
              new ImmutableChangeCapturePubSubMessage<>(
                  key,
                  changeEvent,
                  pubSubTopicPartitionMap.get(partitionId),
                  0,
                  0,
                  0,
                  false));
        } catch (InterruptedException e) {
          LOGGER.error("Thread was interrupted while putting a message into pubSubMessages", e);
          Thread.currentThread().interrupt();
        }
      }
      startLatch.countDown();
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

    @Override
    public void onVersionSwap(int currentVersion, int futureVersion, int partitionId) {
      partitionToVersionToServe.put(partitionId, futureVersion);
      LOGGER.info(
          "Swapped from version: {} to version: {} for partitionId: {}",
          currentVersion,
          futureVersion,
          partitionId);
    }

    @Override
    public void close() throws IOException {

    }
  }
}
