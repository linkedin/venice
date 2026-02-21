package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.START_OF_SEGMENT;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.davinci.repository.NativeMetadataRepository;
import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.davinci.store.memory.InMemoryStorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngineFactory;
import com.linkedin.davinci.utils.ChunkAssembler;
import com.linkedin.davinci.utils.InMemoryChunkAssembler;
import com.linkedin.davinci.utils.RocksDBChunkAssembler;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.StoreVersionNotFoundException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroSpecificStoreDeserializerCache;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.VeniceView;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceChangelogConsumerImpl<K, V> implements VeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceChangelogConsumerImpl.class);
  private static final int MAX_SUBSCRIBE_RETRIES = 5;
  private static final String ROCKSDB_BUFFER_FOLDER = "rocksdb-chunk-buffer";
  protected long subscribeTime = Long.MAX_VALUE;

  protected final ReadWriteLock subscriptionLock = new ReentrantReadWriteLock();

  protected final CompressorFactory compressorFactory = new CompressorFactory();

  protected final VeniceConcurrentHashMap<String, VeniceCompressor> pubSubTopicNameToCompressorMap =
      new VeniceConcurrentHashMap<>();
  protected StoreDeserializerCache<V> storeDeserializerCache;

  protected NativeMetadataRepositoryViewAdapter storeRepository;

  protected final Map<Integer, Boolean> partitionToBootstrapState = new VeniceConcurrentHashMap<>();
  protected final long startTimestamp;

  protected final AtomicBoolean isSubscribed = new AtomicBoolean(false);

  protected final RecordDeserializer<K> keyDeserializer;

  protected final String storeName;

  protected final PubSubConsumerAdapter pubSubConsumer;
  protected final PubSubTopicRepository pubSubTopicRepository;
  protected final PubSubMessageDeserializer pubSubMessageDeserializer;
  protected final ExecutorService seekExecutorService;

  protected final Map<Integer, Long> currentVersionLastHeartbeat = new VeniceConcurrentHashMap<>();

  protected final ChangelogClientConfig changelogClientConfig;

  protected final ChunkAssembler chunkAssembler;

  protected final BasicConsumerStats changeCaptureStats;
  protected final HeartbeatReporterThread heartbeatReporterThread;
  protected final VeniceConcurrentHashMap<Integer, AtomicLong> consumerSequenceIdGeneratorMap;
  protected final long consumerSequenceIdStartingValue;
  private final RocksDBStorageEngineFactory rocksDBStorageEngineFactory;
  private final VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory;
  protected final boolean versionSwapByControlMessage;
  protected final String clientRegionName;
  protected final int totalRegionCount;
  protected final long versionSwapTimeoutInMs;
  protected final AtomicReference<CountDownLatch> onGoingVersionSwapSignal = new AtomicReference<>();
  /**
   * Interaction of this field should acquire the subscriptionLock.writeLock()
   */
  protected volatile VersionSwapMessageState versionSwapMessageState = null;
  protected Time time;

  public VeniceChangelogConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter pubSubConsumer,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory) {
    this(
        changelogClientConfig,
        pubSubConsumer,
        pubSubMessageDeserializer,
        System.nanoTime(),
        veniceChangelogConsumerClientFactory);
  }

  VeniceChangelogConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter pubSubConsumer,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      long consumerSequenceIdStartingValue,
      VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory) {
    Objects.requireNonNull(changelogClientConfig, "ChangelogClientConfig cannot be null");
    this.veniceChangelogConsumerClientFactory = veniceChangelogConsumerClientFactory;
    this.pubSubConsumer = pubSubConsumer;
    PubSubContext pubSubContext = changelogClientConfig.getPubSubContext();
    this.pubSubTopicRepository = pubSubContext.getPubSubTopicRepository();
    this.pubSubMessageDeserializer = pubSubMessageDeserializer;
    this.versionSwapByControlMessage = changelogClientConfig.isVersionSwapByControlMessageEnabled();
    this.totalRegionCount = changelogClientConfig.getTotalRegionCount();
    this.clientRegionName = changelogClientConfig.getClientRegionName();
    this.versionSwapTimeoutInMs = changelogClientConfig.getVersionSwapTimeoutInMs();
    this.time = new SystemTime();
    this.onGoingVersionSwapSignal.set(new CountDownLatch(0));
    if (versionSwapByControlMessage) {
      // Version swap related configs should all be resolved or explicitly set at this point.
      if (this.clientRegionName.isEmpty()) {
        throw new VeniceException(
            "Failed to enable version swap by control message because client region name is missing");
      }
      if (this.totalRegionCount <= 0) {
        throw new VeniceException(
            "Failed to enable version swap by control message because total region count is not set");
      }
      LOGGER.info(
          "VeniceChangelogConsumer version swap by control message is enabled. Client region name: {}, total region count: {}",
          clientRegionName,
          totalRegionCount);
    }

    seekExecutorService = Executors.newFixedThreadPool(10, new DaemonThreadFactory(getClass().getSimpleName()));

    if (changelogClientConfig.getViewName() == null || changelogClientConfig.getViewName().isEmpty()) {
      this.storeName = changelogClientConfig.getStoreName();
      rocksDBStorageEngineFactory = null;
      // The in memory storage engine only relies on the name of store and nothing else. We use an unversioned store
      // name
      // here in order to reduce confusion (as this storage engine can be used across version topics).
      InMemoryStorageEngine inMemoryStorageEngine = new InMemoryStorageEngine(storeName);
      this.chunkAssembler = new InMemoryChunkAssembler(inMemoryStorageEngine);
    } else {
      this.storeName =
          VeniceView.getViewStoreName(changelogClientConfig.getStoreName(), changelogClientConfig.getViewName());
      Properties rocksDBBufferProperties = new Properties();
      String rocksDBBufferPath = changelogClientConfig.getBootstrapFileSystemPath();
      if (rocksDBBufferPath == null || rocksDBBufferPath.isEmpty()) {
        throw new VeniceException("bootstrapFileSystemPath must be configured for consuming view store: " + storeName);
      }
      // Create a new folder in user provided path so if the path contains other important files we don't drop it.
      rocksDBBufferProperties
          .put(DATA_BASE_PATH, RocksDBUtils.composeStoreDbDir(rocksDBBufferPath, ROCKSDB_BUFFER_FOLDER));
      // These properties are required to build a VeniceServerConfig but is never used by RocksDBStorageEngineFactory.
      // Instead of setting these configs, we could refactor RocksDBStorageEngineFactory to take a more generic config.
      rocksDBBufferProperties.put(CLUSTER_NAME, "");
      rocksDBBufferProperties.put(ZOOKEEPER_ADDRESS, "");
      rocksDBBufferProperties
          .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, changelogClientConfig.getRocksDBBlockCacheSizeInBytes());
      rocksDBBufferProperties
          .put(KAFKA_BOOTSTRAP_SERVERS, changelogClientConfig.getConsumerProperties().get(KAFKA_BOOTSTRAP_SERVERS));
      VeniceProperties rocksDBBufferVeniceProperties = new VeniceProperties(rocksDBBufferProperties);
      VeniceServerConfig serverConfig = new VeniceServerConfig(rocksDBBufferVeniceProperties);
      rocksDBStorageEngineFactory = new RocksDBStorageEngineFactory(serverConfig);
      // Compose a view store version 0 for the RocksDBStorageEngine used for the RocksDBChunkAssembler.
      VeniceStoreVersionConfig storeVersionConfig = new VeniceStoreVersionConfig(
          Version.composeKafkaTopic(storeName, 0),
          rocksDBBufferVeniceProperties,
          PersistenceType.ROCKS_DB);
      this.chunkAssembler = new RocksDBChunkAssembler(
          rocksDBStorageEngineFactory.getStorageEngine(storeVersionConfig),
          changelogClientConfig.shouldSkipFailedToAssembleRecords());
    }

    if (changelogClientConfig.getInnerClientConfig().getMetricsRepository() != null) {
      this.changeCaptureStats = new BasicConsumerStats(
          changelogClientConfig.getInnerClientConfig().getMetricsRepository(),
          "vcc-" + changelogClientConfig.getConsumerName(),
          storeName);
    } else {
      changeCaptureStats = null;
    }
    heartbeatReporterThread = new HeartbeatReporterThread();
    this.changelogClientConfig = ChangelogClientConfig.cloneConfig(changelogClientConfig);
    SchemaReader schemaReader = changelogClientConfig.getSchemaReader();
    Schema keySchema = schemaReader.getKeySchema();
    this.keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
    this.startTimestamp = System.currentTimeMillis();
    this.consumerSequenceIdGeneratorMap = new VeniceConcurrentHashMap<>();
    this.consumerSequenceIdStartingValue = consumerSequenceIdStartingValue;
    LOGGER.info(
        "VeniceChangelogConsumer with consumer name: {} created at timestamp: {} with consumer sequence id starting at: {}",
        changelogClientConfig.getConsumerName(),
        startTimestamp,
        consumerSequenceIdStartingValue);

    changelogClientConfig.getConsumerProperties()
        .put(
            CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS,
            String.valueOf(changelogClientConfig.getVersionSwapDetectionIntervalTimeInSeconds()));
    NativeMetadataRepository repository = NativeMetadataRepository.getInstance(
        changelogClientConfig.getInnerClientConfig(),
        new VeniceProperties(changelogClientConfig.getConsumerProperties()),
        null);
    repository.start();
    this.storeRepository = new NativeMetadataRepositoryViewAdapter(repository);
    if (changelogClientConfig.getInnerClientConfig().isSpecificClient()) {
      Class valueClass = changelogClientConfig.getInnerClientConfig().getSpecificValueClass();
      this.storeDeserializerCache = new AvroSpecificStoreDeserializerCache<>(storeRepository, storeName, valueClass);
      LOGGER.info("Using specific value deserializer: {}", valueClass.getName());
    } else {
      this.storeDeserializerCache = new AvroStoreDeserializerCache<>(storeRepository, storeName, true);
      LOGGER.info("Using generic value deserializer");
    }

    LOGGER.info("Start a change log consumer client for store: {}", storeName);
  }

  public void throwIfReadsDisabled() {
    Store store = getStore();
    if (!store.isEnableReads()) {
      throw new StoreDisabledException(store.getName(), "read");
    }
  }

  // Unit test only and read only
  VersionSwapMessageState getVersionSwapMessageState() {
    return this.versionSwapMessageState;
  }

  @Override
  public int getPartitionCount() {
    Store store = getStore();
    return store.getVersion(store.getCurrentVersion()).getPartitionCount();
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    for (int partition: partitions) {
      getPartitionToBootstrapState().put(partition, false);
    }
    subscribeTime = time.getMilliseconds();
    return internalSubscribe(partitions, null);
  }

  protected CompletableFuture<Void> internalSubscribe(Set<Integer> partitions, PubSubTopic topic) {
    throwIfReadsDisabled();
    return CompletableFuture.supplyAsync(() -> {
      try {
        for (int i = 0; i <= MAX_SUBSCRIBE_RETRIES; i++) {
          try {
            storeRepository.subscribe(storeName);
            break;
          } catch (Exception ex) {
            if (i < MAX_SUBSCRIBE_RETRIES) {
              LOGGER.error("Store Repository subscription failed!  Will Retry...", ex);
            } else {
              LOGGER.error("Store Repository subscription failed! Aborting!!", ex);
              throw ex;
            }
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (versionSwapByControlMessage) {
        boolean lockAcquiredAndNoOngoingVersionSwap = false;
        for (int i = 0; i <= MAX_SUBSCRIBE_RETRIES; i++) {
          // If version swap is in progress, wait for it to finish
          try {
            onGoingVersionSwapSignal.get().await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          subscriptionLock.writeLock().lock();
          if (versionSwapMessageState != null) {
            // A new version swap is in progress, wait for it to finish again
            subscriptionLock.writeLock().unlock();
          } else {
            // No version swap is in progress, proceed with subscription
            lockAcquiredAndNoOngoingVersionSwap = true;
            break;
          }
        }
        if (!lockAcquiredAndNoOngoingVersionSwap) {
          // This should be extremely rare where the subscribe request is constantly conflicting with new version swaps
          throw new VeniceException("Unable to subscribe to new partitions due to conflicting version swaps");
        }
      } else {
        subscriptionLock.writeLock().lock();
      }

      try {
        PubSubTopic topicToSubscribe;
        if (topic == null) {
          topicToSubscribe = getCurrentServingVersionTopic();
        } else {
          topicToSubscribe = topic;
        }
        Set<PubSubTopicPartition> topicPartitionSet = getTopicAssignment();
        for (PubSubTopicPartition topicPartition: topicPartitionSet) {
          if (partitions.contains(topicPartition.getPartitionNumber())) {
            pubSubConsumer.unSubscribe(topicPartition);
          }
        }

        List<PubSubTopicPartition> topicPartitionListToSeek =
            getPartitionListToSubscribe(partitions, Collections.EMPTY_SET, topicToSubscribe);
        for (PubSubTopicPartition topicPartition: topicPartitionListToSeek) {
          pubSubConsumer.subscribe(topicPartition, PubSubSymbolicPosition.EARLIEST);
          currentVersionLastHeartbeat.put(topicPartition.getPartitionNumber(), System.currentTimeMillis());
        }
      } finally {
        subscriptionLock.writeLock().unlock();
      }
      if (changeCaptureStats != null) {
        if (!heartbeatReporterThread.isAlive()) {
          heartbeatReporterThread.start();
        }
      }
      isSubscribed.set(true);
      return null;
    }, seekExecutorService);
  }

  protected VeniceCompressor getVersionCompressor(PubSubTopic pubSubTopic) {
    String topicName = pubSubTopic.getName();

    return pubSubTopicNameToCompressorMap.computeIfAbsent(topicName, ignore -> {
      Store store = storeRepository.getStore(storeName);
      int storeVersion = Version.parseVersionFromKafkaTopicName(topicName);
      Version version = store.getVersionOrThrow(storeVersion);
      VeniceCompressor compressor;

      if (CompressionStrategy.ZSTD_WITH_DICT.equals(version.getCompressionStrategy())) {
        compressor = compressorFactory.getVersionSpecificCompressor(topicName);
        if (compressor == null) {
          // we need to retrieve the dictionary from the kafka topic
          ByteBuffer dictionary = DictionaryUtils.readDictionaryFromKafka(
              topicName,
              new VeniceProperties(changelogClientConfig.getConsumerProperties()),
              pubSubMessageDeserializer);
          compressor = compressorFactory.createVersionSpecificCompressorIfNotExist(
              version.getCompressionStrategy(),
              topicName,
              ByteUtils.extractByteArray(dictionary));
        }
      } else {
        compressor = compressorFactory.getCompressor(version.getCompressionStrategy());
      }
      return compressor;
    });
  }

  @Override
  public CompletableFuture<Void> seekToBeginningOfPush(Set<Integer> partitions) {
    // Get latest version topic
    PubSubTopic topic = getCurrentServingVersionTopic();
    return internalSeek(partitions, topic, p -> pubSubConsumer.subscribe(p, PubSubSymbolicPosition.EARLIEST));
  }

  @Override
  public CompletableFuture<Void> seekToBeginningOfPush() {
    Set<PubSubTopicPartition> assignments = getTopicAssignment();
    synchronized (assignments) {
      return seekToBeginningOfPush(
          assignments.stream().map(topicPartition -> topicPartition.getPartitionNumber()).collect(Collectors.toSet()));
    }
  }

  @Override
  public CompletableFuture<Void> seekToEndOfPush(Set<Integer> partitions) {
    PubSubTopic topic = getCurrentServingVersionTopic();
    return internalSeek(partitions, topic, p -> pubSubConsumer.subscribe(p, PubSubSymbolicPosition.EARLIEST));
  }

  @Override
  public void pause() {
    Set<PubSubTopicPartition> assignments = getTopicAssignment();
    synchronized (assignments) {
      this.pause(
          getTopicAssignment().stream().map(PubSubTopicPartition::getPartitionNumber).collect(Collectors.toSet()));
    }
  }

  @Override
  public void resume(Set<Integer> partitions) {
    // We're not changing the subscription set, so we can just lock the read lock
    subscriptionLock.readLock().lock();
    try {
      Set<PubSubTopicPartition> currentSubscriptions = getTopicAssignment();
      for (PubSubTopicPartition partition: currentSubscriptions) {
        if (partitions.contains(partition.getPartitionNumber())) {
          pubSubConsumer.resume(partition);
        }
      }
    } finally {
      subscriptionLock.readLock().unlock();
    }
  }

  @Override
  public void resume() {
    Set<PubSubTopicPartition> assignments = getTopicAssignment();
    synchronized (assignments) {
      this.resume(
          getTopicAssignment().stream().map(PubSubTopicPartition::getPartitionNumber).collect(Collectors.toSet()));
    }
  }

  @Override
  public void pause(Set<Integer> partitions) {
    subscriptionLock.readLock().lock();
    try {
      Set<PubSubTopicPartition> currentSubscriptions = getTopicAssignment();
      synchronized (currentSubscriptions) {
        for (PubSubTopicPartition partition: currentSubscriptions) {
          if (partitions.contains(partition.getPartitionNumber())) {
            pubSubConsumer.pause(partition);
          }
        }
      }
    } finally {
      subscriptionLock.readLock().unlock();
    }
  }

  @Override
  public CompletableFuture<Void> seekToEndOfPush() {
    Set<PubSubTopicPartition> assignments = getTopicAssignment();
    synchronized (assignments) {
      return seekToEndOfPush(
          assignments.stream().map(topicPartition -> topicPartition.getPartitionNumber()).collect(Collectors.toSet()));
    }
  }

  @Override
  public CompletableFuture<Void> seekToTail(Set<Integer> partitions) {
    PubSubTopic topic = getCurrentServingVersionTopic();
    return internalSeek(partitions, topic, p -> pubSubConsumerSeek(p, pubSubConsumer.endPosition(p)));
  }

  protected PubSubTopic getCurrentServingVersionTopic() {
    Store store = storeRepository.getStore(storeName);
    int currentVersion = store.getCurrentVersion();
    if (changelogClientConfig.getViewName() == null || changelogClientConfig.getViewName().isEmpty()) {
      return pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, currentVersion));
    }

    return pubSubTopicRepository.getTopic(store.getVersion(currentVersion).kafkaTopicName());
  }

  @Override
  public CompletableFuture<Void> seekToTail() {
    Set<PubSubTopicPartition> assignments = getTopicAssignment();
    synchronized (assignments) {
      return seekToTail(assignments.stream().map(PubSubTopicPartition::getPartitionNumber).collect(Collectors.toSet()));
    }
  }

  @Override
  public CompletableFuture<Void> seekToCheckpoint(Set<VeniceChangeCoordinate> checkpoints)
      throws VeniceCoordinateOutOfRangeException {
    return CompletableFuture.supplyAsync(() -> {
      synchronousSeekToCheckpoint(checkpoints);
      return null;
    }, seekExecutorService);
  }

  protected void synchronousSeekToCheckpoint(Set<VeniceChangeCoordinate> checkpoints) {
    for (VeniceChangeCoordinate coordinate: checkpoints) {
      checkLiveVersion(coordinate.getTopic());
      PubSubTopic topic = pubSubTopicRepository.getTopic(coordinate.getTopic());
      PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, coordinate.getPartition());
      synchronousSeek(
          Collections.singleton(coordinate.getPartition()),
          topic,
          foo -> pubSubConsumerSeek(pubSubTopicPartition, coordinate.getPosition()));
      pubSubTopicNameToCompressorMap.remove(topic.getName());
    }
  }

  void checkLiveVersion(String topicName) {
    Store store = storeRepository.getStore(storeName);
    try {
      store.getVersionOrThrow(Version.parseVersionFromKafkaTopicName(topicName));
    } catch (StoreVersionNotFoundException ex) {
      throw new VeniceCoordinateOutOfRangeException("Checkpoint is off retention!  Version has been deprecated...", ex);
    }
  }

  private void pubSubConsumerSeek(PubSubTopicPartition topicPartition, PubSubPosition offset)
      throws VeniceCoordinateOutOfRangeException {
    try {
      subscriptionLock.writeLock().lock();
      try {
        pubSubConsumer.subscribe(topicPartition, offset, true);
      } finally {
        subscriptionLock.writeLock().unlock();
      }
    } catch (PubSubTopicDoesNotExistException ex) {
      throw new VeniceCoordinateOutOfRangeException(
          "Version does not exist! Checkpoint contained version: " + topicPartition.getTopicName() + " for partition "
              + topicPartition.getPartitionNumber() + "please seek to beginning!",
          ex);
    }
    LOGGER.info("Topic partition: {} consumer seek to offset: {}", topicPartition, offset);
  }

  @Override
  public CompletableFuture<Void> subscribeAll() {
    return this.subscribe(IntStream.range(0, getPartitionCount()).boxed().collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps) {
    return internalSeekToTimestamps(timestamps);
  }

  public CompletableFuture<Void> internalSeekToTimestamps(Map<Integer, Long> timestamps) {
    return internalSeekToTimestamps(timestamps, LOGGER);
  }

  public CompletableFuture<Void> internalSeekToTimestamps(Map<Integer, Long> timestamps, Logger logger) {
    PubSubTopic topic = getCurrentServingVersionTopic();
    Map<PubSubTopicPartition, Long> topicPartitionLongMap = new HashMap<>();
    for (Map.Entry<Integer, Long> timestampPair: timestamps.entrySet()) {
      PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, timestampPair.getKey());
      topicPartitionLongMap.put(topicPartition, timestampPair.getValue());
    }
    return internalSeek(timestamps.keySet(), topic, partition -> {
      try {
        PubSubPosition offset = pubSubConsumer.getPositionByTimestamp(partition, topicPartitionLongMap.get(partition));
        // As the offset for this timestamp does not exist, we need to seek to the very end of the topic partition.
        if (offset == null) {
          offset = pubSubConsumer.endPosition(partition);
        }
        pubSubConsumerSeek(partition, offset);
      } catch (Exception e) {
        logger.error(
            "Encounter unexpected error trying to seek to timestamp for topic partition: {}, timestamp: {}",
            Utils.getReplicaId(partition),
            topicPartitionLongMap.get(partition),
            e);
        throw e;
      }
    });
  }

  @Override
  public CompletableFuture<Void> seekToTimestamp(Long timestamp) {
    Set<PubSubTopicPartition> topicPartitionSet = getTopicAssignment();
    Map<Integer, Long> partitionsToSeek = new HashMap<>();
    synchronized (topicPartitionSet) {
      for (PubSubTopicPartition partition: topicPartitionSet) {
        partitionsToSeek.put(partition.getPartitionNumber(), timestamp);
      }
    }
    return this.seekToTimestamps(partitionsToSeek);
  }

  @Override
  public boolean isCaughtUp() {
    return getPartitionToBootstrapState().values().stream().allMatch(x -> x);
  }

  Map<Integer, Boolean> getPartitionToBootstrapState() {
    return partitionToBootstrapState;
  }

  // TODO: We can probably delete this function with some refactoring
  protected CompletableFuture<Void> internalSeek(
      Set<Integer> partitions,
      PubSubTopic targetTopic,
      SeekFunction seekAction) {
    return CompletableFuture.supplyAsync(() -> {
      synchronousSeek(partitions, targetTopic, seekAction);
      return null;
    }, seekExecutorService);
  }

  protected void synchronousSeek(Set<Integer> partitions, PubSubTopic targetTopic, SeekFunction seekAction) {
    subscriptionLock.writeLock().lock();
    try {
      // Prune out current subscriptions
      Set<PubSubTopicPartition> assignments = getTopicAssignment();
      for (PubSubTopicPartition topicPartition: assignments) {
        if (partitions.contains(topicPartition.getPartitionNumber())) {
          pubSubConsumer.unSubscribe(topicPartition);
        }
      }

      List<PubSubTopicPartition> topicPartitionListToSeek =
          getPartitionListToSubscribe(partitions, Collections.EMPTY_SET, targetTopic);

      for (PubSubTopicPartition topicPartition: topicPartitionListToSeek) {
        seekAction.apply(topicPartition);
      }
    } finally {
      subscriptionLock.writeLock().unlock();
    }
  }

  @FunctionalInterface
  interface SeekFunction {
    void apply(PubSubTopicPartition partitionToSeek) throws VeniceCoordinateOutOfRangeException;
  }

  protected List<PubSubTopicPartition> getPartitionListToSubscribe(
      Set<Integer> partitions,
      Set<PubSubTopicPartition> topicPartitionSet,
      PubSubTopic topic) {
    List<PubSubTopicPartition> topicPartitionList = new ArrayList<>();
    for (Integer partition: partitions) {
      PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, partition);
      if (!topicPartitionSet.contains(topicPartition)) {
        topicPartitionList.add(topicPartition);
      }
    }
    topicPartitionList.addAll(topicPartitionSet);
    return topicPartitionList;
  }

  @Override
  public void unsubscribe(Set<Integer> partitions) {
    internalUnsubscribe(partitions, false);
  }

  protected void internalUnsubscribe(Set<Integer> partitions, boolean isForVersionSwap) {
    if (partitions.isEmpty()) {
      return;
    }
    // write lock
    subscriptionLock.writeLock().lock();
    try {
      Set<PubSubTopicPartition> topicPartitionSet = getTopicAssignment();
      Set<PubSubTopicPartition> topicPartitionsToUnsub = new HashSet<>();
      for (PubSubTopicPartition topicPartition: topicPartitionSet) {
        if (partitions.contains(topicPartition.getPartitionNumber())) {
          topicPartitionsToUnsub.add(topicPartition);
          currentVersionLastHeartbeat.remove(topicPartition.getPartitionNumber());
        }
      }
      pubSubConsumer.batchUnsubscribe(topicPartitionsToUnsub);
      if (versionSwapMessageState != null && !isForVersionSwap) {
        versionSwapMessageState.handleUnsubscribe(partitions);
      }
    } finally {
      subscriptionLock.writeLock().unlock();
    }
  }

  @Override
  public void unsubscribeAll() {
    Set<Integer> allPartitions = new HashSet<>();
    for (int partition = 0; partition < getPartitionCount(); partition++) {
      allPartitions.add(partition);
    }
    this.unsubscribe(allPartitions);
  }

  private Store getStore() {
    try {
      storeRepository.subscribe(storeName);
    } catch (InterruptedException e) {
      throw new VeniceException("Failed to get store info with exception:", e);
    }
    return storeRepository.getStore(storeName);
  }

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs) {
    return internalPoll(timeoutInMs);
  }

  protected Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> internalPoll(
      long timeoutInMs,
      boolean includeControlMessage) {
    throwIfReadsDisabled();
    Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubMessages = new ArrayList<>();
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messagesMap;
    boolean lockAcquired = false;

    try {
      try {
        // we need an exclusive lock to coordinate multi-step subscription operations and maintain state consistency
        lockAcquired = subscriptionLock.writeLock().tryLock(timeoutInMs, TimeUnit.MILLISECONDS);

        /*
         * If the lock acquisition fails, don't invoke poll on the pubSubConsumer.
         * The lock ensures atomic coordination of subscription state changes and prevents
         * inconsistent state if multiple threads attempt to use this function concurrently.
         */
        if (!lockAcquired) {
          return Collections.emptyList();
        }

        if (versionSwapByControlMessage && versionSwapMessageState != null) {
          /*
           * If version swap by control message is enabled and the consumer is undergoing version swap we need to check
           * and act on two scenarios:
           * 1. If all version swap messages have been received for all partitions, we need to seek to the new topic.
           * 2. If we have reached the timeout for the version swap, we need to forcefully seek to the new topic using
           * the EOP positions for any remaining partitions as our backup plan. See javadoc of
           * VersionSwapMessageState.getVersionSwapStartTimestamp() for more details.
           */
          if (versionSwapMessageState.isVersionSwapMessagesReceivedForAllPartitions()) {
            if (isNewVersionCheckpointsReady(timeoutInMs)) {
              synchronousSeekToCheckpoint(versionSwapMessageState.getNewTopicVersionSwapCheckpoints());
              LOGGER.info(
                  "Version swap completed from topic: {} to topic: {}, generation id: {}",
                  versionSwapMessageState.getOldVersionTopic(),
                  versionSwapMessageState.getNewVersionTopic(),
                  versionSwapMessageState.getVersionSwapGenerationId());
              changeCaptureStats.emitVersionSwapCountMetrics(SUCCESS);
              versionSwapMessageState = null;
              onGoingVersionSwapSignal.get().countDown();
            } else {
              return Collections.emptyList();
            }
          } else if (time.getMilliseconds()
              - versionSwapMessageState.getVersionSwapStartTimestamp() > versionSwapTimeoutInMs) {
            if (!getTopicAssignment().isEmpty()) {
              internalUnsubscribe(versionSwapMessageState.getIncompletePartitions(), true);
            }
            if (isNewVersionCheckpointsReady(timeoutInMs)) {
              synchronousSeekToCheckpoint(versionSwapMessageState.getNewTopicCheckpointsWithEOPAsBackup());
              LOGGER.info(
                  "Version swap completed after timeout from topic: {} to topic: {}, generation id: {}. Partitions: {} are seeked to EOP positions.",
                  versionSwapMessageState.getOldVersionTopic(),
                  versionSwapMessageState.getNewVersionTopic(),
                  versionSwapMessageState.getVersionSwapGenerationId(),
                  versionSwapMessageState.getIncompletePartitions());
              changeCaptureStats.emitVersionSwapCountMetrics(SUCCESS);
              versionSwapMessageState = null;
              onGoingVersionSwapSignal.get().countDown();
            } else {
              LOGGER.warn(
                  "Version swap from topic: {} to topic: {}, generation id: {} already timed out but still unable to find new topic checkpoints to go to.",
                  versionSwapMessageState.getOldVersionTopic(),
                  versionSwapMessageState.getNewVersionTopic(),
                  versionSwapMessageState.getVersionSwapGenerationId());
              return Collections.emptyList();
            }
          }
        }

        messagesMap = pubSubConsumer.poll(timeoutInMs);
        for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messagesMap.entrySet()) {
          PubSubTopicPartition pubSubTopicPartition = entry.getKey();
          List<DefaultPubSubMessage> messageList = entry.getValue();
          for (DefaultPubSubMessage message: messageList) {
            maybeUpdatePartitionToBootstrapMap(message, pubSubTopicPartition);
            if (message.getKey().isControlMessage()) {
              ControlMessage controlMessage = (ControlMessage) message.getValue().getPayloadUnion();
              if (handleControlMessage(
                  controlMessage,
                  pubSubTopicPartition,
                  message.getKey().getKey(),
                  message.getValue().getProducerMetadata().getMessageTimestamp(),
                  message.getPosition())) {
                break;
              }
              if (includeControlMessage) {
                pubSubMessages.add(
                    new ImmutableChangeCapturePubSubMessage<>(
                        null,
                        null,
                        message.getTopicPartition(),
                        message.getPosition(),
                        0,
                        0,
                        false,
                        getNextConsumerSequenceId(message.getPartition())));
              }

            } else {
              Optional<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubMessage =
                  convertPubSubMessageToChangeEvent(message, pubSubTopicPartition);
              pubSubMessage.ifPresent(pubSubMessages::add);
            }
          }
        }
      } catch (InterruptedException exception) {
        LOGGER.info("Thread was interrupted", exception);
        // Restore the interrupt status
        Thread.currentThread().interrupt();
      } finally {
        if (lockAcquired) {
          subscriptionLock.writeLock().unlock();
        }
      }

      int messagesPolled = pubSubMessages.size();

      if (changelogClientConfig.shouldCompactMessages()) {
        Map<K, PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> tempMap = new LinkedHashMap<>();
        // The behavior of LinkedHashMap is such that it maintains the order of insertion, but for values which are
        // replaced, it's put in at the position of the first insertion. This isn't quite what we want, we want to keep
        // only a single key (just as a map would), but we want to keep the position of the last insertion as well
        // So in order to do that, we remove the entry before inserting it.
        for (PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> message: pubSubMessages) {
          if (tempMap.containsKey(message.getKey())) {
            tempMap.remove(message.getKey());
          }
          tempMap.put(message.getKey(), message);
        }
        pubSubMessages = tempMap.values();
      }

      if (changeCaptureStats != null) {
        changeCaptureStats.emitPollCountMetrics(SUCCESS);
        changeCaptureStats.emitRecordsConsumedCountMetrics(messagesPolled);
      }

      return pubSubMessages;
    } catch (Exception exception) {
      if (changeCaptureStats != null) {
        changeCaptureStats.emitPollCountMetrics(FAIL);
      }

      LOGGER.error("Encountered an exception when polling records for store: {}", storeName, exception);
      throw exception;
    }
  }

  private boolean isNewVersionCheckpointsReady(long timeoutInMs) throws InterruptedException {
    if (versionSwapMessageState == null) {
      return false;
    }
    try {
      versionSwapMessageState.getFindNewTopicCheckpointFuture().get(timeoutInMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeoutException) {
      // Still waiting for internalFindNewVersionCheckpoints to complete.
      return false;
    } catch (ExecutionException e) {
      // Re-attempt the seek but should report the error.
      LOGGER.warn(
          "Caught an exception when looking for corresponding checkpoints with generation id: {} in new topic: {}. Retrying.",
          versionSwapMessageState.getVersionSwapGenerationId(),
          versionSwapMessageState.getNewVersionTopic(),
          e);
      changeCaptureStats.emitVersionSwapCountMetrics(FAIL);
      versionSwapMessageState.setFindNewTopicCheckpointFuture(
          internalFindNewVersionCheckpoints(
              versionSwapMessageState.getOldVersionTopic(),
              versionSwapMessageState.getNewVersionTopic(),
              versionSwapMessageState.getVersionSwapGenerationId(),
              versionSwapMessageState.getAssignedPartitions()));
      return false;
    }
    return true;
  }

  void maybeUpdatePartitionToBootstrapMap(DefaultPubSubMessage message, PubSubTopicPartition pubSubTopicPartition) {
    if (getSubscribeTime() - message.getValue().producerMetadata.messageTimestamp <= TimeUnit.MINUTES.toMillis(1)) {
      getPartitionToBootstrapState().put(pubSubTopicPartition.getPartitionNumber(), true);
    }
  }

  protected Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> internalPoll(long timeoutInMs) {
    return internalPoll(timeoutInMs, false);
  }

  /**
   * Handle control message from the given topic. Returns true if a topic switch should occur and records should be returned
   *
   * @param controlMessage the encountered control message
   * @param pubSubTopicPartition the topic that this control message was concerned
   * @return true if a version switch happened, false if otherwise
   */
  protected boolean handleControlMessage(
      ControlMessage controlMessage,
      PubSubTopicPartition pubSubTopicPartition,
      byte[] key,
      long timestamp,
      PubSubPosition position) {
    ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    if (controlMessageType.equals(ControlMessageType.END_OF_PUSH)) {
      LOGGER.info(
          "End of Push message received for version {} for store {}",
          Version.parseVersionFromKafkaTopicName(pubSubTopicPartition.getPubSubTopic().getName()),
          storeName);
      return switchToNewTopic(pubSubTopicPartition);
    }

    if (versionSwapByControlMessage && controlMessageType.equals(ControlMessageType.VERSION_SWAP)
        && Version.isVersionTopic(pubSubTopicPartition.getTopicName())) {
      return handleVersionSwapMessageInVT(controlMessage, pubSubTopicPartition, position);
    }

    if (controlMessage.controlMessageType == START_OF_SEGMENT.getValue()
        && Arrays.equals(key, KafkaKey.HEART_BEAT.getKey())) {
      currentVersionLastHeartbeat.put(pubSubTopicPartition.getPartitionNumber(), timestamp);
    }
    return false;
  }

  /**
   * Converts a raw PubSub message into a change event message suitable for returning to the consumer.
   *
   * During a version swap, a low watermark approach is used for the {@link VeniceChangeCoordinate} returned
   * to prevent users from seeking in between a sequence of related version swap messages within a partition.
   */
  protected Optional<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> convertPubSubMessageToChangeEvent(
      DefaultPubSubMessage message,
      PubSubTopicPartition pubSubTopicPartition) {
    Optional<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubChangeEventMessage = Optional.empty();
    byte[] keyBytes = message.getKey().getKey();
    MessageType messageType = MessageType.valueOf(message.getValue());
    Object assembledObject = null;
    PubSubPosition returnedMessagePosition = message.getPosition();
    if (versionSwapMessageState != null) {
      PubSubPosition versionSwapLowWatermark = versionSwapMessageState
          .getVersionSwapLowWatermarkPosition(pubSubTopicPartition.getTopicName(), message.getPartition());
      if (versionSwapLowWatermark != null) {
        returnedMessagePosition = versionSwapLowWatermark;
      }
    }
    if (messageType.equals(MessageType.DELETE)) {
      // Deletes have a previous and current value of null. So just fill it in!
      ChangeEvent<V> changeEvent = new ChangeEvent<>(null, null);
      pubSubChangeEventMessage = Optional.of(
          new ImmutableChangeCapturePubSubMessage<>(
              keyDeserializer.deserialize(keyBytes),
              changeEvent,
              pubSubTopicPartition,
              returnedMessagePosition,
              message.getPubSubMessageTime(),
              message.getPayloadSize(),
              false,
              getNextConsumerSequenceId(message.getPartition())));
    } else if (messageType.equals(MessageType.PUT)) {
      Put put = (Put) message.getValue().payloadUnion;
      // Select appropriate reader schema and compressors
      int readerSchemaId;
      VeniceCompressor compressor = getVersionCompressor(pubSubTopicPartition.getPubSubTopic());
      // Use writer schema as the reader schema
      readerSchemaId = put.schemaId;

      ByteBufferValueRecord<ByteBuffer> assembledRecord;
      try {
        assembledRecord = chunkAssembler.bufferAndAssembleRecord(
            pubSubTopicPartition,
            put.getSchemaId(),
            keyBytes,
            put.getPutValue(),
            message.getPosition(),
            compressor);

        if (changeCaptureStats != null && ChunkAssembler.isChunkedRecord(put.getSchemaId())) {
          changeCaptureStats.emitChunkedRecordCountMetrics(SUCCESS);
        }

        if (assembledRecord == null) {
          // bufferAndAssembleRecord may have only buffered records and not returned anything yet because
          // it's waiting for more input. In this case, just return an empty optional for now.
          return Optional.empty();
        }
      } catch (Exception exception) {
        if (changeCaptureStats != null && ChunkAssembler.isChunkedRecord(put.getSchemaId())) {
          changeCaptureStats.emitChunkedRecordCountMetrics(FAIL);
        }

        LOGGER.error(
            "Encountered an exception when processing a record in ChunkAssembler for replica: {}",
            Utils.getReplicaId(pubSubTopicPartition),
            exception);
        throw exception;
      }

      if (readerSchemaId < 0) {
        // This was a chunk manifest and the actual writer schema needs to be retrieved
        readerSchemaId = assembledRecord.writerSchemaId();
      }
      RecordDeserializer deserializer;
      try {
        deserializer = storeDeserializerCache.getDeserializer(readerSchemaId, readerSchemaId);
      } catch (InvalidVeniceSchemaException invalidSchemaException) {
        // It's possible that a new schema was just added and our async metadata is outdated
        LOGGER.info("Refreshing the local metadata cache to try again", invalidSchemaException);
        storeRepository.refreshOneStore(storeName);
        deserializer = storeDeserializerCache.getDeserializer(readerSchemaId, readerSchemaId);
      }
      try {
        assembledObject = deserializer.deserialize(
            ChunkAssembler.decompressValueIfNeeded(assembledRecord.value(), put.getSchemaId(), compressor));
      } catch (IOException e) {
        throw new VeniceException(
            "Failed to deserialize or decompress record consumed from topic: "
                + pubSubTopicPartition.getPubSubTopic().getName(),
            e);
      }

      int payloadSize = message.getPayloadSize();
      ChangeEvent<V> changeEvent = new ChangeEvent<>(null, (V) assembledObject);
      pubSubChangeEventMessage = Optional.of(
          new ImmutableChangeCapturePubSubMessage<>(
              keyDeserializer.deserialize(keyBytes),
              changeEvent,
              pubSubTopicPartition,
              returnedMessagePosition,
              message.getPubSubMessageTime(),
              payloadSize,
              false,
              getNextConsumerSequenceId(message.getPartition())));
    }

    return pubSubChangeEventMessage;
  }

  protected boolean handleVersionSwapMessageInVT(
      ControlMessage controlMessage,
      PubSubTopicPartition pubSubTopicPartition,
      PubSubPosition position) {
    VersionSwap versionSwap = (VersionSwap) controlMessage.getControlMessageUnion();
    if (VersionSwapMessageState
        .isVersionSwapRelevant(pubSubTopicPartition.getTopicName(), clientRegionName, versionSwap)) {
      if (versionSwapMessageState == null) {
        Set<PubSubTopicPartition> currentAssignment = getTopicAssignment();
        versionSwapMessageState =
            new VersionSwapMessageState(versionSwap, totalRegionCount, currentAssignment, time.getMilliseconds());
        onGoingVersionSwapSignal.set(new CountDownLatch(1));
        LOGGER.info(
            "New version detected for store: {} through version swap messages. Performing version swap from topic: {} to topic: {}, generation id: {}",
            storeName,
            versionSwapMessageState.getOldVersionTopic(),
            versionSwapMessageState.getNewVersionTopic(),
            versionSwapMessageState.getVersionSwapGenerationId());
        versionSwapMessageState.setFindNewTopicCheckpointFuture(
            internalFindNewVersionCheckpoints(
                versionSwapMessageState.getOldVersionTopic(),
                versionSwapMessageState.getNewVersionTopic(),
                versionSwapMessageState.getVersionSwapGenerationId(),
                versionSwapMessageState.getAssignedPartitions()));
      }
      if (versionSwapMessageState.handleVersionSwap(versionSwap, pubSubTopicPartition, position)) {
        // Stop consuming from the old topic for this partition since we have consumed all the version swap messages.
        internalUnsubscribe(Collections.singleton(pubSubTopicPartition.getPartitionNumber()), true);
        return true;
      }
    }
    return false;
  }

  protected CompletableFuture<Void> internalFindNewVersionCheckpoints(
      String oldVersionTopic,
      String newVersionTopic,
      long generationId,
      Set<Integer> partitions) {
    throw new UnsupportedOperationException("internalSeekToNewVersion not supported by VeniceChangelogConsumerImpl");
  }

  protected Set<PubSubTopicPartition> getTopicAssignment() {
    return Collections.synchronizedSet(pubSubConsumer.getAssignment());
  }

  protected boolean switchToNewTopic(PubSubTopicPartition newTopicPartition) {
    PubSubTopic newTopic = newTopicPartition.getPubSubTopic();
    int partition = newTopicPartition.getPartitionNumber();
    Set<Integer> partitions = Collections.singleton(partition);
    Set<PubSubTopicPartition> assignment = getTopicAssignment();
    for (PubSubTopicPartition currentSubscribedPartition: assignment) {
      if (partition == currentSubscribedPartition.getPartitionNumber()) {
        if (newTopic.getName().equals(currentSubscribedPartition.getPubSubTopic().getName())) {
          // We're being asked to switch to a topic that we're already subscribed to, NoOp this
          return false;
        }
      }
    }
    unsubscribe(partitions);
    try {
      Set<VeniceChangeCoordinate> beginningOfNewTopic = new HashSet<>(partitions.size());
      beginningOfNewTopic
          .add(new VeniceChangeCoordinate(newTopic.getName(), PubSubSymbolicPosition.EARLIEST, partition));
      synchronousSeekToCheckpoint(beginningOfNewTopic);
    } catch (Exception e) {
      throw new VeniceException("Subscribe to new topic:" + newTopic + " is not successful", e);
    }
    return true;
  }

  @Override
  public void close() {
    LOGGER.info("Closing Changelog Consumer with name: {}", changelogClientConfig.getConsumerName());
    subscriptionLock.writeLock().lock();
    try {
      this.unsubscribeAll();
      pubSubConsumer.close();
      heartbeatReporterThread.interrupt();
      seekExecutorService.shutdown();
      compressorFactory.close();

      if (rocksDBStorageEngineFactory != null) {
        rocksDBStorageEngineFactory.close();
      }

      veniceChangelogConsumerClientFactory.deregisterClient(changelogClientConfig.getConsumerName());
      LOGGER.info("Closed Changelog Consumer with name: {}", changelogClientConfig.getConsumerName());
    } finally {
      subscriptionLock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  protected void setStoreRepository(NativeMetadataRepositoryViewAdapter repository) {
    this.storeRepository = repository;
    if (changelogClientConfig.getInnerClientConfig().isSpecificClient()) {
      // If a value class is supplied, we'll use a Specific record adapter
      Class valueClass = changelogClientConfig.getInnerClientConfig().getSpecificValueClass();
      this.storeDeserializerCache = new AvroSpecificStoreDeserializerCache<>(storeRepository, storeName, valueClass);
    } else {
      this.storeDeserializerCache = new AvroStoreDeserializerCache<>(storeRepository, storeName, true);
    }
  }

  protected Long getSubscribeTime() {
    return this.subscribeTime;
  }

  protected HeartbeatReporterThread getHeartbeatReporterThread() {
    return heartbeatReporterThread;
  }

  protected BasicConsumerStats getChangeCaptureStats() {
    return changeCaptureStats;
  }

  protected long getNextConsumerSequenceId(int partition) {
    AtomicLong consumerSequenceIdGenerator =
        consumerSequenceIdGeneratorMap.computeIfAbsent(partition, p -> new AtomicLong(consumerSequenceIdStartingValue));
    return consumerSequenceIdGenerator.incrementAndGet();
  }

  protected class HeartbeatReporterThread extends Thread {
    protected HeartbeatReporterThread() {
      super("Ingestion-Heartbeat-Reporter-Service-Thread");
      // To not block JVM shutdown
      setDaemon(true);
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          recordStats(getLastHeartbeatPerPartition(), changeCaptureStats, getTopicAssignment());
          TimeUnit.SECONDS.sleep(60L);
        } catch (InterruptedException e) {
          LOGGER.warn("Lag Monitoring thread interrupted!  Shutting down...", e);
          Thread.currentThread().interrupt(); // Restore the interrupt status
          break;
        }
      }
    }

    protected void recordStats(
        Map<Integer, Long> currentVersionLastHeartbeat,
        BasicConsumerStats changeCaptureStats,
        Set<PubSubTopicPartition> assignment) {

      long now = System.currentTimeMillis();
      long maxLag = Long.MIN_VALUE;
      for (Long heartBeatTimestamp: currentVersionLastHeartbeat.values()) {
        if (heartBeatTimestamp != null) {
          maxLag = Math.max(maxLag, now - heartBeatTimestamp);
        }
      }

      if (maxLag != Long.MIN_VALUE) {
        changeCaptureStats.emitHeartBeatDelayMetrics(maxLag);
      }

      int maxVersion = -1;
      int minVersion = Integer.MAX_VALUE;
      synchronized (assignment) {
        for (PubSubTopicPartition partition: assignment) {
          int version = Version.parseVersionFromKafkaTopicName(partition.getTopicName());
          maxVersion = Math.max(maxVersion, version);
          minVersion = Math.min(minVersion, version);
        }
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
}
