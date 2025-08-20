package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.START_OF_SEGMENT;
import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_POS;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.davinci.repository.NativeMetadataRepository;
import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.davinci.storage.chunking.SpecificRecordChunkingAdapter;
import com.linkedin.davinci.store.memory.InMemoryStorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngineFactory;
import com.linkedin.davinci.utils.ChunkAssembler;
import com.linkedin.davinci.utils.InMemoryChunkAssembler;
import com.linkedin.davinci.utils.RocksDBChunkAssembler;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.StoreVersionNotFoundException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.AvroSpecificStoreDeserializerCache;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.views.VeniceView;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceChangelogConsumerImpl<K, V> implements VeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceChangelogConsumerImpl.class);
  private static final int MAX_SUBSCRIBE_RETRIES = 5;
  private static final int MAX_VERSION_SWAP_RETRIES = 5;
  private static final String ROCKSDB_BUFFER_FOLDER = "rocksdb-chunk-buffer";
  protected long subscribeTime = Long.MAX_VALUE;

  protected final ReadWriteLock subscriptionLock = new ReentrantReadWriteLock();

  protected static final VeniceCompressor NO_OP_COMPRESSOR = new NoopCompressor();

  protected final CompressorFactory compressorFactory = new CompressorFactory();

  protected final HashMap<Integer, VeniceCompressor> compressorMap = new HashMap<>();
  protected StoreDeserializerCache<V> storeDeserializerCache;
  protected StoreDeserializerCache<GenericRecord> rmdDeserializerCache;
  protected Class specificValueClass;

  protected NativeMetadataRepositoryViewAdapter storeRepository;

  protected final AbstractAvroChunkingAdapter<V> userEventChunkingAdapter;

  protected final SchemaReader schemaReader;
  protected final Map<Integer, AtomicLong> partitionToPutMessageCount = new VeniceConcurrentHashMap<>();
  protected final Map<Integer, AtomicLong> partitionToDeleteMessageCount = new VeniceConcurrentHashMap<>();
  protected final Map<Integer, Boolean> partitionToBootstrapState = new VeniceConcurrentHashMap<>();
  protected final long startTimestamp;

  protected final AtomicBoolean isSubscribed = new AtomicBoolean(false);

  protected final RecordDeserializer<K> keyDeserializer;
  private final D2ControllerClient d2ControllerClient;
  private final RecordDeserializer<RecordChangeEvent> recordChangeDeserializer =
      FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(
          AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema(),
          RecordChangeEvent.class);
  protected final ReplicationMetadataSchemaRepository replicationMetadataSchemaRepository;

  protected final String storeName;

  protected final PubSubConsumerAdapter pubSubConsumer;
  protected final PubSubTopicRepository pubSubTopicRepository;
  protected final PubSubPositionDeserializer pubSubPositionDeserializer;
  protected final ExecutorService seekExecutorService;

  // This member is a map of maps in order to accommodate view topics. If the message we consume has the appropriate
  // footer then we'll use that to infer entry into the wrapped map and compare with it, otherwise we'll infer it from
  // the consumed partition for the given message. We do all this because for a view topic, it may have many
  // upstream RT partitions writing to a given view partition.
  protected final Map<Integer, Map<Integer, List<Long>>> currentVersionHighWatermarks = new VeniceConcurrentHashMap<>();
  protected final ConcurrentHashMap<Integer, Long> currentVersionLastHeartbeat = new VeniceConcurrentHashMap<>();

  protected final ChangelogClientConfig changelogClientConfig;

  protected final ChunkAssembler chunkAssembler;

  protected final BasicConsumerStats changeCaptureStats;
  protected final HeartbeatReporterThread heartbeatReporterThread;
  protected final VeniceConcurrentHashMap<Integer, AtomicLong> consumerSequenceIdGeneratorMap;
  protected final long consumerSequenceIdStartingValue;
  private final RocksDBStorageEngineFactory rocksDBStorageEngineFactory;

  public VeniceChangelogConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter pubSubConsumer) {
    this(changelogClientConfig, pubSubConsumer, System.nanoTime());
  }

  VeniceChangelogConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter pubSubConsumer,
      long consumerSequenceIdStartingValue) {
    Objects.requireNonNull(changelogClientConfig, "ChangelogClientConfig cannot be null");
    this.pubSubConsumer = pubSubConsumer;
    this.pubSubTopicRepository = changelogClientConfig.getPubSubTopicRepository();
    this.pubSubPositionDeserializer = changelogClientConfig.getPubSubPositionDeserializer();

    seekExecutorService = Executors.newFixedThreadPool(10);

    // TODO: putting the change capture case here is a little bit weird. The view abstraction should probably
    // accommodate
    // this case, but it doesn't seem that clean in this case. Change capture topics don't behave like view topics as
    // they only contain nearline data whereas views are full version topics. So for now it seems legit to have this
    // caveat, but we should perhaps think through this in the future if we think we'll have more nearline data only
    // cases for view topics. Change capture topic naming also doesn't follow the view_store_store_name_view_name
    // naming pattern, making tracking it even weirder.
    if (changelogClientConfig.getViewName() == null || changelogClientConfig.getViewName().isEmpty()
        || changelogClientConfig.isBeforeImageView()) {
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

    this.d2ControllerClient = changelogClientConfig.getD2ControllerClient();
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
    this.replicationMetadataSchemaRepository = new ReplicationMetadataSchemaRepository(d2ControllerClient);
    this.schemaReader = changelogClientConfig.getSchemaReader();
    Schema keySchema = schemaReader.getKeySchema();
    this.keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
    this.startTimestamp = System.currentTimeMillis();
    this.consumerSequenceIdGeneratorMap = new VeniceConcurrentHashMap<>();
    this.consumerSequenceIdStartingValue = consumerSequenceIdStartingValue;
    LOGGER.info(
        "VeniceChangelogConsumer created at timestamp: {} with consumer sequence id starting at: {}",
        startTimestamp,
        consumerSequenceIdStartingValue);

    Properties properties = new Properties();
    properties.put(
        CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS,
        String.valueOf(changelogClientConfig.getVersionSwapDetectionIntervalTimeInSeconds()));
    properties.put(
        CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY,
        String.valueOf(changelogClientConfig.isUseRequestBasedMetadataRepository()));
    NativeMetadataRepository repository = NativeMetadataRepository
        .getInstance(changelogClientConfig.getInnerClientConfig(), new VeniceProperties(properties), null);
    repository.start();
    this.storeRepository = new NativeMetadataRepositoryViewAdapter(repository);
    this.rmdDeserializerCache = new RmdDeserializerCache<>(replicationMetadataSchemaRepository, storeName, 1, false);
    if (changelogClientConfig.getInnerClientConfig().isSpecificClient()) {
      // If a value class is supplied, we'll use a Specific record adapter
      Class valueClass = changelogClientConfig.getInnerClientConfig().getSpecificValueClass();
      this.userEventChunkingAdapter = new SpecificRecordChunkingAdapter();
      this.storeDeserializerCache = new AvroSpecificStoreDeserializerCache<>(storeRepository, storeName, valueClass);
      this.specificValueClass = valueClass;
      LOGGER.info("Using specific value deserializer: {}", valueClass.getName());
    } else {
      this.userEventChunkingAdapter = GenericChunkingAdapter.INSTANCE;
      this.storeDeserializerCache = new AvroStoreDeserializerCache<>(storeRepository, storeName, true);
      this.specificValueClass = null;
      LOGGER.info("Using generic value deserializer");

    }

    LOGGER.info("Start a change log consumer client for store: {}", storeName);
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
    subscribeTime = System.currentTimeMillis();
    return internalSubscribe(partitions, null);
  }

  protected CompletableFuture<Void> internalSubscribe(Set<Integer> partitions, PubSubTopic topic) {
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

      PubSubTopic topicToSubscribe;
      if (topic == null) {
        topicToSubscribe = getCurrentServingVersionTopic();
      } else {
        topicToSubscribe = topic;
      }

      subscriptionLock.writeLock().lock();
      try {
        Set<PubSubTopicPartition> topicPartitionSet = getTopicAssignment();
        for (PubSubTopicPartition topicPartition: topicPartitionSet) {
          if (partitions.contains(topicPartition.getPartitionNumber())) {
            pubSubConsumer.unSubscribe(topicPartition);
          }
        }

        List<PubSubTopicPartition> topicPartitionList =
            getPartitionListToSubscribe(partitions, topicPartitionSet, topicToSubscribe);
        List<PubSubTopicPartition> topicPartitionListToSeek =
            getPartitionListToSubscribe(partitions, Collections.EMPTY_SET, topicToSubscribe);

        for (PubSubTopicPartition topicPartition: topicPartitionList) {
          // TODO: we do this because we don't populate the compressor into the change capture view topic, so we
          // take this opportunity to populate it. This could be worth revisiting by either populating the compressor
          // into view topics and consuming, or, expanding the interface to this function to have a compressor provider
          // (and thereby let other view implementations figure out what would be right).
          if (!topicPartition.getPubSubTopic().getName().endsWith(ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX)) {
            compressorMap.put(topicPartition.getPartitionNumber(), getVersionCompressor(topicPartition));
          }
        }
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

  protected VeniceCompressor getVersionCompressor(PubSubTopicPartition topicPartition) {
    Store store = storeRepository.getStore(storeName);
    String topicName = topicPartition.getPubSubTopic().getName();
    Version version = store.getVersionOrThrow(Version.parseVersionFromKafkaTopicName(topicName));
    VeniceCompressor compressor;
    if (CompressionStrategy.ZSTD_WITH_DICT.equals(version.getCompressionStrategy())) {
      compressor = compressorFactory.getVersionSpecificCompressor(topicName);
      if (compressor == null) {
        // we need to retrieve the dictionary from the kafka topic
        ByteBuffer dictionary = DictionaryUtils
            .readDictionaryFromKafka(topicName, new VeniceProperties(changelogClientConfig.getConsumerProperties()));
        compressor = compressorFactory
            .createVersionSpecificCompressorIfNotExist(version.getCompressionStrategy(), topicName, dictionary.array());
      }
    } else {
      compressor = compressorFactory.getCompressor(version.getCompressionStrategy());
    }
    return compressor;
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
    // Get the latest change capture topic
    Store store = getStore();
    int currentVersion = store.getCurrentVersion();
    PubSubTopic topic = pubSubTopicRepository
        .getTopic(Version.composeKafkaTopic(storeName, currentVersion) + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
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
    return internalSeekToTail(partitions, ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
  }

  public CompletableFuture<Void> internalSeekToTail(Set<Integer> partitions, String topicSuffix) {
    // Get the latest change capture topic
    PubSubTopic topic = pubSubTopicRepository.getTopic(getCurrentServingVersionTopic() + topicSuffix);
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
    return internalSeekToTimestamps(timestamps, ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
  }

  public CompletableFuture<Void> internalSeekToTimestamps(Map<Integer, Long> timestamps, String topicSuffix) {
    return internalSeekToTimestamps(timestamps, topicSuffix, LOGGER);
  }

  public CompletableFuture<Void> internalSeekToTimestamps(
      Map<Integer, Long> timestamps,
      String topicSuffix,
      Logger logger) {
    // Get the latest change capture topic
    Store store = storeRepository.getStore(storeName);
    int currentVersion = store.getCurrentVersion();
    String topicName = Version.composeKafkaTopic(storeName, currentVersion) + topicSuffix;
    PubSubTopic topic = pubSubTopicRepository.getTopic(topicName);
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
        currentVersionHighWatermarks.remove(topicPartition.getPartitionNumber());
        if (partitions.contains(topicPartition.getPartitionNumber())) {
          pubSubConsumer.unSubscribe(topicPartition);
        }
      }

      List<PubSubTopicPartition> topicPartitionListToSeek =
          getPartitionListToSubscribe(partitions, Collections.EMPTY_SET, targetTopic);
      for (PubSubTopicPartition topicPartition: topicPartitionListToSeek) {
        if (!topicPartition.getPubSubTopic().getName().endsWith(ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX)) {
          compressorMap.put(topicPartition.getPartitionNumber(), getVersionCompressor(topicPartition));
        }
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
    return internalPoll(timeoutInMs, ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
  }

  protected Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> internalPoll(
      long timeoutInMs,
      String topicSuffix,
      boolean includeControlMessage) {
    Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubMessages = new ArrayList<>();
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messagesMap = Collections.EMPTY_MAP;
    boolean lockAcquired = false;

    try {
      try {
        // the pubsubconsumer internally is completely unthreadsafe, so we need an exclusive lock to poll (ugh)
        lockAcquired = subscriptionLock.writeLock().tryLock(timeoutInMs, TimeUnit.MILLISECONDS);

        /*
         * If the lock acquisition fails, don't invoke poll on the pubSubConsumer.
         * Invoking poll without acquiring the lock can lead to a KafkaConsumer multi-threaded access exception
         * if multiple threads attempt to use this function concurrently.
         */
        if (!lockAcquired) {
          return Collections.emptyList();
        }
        messagesMap = pubSubConsumer.poll(timeoutInMs);
      } catch (InterruptedException exception) {
        LOGGER.info("Thread was interrupted", exception);
        // Restore the interrupt status
        Thread.currentThread().interrupt();
      } finally {
        if (lockAcquired) {
          subscriptionLock.writeLock().unlock();
        }
      }
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
                topicSuffix,
                message.getKey().getKey(),
                message.getValue().getProducerMetadata().getMessageTimestamp())) {
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
                convertPubSubMessageToPubSubChangeEventMessage(message, pubSubTopicPartition);
            pubSubMessage.ifPresent(pubSubMessages::add);
          }
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

      LOGGER.error("Encountered an exception when polling records for store: {}", storeName);
      throw exception;
    }
  }

  void maybeUpdatePartitionToBootstrapMap(DefaultPubSubMessage message, PubSubTopicPartition pubSubTopicPartition) {
    if (getSubscribeTime() - message.getValue().producerMetadata.messageTimestamp <= TimeUnit.MINUTES.toMillis(1)) {
      getPartitionToBootstrapState().put(pubSubTopicPartition.getPartitionNumber(), true);
    }
  }

  protected Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> internalPoll(
      long timeoutInMs,
      String topicSuffix) {
    return internalPoll(timeoutInMs, topicSuffix, false);
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
      String topicSuffix,
      byte[] key,
      long timestamp) {
    ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    // TODO: Find a better way to avoid data gap between version topic and change capture topic due to log compaction.
    if (controlMessageType.equals(ControlMessageType.END_OF_PUSH)) {
      LOGGER.info(
          "End of Push message received for version {} for store {}",
          Version.parseVersionFromKafkaTopicName(pubSubTopicPartition.getPubSubTopic().getName()),
          storeName);
      // Jump to next topic
      return switchToNewTopic(
          pubSubTopicPartition.getPubSubTopic(),
          topicSuffix,
          pubSubTopicPartition.getPartitionNumber());
    }

    // VERSION_SWAP is now being emitted to VT by the Leader. Make sure to only process VERSION_SWAP messages from
    // the change capture topic
    if (controlMessageType.equals(ControlMessageType.VERSION_SWAP)
        && !Version.isVersionTopic(pubSubTopicPartition.getTopicName())) {
      // TODO: In view topics, we need to know the partition of the upstream RT
      // how we transmit this information has yet to be determined, so once we finalize
      // that, we'll need to tweak this. For now, we'll just pass in the same partition number
      return handleVersionSwapControlMessage(
          controlMessage,
          pubSubTopicPartition,
          topicSuffix,
          pubSubTopicPartition.getPartitionNumber());
    }
    if (controlMessage.controlMessageType == START_OF_SEGMENT.getValue()
        && Arrays.equals(key, KafkaKey.HEART_BEAT.getKey())) {
      currentVersionLastHeartbeat.put(pubSubTopicPartition.getPartitionNumber(), timestamp);
    }
    return false;
  }

  // This function exists for wrappers of this class to be able to do any kind of preprocessing on the raw bytes of the
  // data consumed in the change stream to avoid having to do any duplicate deserialization/serialization.
  // Wrappers which depend on solely on the data post deserialization
  protected <T> T processRecordBytes(
      ByteBuffer decompressedBytes,
      T deserializedValue,
      byte[] key,
      ByteBuffer value,
      PubSubTopicPartition partition,
      int valueSchemaId,
      PubSubPosition recordOffset) throws IOException {
    return deserializedValue;
  }

  protected Optional<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> convertPubSubMessageToPubSubChangeEventMessage(
      DefaultPubSubMessage message,
      PubSubTopicPartition pubSubTopicPartition) {
    Optional<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubChangeEventMessage = Optional.empty();
    byte[] keyBytes = message.getKey().getKey();
    MessageType messageType = MessageType.valueOf(message.getValue());
    RecordChangeEvent recordChangeEvent;
    Object assembledObject = null;
    List<Long> replicationCheckpoint = null;
    if (messageType.equals(MessageType.DELETE)) {
      Delete delete = (Delete) message.getValue().payloadUnion;

      // Deletes have a previous and current value of null. So just fill it in!
      ChangeEvent<V> changeEvent = new ChangeEvent<>(null, null);
      pubSubChangeEventMessage = Optional.of(
          new ImmutableChangeCapturePubSubMessage<>(
              keyDeserializer.deserialize(keyBytes),
              changeEvent,
              pubSubTopicPartition,
              message.getPosition(),
              message.getPubSubMessageTime(),
              message.getPayloadSize(),
              false,
              getNextConsumerSequenceId(message.getPartition())));

      try {
        /*
         * OffsetVector is extracted, but we currently do nothing with it as CDC doesn't currently leverage the
         * VERSION_SWAP control message. Because of this, filterRecordByVersionSwapHighWatermarks is a NO_OP for now.
         */
        replicationCheckpoint = extractOffsetVectorFromMessage(
            delete.getSchemaId(),
            delete.getReplicationMetadataVersionId(),
            delete.getReplicationMetadataPayload());
      } catch (Exception e) {
        LOGGER.info(
            "Encounter RMD extraction exception for delete OP. partition={}, offset={}, key={}, rmd={}, rmd_id={}",
            message.getPartition(),
            message.getPosition(),
            keyDeserializer.deserialize(keyBytes),
            delete.getReplicationMetadataPayload(),
            delete.getReplicationMetadataVersionId(),
            e);
        RmdSchemaEntry rmdSchema =
            replicationMetadataSchemaRepository.getReplicationMetadataSchemaById(storeName, delete.getSchemaId());
        LOGGER.info("Using: {} {} {}", rmdSchema.getId(), rmdSchema.getValueSchemaID(), rmdSchema.getSchemaStr());
        throw e;
      }

      partitionToDeleteMessageCount.computeIfAbsent(message.getPartition(), x -> new AtomicLong(0)).incrementAndGet();
    } else if (messageType.equals(MessageType.PUT)) {
      Put put = (Put) message.getValue().payloadUnion;
      // Select appropriate reader schema and compressors
      RecordDeserializer deserializer = null;
      int readerSchemaId;
      VeniceCompressor compressor;
      if (pubSubTopicPartition.getPubSubTopic().isViewTopic() && changelogClientConfig.isBeforeImageView()) {
        deserializer = recordChangeDeserializer;
        readerSchemaId = this.schemaReader.getLatestValueSchemaId();
        compressor = NO_OP_COMPRESSOR;
      } else {
        // Use writer schema as the reader schema
        readerSchemaId = put.schemaId;
        compressor = compressorMap.get(pubSubTopicPartition.getPartitionNumber());
      }

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
            "Encountered an exception when processing a record in ChunkAssembler for topic: {} and partition: {}",
            pubSubTopicPartition.getTopicName(),
            pubSubTopicPartition.getPartitionNumber());
        throw exception;
      }

      if (readerSchemaId < 0) {
        // This was a chunk manifest and the actual writer schema needs to be retrieved
        readerSchemaId = assembledRecord.writerSchemaId();
      }
      if (deserializer == null) {
        // This is not before image view consumer, and we need to set the proper deserializer
        try {
          deserializer = storeDeserializerCache.getDeserializer(readerSchemaId, readerSchemaId);
        } catch (InvalidVeniceSchemaException invalidSchemaException) {
          // It's possible that a new schema was just added and our async metadata is outdated
          LOGGER.info("{}. Refreshing the local metadata cache to try again", invalidSchemaException.getMessage());
          storeRepository.refreshOneStore(storeName);
          deserializer = storeDeserializerCache.getDeserializer(readerSchemaId, readerSchemaId);
        }
      }
      try {
        assembledObject = deserializer.deserialize(compressor.decompress(assembledRecord.value()));
      } catch (IOException e) {
        throw new VeniceException(
            "Failed to deserialize or decompress record consumed from topic: "
                + pubSubTopicPartition.getPubSubTopic().getName(),
            e);
      }
      try {
        assembledObject = processRecordBytes(
            compressor.decompress(put.getPutValue()),
            assembledObject,
            keyBytes,
            put.getPutValue(),
            pubSubTopicPartition,
            readerSchemaId,
            message.getPosition());
      } catch (Exception ex) {
        throw new VeniceException(ex);
      }
      if (assembledObject instanceof RecordChangeEvent) {
        recordChangeEvent = (RecordChangeEvent) assembledObject;
        replicationCheckpoint = recordChangeEvent.replicationCheckpointVector;
      } else {
        try {
          /*
           * OffsetVector is extracted, but we currently do nothing with it as CDC doesn't currently leverage the
           * VERSION_SWAP control message. Because of this, filterRecordByVersionSwapHighWatermarks is a NO_OP for now.
           */
          replicationCheckpoint = extractOffsetVectorFromMessage(
              put.getSchemaId(),
              put.getReplicationMetadataVersionId(),
              put.getReplicationMetadataPayload());
        } catch (Exception e) {
          LOGGER.info(
              "Encounter RMD extraction exception for PUT OP. partition={}, offset={}, key={}, value={}, rmd={}, rmd_id={}",
              message.getPartition(),
              message.getPosition(),
              keyDeserializer.deserialize(keyBytes),
              assembledObject,
              put.getReplicationMetadataPayload(),
              put.getReplicationMetadataVersionId(),
              e);
          RmdSchemaEntry rmdSchema =
              replicationMetadataSchemaRepository.getReplicationMetadataSchemaById(storeName, put.getSchemaId());
          LOGGER.info("Using: {} {} {}", rmdSchema.getId(), rmdSchema.getValueSchemaID(), rmdSchema.getSchemaStr());
          throw e;
        }
      }

      // Now that we've assembled the object, we need to extract the replication vector depending on if it's from VT
      // or from the record change event. Records from VT 'typically' don't have an offset vector, but they will in
      // repush scenarios (which we want to be opaque to the user and filter accordingly).
      int payloadSize = message.getPayloadSize();
      if (assembledObject instanceof RecordChangeEvent) {
        recordChangeEvent = (RecordChangeEvent) assembledObject;
        pubSubChangeEventMessage = Optional.of(
            convertChangeEventToPubSubMessage(
                recordChangeEvent,
                keyDeserializer.deserialize(keyBytes),
                pubSubTopicPartition,
                message.getPosition(),
                message.getPubSubMessageTime(),
                payloadSize));
      } else {
        ChangeEvent<V> changeEvent = new ChangeEvent<>(null, (V) assembledObject);
        pubSubChangeEventMessage = Optional.of(
            new ImmutableChangeCapturePubSubMessage<>(
                keyDeserializer.deserialize(keyBytes),
                changeEvent,
                pubSubTopicPartition,
                message.getPosition(),
                message.getPubSubMessageTime(),
                payloadSize,
                false,
                getNextConsumerSequenceId(message.getPartition())));
      }
      partitionToPutMessageCount.computeIfAbsent(message.getPartition(), x -> new AtomicLong(0)).incrementAndGet();
    }

    // TODO: Once we settle on how to extract upstream RT partition we need to update this with the correct RT partition
    // Determine if the event should be filtered or not
    if (filterRecordByVersionSwapHighWatermarks(
        replicationCheckpoint,
        pubSubTopicPartition,
        pubSubTopicPartition.getPartitionNumber())) {
      return Optional.empty();
    }

    return pubSubChangeEventMessage;
  }

  protected List<Long> extractOffsetVectorFromMessage(
      int valueSchemaId,
      int rmdProtocolId,
      ByteBuffer replicationMetadataPayload) {
    if (rmdProtocolId > 0 && replicationMetadataPayload.remaining() > 0) {
      RecordDeserializer<GenericRecord> deserializer =
          rmdDeserializerCache.getDeserializer(valueSchemaId, valueSchemaId);
      GenericRecord replicationMetadataRecord = deserializer.deserialize(replicationMetadataPayload);
      GenericData.Array replicationCheckpointVector =
          (GenericData.Array) replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_POS);
      List<Long> offsetVector = new ArrayList<>();
      for (Object o: replicationCheckpointVector) {
        offsetVector.add((Long) o);
      }
      return offsetVector;
    }
    return new ArrayList<>();
  }

  protected boolean handleVersionSwapControlMessage(
      ControlMessage controlMessage,
      PubSubTopicPartition pubSubTopicPartition,
      String topicSuffix,
      Integer upstreamPartition) {
    ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    if (controlMessageType.equals(ControlMessageType.VERSION_SWAP)) {
      for (int attempt = 1; attempt <= MAX_VERSION_SWAP_RETRIES; attempt++) {
        try {
          VersionSwap versionSwap = (VersionSwap) controlMessage.controlMessageUnion;
          LOGGER.info(
              "Obtain version swap message: {} and versions swap high watermarks: {} for: {}",
              versionSwap,
              versionSwap.getLocalHighWatermarks(),
              pubSubTopicPartition);
          PubSubTopic newServingVersionTopic =
              pubSubTopicRepository.getTopic(versionSwap.newServingVersionTopic.toString());

          // TODO: There seems to exist a condition in the server where highwatermark offsets may regress when
          // transmitting the version swap message it seems like this can potentially happen if a repush occurs
          // and no data is consumed on that previous version.
          // To make the client handle this gracefully, we instate the below condition that says the hwm in the
          // client should never go backwards.
          List<Long> localOffset = (List<Long>) currentVersionHighWatermarks
              .getOrDefault(pubSubTopicPartition.getPartitionNumber(), Collections.EMPTY_MAP)
              .getOrDefault(upstreamPartition, Collections.EMPTY_LIST);
          // safety checks
          if (localOffset == null) {
            localOffset = new ArrayList<>();
          }
          List<Long> highWatermarkOffsets = versionSwap.localHighWatermarks == null
              ? new ArrayList<>()
              : new ArrayList<>(versionSwap.getLocalHighWatermarks());
          if (RmdUtils.hasOffsetAdvanced(localOffset, highWatermarkOffsets)) {

            currentVersionHighWatermarks
                .putIfAbsent(pubSubTopicPartition.getPartitionNumber(), new ConcurrentHashMap<>());
            currentVersionHighWatermarks.get(pubSubTopicPartition.getPartitionNumber())
                .put(upstreamPartition, highWatermarkOffsets);
          }
          switchToNewTopic(newServingVersionTopic, topicSuffix, pubSubTopicPartition.getPartitionNumber());
          chunkAssembler.clearBuffer();

          if (changeCaptureStats != null) {
            changeCaptureStats.emitVersionSwapCountMetrics(SUCCESS);
          }

          LOGGER.info(
              "Version Swap succeeded when switching to topic: {} for partition: {} after {} attempts",
              pubSubTopicPartition.getTopicName(),
              pubSubTopicPartition.getPartitionNumber(),
              attempt);

          return true;
        } catch (Exception error) {
          if (attempt == MAX_VERSION_SWAP_RETRIES) {
            if (changeCaptureStats != null) {
              changeCaptureStats.emitVersionSwapCountMetrics(FAIL);
            }

            LOGGER.error(
                "Version Swap failed when switching to topic: {} for partition: {} after {} attempts",
                pubSubTopicPartition.getTopicName(),
                pubSubTopicPartition.getPartitionNumber(),
                attempt);
            throw error;
          } else {
            LOGGER.error(
                "Version Swap failed when switching to topic: {} for partition: {} on attempt {}/{}. Retrying.",
                pubSubTopicPartition.getTopicName(),
                pubSubTopicPartition.getPartitionNumber(),
                attempt,
                MAX_VERSION_SWAP_RETRIES);

            Utils.sleep(Duration.ofSeconds(1).toMillis() * attempt);
          }
        }
      }
    }
    return false;
  }

  private PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> convertChangeEventToPubSubMessage(
      RecordChangeEvent recordChangeEvent,
      K currentKey,
      PubSubTopicPartition pubSubTopicPartition,
      PubSubPosition pubSubPosition,
      Long timestamp,
      int payloadSize) {
    V currentValue = null;
    if (recordChangeEvent.currentValue != null && recordChangeEvent.currentValue.getSchemaId() > 0) {
      currentValue = deserializeValueFromBytes(
          recordChangeEvent.currentValue.getValue(),
          recordChangeEvent.currentValue.getSchemaId());
    }
    V previousValue = null;
    if (recordChangeEvent.previousValue != null && recordChangeEvent.previousValue.getSchemaId() > 0) {
      previousValue = deserializeValueFromBytes(
          recordChangeEvent.previousValue.getValue(),
          recordChangeEvent.previousValue.getSchemaId());
    }
    ChangeEvent<V> changeEvent = new ChangeEvent<>(previousValue, currentValue);
    return new ImmutableChangeCapturePubSubMessage<>(
        currentKey,
        changeEvent,
        pubSubTopicPartition,
        pubSubPosition,
        timestamp,
        payloadSize,
        false,
        getNextConsumerSequenceId(pubSubTopicPartition.getPartitionNumber()));
  }

  private V deserializeValueFromBytes(ByteBuffer byteBuffer, int valueSchemaId) {
    RecordDeserializer<V> valueDeserializer = storeDeserializerCache.getDeserializer(valueSchemaId, valueSchemaId);
    if (byteBuffer != null) {
      return valueDeserializer.deserialize(byteBuffer);
    }
    return null;
  }

  /**
   * This method is currently dead and is a NO_OP due to VERSION_SWAP messages not being used currently by the CDC
   * client. However, we plan to leverage it in the future for record filtering for version swaps.
   */
  private boolean filterRecordByVersionSwapHighWatermarks(
      List<Long> recordCheckpointVector,
      PubSubTopicPartition pubSubTopicPartition,
      Integer upstreamPartition) {
    int partitionId = pubSubTopicPartition.getPartitionNumber();
    List<Long> localOffset = (List<Long>) currentVersionHighWatermarks.getOrDefault(partitionId, Collections.EMPTY_MAP)
        .getOrDefault(upstreamPartition, new ArrayList<>());
    if (recordCheckpointVector != null) {
      return !RmdUtils.hasOffsetAdvanced(localOffset, recordCheckpointVector);
    }
    // Has not met version swap message after client initialization.
    return false;
  }

  protected Set<PubSubTopicPartition> getTopicAssignment() {
    return Collections.synchronizedSet(pubSubConsumer.getAssignment());
  }

  protected boolean switchToNewTopic(PubSubTopic newTopic, String topicSuffix, Integer partition) {
    PubSubTopic mergedTopicName = pubSubTopicRepository.getTopic(newTopic.getName() + topicSuffix);
    Set<Integer> partitions = Collections.singleton(partition);
    Set<PubSubTopicPartition> assignment = getTopicAssignment();
    for (PubSubTopicPartition currentSubscribedPartition: assignment) {
      if (partition.equals(currentSubscribedPartition.getPartitionNumber())) {
        if (mergedTopicName.getName().equals(currentSubscribedPartition.getPubSubTopic().getName())) {
          // We're being asked to switch to a topic that we're already subscribed to, NoOp this
          return false;
        }
      }
    }
    unsubscribe(partitions);
    try {
      internalSubscribe(partitions, mergedTopicName).get();
    } catch (Exception e) {
      throw new VeniceException("Subscribe to new topic:" + mergedTopicName + " is not successful, error: " + e);
    }
    return true;
  }

  @Override
  public void close() {
    LOGGER.info("Closing Changelog Consumer with name: " + changelogClientConfig.getConsumerName());
    subscriptionLock.writeLock().lock();
    try {
      this.unsubscribeAll();
      pubSubConsumer.close();
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

  protected VeniceChangeCoordinate getLatestCoordinate(Integer partition) {
    Set<PubSubTopicPartition> topicPartitionSet = getTopicAssignment();
    synchronized (topicPartitionSet) {
      Optional<PubSubTopicPartition> topicPartition =
          topicPartitionSet.stream().filter(tp -> tp.getPartitionNumber() == partition).findFirst();
      if (!topicPartition.isPresent()) {
        throw new VeniceException(
            "Cannot get latest coordinate position for partition " + partition + "! Consumer isn't subscribed!");
      }
      subscriptionLock.readLock().lock();
      try {
        return new VeniceChangeCoordinate(
            topicPartition.get().getPubSubTopic().getName(),
            pubSubConsumer.endPosition(topicPartition.get()),
            partition);
      } finally {
        subscriptionLock.readLock().unlock();
      }
    }
  }

  protected PubSubTopicPartition getTopicPartition(Integer partition) {
    Set<PubSubTopicPartition> topicPartitionSet = getTopicAssignment();
    synchronized (topicPartitionSet) {
      Optional<PubSubTopicPartition> topicPartition =
          topicPartitionSet.stream().filter(tp -> tp.getPartitionNumber() == partition).findFirst();
      if (!topicPartition.isPresent()) {
        throw new VeniceException(
            "Cannot get latest coordinate position for partition " + partition + "! Consumer isn't subscribed!");
      }
      return topicPartition.get();
    }
  }

  protected ChangelogClientConfig getChangelogClientConfig() {
    return changelogClientConfig;
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
          recordStats(currentVersionLastHeartbeat, changeCaptureStats, getTopicAssignment());
          TimeUnit.SECONDS.sleep(60L);
        } catch (InterruptedException e) {
          LOGGER.warn("Lag Monitoring thread interrupted!  Shutting down...", e);
          Thread.currentThread().interrupt(); // Restore the interrupt status
          break;
        }
      }
    }

    protected void recordStats(
        ConcurrentHashMap<Integer, Long> currentVersionLastHeartbeat,
        BasicConsumerStats changeCaptureStats,
        Set<PubSubTopicPartition> assignment) {

      Iterator<Map.Entry<Integer, Long>> heartbeatIterator = currentVersionLastHeartbeat.entrySet().iterator();
      long maxLag = Long.MIN_VALUE;

      while (heartbeatIterator.hasNext()) {
        maxLag = Math.max(maxLag, System.currentTimeMillis() - heartbeatIterator.next().getValue());
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
}
