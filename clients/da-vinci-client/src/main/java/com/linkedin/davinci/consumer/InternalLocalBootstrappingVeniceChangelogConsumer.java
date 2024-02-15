package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OFFSET_API_TIMEOUT_DURATION_DEFAULT_VALUE;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageEngineMetadataService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.ChangeCaptureView;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class InternalLocalBootstrappingVeniceChangelogConsumer<K, V> extends VeniceChangelogConsumerImpl<K, V>
    implements BootstrappingVeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(InternalLocalBootstrappingVeniceChangelogConsumer.class);
  private StorageService storageService;
  private StorageMetadataService storageMetadataService;

  private static final String CHANGE_CAPTURE_COORDINATE = "ChangeCaptureCoordinatePosition";

  // This is the name of a non-existent topic. We use it as a handle when interfacing with local storage so we can
  // make decisions about easily about weather or not to clear out the local state data or not across version for a
  // store
  // (we'll keep the local data in the event of a repush, but clear out if a user push comes through)
  private static final String LOCAL_STATE_TOPIC_SUFFIX = "_Bootstrap_v1";
  private final String localStateTopicName;
  private final VeniceConcurrentHashMap<Integer, BootstrapState> bootstrapStateMap = new VeniceConcurrentHashMap<>();
  private final Thread checkpointTask;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  private VeniceConfigLoader configLoader;

  boolean isStarted = false;

  private int bootstrapCompletedCount = 0;

  public InternalLocalBootstrappingVeniceChangelogConsumer(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter pubSubConsumer) {
    super(changelogClientConfig, pubSubConsumer);
    localStateTopicName = changelogClientConfig.getStoreName() + LOCAL_STATE_TOPIC_SUFFIX;
    configLoader = buildVeniceConfig();
    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(
        changelogClientConfig.getInnerClientConfig().getMetricsRepository(),
        this.storeRepository,
        true);
    SchemaReader partitionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(changelogClientConfig.getInnerClientConfig())
            .setStoreName(AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName()),
        null);
    partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    partitionStateSerializer.setSchemaReader(partitionStateSchemaReader);

    SchemaReader versionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(changelogClientConfig.getInnerClientConfig())
            .setStoreName(AvroProtocolDefinition.STORE_VERSION_STATE.getSystemStoreName()),
        null);
    InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
    storeVersionStateSerializer.setSchemaReader(versionStateSchemaReader);

    storageService = new StorageService(
        configLoader,
        storageEngineStats,
        null,
        storeVersionStateSerializer,
        partitionStateSerializer,
        this.storeRepository,
        true,
        true,
        functionToCheckWhetherStorageEngineShouldBeKeptOrNot());
    storageMetadataService =
        new StorageEngineMetadataService(storageService.getStorageEngineRepository(), partitionStateSerializer);
    checkpointTask = new VeniceChangelogCheckpointThread();
  }

  private Function<String, Boolean> functionToCheckWhetherStorageEngineShouldBeKeptOrNot() {
    return storageEngineName -> {
      // This function needs to determine if the local files need to be cleared out or not. The way it should do
      // that
      // is by reading the local storagemetadata bootstrap coordinate, and see if the internal client is able to
      // subscribe to that position. If it's not able to, that means that the local state is off Venice retention,
      // and therefore should be completely rebootstrapped.
      for (Integer partition: bootstrapStateMap.keySet()) {
        OffsetRecord offsetRecord = storageMetadataService.getLastOffset(localStateTopicName, partition);
        if (offsetRecord == null) {
          // No offset info in local, need to bootstrap from beginning.
          return false;
        }

        VeniceChangeCoordinate localCheckpoint;
        try {
          localCheckpoint = VeniceChangeCoordinate.decodeStringAndConvertToVeniceChangeCoordinate(
              offsetRecord.getDatabaseInfo().get(CHANGE_CAPTURE_COORDINATE));
        } catch (IOException | ClassNotFoundException e) {
          throw new VeniceException("Failed to decode local change capture coordinate checkpoint with exception: ", e);
        }

        PubSubTopicPartition topicPartition = getTopicPartition(partition);
        Long earliestOffset =
            pubSubConsumer.beginningOffset(topicPartition, PUBSUB_OFFSET_API_TIMEOUT_DURATION_DEFAULT_VALUE);
        VeniceChangeCoordinate earliestCheckpoint = earliestOffset == null
            ? null
            : new VeniceChangeCoordinate(
                topicPartition.getPubSubTopic().getName(),
                new ApacheKafkaOffsetPosition(earliestOffset),
                partition);

        // If earliest offset is larger than the local, we should just bootstrap from beginning.
        if (earliestCheckpoint != null && earliestCheckpoint.comparePosition(localCheckpoint) > -1) {
          return false;
        }
      }

      return true;
    };
  }

  @Override
  protected boolean handleVersionSwapControlMessage(
      ControlMessage controlMessage,
      PubSubTopicPartition pubSubTopicPartition,
      String topicSuffix) {
    ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    if (controlMessageType.equals(ControlMessageType.VERSION_SWAP)) {
      VersionSwap versionSwap = (VersionSwap) controlMessage.controlMessageUnion;
      if (!versionSwap.isRepush) {
        // Clean up all local data and seek existing
        storageMetadataService.clearStoreVersionState(localStateTopicName);
        this.storageService.cleanupAllStores(this.configLoader);
        seekToBeginningOfPush(Collections.singleton(pubSubTopicPartition.getPartitionNumber()));
      }

      return true;
    }

    return false;
  }

  private VeniceConfigLoader buildVeniceConfig() {
    VeniceProperties config = new PropertyBuilder().put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, 4) // RocksDB
        // default config
        .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER, 20) // RocksDB default config
        .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER, 36) // RocksDB default config
        .put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION, 40)
        .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION, 60)
        .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION, 80)
        .put(changelogClientConfig.getConsumerProperties())
        .put(DATA_BASE_PATH, changelogClientConfig.getBootstrapFileSystemPath())
        .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false)
        .build();
    return new VeniceConfigLoader(config, config);
  }

  @Override
  protected Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> internalPoll(
      long timeoutInMs,
      String topicSuffix) {
    if (!isStarted) {
      throw new VeniceException("Client isn't started yet!!");
    }
    // If there are any partitions which are in BOOTSTRAPPING state, play messages from those partitions first
    for (Map.Entry<Integer, BootstrapState> state: bootstrapStateMap.entrySet()) {
      if (state.getValue().bootstrapState.equals(PollState.BOOTSTRAPPING)) {
        // read from storage engine
        Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> resultSet = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        storageService.getStorageEngine(localStateTopicName)
            .getByKeyPrefix(state.getKey(), null, new BytesStreamingCallback() {
              @Override
              public void onRecordReceived(byte[] key, byte[] value) {
                onRecordReceivedForStorage(key, value, state.getKey(), resultSet);
              }

              @Override
              public void onCompletion() {
                onCompletionForStorage(state.getKey(), state.getValue(), resultSet, completed);
              }
            });
        if (!completed.get()) {
          throw new VeniceException("Interrupted while reading local bootstrap data!");
        }
        return resultSet;
      }
    }
    return super.internalPoll(timeoutInMs, topicSuffix);
  }

  @VisibleForTesting
  void onRecordReceivedForStorage(
      byte[] key,
      byte[] value,
      int partition,
      Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> resultSet) {
    // Transform and populate into the collection that we return.
    // TODO: this is a shortcoming of both this interface and the change capture client, we need to specify
    // a user
    // schema for deserialization
    ValueRecord valueRecord = ValueRecord.parseAndCreate(value);

    // Create a change event to wrap the record we pulled from disk and deserialize the record
    ChangeEvent<V> changeEvent = new ChangeEvent<>(
        null,
        (V) storeDeserializerCache.getDeserializer(valueRecord.getSchemaId(), valueRecord.getSchemaId())
            .deserialize(valueRecord.getDataInBytes()));

    PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> record = new ImmutableChangeCapturePubSubMessage<>(
        keyDeserializer.deserialize(key),
        changeEvent,
        getTopicPartition(partition),
        0,
        0,
        value.length * 8,
        false);
    resultSet.add(record);
  }

  @VisibleForTesting
  void onCompletionForStorage(
      int partition,
      BootstrapState state,
      Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> resultSet,
      AtomicBoolean completed) {
    // Update the map so that we're no longer in bootstrap mode
    state.bootstrapState = PollState.CONSUMING;
    bootstrapCompletedCount++;
    if (bootstrapCompletedCount == bootstrapStateMap.size()) {
      // Add a dummy record to mark the end of the bootstrap.
      resultSet.add(new ImmutableChangeCapturePubSubMessage<>(null, null, getTopicPartition(partition), 0, 0, 0, true));
    }

    // Notify that we've caught up
    completed.set(true);
  }

  @VisibleForTesting
  int getBootstrapCompletedCount() {
    return bootstrapCompletedCount;
  }

  @VisibleForTesting
  VeniceConcurrentHashMap<Integer, BootstrapState> getBootstrapStateMap() {
    return bootstrapStateMap;
  }

  /**
   * Polls change capture client and persist the results to local disk. Also updates the bootstrapStateMap with latest offsets
   * and if the client has caught up or not.
   *
   * @param timeoutInMs timeout on Poll
   * @param topicSuffix internal topic suffix
   * @return
   */
  private Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pollAndCatchup(
      long timeoutInMs,
      String topicSuffix) {
    Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> polledResults =
        super.internalPoll(timeoutInMs, topicSuffix);
    for (PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> record: polledResults) {
      BootstrapState currentPartitionState = bootstrapStateMap.get(record.getPartition());
      currentPartitionState.currentPubSubPosition = record.getOffset();
      if (currentPartitionState.bootstrapState.equals(PollState.CATCHING_UP)) {
        if (currentPartitionState.isCaughtUp()) {
          LOGGER.info(
              "pollAndCatchup completed for partition: {} with offset: {}",
              record.getPartition(),
              getOffset(record.getOffset()));
          currentPartitionState.bootstrapState = PollState.BOOTSTRAPPING;
        }
      }
    }
    return polledResults;
  }

  @Override
  protected <T> T processRecordBytes(
      ByteBuffer decompressedBytes,
      T deserializedValue,
      byte[] key,
      ByteBuffer value,
      PubSubTopicPartition partition,
      int readerSchemaId,
      long recordOffset) throws IOException {
    if (deserializedValue instanceof RecordChangeEvent) {
      RecordChangeEvent recordChangeEvent = (RecordChangeEvent) deserializedValue;
      if (recordChangeEvent.currentValue == null) {
        storageService.getStorageEngine(localStateTopicName).delete(partition.getPartitionNumber(), key);
      } else {
        storageService.getStorageEngine(localStateTopicName)
            .put(
                partition.getPartitionNumber(),
                key,
                ValueRecord
                    .create(recordChangeEvent.currentValue.schemaId, recordChangeEvent.currentValue.value.array())
                    .serialize());
      }
    } else {
      storageService.getStorageEngine(localStateTopicName)
          .put(
              partition.getPartitionNumber(),
              key,
              ValueRecord.create(readerSchemaId, decompressedBytes.array()).serialize());
    }

    // Update currentPubSubPosition for a partition, this will later be saved in RocksDb by
    // VeniceChangelogCheckpointThread
    VeniceChangeCoordinate currentPubSubPosition =
        bootstrapStateMap.get(partition.getPartitionNumber()).currentPubSubPosition;
    bootstrapStateMap.get(partition.getPartitionNumber()).currentPubSubPosition = new VeniceChangeCoordinate(
        currentPubSubPosition.getTopic(),
        new ApacheKafkaOffsetPosition(recordOffset),
        currentPubSubPosition.getPartition());
    return deserializedValue;
  }

  public CompletableFuture<Void> seekWithBootStrap(Set<Integer> partitions) {
    return CompletableFuture.supplyAsync(() -> {
      // Seek everything to tail in order to get the high offset
      try {
        this.seekToTail(partitions).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new VeniceException("Failed to bootstrap change log consumer with exception: ", e);
      }
      for (Integer partition: partitions) {
        // We'll always try to open for new partition during bootstrap. If a partition has been restored previously,
        // it will be skipped in openStoreForNewPartition.
        storageService.openStoreForNewPartition(
            configLoader.getStoreConfig(localStateTopicName, PersistenceType.ROCKS_DB),
            partition,
            () -> null);
        OffsetRecord offsetRecord = storageMetadataService.getLastOffset(localStateTopicName, partition);
        // Where we're at now
        String offsetString = offsetRecord.getDatabaseInfo().get(CHANGE_CAPTURE_COORDINATE);
        VeniceChangeCoordinate localCheckpoint;
        try {
          if (StringUtils.isEmpty(offsetString)) {
            LOGGER.info("No local checkpoint found for partition: {}", partition);
            localCheckpoint = new VeniceChangeCoordinate(
                getTopicPartition(partition).getPubSubTopic().getName(),
                new ApacheKafkaOffsetPosition(offsetRecord.getLocalVersionTopicOffset()),
                partition);
          } else {
            localCheckpoint = VeniceChangeCoordinate.decodeStringAndConvertToVeniceChangeCoordinate(offsetString);
            if (!partition.equals(localCheckpoint.getPartition())) {
              throw new IllegalStateException(
                  String.format(
                      "Local checkpoint partition: %s doesn't match with targeted partition: %s",
                      localCheckpoint.getPartition(),
                      partition));
            }

            LOGGER.info("Got local checkpoint for partition: {}, offset: {}", partition, getOffset(localCheckpoint));
          }
        } catch (IOException | ClassNotFoundException e) {
          throw new VeniceException("Failed to decode local change capture coordinate checkpoint with exception: ", e);
        }

        // Where we need to catch up to
        VeniceChangeCoordinate targetCheckpoint = this.getLatestCoordinate(partition);
        LOGGER.info("Got latest offset: {} for partition: {}", getOffset(targetCheckpoint), partition);

        synchronized (bootstrapStateMap) {
          BootstrapState newState = new BootstrapState();
          newState.currentPubSubPosition = localCheckpoint;
          newState.targetPubSubPosition = targetCheckpoint;
          newState.bootstrapState = newState.isCaughtUp() ? PollState.BOOTSTRAPPING : PollState.CATCHING_UP;
          bootstrapStateMap.put(partition, newState);
        }
      }

      // Seek to the current position so we can catch up from there to target
      seekToCheckpoint(
          bootstrapStateMap.values().stream().map(state -> state.currentPubSubPosition).collect(Collectors.toSet()));

      // Poll until we've caught up completely for all subscribed partitions.
      while (bootstrapStateMap.entrySet()
          .stream()
          .anyMatch(s -> s.getValue().bootstrapState.equals(PollState.CATCHING_UP))) {
        pollAndCatchup(5000L, ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
      }

      LOGGER.info("Bootstrap completed!");
      this.isStarted = true;
      return null;
    });
  }

  public CompletableFuture<Void> start(Set<Integer> partitions) {
    if (isStarted) {
      throw new VeniceException("Bootstrapping Changelog client is already started!");
    }

    storageService.start();
    try {
      storeRepository.start();
      storeRepository.subscribe(storeName);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    checkpointTask.start();
    return seekWithBootStrap(partitions);
  }

  @Override
  public CompletableFuture<Void> start() {
    Set<Integer> allPartitions = new HashSet<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      allPartitions.add(partition);
    }
    return this.start(allPartitions);
  }

  @Override
  public void stop() throws Exception {
    storageService.stop();
    ((AbstractVeniceService) storageMetadataService).stop();
    storeRepository.clear();
    checkpointTask.interrupt();
    LOGGER.info("Successfully stopped the BootstrappingVeniceChangelogConsumer");
  }

  @VisibleForTesting
  void setStorageAndMetadataService(StorageService storageService, StorageMetadataService storageMetadataService) {
    this.storageService = storageService;
    this.storageMetadataService = storageMetadataService;
  }

  /**
   * Helper method to get offset in long value from VeniceChangeCoordinate.
   */
  private long getOffset(VeniceChangeCoordinate veniceChangeCoordinate) {
    return ((ApacheKafkaOffsetPosition) (veniceChangeCoordinate.getPosition())).getOffset();
  }

  enum PollState {
    CATCHING_UP, BOOTSTRAPPING, CONSUMING
  }

  static class BootstrapState {
    PollState bootstrapState;
    VeniceChangeCoordinate currentPubSubPosition;
    VeniceChangeCoordinate targetPubSubPosition;

    boolean isCaughtUp() {
      return currentPubSubPosition.comparePosition(targetPubSubPosition) > -1;
    }
  }

  private class VeniceChangelogCheckpointThread extends Thread {
    VeniceChangelogCheckpointThread() {
      super("Venice-Changelog-Checkpoint-Thread");
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        for (Map.Entry<Integer, BootstrapState> state: bootstrapStateMap.entrySet()) {
          OffsetRecord lastOffset = storageMetadataService.getLastOffset(localStateTopicName, state.getKey());
          Map<String, String> dbInfo = lastOffset.getDatabaseInfo();
          try {
            dbInfo.put(
                CHANGE_CAPTURE_COORDINATE,
                VeniceChangeCoordinate
                    .convertVeniceChangeCoordinateToStringAndEncode(state.getValue().currentPubSubPosition));
            LOGGER.info(
                "Update checkpoint for partition: {}, new offset: {}",
                state.getKey(),
                getOffset(state.getValue().currentPubSubPosition));
          } catch (IOException e) {
            LOGGER.error(
                "Failed to update change capture coordinate position: {}",
                state.getValue().currentPubSubPosition);
          }

          lastOffset.setDatabaseInfo(dbInfo);
          storageMetadataService.put(localStateTopicName, state.getKey(), lastOffset);
        }
        try {
          TimeUnit.SECONDS.sleep(20);
        } catch (InterruptedException e) {
          // We've received an interrupt which is to be expected, so we'll just leave the loop and log
          break;
        }
      }
    }
  }
}
