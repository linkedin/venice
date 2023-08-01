package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;

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
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.ChangeCaptureView;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;


public class BootstrappingVeniceChangelogConsumerImpl<K, V> extends VeniceChangelogConsumerImpl<K, V>
    implements BootstrappingVeniceChangelogConsumer<K, V> {
  private final StorageService storageService;
  private final StorageMetadataService storageMetadataService;

  private static final String CHANGE_CAPTURE_COORDINATE = "ChangeCaptureCoordinatePosition";

  // This is the name of a non-existent topic. We use it as a handle when interfacing with local storage so we can
  // make decisions about easily about weather or not to clear out the local state data or not across version for a
  // store
  // (we'll keep the local data in the event of a repush, but clear out if a user push comes through)
  private static final String LOCAL_STATE_TOPIC_NAME = "ChangeCaptureBootstrap_v1";

  private final VeniceConcurrentHashMap<Integer, BootstrapState> bootstrapStateMap = new VeniceConcurrentHashMap<>();
  private final Thread checkpointTask;

  private VeniceConfigLoader configLoader;

  boolean isStarted = false;

  public BootstrappingVeniceChangelogConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter pubSubConsumer,
      String fileSystemPath) {
    super(changelogClientConfig, pubSubConsumer);
    configLoader = buildVeniceConfig(fileSystemPath);
    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(
        changelogClientConfig.getInnerClientConfig().getMetricsRepository(),
        this.storeRepository,
        true);
    SchemaReader partitionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(changelogClientConfig.getInnerClientConfig())
            .setStoreName(AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName()),
        null);
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
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
      // TODO: This function needs to determine if the local files need to be cleared out or not. The way it should do
      // that
      // is by reading the local storagemetadata bootstrap coordinate, and see if the internal client is able to
      // subscribe to that position. If it's not able to, that means that the local state is off Venice retention,
      // and therefore should be completely rebootstrapped.
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
        this.storageService.cleanupAllStores(this.configLoader);
        storageMetadataService.clearStoreVersionState(LOCAL_STATE_TOPIC_NAME);
      }
    }

    return super.handleControlMessage(controlMessage, pubSubTopicPartition, topicSuffix);
  }

  private VeniceConfigLoader buildVeniceConfig(String baseDataPath) {
    VeniceProperties config = new PropertyBuilder().put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, 4) // RocksDB
                                                                                                       // default config
        .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER, 20) // RocksDB default config
        .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER, 36) // RocksDB default config
        .put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION, 40)
        .put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION, 60)
        .put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION, 80)
        .put(DATA_BASE_PATH, baseDataPath)
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
        storageService.getStorageEngine(LOCAL_STATE_TOPIC_NAME)
            .getByKeyPrefix(state.getKey(), null, new BytesStreamingCallback() {
              @Override
              public void onRecordReceived(byte[] key, byte[] value) {
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

                PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> record =
                    new ImmutableChangeCapturePubSubMessage<>(
                        keyDeserializer.deserialize(key),
                        changeEvent,
                        getTopicPartition(state.getKey()),
                        0,
                        0,
                        value.length * 8);
                resultSet.add(record);
              }

              @Override
              public void onCompletion() {
                // Update the map so that we're no longer in bootstrap mode
                state.getValue().bootstrapState = PollState.CONSUMING;
                // Notify that we've caught up
                completed.set(true);
                completed.notify();
              }
            });
        try {
          while (!completed.get()) {
            completed.wait();
          }
        } catch (InterruptedException e) {
          throw new VeniceException("Interrupted while reading local bootstrap data! Exception:", e);
        }
        return resultSet;
      }
    }
    return super.internalPoll(timeoutInMs, topicSuffix);
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
      BootstrapState currentPartitionState = bootstrapStateMap.get(record.getKey());
      currentPartitionState.currentPubSubPosition = record.getOffset();
      if (currentPartitionState.bootstrapState.equals(PollState.CATCHING_UP)) {
        if (currentPartitionState.isCaughtUp()) {
          currentPartitionState.bootstrapState = PollState.BOOTSTRAPPING;
        }
      }
      bootstrapStateMap.put(record.getPartition(), currentPartitionState);
    }
    return polledResults;
  }

  @Override
  protected <T> T processRecordBytes(
      RecordDeserializer<T> deserializer,
      VeniceCompressor compressor,
      byte[] key,
      ByteBuffer value,
      PubSubTopicPartition partition,
      int readerSchemaId) throws IOException {
    ByteBuffer decompressedBytes = compressor.decompress(value);
    T deserializedValue = deserializer.deserialize(decompressedBytes);
    if (deserializedValue instanceof RecordChangeEvent) {
      RecordChangeEvent recordChangeEvent = (RecordChangeEvent) deserializedValue;
      storageService.getStorageEngine(LOCAL_STATE_TOPIC_NAME)
          .put(
              partition.getPartitionNumber(),
              key,
              ValueRecord.create(recordChangeEvent.currentValue.schemaId, recordChangeEvent.currentValue.value.array())
                  .serialize());
    } else {
      storageService.getStorageEngine(LOCAL_STATE_TOPIC_NAME)
          .put(
              partition.getPartitionNumber(),
              key,
              ValueRecord.create(readerSchemaId, decompressedBytes.array()).serialize());
    }
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
        OffsetRecord offsetRecord = storageMetadataService.getLastOffset(LOCAL_STATE_TOPIC_NAME, partition);

        // Where we're at now
        VeniceChangeCoordinate localCheckpoint = null;
        try {
          localCheckpoint = VeniceChangeCoordinate.decodeStringAndConvertToVeniceChangeCoordinate(
              offsetRecord.getDatabaseInfo().get(CHANGE_CAPTURE_COORDINATE));
        } catch (IOException | ClassNotFoundException e) {
          throw new VeniceException("Failed to decode local hhange capture coordinate chekcpoint with exception: ", e);
        }

        // Where we need to catch up to
        VeniceChangeCoordinate targetCheckpoint = this.getLatestCoordinate(partition);

        synchronized (bootstrapStateMap) {
          BootstrapState newState = new BootstrapState();
          newState.bootstrapState = PollState.CATCHING_UP;
          newState.currentPubSubPosition = localCheckpoint;
          newState.targetPubSubPosition = targetCheckpoint;
          bootstrapStateMap.put(partition, newState);
        }
      }

      // Poll until we've caught up completely for all subscribed partitions.
      while (bootstrapStateMap.entrySet()
          .stream()
          .anyMatch(s -> s.getValue().bootstrapState.equals(PollState.CATCHING_UP))) {
        pollAndCatchup(5000L, ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
      }

      this.isStarted = true;
      return null;
    });
  }

  public CompletableFuture<Void> start(Set<Integer> partitions) {
    if (isStarted) {
      throw new VeniceException("Bootstrapping Changelog client is already started!");
    }
    storageService.start();
    storeRepository.start();
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
    checkpointTask.interrupt();
  }

  private enum PollState {
    CATCHING_UP, BOOTSTRAPPING, CONSUMING
  }

  private class BootstrapState {
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
          OffsetRecord lastOffset = storageMetadataService.getLastOffset(LOCAL_STATE_TOPIC_NAME, state.getKey());
          Map<String, String> dbInfo = lastOffset.getDatabaseInfo();
          try {
            dbInfo.put(
                CHANGE_CAPTURE_COORDINATE,
                VeniceChangeCoordinate
                    .convertVeniceChangeCoordinateToStringAndEncode(state.getValue().currentPubSubPosition));
          } catch (IOException e) {
            // TODO log error
          }
          storageMetadataService.put(LOCAL_STATE_TOPIC_NAME, state.getKey(), lastOffset);
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
