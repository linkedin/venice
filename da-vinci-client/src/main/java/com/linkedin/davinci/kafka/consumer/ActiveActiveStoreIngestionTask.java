package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.davinci.replication.merge.MergeConflictResolver;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.stats.AggStoreIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedStorageIngestionStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.storage.chunking.RawBytesChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Lazy;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;


/**
 * This class contains logic that SNs must perform if a store-version is running in Active/Active mode.
 */
public class ActiveActiveStoreIngestionTask extends LeaderFollowerStoreIngestionTask {

  private final int replicationMetadataVersionId;
  private final MergeConflictResolver mergeConflictResolver;

  public ActiveActiveStoreIngestionTask(
      Store store,
      Version version,
      VeniceWriterFactory writerFactory,
      KafkaClientFactory consumerFactory,
      Properties kafkaConsumerProperties,
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService,
      Queue<VeniceNotifier> notifiers,
      EventThrottler bandwidthThrottler,
      EventThrottler recordsThrottler,
      EventThrottler unorderedBandwidthThrottler,
      EventThrottler unorderedRecordsThrottler,
      ReadOnlySchemaRepository schemaRepo,
      ReadOnlyStoreRepository metadataRepo,
      TopicManagerRepository topicManagerRepository,
      TopicManagerRepository topicManagerRepositoryJavaBased,
      AggStoreIngestionStats storeIngestionStats,
      AggVersionedDIVStats versionedDIVStats,
      AggVersionedStorageIngestionStats versionedStorageIngestionStats,
      AbstractStoreBufferService storeBufferService,
      BooleanSupplier isCurrentVersion,
      VeniceStoreConfig storeConfig,
      DiskUsage diskUsage,
      RocksDBMemoryStats rocksDBMemoryStats,
      AggKafkaConsumerService aggKafkaConsumerService,
      VeniceServerConfig serverConfig,
      int partitionId,
      ExecutorService cacheWarmingThreadPool,
      long startReportingReadyToServeTimestamp,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      boolean isIsolatedIngestion,
      StorageEngineBackedCompressorFactory compressorFactory,
      Optional<ObjectCacheBackend> cacheBackend) {
    super(
        store,
        version,
        writerFactory,
        consumerFactory,
        kafkaConsumerProperties,
        storageEngineRepository,
        storageMetadataService,
        notifiers,
        bandwidthThrottler,
        recordsThrottler,
        unorderedBandwidthThrottler,
        unorderedRecordsThrottler,
        schemaRepo,
        metadataRepo,
        topicManagerRepository,
        topicManagerRepositoryJavaBased,
        storeIngestionStats,
        versionedDIVStats,
        versionedStorageIngestionStats,
        storeBufferService,
        isCurrentVersion,
        storeConfig,
        diskUsage,
        rocksDBMemoryStats,
        aggKafkaConsumerService,
        serverConfig,
        partitionId,
        cacheWarmingThreadPool,
        startReportingReadyToServeTimestamp,
        partitionStateSerializer,
        isIsolatedIngestion,
        compressorFactory,
        cacheBackend);

    this.replicationMetadataVersionId = version.getTimestampMetadataVersionId();
    this.mergeConflictResolver = new MergeConflictResolver(schemaRepo, storeName, replicationMetadataVersionId);
  }

  @Override
  protected void putInStorageEngine(AbstractStorageEngine storageEngine, int partition, byte[] keyBytes,
      Put put) {
    // TODO: Honor BatchConflictResolutionPolicy and maybe persist RMD for batch push records.
    if (partitionConsumptionStateMap.get(partition).isEndOfPushReceived()) {
      byte[] metadataBytesWithValueSchemaId = prependReplicationMetadataBytesWithValueSchemaId(put.timestampMetadataPayload, put.schemaId);
      storageEngine.putWithReplicationMetadata(partition, keyBytes, put.putValue, metadataBytesWithValueSchemaId);
    } else {
      storageEngine.put(partition, keyBytes, put.putValue);
    }
  }

  @Override
  protected void removeFromStorageEngine(AbstractStorageEngine storageEngine, int partition, byte[] keyBytes,
      Delete delete) {
    // TODO: Honor BatchConflictResolutionPolicy and maybe persist RMD for batch push records.
    if (partitionConsumptionStateMap.get(partition).isEndOfPushReceived()) {
      byte[] metadataBytesWithValueSchemaId = prependReplicationMetadataBytesWithValueSchemaId(delete.timestampMetadataPayload, delete.schemaId);
      storageEngine.deleteWithReplicationMetadata(partition, keyBytes, metadataBytesWithValueSchemaId);
    } else {
      storageEngine.delete(partition, keyBytes);
    }
  }

  private byte[] prependReplicationMetadataBytesWithValueSchemaId(ByteBuffer metadata, int valueSchemaId) {
    // TODO: Currently this function makes a copy of the data in the original buffer. It may be possible to pre-allocate
    // space for the 4-byte header and reuse the original replication metadata ByteBuffer.
    ByteBuffer bufferWithHeader = ByteUtils.prependIntHeaderToByteBuffer(metadata, valueSchemaId, false);
    bufferWithHeader.position(bufferWithHeader.position() - ByteUtils.SIZE_OF_INT);
    byte[] replicationMetadataBytesWithValueSchemaId = ByteUtils.extractByteArray(bufferWithHeader);
    bufferWithHeader.position(bufferWithHeader.position() + ByteUtils.SIZE_OF_INT);
    return replicationMetadataBytesWithValueSchemaId;
  }

  /**
   * Find the existing value schema id and replication metadata. If information for this key is found from the transient
   * map then use that, otherwise get it from storage engine.
   * @param partitionConsumptionState The {@link PartitionConsumptionState} of the current partition
   * @param key The key bytes of the incoming record.
   * @param subPartition The partition to fetch the replication metadata from storage engine
   * @return The wrapper object containing replication metadata and value schema id
   */
  private ReplicationMetadataWithValueSchemaId getReplicationMetadataAndSchemaId(PartitionConsumptionState partitionConsumptionState, byte[] key, int subPartition) {
    PartitionConsumptionState.TransientRecord transientRecord =
        partitionConsumptionState.getTransientRecord(key);
    if (transientRecord == null) {
      long lookupStartTimeInNS = System.nanoTime();
      byte[] replicationMetadataWithValueSchemaBytes = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic).getReplicationMetadata(subPartition, key);
      storeIngestionStats.recordIngestionReplicationMetadataLookUpLatency(storeName, LatencyUtils.getLatencyInMS(lookupStartTimeInNS));
      return ReplicationMetadataWithValueSchemaId.getFromStorageEngineBytes(replicationMetadataWithValueSchemaBytes);
    } else {
      storeIngestionStats.recordIngestionReplicationMetadataCacheHitCount(storeName);
      return new ReplicationMetadataWithValueSchemaId(transientRecord.getReplicationMetadata(), transientRecord.getValueSchemaId());
    }
  }

  // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this function.
  protected void processMessageAndMaybeProduceToKafka(VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, PartitionConsumptionState partitionConsumptionState, int subPartition) {
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = consumerRecordWrapper.consumerRecord();
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    byte[] keyBytes = kafkaKey.getKey();
    MessageType msgType = MessageType.valueOf(kafkaValue.messageType);
    boolean isChunkedTopic = storageMetadataService.isStoreVersionChunked(kafkaVersionTopic);
    final int incomingValueSchemaId;
    switch (msgType) {
      case PUT:
        incomingValueSchemaId = ((Put) kafkaValue.payloadUnion).schemaId;
        break;
      case UPDATE:
        incomingValueSchemaId = ((Update) kafkaValue.payloadUnion).schemaId;
        break;
      case DELETE:
        incomingValueSchemaId = -1; // Ignored since we don't need the schema id for DELETE operations.
        break;
      default:
        throw new VeniceMessageException(consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }

    Lazy<ByteBuffer>
        lazyOldValue = Lazy.of(() -> getValueBytesForKey(partitionConsumptionState, keyBytes, consumerRecord.topic(), consumerRecord.partition(), isChunkedTopic));
    ReplicationMetadataWithValueSchemaId replicationMetadataWithValueSchemaId = getReplicationMetadataAndSchemaId(partitionConsumptionState, keyBytes, subPartition);

    ByteBuffer replicationMetadataOfOldValue = null;
    int schemaIdOfOldValue = -1;
    if (replicationMetadataWithValueSchemaId != null) {
      replicationMetadataOfOldValue = replicationMetadataWithValueSchemaId.getReplicationMetadata();
      schemaIdOfOldValue = replicationMetadataWithValueSchemaId.getValueSchemaId();
    }

    long writeTimestamp = mergeConflictResolver.getWriteTimestampFromKME(kafkaValue);

    final MergeConflictResult mergeConflictResult;
    switch (msgType) {
      case PUT:
        mergeConflictResult = mergeConflictResolver.put(lazyOldValue, replicationMetadataOfOldValue,
            ((Put) kafkaValue.payloadUnion).putValue, writeTimestamp, schemaIdOfOldValue, incomingValueSchemaId);
        break;
      case DELETE:
        mergeConflictResult = mergeConflictResolver.delete(replicationMetadataOfOldValue, schemaIdOfOldValue, writeTimestamp);
        break;
      case UPDATE:
        throw new VeniceUnsupportedOperationException("Update operation not yet supported in Active/Active.");
//        mergeConflictResult = mergeConflictResolver.update(lazyOriginalValue, Lazy.of(() -> {
//          Update update = (Update) kafkaValue.payloadUnion;
//          return ingestionTaskWriteComputeAdapter.deserializeWriteComputeRecord(update.updateValue,
//              update.schemaId, update.updateSchemaId);
//        }), writeTimestamp);
//        break;
      default:
        throw new VeniceMessageException(consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }

    if (mergeConflictResult.isUpdateIgnored()) {
      storeIngestionStats.recordConflictResolutionUpdateIgnored(storeName);
    } else {
      // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this call.
      producePutOrDeleteToKafka(mergeConflictResult, partitionConsumptionState, keyBytes, isChunkedTopic, consumerRecordWrapper);
    }
  }

  private ByteBuffer getValueBytesForKey(PartitionConsumptionState partitionConsumptionState, byte[] key, String topic, int partition, boolean isChunkedTopic) {
    ByteBuffer originalValue = null;
    // Find the existing value. If a value for this key is found from the transient map then use that value, otherwise get it from DB.
    PartitionConsumptionState.TransientRecord transientRecord =
        partitionConsumptionState.getTransientRecord(key);
    if (transientRecord == null) {
      long lookupStartTimeInNS = System.nanoTime();
      originalValue = RawBytesChunkingAdapter.INSTANCE.get(
          storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic),
          getSubPartitionId(key, topic, partition), ByteBuffer.wrap(key), isChunkedTopic, null, null, null,
          storageMetadataService.getStoreVersionCompressionStrategy(kafkaVersionTopic),
          serverConfig.isComputeFastAvroEnabled(), schemaRepository, storeName, compressorFactory);
      storeIngestionStats.recordIngestionValueBytesLookUpLatency(storeName,
          LatencyUtils.getLatencyInMS(lookupStartTimeInNS));
    } else {
      storeIngestionStats.recordIngestionValueBytesCacheHitCount(storeName);
      //construct originalValue from this transient record only if it's not null.
      if (transientRecord.getValue() != null) {
        originalValue = RawBytesChunkingAdapter.INSTANCE.constructValue(transientRecord.getValueSchemaId(), transientRecord.getValue(),
            transientRecord.getValueOffset(), transientRecord.getValueLen(),
            serverConfig.isComputeFastAvroEnabled(), schemaRepository, storeName);
      }
    }
    return originalValue;
  }

  /**
   * This function parses the {@link MergeConflictResult} and decides if the update should be ignored or emit a PUT or a
   * DELETE record to VT.
   *
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this function.
   *
   * @param mergeConflictResult The result of conflict resolution.
   * @param partitionConsumptionState The {@link PartitionConsumptionState} of the current partition
   * @param key The key bytes of the incoming record.
   * @param isChunkedTopic If the store version is chunked.
   * @param consumerRecordWrapper The {@link VeniceConsumerRecordWrapper} for the current record.
   */
  private void producePutOrDeleteToKafka(MergeConflictResult mergeConflictResult,
      PartitionConsumptionState partitionConsumptionState, byte[] key,
      boolean isChunkedTopic, VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper) {
    final ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = consumerRecordWrapper.consumerRecord();

    final ByteBuffer updatedValueBytes = mergeConflictResult.getValue();
    final int valueSchemaId = mergeConflictResult.getValueSchemaID();
    final ByteBuffer updatedReplicationMetadataBytes = mergeConflictResult.getReplicationMetadata();
    // finally produce and update the transient record map.
    if (updatedValueBytes == null) {
      storeIngestionStats.recordConflictResolutionTombstoneCreated(storeName);
      partitionConsumptionState.setTransientRecord(consumerRecord.offset(), key, updatedReplicationMetadataBytes);
      LeaderProducedRecordContext leaderProducedRecordContext = LeaderProducedRecordContext.newDeleteRecord(consumerRecord.offset(), key);
      produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
          (callback, sourceTopicOffset) -> veniceWriter.get().delete(key, callback, sourceTopicOffset,
              new DeleteMetadata(valueSchemaId, replicationMetadataVersionId, updatedReplicationMetadataBytes)));
    } else {
      int valueLen = updatedValueBytes.remaining();
      partitionConsumptionState.setTransientRecord(consumerRecord.offset(), key, updatedValueBytes.array(), updatedValueBytes.position(),
          valueLen, valueSchemaId, updatedReplicationMetadataBytes);

      boolean doesResultReuseInput = mergeConflictResult.doesResultReuseInput();
      final int previousHeaderForPutValue = doesResultReuseInput ? ByteUtils.getIntHeaderFromByteBuffer(updatedValueBytes) : -1;

      Put updatedPut = new Put();
      updatedPut.putValue = ByteUtils.prependIntHeaderToByteBuffer(updatedValueBytes, valueSchemaId, doesResultReuseInput);
      updatedPut.schemaId = valueSchemaId;
      updatedPut.timestampMetadataVersionId = replicationMetadataVersionId;
      updatedPut.timestampMetadataPayload = updatedReplicationMetadataBytes;

      byte[] updatedKeyBytes = key;
      if (isChunkedTopic) {
        // Since data is not chunked in RT but chunked in VT, creating the key for the small record case or CVM to be
        // used to persist on disk after producing to Kafka.
        updatedKeyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
      }
      LeaderProducedRecordContext leaderProducedRecordContext = LeaderProducedRecordContext.newPutRecord(consumerRecord.offset(), updatedKeyBytes, updatedPut);

      produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
        (callback, sourceTopicOffset) -> {
          final Callback newCallback = (recordMetadata, exception) -> {
            if (doesResultReuseInput) {
              // Restore the original header so this function is eventually idempotent as the original KME ByteBuffer
              // will be recovered after producing the message to Kafka or if the production failing.
              ByteUtils.prependIntHeaderToByteBuffer(updatedValueBytes, previousHeaderForPutValue, true);
            }
            callback.onCompletion(recordMetadata, exception);
          };
          return veniceWriter.get().put(key, ByteUtils.extractByteArray(updatedValueBytes),
              valueSchemaId, newCallback, sourceTopicOffset, new PutMetadata(replicationMetadataVersionId, updatedReplicationMetadataBytes));
        });
    }
  }

  @Override
  protected void startConsumingAsLeaderInTransitionFromStandby(PartitionConsumptionState partitionConsumptionState) {
    // TODO: provide its own A/A-specific implementation
    super.startConsumingAsLeaderInTransitionFromStandby(partitionConsumptionState);
  }

  @Override
  protected void leaderExecuteTopicSwitch(PartitionConsumptionState partitionConsumptionState, TopicSwitch topicSwitch) {
    // TODO: provide its own A/A-specific implementation
    super.leaderExecuteTopicSwitch(partitionConsumptionState, topicSwitch);
  }

  @Override
  protected void processTopicSwitch(ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) {
    // TODO: provide its own A/A-specific implementation
    super.processTopicSwitch(controlMessage, partition, offset, partitionConsumptionState);
  }

  @Override
  protected void updateOffsetRecord(PartitionConsumptionState partitionConsumptionState, OffsetRecord offsetRecord,
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, LeaderProducedRecordContext leaderProducedRecordContext) {

    // TODO: provide its own A/A-specific implementation
    super.updateOffsetRecord(partitionConsumptionState, offsetRecord, consumerRecordWrapper, leaderProducedRecordContext);
  }
}
