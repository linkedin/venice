package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.davinci.replication.merge.MergeConflictResolver;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.replication.merge.MergeUtils;
import com.linkedin.davinci.replication.merge.ReplicationMetadataSerDe;
import com.linkedin.davinci.stats.AggVersionedStorageIngestionStats;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.storage.chunking.RawBytesChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Lazy;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.*;
import static com.linkedin.venice.VeniceConstants.*;


/**
 * This class contains logic that SNs must perform if a store-version is running in Active/Active mode.
 */
public class ActiveActiveStoreIngestionTask extends LeaderFollowerStoreIngestionTask {
  private static final Logger logger = LogManager.getLogger(ActiveActiveStoreIngestionTask.class);
  private final int replicationMetadataVersionId;
  private final MergeConflictResolver mergeConflictResolver;
  private final ReplicationMetadataSerDe replicationMetadataSerDe;
  private final Lazy<KeyLevelLocksManager> keyLevelLocksManager;
  private final AggVersionedStorageIngestionStats aggVersionedStorageIngestionStats;

  public ActiveActiveStoreIngestionTask(
      StoreIngestionTaskFactory.Builder builder,
      Store store,
      Version version,
      Properties kafkaConsumerProperties,
      BooleanSupplier isCurrentVersion,
      VeniceStoreVersionConfig storeConfig,
      int errorPartitionId,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend,
      StorageEngineBackedCompressorFactory compressorFactory) {
    super(builder,
        store,
        version,
        kafkaConsumerProperties,
        isCurrentVersion,
        storeConfig,
        errorPartitionId,
        isIsolatedIngestion,
        cacheBackend,
        compressorFactory);

    this.replicationMetadataVersionId = version.getReplicationMetadataVersionId();
    this.aggVersionedStorageIngestionStats = versionedStorageIngestionStats;
    int knownKafkaClusterNumber = serverConfig.getKafkaClusterIdToUrlMap().size();
    int consumerPoolSizePerKafkaCluster = serverConfig.getConsumerPoolSizePerKafkaCluster();
    int initialPoolSize = knownKafkaClusterNumber + 1;
    /**
     * In theory, the maximum # of keys each ingestion task can process is the # of consumers allocated for it.
     */
    int maxKeyLevelLocksPoolSize = Math.min(storeVersionPartitionCount, consumerPoolSizePerKafkaCluster) * knownKafkaClusterNumber + 1;
    this.keyLevelLocksManager = Lazy.of(() -> new KeyLevelLocksManager(getVersionTopic(), initialPoolSize, maxKeyLevelLocksPoolSize));
    this.replicationMetadataSerDe = new ReplicationMetadataSerDe(builder.getSchemaRepo(), storeName, replicationMetadataVersionId);
    this.mergeConflictResolver = new MergeConflictResolver(
        builder.getSchemaRepo(),
        storeName,
        valueSchemaID -> new GenericData.Record(replicationMetadataSerDe.getReplicationMetadataSchema(valueSchemaID))
    );
  }

  @Override
  protected DelegateConsumerRecordResult delegateConsumerRecord(
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper) {
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = consumerRecordWrapper.consumerRecord();
    if (!Version.isRealTimeTopic(consumerRecord.topic())) {
      /**
       * We don't need to lock the partition here because during VT consumption there is only one consumption source.
       */
      return super.delegateConsumerRecord(consumerRecordWrapper);
    } else {
      /**
       * The below flow must be executed in a critical session for the same key:
       * Read existing value/RMD from transient record cache/disk -> perform DCR and decide incoming value wins
       * -> update transient record cache -> produce to VT (just call send, no need to wait for the produce future in the critical session)
       *
       * Otherwise, there could be race conditions:
       * [fabric A thread]Read from transient record cache -> [fabric A thread]perform DCR and decide incoming value wins
       * -> [fabric B thread]read from transient record cache -> [fabric B thread]perform DCR and decide incoming value wins
       * -> [fabric B thread]update transient record cache -> [fabric B thread]produce to VT -> [fabric A thread]update transient record cache
       * -> [fabric A thread]produce to VT
       */
      final long delegateRealTimeTopicRecordStartTimeInNs = System.nanoTime();
      final ByteBuffer keyByteBuffer = ByteBuffer.wrap(consumerRecord.key().getKey());
      ReentrantLock keyLevelLock = this.keyLevelLocksManager.get().acquireLockByKey(keyByteBuffer);
      keyLevelLock.lock();
      try {
        return super.delegateConsumerRecord(consumerRecordWrapper);
      } finally {
        keyLevelLock.unlock();
        this.keyLevelLocksManager.get().releaseLock(keyByteBuffer);
        storeIngestionStats.recordLeaderDelegateRealTimeRecordLatency(storeName, LatencyUtils.getLatencyInMS(delegateRealTimeTopicRecordStartTimeInNs));
      }
    }
  }

  @Override
  protected void putInStorageEngine(AbstractStorageEngine storageEngine, int partition, byte[] keyBytes,
      Put put) {
    // TODO: Honor BatchConflictResolutionPolicy and maybe persist RMD for batch push records.
    if (partitionConsumptionStateMap.get(partition).isEndOfPushReceived() && !isDaVinciClient) {
      byte[] metadataBytesWithValueSchemaId = prependReplicationMetadataBytesWithValueSchemaId(put.replicationMetadataPayload, put.schemaId);
      storageEngine.putWithReplicationMetadata(partition, keyBytes, put.putValue, metadataBytesWithValueSchemaId);
    } else {
      storageEngine.put(partition, keyBytes, put.putValue);
    }
  }

  @Override
  protected void removeFromStorageEngine(AbstractStorageEngine storageEngine, int partition, byte[] keyBytes,
      Delete delete) {
    // TODO: Honor BatchConflictResolutionPolicy and maybe persist RMD for batch push records.
    if (partitionConsumptionStateMap.get(partition).isEndOfPushReceived() && !isDaVinciClient) {
      byte[] metadataBytesWithValueSchemaId = prependReplicationMetadataBytesWithValueSchemaId(delete.replicationMetadataPayload, delete.schemaId);
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
    PartitionConsumptionState.TransientRecord cachedRecord = partitionConsumptionState.getTransientRecord(key);
    if (cachedRecord != null) {
      storeIngestionStats.recordIngestionReplicationMetadataCacheHitCount(storeName);
      return new ReplicationMetadataWithValueSchemaId(cachedRecord.getValueSchemaId(), cachedRecord.getReplicationMetadataRecord());
    }

    final long lookupStartTimeInNS = System.nanoTime();
    byte[] replicationMetadataWithValueSchemaBytes = storageEngineRepository
        .getLocalStorageEngine(kafkaVersionTopic)
        .getReplicationMetadata(subPartition, key);
    storeIngestionStats.recordIngestionReplicationMetadataLookUpLatency(storeName, LatencyUtils.getLatencyInMS(lookupStartTimeInNS));
    return replicationMetadataSerDe.deserializeValueSchemaIdPrependedRmdBytes(replicationMetadataWithValueSchemaBytes);
  }

  // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this function.
  protected void processMessageAndMaybeProduceToKafka(VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper,
      PartitionConsumptionState partitionConsumptionState, int subPartition) {
    /**
     * With {@link com.linkedin.davinci.replication.BatchConflictResolutionPolicy.BATCH_WRITE_LOSES} there is no need
     * to perform DCR before EOP and L/F DIV passthrough mode should be used. If the version is going through data
     * recovery then there is no need to perform DCR until we completed data recovery and switched to consume from RT.
     * TODO. We need to refactor this logic when we support other batch conflict resolution policy.
     */
    if (!partitionConsumptionState.isEndOfPushReceived() || isDataRecovery && partitionConsumptionState.getTopicSwitch() != null) {
      super.processMessageAndMaybeProduceToKafka(consumerRecordWrapper, partitionConsumptionState, subPartition);
      return;
    }
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

    Lazy<ByteBuffer> lazyOldValue = Lazy.of(() ->
        getValueBytesForKey(partitionConsumptionState, keyBytes, consumerRecord.topic(), consumerRecord.partition(), isChunkedTopic));

    ReplicationMetadataWithValueSchemaId replicationMetadataWithValueSchemaId = getReplicationMetadataAndSchemaId(
        partitionConsumptionState,
        keyBytes,
        subPartition
    );

    int oldValueSchemaId = -1;
    GenericRecord oldReplicationMetadataRecord = null;

    if (replicationMetadataWithValueSchemaId != null) {
      oldValueSchemaId = replicationMetadataWithValueSchemaId.getValueSchemaId();
      oldReplicationMetadataRecord = replicationMetadataWithValueSchemaId.getReplicationMetadataRecord();
    }
    final long writeTimestamp = getWriteTimestampFromKME(kafkaValue);
    final long offsetSumPreOperation = MergeUtils.extractOffsetVectorSumFromReplicationMetadata(oldReplicationMetadataRecord);
    List<Long> recordTimestampsPreOperation = MergeUtils.extractTimestampFromReplicationMetadata(oldReplicationMetadataRecord);
    // get the source offset and the id
    int sourceKafkaClusterId = kafkaClusterUrlToIdMap.getOrDefault(consumerRecordWrapper.kafkaUrl(), -1);
    long sourceOffset = consumerRecordWrapper.consumerRecord().offset();
    final MergeConflictResult mergeConflictResult;

    aggVersionedStorageIngestionStats.recordTotalDCR(storeName, versionNumber);

    switch (msgType) {
      case PUT:
        mergeConflictResult = mergeConflictResolver.put(lazyOldValue, oldReplicationMetadataRecord,
            ((Put) kafkaValue.payloadUnion).putValue, writeTimestamp, oldValueSchemaId, incomingValueSchemaId, sourceOffset, sourceKafkaClusterId);
        break;
      case DELETE:
        mergeConflictResult = mergeConflictResolver.delete(oldReplicationMetadataRecord, oldValueSchemaId, writeTimestamp, sourceOffset, sourceKafkaClusterId);
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
      storeIngestionStats.recodUpdateIgnoredDCR();
      aggVersionedStorageIngestionStats.recordUpdateIgnoredDCR(storeName, versionNumber);
    } else {
      validatePostOperationResultsAndRecord(mergeConflictResult, offsetSumPreOperation, recordTimestampsPreOperation);
      // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this call.
      producePutOrDeleteToKafka(mergeConflictResult, partitionConsumptionState, keyBytes, isChunkedTopic, consumerRecordWrapper);
    }
  }

  private long getWriteTimestampFromKME(KafkaMessageEnvelope kme) {
    if (kme.producerMetadata.logicalTimestamp >= 0) {
      return kme.producerMetadata.logicalTimestamp;
    } else {
      return kme.producerMetadata.messageTimestamp;
    }
  }

  private void validatePostOperationResultsAndRecord(MergeConflictResult mergeConflictResult, Long offsetSumPreOperation, List<Long> timestampsPreOperation) {
    // Nothing was applied, no harm no foul
    if (mergeConflictResult.isUpdateIgnored()) {
      return;
    }
    // Post Validation checks on resolution
    GenericRecord replicationMetadataRecord = mergeConflictResult.getReplicationMetadataRecord();
    if (offsetSumPreOperation > MergeUtils.extractOffsetVectorSumFromReplicationMetadata(replicationMetadataRecord)) {
      // offsets went backwards, raise an alert!
      storeIngestionStats.recordOffsetRegressionDCRError();
      aggVersionedStorageIngestionStats.recordOffsetRegressionDCRError(storeName, versionNumber);
      logger.error(String.format("Offset vector found to have gone backwards!! New invalid replication metadata result:%s",
          replicationMetadataRecord));
    }

    // TODO: This comparison doesn't work well for write compute+schema evolution (can spike up). VENG-8129
    // this works fine for now however as we do not fully support A/A write compute operations (as we only do root timestamp comparisons).

    List<Long> timestampsPostOperation = MergeUtils.extractTimestampFromReplicationMetadata(replicationMetadataRecord);
    for(int i = 0; i < timestampsPreOperation.size(); i++) {
      if (timestampsPreOperation.get(i) > timestampsPostOperation.get(i)) {
        // timestamps went backwards, raise an alert!
        storeIngestionStats.recordTimestampRegressionDCRError();
        aggVersionedStorageIngestionStats.recordTimestampRegressionDCRError(storeName, versionNumber);
        logger.error(String.format("Timestamp found to have gone backwards!! Invalid replication metadata result:%s", mergeConflictResult.getReplicationMetadataRecord()));
      }
    }
  }

  /**
   * Get the value bytes for a key from {@link PartitionConsumptionState.TransientRecord} or from disk. The assumption
   * is that the {@link PartitionConsumptionState.TransientRecord} only contains the full value.
   * @param partitionConsumptionState The {@link PartitionConsumptionState} of the current partition
   * @param key The key bytes of the incoming record.
   * @param topic The topic from which the incomign record was consumed
   * @param partition The Kafka partition from which the incomign record was consumed
   * @param isChunkedTopic If the store version is chunked.
   * @return
   */
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
          serverConfig.isComputeFastAvroEnabled(), schemaRepository, storeName, compressorFactory, true);
      storeIngestionStats.recordIngestionValueBytesLookUpLatency(storeName,
          LatencyUtils.getLatencyInMS(lookupStartTimeInNS));
    } else {
      storeIngestionStats.recordIngestionValueBytesCacheHitCount(storeName);
      //construct originalValue from this transient record only if it's not null.
      if (transientRecord.getValue() != null) {
        originalValue = ByteBuffer.wrap(transientRecord.getValue(), transientRecord.getValueOffset(), transientRecord.getValueLen());
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

    final ByteBuffer updatedValueBytes = mergeConflictResult.getNewValue();
    final int valueSchemaId = mergeConflictResult.getValueSchemaId();

    GenericRecord replicationMetadataRecord = mergeConflictResult.getReplicationMetadataRecord();
    final ByteBuffer updatedReplicationMetadataBytes = replicationMetadataSerDe.serializeReplicationMetadata(
        mergeConflictResult.getValueSchemaId(),
        mergeConflictResult.getReplicationMetadataRecord()
    );

    // finally produce and update the transient record map.
    if (updatedValueBytes == null) {
      storeIngestionStats.recorTombstoneCreatedDCR();
      aggVersionedStorageIngestionStats.recordTombStoneCreationDCR(storeName, versionNumber);
      partitionConsumptionState.setTransientRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), key, valueSchemaId, replicationMetadataRecord);
      Delete deletePayload = new Delete();
      deletePayload.schemaId = valueSchemaId;
      deletePayload.replicationMetadataVersionId = replicationMetadataVersionId;
      deletePayload.replicationMetadataPayload = updatedReplicationMetadataBytes;
      LeaderProducedRecordContext leaderProducedRecordContext = LeaderProducedRecordContext.newDeleteRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), key, deletePayload);
      produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
          (callback, sourceTopicOffset) -> veniceWriter.get().delete(key, callback, sourceTopicOffset,
              new DeleteMetadata(valueSchemaId, replicationMetadataVersionId, updatedReplicationMetadataBytes)));
    } else {
      int valueLen = updatedValueBytes.remaining();
      partitionConsumptionState.setTransientRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), key, updatedValueBytes.array(), updatedValueBytes.position(),
          valueLen, valueSchemaId, replicationMetadataRecord);

      boolean doesResultReuseInput = mergeConflictResult.doesResultReuseInput();
      final int previousHeaderForPutValue = doesResultReuseInput ? ByteUtils.getIntHeaderFromByteBuffer(updatedValueBytes) : -1;

      Put updatedPut = new Put();
      updatedPut.putValue = ByteUtils.prependIntHeaderToByteBuffer(updatedValueBytes, valueSchemaId, doesResultReuseInput);
      updatedPut.schemaId = valueSchemaId;
      updatedPut.replicationMetadataVersionId = replicationMetadataVersionId;
      updatedPut.replicationMetadataPayload = updatedReplicationMetadataBytes;

      byte[] updatedKeyBytes = key;
      if (isChunkedTopic) {
        // Since data is not chunked in RT but chunked in VT, creating the key for the small record case or CVM to be
        // used to persist on disk after producing to Kafka.
        updatedKeyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
      }
      LeaderProducedRecordContext leaderProducedRecordContext = LeaderProducedRecordContext.newPutRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), updatedKeyBytes, updatedPut);

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
  protected void startConsumingAsLeader(PartitionConsumptionState partitionConsumptionState) {
    final int partition = partitionConsumptionState.getPartition();
    final OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();

    /**
     * Note that this function is called after the new leader has waited for 5 minutes of inactivity on the local VT topic.
     * The new leader might NOT need to switch to remote consumption in a case where map-reduce jobs of a batch job stuck
     * on producing to the local VT so that there is no activity in the local VT.
     */
    if (shouldNewLeaderSwitchToRemoteConsumption(partitionConsumptionState)) {
      partitionConsumptionState.setConsumeRemotely(true);
      logger.info(consumerTaskId + " enabled remote consumption from topic " + offsetRecord.getLeaderTopic() + " partition " + partition);
    }

    partitionConsumptionState.setLeaderFollowerState(LEADER);
    final String leaderTopic = offsetRecord.getLeaderTopic();
    Set<String> leaderSourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    Map<String, Long> leaderOffsetByKafkaURL = new HashMap<>(leaderSourceKafkaURLs.size());
    leaderSourceKafkaURLs.forEach(kafkaURL -> leaderOffsetByKafkaURL.put(kafkaURL, partitionConsumptionState.getLeaderOffset(kafkaURL)));
    logger.info(String.format("%s is promoted to leader for partition %d and it is going to start consuming from " +
            "topic %s with offset by Kafka URL mapping %s",
        consumerTaskId, partition, leaderTopic, leaderOffsetByKafkaURL));

    // subscribe to the new upstream
    leaderOffsetByKafkaURL.forEach((kafkaURL, leaderStartOffset) -> {
      consumerSubscribe(
          leaderTopic,
          partitionConsumptionState.getSourceTopicPartition(leaderTopic),
          leaderStartOffset,
          kafkaURL
      );
    });

    syncConsumedUpstreamRTOffsetMapIfNeeded(partitionConsumptionState, leaderOffsetByKafkaURL);

    logger.info(String.format("%s, as a leader, started consuming from topic %s partition %d with offset by Kafka URL mapping %s",
        consumerTaskId, offsetRecord.getLeaderTopic(), partition, leaderOffsetByKafkaURL));
  }

  private long calculateRewindStartTime(PartitionConsumptionState partitionConsumptionState) {
    long rewindStartTime = 0;
    long rewindTimeInMs = hybridStoreConfig.get().getRewindTimeInSeconds() * Time.MS_PER_SECOND;
    if (isDataRecovery) {
      // Override the user rewind if the version is under data recovery to avoid data loss when user have short rewind.
      rewindTimeInMs = Math.max(TopicManager.BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN, rewindTimeInMs);
    }
    switch (hybridStoreConfig.get().getBufferReplayPolicy()) {
      case REWIND_FROM_SOP:
        rewindStartTime =  partitionConsumptionState.getStartOfPushTimestamp() - rewindTimeInMs;
        break;
      case REWIND_FROM_EOP:
      default:
        rewindStartTime = partitionConsumptionState.getEndOfPushTimestamp() - rewindTimeInMs;
    }
    return rewindStartTime;
  }

  @Override
  protected void leaderExecuteTopicSwitch(PartitionConsumptionState partitionConsumptionState, TopicSwitch topicSwitch) {
    if (partitionConsumptionState.getLeaderFollowerState() != LEADER) {
      throw new VeniceException(String.format("Expect state %s but got %s", LEADER, partitionConsumptionState.toString()));
    }
    if (topicSwitch.sourceKafkaServers.isEmpty()) {
      throw new VeniceException("In the A/A mode, source Kafka URL list cannot be empty in Topic Switch control message.");
    }

    final int partition = partitionConsumptionState.getPartition();
    final String currentLeaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
    final String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    final int sourceTopicPartition = partitionConsumptionState.getSourceTopicPartition(newSourceTopicName);
    Map<String, Long> upstreamOffsetsByKafkaURLs = new HashMap<>(topicSwitch.sourceKafkaServers.size());

    topicSwitch.sourceKafkaServers.forEach(sourceKafkaURL -> {
      Long upstreamStartOffset = partitionConsumptionState.getLatestProcessedUpstreamRTOffsetWithNoDefault(sourceKafkaURL.toString());
      if (upstreamStartOffset == null || upstreamStartOffset < 0) {
        long rewindStartTimestamp = 0;
        //calculate the rewind start time here if controller asked to do so by using this sentinel value.
        if (topicSwitch.rewindStartTimestamp == REWIND_TIME_DECIDED_BY_SERVER) {
          rewindStartTimestamp = calculateRewindStartTime(partitionConsumptionState);
          logger.info(String.format("%s leader calculated rewindStartTimestamp %d for topic %s partition %d", consumerTaskId, rewindStartTimestamp, newSourceTopicName, sourceTopicPartition));
        } else {
          rewindStartTimestamp = topicSwitch.rewindStartTimestamp;
        }
        if (rewindStartTimestamp > 0) {
          upstreamStartOffset = getTopicPartitionOffsetByKafkaURL(
                  sourceKafkaURL,
                  newSourceTopicName,
                  sourceTopicPartition,
                  rewindStartTimestamp
          );
        } else {
          upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
        }
      }
      upstreamOffsetsByKafkaURLs.put(sourceKafkaURL.toString(), upstreamStartOffset);
    });

    // unsubscribe the old source and subscribe to the new source
    consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);
    waitForLastLeaderPersistFuture(
            partitionConsumptionState,
            String.format(
                    "Leader failed to produce the last message to version topic before switching feed topic from %s to %s on partition %s",
                    currentLeaderTopic, newSourceTopicName, partition)
    );

    if (topicSwitch.sourceKafkaServers.size() != 1
            || (!Objects.equals(topicSwitch.sourceKafkaServers.get(0).toString(), localKafkaServer))) {
      partitionConsumptionState.setConsumeRemotely(true);
      logger.info(String.format("%s enabled remote consumption and switch to topic %s partition %d with offset " +
                      "by Kafka URL mapping %s", consumerTaskId, newSourceTopicName, sourceTopicPartition, upstreamOffsetsByKafkaURLs));
    }

    partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopicName);
    upstreamOffsetsByKafkaURLs.forEach((upstreamKafkaURL, upstreamStartOffset) -> {
      partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(
              upstreamKafkaURL,
              upstreamStartOffset
      );
    });

    upstreamOffsetsByKafkaURLs.forEach((kafkaURL, upstreamStartOffset) -> {
      consumerSubscribe(
              newSourceTopicName,
              partitionConsumptionState.getSourceTopicPartition(newSourceTopicName),
              upstreamStartOffset,
              kafkaURL
      );
    });

    syncConsumedUpstreamRTOffsetMapIfNeeded(partitionConsumptionState, upstreamOffsetsByKafkaURLs);

    logger.info(String.format("%s leader successfully switch feed topic from %s to %s on partition %d with offset by " +
            "Kafka URL mapping %s", consumerTaskId, currentLeaderTopic, newSourceTopicName, partition, upstreamOffsetsByKafkaURLs));

    // In case new topic is empty and leader can never become online
    defaultReadyToServeChecker.apply(partitionConsumptionState);
  }

  @Override
  protected void processTopicSwitch(ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) {

    TopicSwitch topicSwitch = (TopicSwitch) controlMessage.controlMessageUnion;
    reportStatusAdapter.reportTopicSwitchReceived(partitionConsumptionState);

    // Calculate the start offset based on start timestamp
    final String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    Map<String, Long> upstreamStartOffsetByKafkaURL = new HashMap<>(topicSwitch.sourceKafkaServers.size());
    if (!isDaVinciClient) {
      final int newSourceTopicPartition = partitionConsumptionState.getSourceTopicPartition(newSourceTopicName);
      topicSwitch.sourceKafkaServers.forEach(sourceKafkaURL -> {
        long rewindStartTimestamp = 0;
        //calculate the rewind start time here if controller asked to do so by using this sentinel value.
        if (topicSwitch.rewindStartTimestamp == REWIND_TIME_DECIDED_BY_SERVER) {
          rewindStartTimestamp = calculateRewindStartTime(partitionConsumptionState);
          logger.info(
              String.format("%s leader calculated rewindStartTimestamp %d for topic %s partition %d", consumerTaskId,
                  rewindStartTimestamp, newSourceTopicName, newSourceTopicPartition));
        } else {
          rewindStartTimestamp = topicSwitch.rewindStartTimestamp;
        }
        if (rewindStartTimestamp > 0) {
          long upstreamStartOffset = getTopicManager(sourceKafkaURL.toString()).getPartitionOffsetByTime(newSourceTopicName,
              newSourceTopicPartition, rewindStartTimestamp);
          if (upstreamStartOffset != OffsetRecord.LOWEST_OFFSET) {
            upstreamStartOffset -= 1;
          }
          upstreamStartOffsetByKafkaURL.put(sourceKafkaURL.toString(), upstreamStartOffset);
        } else {
          upstreamStartOffsetByKafkaURL.put(sourceKafkaURL.toString(), OffsetRecord.LOWEST_OFFSET);
        }
      });

      upstreamStartOffsetByKafkaURL.forEach((sourceKafkaURL, upstreamStartOffset) -> {
        partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(sourceKafkaURL, upstreamStartOffset);
      });
    }
    /**
     * TopicSwitch needs to be persisted locally for both servers and DaVinci clients so that read-to-serve check
     * can make the correct decision.
     */
    syncTopicSwitchToIngestionMetadataService(topicSwitch, partitionConsumptionState, upstreamStartOffsetByKafkaURL);
    if (!isLeader(partitionConsumptionState)) {
      partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopicName);
      this.defaultReadyToServeChecker.apply(partitionConsumptionState);
    }
  }

  @Override
  protected void updateOffsetMetadataInOffsetRecord(PartitionConsumptionState partitionConsumptionState, OffsetRecord offsetRecord,
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, LeaderProducedRecordContext leaderProducedRecordContext) {
    updateOffsets(partitionConsumptionState,
        consumerRecordWrapper,
        leaderProducedRecordContext,
        (versionTopicOffset)
            -> offsetRecord.setCheckpointLocalVersionTopicOffset(versionTopicOffset),
        (sourceKafkaUrl, upstreamTopicName, upstreamTopicOffset) -> {
          if (Version.isRealTimeTopic(upstreamTopicName)) {
            offsetRecord.setLeaderUpstreamOffset(sourceKafkaUrl, upstreamTopicOffset);
          } else {
            offsetRecord.setCheckpointUpstreamVersionTopicOffset(upstreamTopicOffset);
          }
        },
        (sourceKafkaUrl, upstreamTopicName)
            -> Version.isRealTimeTopic(upstreamTopicName)
               ? offsetRecord.getUpstreamOffset(sourceKafkaUrl)
               : offsetRecord.getCheckpointUpstreamVersionTopicOffset(),
        () -> {
          final String upstreamKafkaURL;
          if (isLeader(partitionConsumptionState)) {
            upstreamKafkaURL = consumerRecordWrapper.kafkaUrl(); // Wherever leader consumes from is considered as "upstream"
          } else {
            KafkaMessageEnvelope kafkaValue = consumerRecordWrapper.consumerRecord().value();
            if (kafkaValue.leaderMetadataFooter == null) {
              /**
               * This "leaderMetadataFooter" field do not get populated in 2 cases:
               *
               * 1. The source fabric is the same as the local fabric since the VT source will be one of the prod fabric after
               *    batch NR is fully ramped.
               *
               * 2. Leader/Follower stores that have not ramped to NR yet. KMM mirrors data to local VT.
               *
               * In both 2 above cases, the leader replica consumes from local Kafka URL. Hence, from a follower's perspective,
               * the upstream Kafka cluster which the leader consumes from should be the local Kafka URL.
               */
              upstreamKafkaURL = localKafkaServer;
            } else {
              upstreamKafkaURL = getUpstreamKafkaUrlFromKafkaValue(kafkaValue);
            }
          }
          return upstreamKafkaURL;
        });
  }

  @Override
  protected void updateLatestInMemoryProcessedOffset(PartitionConsumptionState partitionConsumptionState,
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, LeaderProducedRecordContext leaderProducedRecordContext) {
    updateOffsets(partitionConsumptionState,
        consumerRecordWrapper,
        leaderProducedRecordContext,
        (versionTopicOffset)
            -> partitionConsumptionState.updateLatestProcessedLocalVersionTopicOffset(versionTopicOffset),
        (sourceKafkaUrl, upstreamTopicName, upstreamTopicOffset) -> {
          if (Version.isRealTimeTopic(upstreamTopicName)) {
            partitionConsumptionState.updateLatestProcessedUpstreamRTOffset(sourceKafkaUrl, upstreamTopicOffset);
          } else {
            partitionConsumptionState.updateLatestProcessedUpstreamVersionTopicOffset(upstreamTopicOffset);
          }
        },
        (sourceKafkaUrl, upstreamTopicName)
            -> Version.isRealTimeTopic(upstreamTopicName)
               ? partitionConsumptionState.getLatestProcessedUpstreamRTOffset(sourceKafkaUrl)
               : partitionConsumptionState.getLatestProcessedUpstreamVersionTopicOffset(),
        () -> {
          final String upstreamKafkaURL;
          if (isLeader(partitionConsumptionState)) {
            upstreamKafkaURL = consumerRecordWrapper.kafkaUrl(); // Wherever leader consumes from is considered as "upstream"
          } else {
            KafkaMessageEnvelope kafkaValue = consumerRecordWrapper.consumerRecord().value();
            if (kafkaValue.leaderMetadataFooter == null) {
              /**
               * This "leaderMetadataFooter" field do not get populated in 2 cases:
               *
               * 1. The source fabric is the same as the local fabric since the VT source will be one of the prod fabric after
               *    batch NR is fully ramped.
               *
               * 2. Leader/Follower stores that have not ramped to NR yet. KMM mirrors data to local VT.
               *
               * In both 2 above cases, the leader replica consumes from local Kafka URL. Hence, from a follower's perspective,
               * the upstream Kafka cluster which the leader consumes from should be the local Kafka URL.
               */
              upstreamKafkaURL = localKafkaServer;
            } else {
              upstreamKafkaURL = getUpstreamKafkaUrlFromKafkaValue(kafkaValue);
            }
          }
          return upstreamKafkaURL;
        });
  }

  @Override
  protected boolean isRealTimeBufferReplayStarted(PartitionConsumptionState partitionConsumptionState) {
    TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch();
    if (topicSwitch == null) {
      return false;
    }
    if (topicSwitch.sourceKafkaServers.isEmpty()) {
      throw new VeniceException("Got empty source Kafka URLs in Topic Switch.");
    }
    return Version.isRealTimeTopic(topicSwitch.sourceTopicName.toString());
  }

  /**
   * For A/A, there are multiple entries in upstreamOffsetMap during RT ingestion.
   * If the current DataReplicationPolicy is on Aggregate mode, A/A will check the upstream offset lags from all regions;
   * otherwise, only check the upstream offset lag from the local region.
   */
  @Override
  protected long getLatestPersistedUpstreamOffsetForHybridOffsetLagMeasurement(PartitionConsumptionState pcs, String upstreamKafkaUrl) {
    return pcs.getLatestProcessedUpstreamRTOffset(upstreamKafkaUrl);
  }

  /**
   * Different from the persisted upstream offset map in OffsetRecord, latest consumed upstream offset map is maintained
   * for each individual Kafka url.
   */
  @Override
  protected long getLatestConsumedUpstreamOffsetForHybridOffsetLagMeasurement(PartitionConsumptionState pcs, String upstreamKafkaUrl) {
    return pcs.getLeaderConsumedUpstreamRTOffset(upstreamKafkaUrl);
  }

  @Override
  protected void updateLatestInMemoryLeaderConsumedRTOffset(PartitionConsumptionState pcs, String kafkaUrl, long offset) {
    pcs.updateLeaderConsumedUpstreamRTOffset(kafkaUrl, offset);
  }

  private String getUpstreamKafkaUrlFromKafkaValue(KafkaMessageEnvelope kafkaValue) {
    if (kafkaValue.leaderMetadataFooter == null) {
      throw new VeniceException("leaderMetadataFooter field in KME should have been set.");
    }
    String upstreamKafkaURL = this.kafkaClusterIdToUrlMap.get(kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId);
    if (upstreamKafkaURL == null) {
      throw new VeniceException(String.format("No Kafka cluster ID found in the cluster ID to Kafka URL map. " +
              "Got cluster ID %d and ID to cluster URL map %s",
              kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId, kafkaClusterIdToUrlMap));
    }
    return upstreamKafkaURL;
  }

  /**
   * For Active-Active this buffer is always used.
   * @return
   */
  @Override
  protected boolean isTransientRecordBufferUsed() {
    return true;
  }

  @Override
  public long getRegionHybridOffsetLag(int regionId) {
    Optional<StoreVersionState> svs = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (!svs.isPresent()) {
      /**
       * Store version metadata is created for the first time when the first START_OF_PUSH message is processed;
       * however, the ingestion stat is created the moment an ingestion task is created, so there is a short time
       * window where there is no version metadata, which is not an error.
       */
      return 0;
    }

    if (partitionConsumptionStateMap.isEmpty()) {
      /**
       * Partition subscription happens after the ingestion task and stat are created, it's not an error.
       */
      return 0;
    }

    long offsetLag = partitionConsumptionStateMap.values().stream()
        .filter(LeaderFollowerStoreIngestionTask.LEADER_OFFSET_LAG_FILTER)
        // Leader consumption upstream RT offset is only available in leader subPartition
        .filter(pcs -> amplificationAdapter.isLeaderSubPartition(pcs.getPartition()))
        // the lag is (latest fabric RT offset - consumed fabric RT offset)
        .mapToLong((pcs) -> {
          String currentLeaderTopic = pcs.getOffsetRecord().getLeaderTopic();
          if (currentLeaderTopic == null || currentLeaderTopic.isEmpty() || !Version.isRealTimeTopic(currentLeaderTopic)) {
            // Leader topic not found, indicating that it is VT topic.
            return 0;
          }
          String kafkaSourceAddress = kafkaClusterIdToUrlMap.get(regionId);
          // This storage node does not register with the given region ID.
          if (kafkaSourceAddress == null) {
            return 0;
          }
          KafkaConsumerWrapper kafkaConsumer = consumerMap.get(kafkaSourceAddress);
          // Consumer might not existed in the map after the consumption state is created, but before attaching the
          // corresponding consumer in consumerMap.
          if (kafkaConsumer != null) {
            Optional<Long> offsetLagOptional = kafkaConsumer.getOffsetLag(currentLeaderTopic, pcs.getUserPartition());
            if (offsetLagOptional.isPresent()) {
              return offsetLagOptional.get();
            }
          }
          // Fall back to calculate offset lag in the old way
          return (cachedKafkaMetadataGetter.getOffset(getTopicManager(kafkaSourceAddress), currentLeaderTopic, pcs.getUserPartition()) - 1)
              - pcs.getLeaderConsumedUpstreamRTOffset(kafkaSourceAddress);
        }).sum();

    return minZeroLag(offsetLag);
  }

  /**
   * For stores in aggregate mode this is optimistic and returns the minimum lag of all fabric. This is because in
   * aggregate mode duplicate msg consumption happen from all fabric. So it should be fine to consider the lowest lag.
   *
   * For stores in active/active mode, if no fabric is unreachable, return the maximum lag of all fabrics. If only one
   * fabric is unreachable, return the maximum lag of other fabrics. If more than one fabrics are unreachable, return
   * Long.MAX_VALUE, which means the partition is not ready-to-serve.
   * TODO: For active/active incremental push stores or stores with only one samza job, we should consider the weight of
   * unreachable fabric and make the decision. For example, we should not let partition ready-to-serve when the only
   * source fabric is unreachable.
   *
   * In non-aggregate mode of consumption only return the local fabric lag
   * @param sourceRealTimeTopicKafkaURLs
   * @param partitionConsumptionState
   * @param shouldLogLag
   * @return
   */
  @Override
  protected long measureRTOffsetLagForMultiRegions(Set<String> sourceRealTimeTopicKafkaURLs, PartitionConsumptionState partitionConsumptionState, boolean shouldLogLag) {
    if (this.hybridStoreConfig.get().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)) {
      long minLag = Long.MAX_VALUE;
      for (String sourceRealTimeTopicKafkaURL : sourceRealTimeTopicKafkaURLs) {
        long lag = measureRTOffsetLagForSingleRegion(sourceRealTimeTopicKafkaURL, partitionConsumptionState, shouldLogLag);
        if (minLag > lag) {
          minLag = lag;
        }
      }
      return minLag;
    } else if (this.hybridStoreConfig.get().getDataReplicationPolicy().equals(DataReplicationPolicy.ACTIVE_ACTIVE)) {
      long maxLag = Long.MIN_VALUE;
      int numberOfUnreachableRegions = 0;
      for (String sourceRealTimeTopicKafkaURL : sourceRealTimeTopicKafkaURLs) {
        try {
          long lag = measureRTOffsetLagForSingleRegion(sourceRealTimeTopicKafkaURL, partitionConsumptionState, shouldLogLag);
          maxLag = Math.max(lag, maxLag);
        } catch (Exception e) {
          logger.error("Failed to measure RT offset lag for topic {} partition id {} in {}",
              partitionConsumptionState.getOffsetRecord().getLeaderTopic(), partitionConsumptionState.getPartition(),
              sourceRealTimeTopicKafkaURL, e);
          if (++numberOfUnreachableRegions > 1) {
            logger.error("More than one regions are unreachable. Return {} as it is not ready-to-serve", Long.MAX_VALUE);
            return Long.MAX_VALUE;
          }
        }
      }
      return maxLag;
    } else {
      if (sourceRealTimeTopicKafkaURLs.contains(localKafkaServer)) {
        return measureRTOffsetLagForSingleRegion(localKafkaServer, partitionConsumptionState, shouldLogLag);
      } else {
        throw new VeniceException(String.format("Expect source RT Kafka URLs contains local Kafka URL. Got local " +
            "Kafka URL %s and RT source Kafka URLs %s", localKafkaServer, sourceRealTimeTopicKafkaURLs));
      }
    }
  }
}
