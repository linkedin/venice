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
import com.linkedin.venice.exceptions.VeniceException;
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
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.*;


/**
 * This class contains logic that SNs must perform if a store-version is running in Active/Active mode.
 */
public class ActiveActiveStoreIngestionTask extends LeaderFollowerStoreIngestionTask {
  private static final Logger logger = Logger.getLogger(ActiveActiveStoreIngestionTask.class);
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
    if (partitionConsumptionStateMap.get(partition).isEndOfPushReceived()) {
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
    /**
     * With {@link com.linkedin.davinci.replication.BatchConflictResolutionPolicy.BATCH_WRITE_LOSES} there is no need
     * to perform DCR before EOP and L/F DIV passthrough mode should be used.
     * TODO. We need to refactor this logic when we support other batch conflict resolution policy.
     */
    if (!partitionConsumptionState.isEndOfPushReceived()) {
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
    long offsetSumPreOperation = mergeConflictResolver.extractOffsetVectorSumFromReplicationMetadata(replicationMetadataOfOldValue, schemaIdOfOldValue);
    List<Long> recordTimestampsPreOperation = mergeConflictResolver.extractTimestampFromReplicationMetadata(replicationMetadataOfOldValue, schemaIdOfOldValue);
    // get the source offset and the id
    int sourceKafkaClusterId = kafkaClusterUrlToIdMap.getOrDefault(consumerRecordWrapper.kafkaUrl(), -1);
    long sourceOffset = consumerRecordWrapper.consumerRecord().offset();
    final MergeConflictResult mergeConflictResult;

    switch (msgType) {
      case PUT:
        mergeConflictResult = mergeConflictResolver.put(lazyOldValue, replicationMetadataOfOldValue,
            ((Put) kafkaValue.payloadUnion).putValue, writeTimestamp, schemaIdOfOldValue, incomingValueSchemaId, sourceOffset, sourceKafkaClusterId);
        break;
      case DELETE:
        mergeConflictResult = mergeConflictResolver.delete(replicationMetadataOfOldValue, schemaIdOfOldValue, writeTimestamp, sourceOffset, sourceKafkaClusterId);
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
      validatePostOperationResultsAndRecord(mergeConflictResult, offsetSumPreOperation, recordTimestampsPreOperation);
      // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this call.
      producePutOrDeleteToKafka(mergeConflictResult, partitionConsumptionState, keyBytes, isChunkedTopic, consumerRecordWrapper);
    }
  }

  private void validatePostOperationResultsAndRecord(MergeConflictResult mergeConflictResult, Long offsetSumPreOperation, List<Long> timestampsPreOperation) {
    // Nothing was applied, no harm no foul
    if (mergeConflictResult.isUpdateIgnored()) {
      return;
    }
    // Post Validation checks on resolution
    if (offsetSumPreOperation > mergeConflictResolver.extractOffsetVectorSumFromReplicationMetadata(mergeConflictResult.getReplicationMetadata(), mergeConflictResult.getValueSchemaID())) {
      // offsets went backwards, raise an alert!
      storeIngestionStats.recordOffsetRegressionDCRError();
      logger.error(String.format("Offset vector found to have gone backwards!! New invalid replication metadata result:%s",
          mergeConflictResolver.printReplicationMetadata(mergeConflictResult.getReplicationMetadata(), mergeConflictResult.getValueSchemaID())));
    }

    // TODO: This comparison doesn't work well for write compute+schema evolution (can spike up). VENG-8129
    // this works fine for now however as we do not fully support A/A write compute operations (as we only do root timestamp comparisons).
    List<Long> timestampsPostOperation = mergeConflictResolver.extractTimestampFromReplicationMetadata(mergeConflictResult.getReplicationMetadata(), mergeConflictResult.getValueSchemaID());
    for(int i = 0; i < timestampsPreOperation.size(); i++) {
      if (timestampsPreOperation.get(i) > timestampsPostOperation.get(i)) {
        // timestamps went backwards, raise an alert!
        storeIngestionStats.recordTimeStampRegressionDCRError();
        logger.error(String.format("Timestamp found to have gone backwards!! Invalid replication metadata result:%s",
            mergeConflictResolver.printReplicationMetadata(mergeConflictResult.getReplicationMetadata(), mergeConflictResult.getValueSchemaID())));
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
          serverConfig.isComputeFastAvroEnabled(), schemaRepository, storeName, compressorFactory);
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
      updatedPut.replicationMetadataVersionId = replicationMetadataVersionId;
      updatedPut.replicationMetadataPayload = updatedReplicationMetadataBytes;

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
    if (partitionConsumptionState.getLeaderFollowerState() != IN_TRANSITION_FROM_STANDBY_TO_LEADER) {
      throw new VeniceException(String.format("Expect state %s but got %s",
          IN_TRANSITION_FROM_STANDBY_TO_LEADER, partitionConsumptionState.toString()
      ));
    }
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

    // subscribe to the new upstream
    final String leaderTopic = offsetRecord.getLeaderTopic();
    Set<String> leaderSourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    Map<String, Long> leaderOffsetByKafkaURL = new HashMap<>(leaderSourceKafkaURLs.size());
    leaderSourceKafkaURLs.forEach(kafkaURL -> {
      final long leaderStartOffset = offsetRecord.getLeaderOffset(kafkaURL);
      consumerSubscribe(
              leaderTopic,
              partitionConsumptionState.getSourceTopicPartition(leaderTopic),
              leaderStartOffset,
              kafkaURL
      );
      leaderOffsetByKafkaURL.put(kafkaURL, leaderStartOffset);
    });

    partitionConsumptionState.setLeaderFollowerState(LEADER);
    logger.info(consumerTaskId + " promoted to leader for partition " + partition
            + "; start consuming from " + offsetRecord.getLeaderTopic() + " with offset by Kafka URL mapping " + leaderOffsetByKafkaURL);
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
      Long upstreamStartOffset = partitionConsumptionState.getOffsetRecord().getUpstreamOffsetWithNoDefault(sourceKafkaURL.toString());
      if (upstreamStartOffset == null || upstreamStartOffset < 0) {
        if (topicSwitch.rewindStartTimestamp > 0) {
          upstreamStartOffset = getTopicPartitionOffsetByKafkaURL(
                  sourceKafkaURL,
                  newSourceTopicName,
                  sourceTopicPartition,
                  topicSwitch.rewindStartTimestamp
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
    final int newSourceTopicPartition = partitionConsumptionState.getSourceTopicPartition(newSourceTopicName);
    topicSwitch.sourceKafkaServers.forEach(sourceKafkaURL -> {
      if (topicSwitch.rewindStartTimestamp > 0) {
        long upstreamStartOffset = getTopicManager(sourceKafkaURL.toString())
            .getPartitionOffsetByTime(newSourceTopicName, newSourceTopicPartition, topicSwitch.rewindStartTimestamp);
        if (upstreamStartOffset != OffsetRecord.LOWEST_OFFSET) {
          upstreamStartOffset -= 1;
        }
        upstreamStartOffsetByKafkaURL.put(sourceKafkaURL.toString(), upstreamStartOffset);
      } else {
        upstreamStartOffsetByKafkaURL.put(sourceKafkaURL.toString(), OffsetRecord.LOWEST_OFFSET);
      }
    });

    syncTopicSwitchToIngestionMetadataService(
        topicSwitch,
        partitionConsumptionState,
        upstreamStartOffsetByKafkaURL
    );

    upstreamStartOffsetByKafkaURL.forEach((sourceKafkaURL, upstreamStartOffset) -> {
      partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(sourceKafkaURL, upstreamStartOffset);
    });
    if (!isLeader(partitionConsumptionState)) {
      partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopicName);
      this.defaultReadyToServeChecker.apply(partitionConsumptionState);
    }
  }

  @Override
  protected void updateOffsetRecord(PartitionConsumptionState partitionConsumptionState, OffsetRecord offsetRecord,
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, LeaderProducedRecordContext leaderProducedRecordContext) {

    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = consumerRecordWrapper.consumerRecord();
    // Only update the metadata if this replica should NOT produce to version topic.
    if (!shouldProduceToVersionTopic(partitionConsumptionState)) {
      /**
       * If either (1) this is a follower replica or (2) this is a leader replica who is consuming from version topic
       * in a local Kafka cluster, we can update the offset metadata in offset record right after consuming a message;
       * otherwise, if the leader is consuming from real-time topic or grandfathering topic, it should update offset
       * metadata after successfully produce a corresponding message.
       */
      KafkaMessageEnvelope kafkaValue = consumerRecord.value();
      offsetRecord.setLocalVersionTopicOffset(consumerRecord.offset());

      // also update the leader topic offset using the upstream offset in ProducerMetadata
      if (kafkaValue.producerMetadata.upstreamOffset >= 0
          || (kafkaValue.leaderMetadataFooter != null && kafkaValue.leaderMetadataFooter.upstreamOffset >= 0)) {

        final long newUpstreamOffset =
            kafkaValue.leaderMetadataFooter == null ? kafkaValue.producerMetadata.upstreamOffset : kafkaValue.leaderMetadataFooter.upstreamOffset;
        final String upstreamKafkaURL;
        if (isLeader(partitionConsumptionState)) {
          upstreamKafkaURL = consumerRecordWrapper.kafkaUrl(); // Wherever leader consumes from is considered as "upstream"
        } else {
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

        final long previousUpstreamOffset = offsetRecord.getUpstreamOffset(upstreamKafkaURL);
        checkAndHandleUpstreamOffsetRewind(
            partitionConsumptionState, offsetRecord, consumerRecordWrapper.consumerRecord(), newUpstreamOffset, previousUpstreamOffset);
        /**
         * Keep updating the upstream offset no matter whether there is a rewind or not; rewind could happen
         * to the true leader when the old leader doesn't stop producing.
         */
        offsetRecord.setLeaderUpstreamOffset(upstreamKafkaURL, newUpstreamOffset);
      }
      // update leader producer GUID
      offsetRecord.setLeaderGUID(kafkaValue.producerMetadata.producerGUID);
      if (kafkaValue.leaderMetadataFooter != null) {
        offsetRecord.setLeaderHostId(kafkaValue.leaderMetadataFooter.hostName.toString());
      }
    } else {
      updateOffsetRecordAsRemoteConsumeLeader(
              leaderProducedRecordContext,
              offsetRecord,
              consumerRecordWrapper.kafkaUrl(),
              consumerRecord
      );
    }
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
}
