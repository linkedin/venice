package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.STANDBY;
import static com.linkedin.venice.VeniceConstants.REWIND_TIME_DECIDED_BY_SERVER;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.MergeConflictResolver;
import com.linkedin.davinci.replication.merge.MergeConflictResolverFactory;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.storage.chunking.RawBytesChunkingAdapter;
import com.linkedin.davinci.storage.chunking.SingleGetChunkingAdapter;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.TopicManager;
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
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.serialization.RawBytesStoreDeserializerCache;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.ChunkAwareCallback;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.PutMetadata;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains logic that SNs must perform if a store-version is running in Active/Active mode.
 */
public class ActiveActiveStoreIngestionTask extends LeaderFollowerStoreIngestionTask {
  private static final Logger LOGGER = LogManager.getLogger(ActiveActiveStoreIngestionTask.class);
  private static final byte[] BINARY_DECODER_PARAM = new byte[16];

  private final int rmdProtocolVersionID;
  private final MergeConflictResolver mergeConflictResolver;
  private final RmdSerDe rmdSerDe;
  private final Lazy<KeyLevelLocksManager> keyLevelLocksManager;
  private final AggVersionedIngestionStats aggVersionedIngestionStats;
  private final RemoteIngestionRepairService remoteIngestionRepairService;

  private static class ReusableObjects {
    // reuse buffer for rocksDB value object
    final ByteBuffer reusedByteBuffer = ByteBuffer.allocate(1024 * 1024);
    final BinaryDecoder binaryDecoder =
        AvroCompatibilityHelper.newBinaryDecoder(BINARY_DECODER_PARAM, 0, BINARY_DECODER_PARAM.length, null);
  }

  private final ThreadLocal<ReusableObjects> threadLocalReusableObjects = ThreadLocal.withInitial(ReusableObjects::new);

  public ActiveActiveStoreIngestionTask(
      StoreIngestionTaskFactory.Builder builder,
      Store store,
      Version version,
      Properties kafkaConsumerProperties,
      BooleanSupplier isCurrentVersion,
      VeniceStoreVersionConfig storeConfig,
      int errorPartitionId,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend) {
    super(
        builder,
        store,
        version,
        kafkaConsumerProperties,
        isCurrentVersion,
        storeConfig,
        errorPartitionId,
        isIsolatedIngestion,
        cacheBackend);

    this.rmdProtocolVersionID = version.getRmdVersionId();

    this.aggVersionedIngestionStats = versionedIngestionStats;
    int knownKafkaClusterNumber = serverConfig.getKafkaClusterIdToUrlMap().size();
    int consumerPoolSizePerKafkaCluster = serverConfig.getConsumerPoolSizePerKafkaCluster();
    int initialPoolSize = knownKafkaClusterNumber + 1;
    /**
     * In theory, the maximum # of keys each ingestion task can process is the # of consumers allocated for it.
     */
    int maxKeyLevelLocksPoolSize =
        Math.min(storeVersionPartitionCount, consumerPoolSizePerKafkaCluster) * knownKafkaClusterNumber + 1;
    this.keyLevelLocksManager =
        Lazy.of(() -> new KeyLevelLocksManager(getVersionTopic().getName(), initialPoolSize, maxKeyLevelLocksPoolSize));
    StringAnnotatedStoreSchemaCache annotatedReadOnlySchemaRepository =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);

    this.rmdSerDe = new RmdSerDe(annotatedReadOnlySchemaRepository, rmdProtocolVersionID);
    this.mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            annotatedReadOnlySchemaRepository,
            rmdSerDe,
            getStoreName(),
            isWriteComputationEnabled);
    this.remoteIngestionRepairService = builder.getRemoteIngestionRepairService();
  }

  @Override
  protected DelegateConsumerRecordResult delegateConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      int subPartition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      long currentTimeForMetricsMs) {
    if (!consumerRecord.getTopicPartition().getPubSubTopic().isRealTime()) {
      /**
       * We don't need to lock the partition here because during VT consumption there is only one consumption source.
       */
      return super.delegateConsumerRecord(
          consumerRecord,
          subPartition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs,
          currentTimeForMetricsMs);
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
      final ByteArrayKey byteArrayKey = ByteArrayKey.wrap(consumerRecord.getKey().getKey());
      ReentrantLock keyLevelLock = this.keyLevelLocksManager.get().acquireLockByKey(byteArrayKey);
      keyLevelLock.lock();
      try {
        return super.delegateConsumerRecord(
            consumerRecord,
            subPartition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingRecordTimestampNs,
            currentTimeForMetricsMs);
      } finally {
        keyLevelLock.unlock();
        this.keyLevelLocksManager.get().releaseLock(byteArrayKey);
      }
    }
  }

  @Override
  protected void putInStorageEngine(int partition, byte[] keyBytes, Put put) {
    try {
      // TODO: Honor BatchConflictResolutionPolicy and maybe persist RMD for batch push records.
      StorageOperationType storageOperationType =
          getStorageOperationType(partition, put.putValue, put.replicationMetadataPayload);
      switch (storageOperationType) {
        case VALUE_AND_RMD:
          storageEngine.putWithReplicationMetadata(
              partition,
              keyBytes,
              put.putValue,
              prependReplicationMetadataBytesWithValueSchemaId(put.replicationMetadataPayload, put.schemaId));
          break;
        case RMD_CHUNK:
          storageEngine.putReplicationMetadata(
              partition,
              keyBytes,
              prependReplicationMetadataBytesWithValueSchemaId(put.replicationMetadataPayload, put.schemaId));
          break;
        case VALUE:
          storageEngine.put(partition, keyBytes, put.putValue);
          break;
        default:
          // do nothing
          break;
      }
    } catch (PersistenceFailureException e) {
      throwOrLogStorageFailureDependingIfStillSubscribed(partition, e);
    }
  }

  @Override
  protected void removeFromStorageEngine(int partition, byte[] keyBytes, Delete delete) {
    try {
      // TODO: Honor BatchConflictResolutionPolicy and maybe persist RMD for batch push records.
      switch (getStorageOperationType(partition, null, delete.replicationMetadataPayload)) {
        case VALUE_AND_RMD:
          byte[] metadataBytesWithValueSchemaId =
              prependReplicationMetadataBytesWithValueSchemaId(delete.replicationMetadataPayload, delete.schemaId);
          storageEngine.deleteWithReplicationMetadata(partition, keyBytes, metadataBytesWithValueSchemaId);
          break;
        case VALUE:
          storageEngine.delete(partition, keyBytes);
          break;
        default:
          // do nothing
          break;
      }
    } catch (PersistenceFailureException e) {
      throwOrLogStorageFailureDependingIfStillSubscribed(partition, e);
    }
  }

  /** @return what kind of storage operation to execute, if any. */
  private StorageOperationType getStorageOperationType(int partition, ByteBuffer valuePayload, ByteBuffer rmdPayload) {
    PartitionConsumptionState pcs = partitionConsumptionStateMap.get(partition);
    if (pcs == null) {
      logStorageOperationWhileUnsubscribed(partition);
      return StorageOperationType.NONE;
    }
    if (isDaVinciClient) {
      return StorageOperationType.VALUE;
    }
    if (rmdPayload == null) {
      throw new IllegalArgumentException("Replication metadata payload not found.");
    }
    if (pcs.isEndOfPushReceived() || rmdPayload.remaining() > 0) {
      // value payload == null means it is a DELETE request, while value payload size > 0 means it is a PUT request.
      if (valuePayload == null || valuePayload.remaining() > 0) {
        return StorageOperationType.VALUE_AND_RMD;
      }
      return StorageOperationType.RMD_CHUNK;
    } else {
      return StorageOperationType.VALUE;
    }
  }

  private enum StorageOperationType {
    VALUE_AND_RMD, // Operate on value associated with RMD
    VALUE, // Operate on full or chunked value
    RMD_CHUNK, // Operate on chunked RMD
    NONE
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
   * Get the existing value schema ID and RMD associated with the given key. If information for this key is found from
   * the transient map then use that, otherwise get it from storage engine.
   *
   * @param partitionConsumptionState The {@link PartitionConsumptionState} of the current partition
   * @param key Bytes of key.
   * @param subPartition The partition to fetch the replication metadata from storage engine
   * @return The object containing RMD and value schema id. If nothing is found, return null
   */
  RmdWithValueSchemaId getReplicationMetadataAndSchemaId(
      PartitionConsumptionState partitionConsumptionState,
      byte[] key,
      int subPartition,
      long currentTimeForMetricsMs) {
    PartitionConsumptionState.TransientRecord cachedRecord = partitionConsumptionState.getTransientRecord(key);
    if (cachedRecord != null) {
      getHostLevelIngestionStats().recordIngestionReplicationMetadataCacheHitCount(currentTimeForMetricsMs);
      return new RmdWithValueSchemaId(
          cachedRecord.getValueSchemaId(),
          getRmdProtocolVersionID(),
          cachedRecord.getReplicationMetadataRecord(),
          cachedRecord.getRmdManifest());
    }
    ChunkedValueManifestContainer rmdManifestContainer = new ChunkedValueManifestContainer();
    byte[] replicationMetadataWithValueSchemaBytes =
        getRmdWithValueSchemaByteBufferFromStorage(subPartition, key, rmdManifestContainer, currentTimeForMetricsMs);
    if (replicationMetadataWithValueSchemaBytes == null) {
      return null; // No RMD for this key
    }
    RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId();
    // Get old RMD manifest value from RMD Manifest container object.
    rmdWithValueSchemaId.setRmdManifest(rmdManifestContainer.getManifest());
    getRmdSerDe()
        .deserializeValueSchemaIdPrependedRmdBytes(replicationMetadataWithValueSchemaBytes, rmdWithValueSchemaId);
    return rmdWithValueSchemaId;
  }

  public RmdSerDe getRmdSerDe() {
    return rmdSerDe;
  }

  /**
   * This method tries to retrieve the RMD bytes with prepended value schema ID from storage engine. It will also store
   * RMD manifest into passed-in {@link ChunkedValueManifestContainer} container object if current RMD value is chunked.
   */
  byte[] getRmdWithValueSchemaByteBufferFromStorage(
      int subPartition,
      byte[] key,
      ChunkedValueManifestContainer rmdManifestContainer,
      long currentTimeForMetricsMs) {
    final long lookupStartTimeInNS = System.nanoTime();
    ValueRecord result = SingleGetChunkingAdapter
        .getReplicationMetadata(getStorageEngine(), subPartition, key, isChunked(), null, rmdManifestContainer);
    getHostLevelIngestionStats().recordIngestionReplicationMetadataLookUpLatency(
        LatencyUtils.getLatencyInMS(lookupStartTimeInNS),
        currentTimeForMetricsMs);
    if (result == null) {
      return null;
    }
    return result.serialize();
  }

  // This function may modify the original record in KME, it is unsafe to use the payload from KME directly after
  // this function.
  protected void processMessageAndMaybeProduceToKafka(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      int subPartition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      long currentTimeForMetricsMs) {
    /**
     * With {@link com.linkedin.davinci.replication.BatchConflictResolutionPolicy.BATCH_WRITE_LOSES} there is no need
     * to perform DCR before EOP and L/F DIV passthrough mode should be used. If the version is going through data
     * recovery then there is no need to perform DCR until we completed data recovery and switched to consume from RT.
     * TODO. We need to refactor this logic when we support other batch conflict resolution policy.
     */
    if (!partitionConsumptionState.isEndOfPushReceived()
        || isDataRecovery && partitionConsumptionState.getTopicSwitch() != null) {
      super.processMessageAndMaybeProduceToKafka(
          consumerRecord,
          partitionConsumptionState,
          subPartition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs,
          currentTimeForMetricsMs);
      return;
    }
    KafkaKey kafkaKey = consumerRecord.getKey();
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    byte[] keyBytes = kafkaKey.getKey();
    MessageType msgType = MessageType.valueOf(kafkaValue.messageType);
    final int incomingValueSchemaId;
    final int incomingWriteComputeSchemaId;

    switch (msgType) {
      case PUT:
        incomingValueSchemaId = ((Put) kafkaValue.payloadUnion).schemaId;
        incomingWriteComputeSchemaId = -1;
        break;
      case UPDATE:
        Update incomingUpdate = (Update) kafkaValue.payloadUnion;
        incomingValueSchemaId = incomingUpdate.schemaId;
        incomingWriteComputeSchemaId = incomingUpdate.updateSchemaId;
        break;
      case DELETE:
        incomingValueSchemaId = -1; // Ignored since we don't need the schema id for DELETE operations.
        incomingWriteComputeSchemaId = -1;
        break;
      default:
        throw new VeniceMessageException(
            consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }
    final ChunkedValueManifestContainer valueManifestContainer = new ChunkedValueManifestContainer();
    Lazy<ByteBuffer> oldValueProvider = Lazy.of(
        () -> getValueBytesForKey(
            partitionConsumptionState,
            keyBytes,
            consumerRecord.getTopicPartition(),
            valueManifestContainer,
            currentTimeForMetricsMs));

    final RmdWithValueSchemaId rmdWithValueSchemaID =
        getReplicationMetadataAndSchemaId(partitionConsumptionState, keyBytes, subPartition, currentTimeForMetricsMs);

    final long writeTimestamp = getWriteTimestampFromKME(kafkaValue);
    final long offsetSumPreOperation =
        rmdWithValueSchemaID != null ? RmdUtils.extractOffsetVectorSumFromRmd(rmdWithValueSchemaID.getRmdRecord()) : 0;
    List<Long> recordTimestampsPreOperation = rmdWithValueSchemaID != null
        ? RmdUtils.extractTimestampFromRmd(rmdWithValueSchemaID.getRmdRecord())
        : Collections.singletonList(0L);
    // get the source offset and the id
    long sourceOffset = consumerRecord.getOffset();
    final MergeConflictResult mergeConflictResult;

    aggVersionedIngestionStats.recordTotalDCR(storeName, versionNumber);

    switch (msgType) {
      case PUT:
        mergeConflictResult = mergeConflictResolver.put(
            oldValueProvider,
            rmdWithValueSchemaID,
            ((Put) kafkaValue.payloadUnion).putValue,
            writeTimestamp,
            incomingValueSchemaId,
            sourceOffset,
            kafkaClusterId,
            kafkaClusterId // Use the kafka cluster ID as the colo ID for now because one colo/fabric has only one
                           // Kafka cluster. TODO: evaluate whether it is enough this way, or we need to add a new
                           // config to represent the mapping from Kafka server URLs to colo ID.
        );
        break;

      case DELETE:
        mergeConflictResult = mergeConflictResolver.delete(
            oldValueProvider,
            rmdWithValueSchemaID,
            writeTimestamp,
            sourceOffset,
            kafkaClusterId,
            kafkaClusterId);
        break;

      case UPDATE:
        mergeConflictResult = mergeConflictResolver.update(
            oldValueProvider,
            rmdWithValueSchemaID,
            ((Update) kafkaValue.payloadUnion).updateValue,
            incomingValueSchemaId,
            incomingWriteComputeSchemaId,
            writeTimestamp,
            sourceOffset,
            kafkaClusterId,
            kafkaClusterId);
        break;
      default:
        throw new VeniceMessageException(
            consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }

    aggVersionedIngestionStats.recordConsumedRecordEndToEndProcessingLatency(
        storeName,
        versionNumber,
        LatencyUtils.getLatencyInMS(beforeProcessingRecordTimestampNs),
        currentTimeForMetricsMs);

    if (mergeConflictResult.isUpdateIgnored()) {
      hostLevelIngestionStats.recordUpdateIgnoredDCR();
      // Record the last ignored offset
      partitionConsumptionState
          .updateLatestIgnoredUpstreamRTOffset(kafkaClusterIdToUrlMap.get(kafkaClusterId), sourceOffset);
    } else {
      validatePostOperationResultsAndRecord(mergeConflictResult, offsetSumPreOperation, recordTimestampsPreOperation);

      // Apply this update to any views for this store
      // TODO: It'd be good to be able to do this in LeaderFollowerStoreIngestionTask instead, however, AA currently is
      // the
      // only extension of IngestionTask which does a read from disk before applying the record. This makes the
      // following function
      // call in this context much less obtrusive, however, it implies that all views can only work for AA stores
      int valueSchemaId =
          rmdWithValueSchemaID != null ? rmdWithValueSchemaID.getValueSchemaId() : incomingValueSchemaId;
      this.viewWriters.forEach(
          (k, v) -> v.processRecord(
              mergeConflictResult.getNewValue(),
              oldValueProvider.get(),
              keyBytes,
              versionNumber,
              incomingValueSchemaId,
              valueSchemaId,
              mergeConflictResult.getRmdRecord()));

      // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after
      // this call.
      producePutOrDeleteToKafka(
          mergeConflictResult,
          partitionConsumptionState,
          keyBytes,
          consumerRecord,
          subPartition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs,
          valueManifestContainer.getManifest(),
          rmdWithValueSchemaID == null ? null : rmdWithValueSchemaID.getRmdManifest());
    }
  }

  private long getWriteTimestampFromKME(KafkaMessageEnvelope kme) {
    if (kme.producerMetadata.logicalTimestamp >= 0) {
      return kme.producerMetadata.logicalTimestamp;
    } else {
      return kme.producerMetadata.messageTimestamp;
    }
  }

  private void validatePostOperationResultsAndRecord(
      MergeConflictResult mergeConflictResult,
      Long offsetSumPreOperation,
      List<Long> timestampsPreOperation) {
    // Nothing was applied, no harm no foul
    if (mergeConflictResult.isUpdateIgnored()) {
      return;
    }
    // Post Validation checks on resolution
    GenericRecord rmdRecord = mergeConflictResult.getRmdRecord();
    if (offsetSumPreOperation > RmdUtils.extractOffsetVectorSumFromRmd(rmdRecord)) {
      // offsets went backwards, raise an alert!
      hostLevelIngestionStats.recordOffsetRegressionDCRError();
      aggVersionedIngestionStats.recordOffsetRegressionDCRError(storeName, versionNumber);
      LOGGER
          .error("Offset vector found to have gone backwards!! New invalid replication metadata result: {}", rmdRecord);
    }

    // TODO: This comparison doesn't work well for write compute+schema evolution (can spike up). VENG-8129
    // this works fine for now however as we do not fully support A/A write compute operations (as we only do root
    // timestamp comparisons).

    List<Long> timestampsPostOperation = RmdUtils.extractTimestampFromRmd(rmdRecord);
    for (int i = 0; i < timestampsPreOperation.size(); i++) {
      if (timestampsPreOperation.get(i) > timestampsPostOperation.get(i)) {
        // timestamps went backwards, raise an alert!
        hostLevelIngestionStats.recordTimestampRegressionDCRError();
        aggVersionedIngestionStats.recordTimestampRegressionDCRError(storeName, versionNumber);
        LOGGER.error(
            "Timestamp found to have gone backwards!! Invalid replication metadata result: {}",
            mergeConflictResult.getRmdRecord());
      }
    }
  }

  /**
   * Get the value bytes for a key from {@link PartitionConsumptionState.TransientRecord} or from disk. The assumption
   * is that the {@link PartitionConsumptionState.TransientRecord} only contains the full value.
   * @param partitionConsumptionState The {@link PartitionConsumptionState} of the current partition
   * @param key The key bytes of the incoming record.
   * @param topicPartition The {@link PubSubTopicPartition} from which the incoming record was consumed
   * @return
   */
  private ByteBuffer getValueBytesForKey(
      PartitionConsumptionState partitionConsumptionState,
      byte[] key,
      PubSubTopicPartition topicPartition,
      ChunkedValueManifestContainer valueManifestContainer,
      long currentTimeForMetricsMs) {
    ByteBuffer originalValue = null;
    // Find the existing value. If a value for this key is found from the transient map then use that value, otherwise
    // get it from DB.
    PartitionConsumptionState.TransientRecord transientRecord = partitionConsumptionState.getTransientRecord(key);
    if (transientRecord == null) {
      long lookupStartTimeInNS = System.nanoTime();
      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      ByteBuffer reusedRawValue = reusableObjects.reusedByteBuffer;
      BinaryDecoder binaryDecoder = reusableObjects.binaryDecoder;

      originalValue = RawBytesChunkingAdapter.INSTANCE.get(
          storageEngine,
          getSubPartitionId(key, topicPartition),
          ByteBuffer.wrap(key),
          isChunked,
          reusedRawValue,
          binaryDecoder,
          null,
          schemaRepository.getSupersetOrLatestValueSchema(storeName).getId(),
          RawBytesStoreDeserializerCache.getInstance(),
          compressor.get(),
          valueManifestContainer);
      hostLevelIngestionStats.recordIngestionValueBytesLookUpLatency(
          LatencyUtils.getLatencyInMS(lookupStartTimeInNS),
          currentTimeForMetricsMs);
    } else {
      hostLevelIngestionStats.recordIngestionValueBytesCacheHitCount(currentTimeForMetricsMs);
      // construct originalValue from this transient record only if it's not null.
      if (transientRecord.getValue() != null) {
        if (valueManifestContainer != null) {
          valueManifestContainer.setManifest(transientRecord.getValueManifest());
        }
        originalValue = ByteBuffer
            .wrap(transientRecord.getValue(), transientRecord.getValueOffset(), transientRecord.getValueLen());
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
   * @param consumerRecord The {@link PubSubMessage} for the current record.
   * @param subPartition
   * @param kafkaUrl
   */
  private void producePutOrDeleteToKafka(
      MergeConflictResult mergeConflictResult,
      PartitionConsumptionState partitionConsumptionState,
      byte[] key,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      int subPartition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {

    final ByteBuffer updatedValueBytes = maybeCompressData(
        consumerRecord.getTopicPartition().getPartitionNumber(),
        mergeConflictResult.getNewValue(),
        partitionConsumptionState);
    final int valueSchemaId = mergeConflictResult.getValueSchemaId();

    GenericRecord rmdRecord = mergeConflictResult.getRmdRecord();
    final ByteBuffer updatedRmdBytes =
        rmdSerDe.serializeRmdRecord(mergeConflictResult.getValueSchemaId(), mergeConflictResult.getRmdRecord());
    // finally produce and update the transient record map.
    if (updatedValueBytes == null) {
      hostLevelIngestionStats.recordTombstoneCreatedDCR();
      aggVersionedIngestionStats.recordTombStoneCreationDCR(storeName, versionNumber);
      partitionConsumptionState
          .setTransientRecord(kafkaClusterId, consumerRecord.getOffset(), key, valueSchemaId, rmdRecord);
      Delete deletePayload = new Delete();
      deletePayload.schemaId = valueSchemaId;
      deletePayload.replicationMetadataVersionId = rmdProtocolVersionID;
      deletePayload.replicationMetadataPayload = updatedRmdBytes;
      BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceToTopicFunction =
          (callback, sourceTopicOffset) -> veniceWriter.get()
              .delete(
                  key,
                  callback,
                  sourceTopicOffset,
                  APP_DEFAULT_LOGICAL_TS,
                  new DeleteMetadata(valueSchemaId, rmdProtocolVersionID, updatedRmdBytes),
                  oldValueManifest,
                  oldRmdManifest);
      LeaderProducedRecordContext leaderProducedRecordContext =
          LeaderProducedRecordContext.newDeleteRecord(kafkaClusterId, consumerRecord.getOffset(), key, deletePayload);
      produceToLocalKafka(
          consumerRecord,
          partitionConsumptionState,
          leaderProducedRecordContext,
          produceToTopicFunction,
          subPartition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs);
    } else {
      int valueLen = updatedValueBytes.remaining();
      partitionConsumptionState.setTransientRecord(
          kafkaClusterId,
          consumerRecord.getOffset(),
          key,
          updatedValueBytes.array(),
          updatedValueBytes.position(),
          valueLen,
          valueSchemaId,
          rmdRecord);

      Put updatedPut = new Put();
      updatedPut.putValue = ByteUtils
          .prependIntHeaderToByteBuffer(updatedValueBytes, valueSchemaId, mergeConflictResult.doesResultReuseInput());
      updatedPut.schemaId = valueSchemaId;
      updatedPut.replicationMetadataVersionId = rmdProtocolVersionID;
      updatedPut.replicationMetadataPayload = updatedRmdBytes;

      BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceToTopicFunction = getProduceToTopicFunction(
          key,
          updatedValueBytes,
          updatedRmdBytes,
          oldValueManifest,
          oldRmdManifest,
          valueSchemaId,
          mergeConflictResult.doesResultReuseInput());
      produceToLocalKafka(
          consumerRecord,
          partitionConsumptionState,
          LeaderProducedRecordContext.newPutRecord(kafkaClusterId, consumerRecord.getOffset(), key, updatedPut),
          produceToTopicFunction,
          subPartition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs);
    }
  }

  @Override
  protected void produceToLocalKafka(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceFunction,
      int subPartition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs) {
    super.produceToLocalKafka(
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        produceFunction,
        subPartition,
        kafkaUrl,
        kafkaClusterId,
        beforeProcessingRecordTimestampNs);
    // Update the partition consumption state to say that we've transmitted the message to kafka (but haven't
    // necessarily received an ack back yet).
    if (partitionConsumptionState.getLeaderFollowerState() == LEADER && partitionConsumptionState.isHybrid()
        && consumerRecord.getTopicPartition().getPubSubTopic().isRealTime()) {
      partitionConsumptionState.updateLatestRTOffsetTriedToProduceToVTMap(kafkaUrl, consumerRecord.getOffset());
    }
  }

  @Override
  protected void startConsumingAsLeader(PartitionConsumptionState partitionConsumptionState) {
    final int partition = partitionConsumptionState.getPartition();
    final OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
    final PubSubTopic leaderTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);

    /**
     * Note that this function is called after the new leader has waited for 5 minutes of inactivity on the local VT topic.
     * The new leader might NOT need to switch to remote consumption in a case where map-reduce jobs of a batch job stuck
     * on producing to the local VT so that there is no activity in the local VT.
     */
    if (shouldNewLeaderSwitchToRemoteConsumption(partitionConsumptionState)) {
      partitionConsumptionState.setConsumeRemotely(true);
      LOGGER.info("{} enabled remote consumption from topic {} partition {}", consumerTaskId, leaderTopic, partition);
    }

    partitionConsumptionState.setLeaderFollowerState(LEADER);
    Set<String> leaderSourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    Map<String, Long> leaderOffsetByKafkaURL = new HashMap<>(leaderSourceKafkaURLs.size());
    leaderSourceKafkaURLs.forEach(
        kafkaURL -> leaderOffsetByKafkaURL
            .put(kafkaURL, partitionConsumptionState.getLeaderOffset(kafkaURL, pubSubTopicRepository)));
    LOGGER.info(
        "{} is promoted to leader for partition {} and it is going to start consuming from "
            + "topic {} with offset by Kafka URL mapping {}",
        consumerTaskId,
        partition,
        leaderTopic,
        leaderOffsetByKafkaURL);

    // subscribe to the new upstream
    leaderOffsetByKafkaURL.forEach((kafkaURL, leaderStartOffset) -> {
      consumerSubscribe(partitionConsumptionState.getSourceTopicPartition(leaderTopic), leaderStartOffset, kafkaURL);
    });

    syncConsumedUpstreamRTOffsetMapIfNeeded(partitionConsumptionState, leaderOffsetByKafkaURL);

    LOGGER.info(
        "{}, as a leader, started consuming from topic {} partition {} with offset by Kafka URL mapping {}",
        consumerTaskId,
        leaderTopic,
        partition,
        leaderOffsetByKafkaURL);
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
        rewindStartTime = partitionConsumptionState.getStartOfPushTimestamp() - rewindTimeInMs;
        break;
      case REWIND_FROM_EOP:
      default:
        rewindStartTime = partitionConsumptionState.getEndOfPushTimestamp() - rewindTimeInMs;
    }
    return rewindStartTime;
  }

  @Override
  protected void leaderExecuteTopicSwitch(
      PartitionConsumptionState partitionConsumptionState,
      TopicSwitch topicSwitch,
      PubSubTopic newSourceTopic) {
    if (partitionConsumptionState.getLeaderFollowerState() != LEADER) {
      throw new VeniceException(
          String.format("Expect state %s but got %s", LEADER, partitionConsumptionState.toString()));
    }
    if (topicSwitch.sourceKafkaServers.isEmpty()) {
      throw new VeniceException(
          "In the A/A mode, source Kafka URL list cannot be empty in Topic Switch control message.");
    }

    final int partition = partitionConsumptionState.getPartition();
    final PubSubTopic currentLeaderTopic =
        partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    final PubSubTopicPartition sourceTopicPartition = partitionConsumptionState.getSourceTopicPartition(newSourceTopic);
    Map<String, Long> upstreamOffsetsByKafkaURLs = new HashMap<>(topicSwitch.sourceKafkaServers.size());

    List<CharSequence> unreachableBrokerList = new ArrayList<>();
    topicSwitch.sourceKafkaServers.forEach(sourceKafkaURL -> {
      Long upstreamStartOffset =
          partitionConsumptionState.getLatestProcessedUpstreamRTOffsetWithNoDefault(sourceKafkaURL.toString());
      if (upstreamStartOffset == null || upstreamStartOffset < 0) {
        long rewindStartTimestamp = 0;
        // calculate the rewind start time here if controller asked to do so by using this sentinel value.
        if (topicSwitch.rewindStartTimestamp == REWIND_TIME_DECIDED_BY_SERVER) {
          rewindStartTimestamp = calculateRewindStartTime(partitionConsumptionState);
          LOGGER.info(
              "{} leader calculated rewindStartTimestamp {} for {}",
              consumerTaskId,
              rewindStartTimestamp,
              sourceTopicPartition);
        } else {
          rewindStartTimestamp = topicSwitch.rewindStartTimestamp;
        }
        if (rewindStartTimestamp > 0) {
          PubSubTopicPartition newSourceTopicPartition =
              new PubSubTopicPartitionImpl(newSourceTopic, sourceTopicPartition.getPartitionNumber());
          try {
            upstreamStartOffset =
                getTopicPartitionOffsetByKafkaURL(sourceKafkaURL, newSourceTopicPartition, rewindStartTimestamp);
          } catch (Exception e) {
            /**
             * This is actually tricky. Potentially we could return a -1 value here, but this has the gotcha that if we
             * have a non symmetrical failure (like, lor1 can't talk to the ltx1 broker) this will result in a remote
             * colo rewinding to a potentially non deterministic offset when the remote becomes available again. So
             * instead, we record the context of this call and commit it to a repair queue so as to rewind to a
             * consistent place on the RT
             *
             * NOTE: It is possible that the outage is so long and the rewind so large that we fall off retention. If
             * this happens we can detect and repair the inconsistency from the offline DCR validator.
             */
            unreachableBrokerList.add(sourceKafkaURL);
            upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
            LOGGER.error(
                "Failed contacting broker {} when processing topic switch! for {}. Setting upstream start offset to {}",
                sourceKafkaURL,
                sourceTopicPartition,
                upstreamStartOffset);
            hostLevelIngestionStats.recordIngestionFailure();

            // Add to repair queue
            if (remoteIngestionRepairService != null) {
              this.remoteIngestionRepairService.registerRepairTask(
                  this,
                  buildRepairTask(
                      sourceKafkaURL.toString(),
                      sourceTopicPartition,
                      rewindStartTimestamp,
                      partitionConsumptionState));
            } else {
              // If there isn't an available repair service, then we need to abort in order to make sure the error is
              // propagated up
              throw new VeniceException(
                  String.format(
                      "Failed contacting broker (%s) and no repair service available!  Aborting topic switch processing for %s. Setting upstream start offset to %d",
                      sourceKafkaURL,
                      sourceTopicPartition.toString(),
                      upstreamStartOffset));
            }
          }
        } else {
          upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
        }
      }
      upstreamOffsetsByKafkaURLs.put(sourceKafkaURL.toString(), upstreamStartOffset);
    });

    if (unreachableBrokerList.size() >= ((topicSwitch.sourceKafkaServers.size() + 1) / 2)) {
      // We couldn't reach a quorum of brokers and thats a red flag, so throw exception and abort!
      throw new VeniceException("Couldn't reach any broker!!  Aborting topic switch triggered consumer subscription!");
    }

    // unsubscribe the old source and subscribe to the new source
    consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);
    waitForLastLeaderPersistFuture(
        partitionConsumptionState,
        String.format(
            "Leader failed to produce the last message to version topic before switching feed topic from %s to %s on partition %s",
            currentLeaderTopic,
            newSourceTopic,
            partition));

    if (topicSwitch.sourceKafkaServers.size() != 1
        || (!Objects.equals(topicSwitch.sourceKafkaServers.get(0).toString(), localKafkaServer))) {
      partitionConsumptionState.setConsumeRemotely(true);
      LOGGER.info(
          "{} enabled remote consumption and switch to topic {} partition {} with offset by Kafka URL mapping {}",
          consumerTaskId,
          newSourceTopic,
          sourceTopicPartition,
          upstreamOffsetsByKafkaURLs);
    }

    partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopic);
    upstreamOffsetsByKafkaURLs.forEach((upstreamKafkaURL, upstreamStartOffset) -> {
      partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(upstreamKafkaURL, upstreamStartOffset);
    });

    if (unreachableBrokerList.size() > 0) {
      LOGGER.warn(
          "Failed to reach broker urls {}, will schedule retry to compute upstream offset and resubscribe!",
          unreachableBrokerList.toString());
      // We won't attempt to resubscribe for brokers we couldn't compute an upstream offset accurately for. We'll
      // reattempt subscription later
      // Queue up repair here:
      for (CharSequence unreachableBroker: unreachableBrokerList) {
        upstreamOffsetsByKafkaURLs.remove(unreachableBroker.toString());
      }
    }

    upstreamOffsetsByKafkaURLs.forEach((kafkaURL, upstreamStartOffset) -> {
      consumerSubscribe(
          partitionConsumptionState.getSourceTopicPartition(newSourceTopic),
          upstreamStartOffset,
          kafkaURL);
    });

    syncConsumedUpstreamRTOffsetMapIfNeeded(partitionConsumptionState, upstreamOffsetsByKafkaURLs);
    LOGGER.info(
        "{} leader successfully switch feed topic from {} to {} on partition {} with offset by Kafka URL mapping {}",
        consumerTaskId,
        currentLeaderTopic,
        newSourceTopic,
        partition,
        upstreamOffsetsByKafkaURLs);

    // In case new topic is empty and leader can never become online
    defaultReadyToServeChecker.apply(partitionConsumptionState);
  }

  /**
   * Process {@link TopicSwitch} control message at given partition offset for a specific {@link PartitionConsumptionState}.
   * Return whether we need to execute additional ready-to-serve check after this message is processed.
   */
  @Override
  protected boolean processTopicSwitch(
      ControlMessage controlMessage,
      int partition,
      long offset,
      PartitionConsumptionState partitionConsumptionState) {
    /**
     * During batch push, all subPartitions in LEADER will consume from leader topic (either local or remote VT)
     * Once we switch into RT topic consumption, only leaderSubPartition should be acting as LEADER role.
     * Hence, before processing TopicSwitch message, we need to force downgrade other subPartitions into FOLLOWER.
     */
    if (isLeader(partitionConsumptionState) && !amplificationFactorAdapter.isLeaderSubPartition(partition)) {
      LOGGER.info("SubPartition: {} is demoted from LEADER to STANDBY.", partitionConsumptionState.getPartition());
      PubSubTopic currentLeaderTopic =
          partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
      consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);

      waitForLastLeaderPersistFuture(
          partitionConsumptionState,
          String.format(
              "Leader failed to produce the last message to version topic before switching feed topic from %s to %s on partition %s",
              currentLeaderTopic,
              kafkaVersionTopic,
              partition));
      partitionConsumptionState.setConsumeRemotely(false);
      partitionConsumptionState.setLeaderFollowerState(STANDBY);
      consumerSubscribe(
          partitionConsumptionState.getSourceTopicPartition(versionTopic),
          partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset(),
          localKafkaServer);
    }

    TopicSwitch topicSwitch = (TopicSwitch) controlMessage.controlMessageUnion;
    statusReportAdapter.reportTopicSwitchReceived(partitionConsumptionState);

    // Calculate the start offset based on start timestamp
    final String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    PubSubTopic newSourceTopic = pubSubTopicRepository.getTopic(newSourceTopicName);
    Map<String, Long> upstreamStartOffsetByKafkaURL = new HashMap<>(topicSwitch.sourceKafkaServers.size());
    if (!isDaVinciClient) {
      final int newSourceTopicPartition = partitionConsumptionState.getSourceTopicPartitionNumber(newSourceTopic);
      AtomicInteger numberOfContactedBrokers = new AtomicInteger(0);
      topicSwitch.sourceKafkaServers.forEach(sourceKafkaURL -> {
        long rewindStartTimestamp;
        // calculate the rewind start time here if controller asked to do so by using this sentinel value.
        if (topicSwitch.rewindStartTimestamp == REWIND_TIME_DECIDED_BY_SERVER) {
          rewindStartTimestamp = calculateRewindStartTime(partitionConsumptionState);
          LOGGER.info(
              "{} leader calculated rewindStartTimestamp {} for topic {} partition {}",
              consumerTaskId,
              rewindStartTimestamp,
              newSourceTopicName,
              newSourceTopicPartition);
        } else {
          rewindStartTimestamp = topicSwitch.rewindStartTimestamp;
        }
        if (rewindStartTimestamp > 0) {
          long upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
          try {
            PubSubTopicPartition newSourceTP = new PubSubTopicPartitionImpl(newSourceTopic, newSourceTopicPartition);
            upstreamStartOffset =
                getTopicManager(sourceKafkaURL.toString()).getPartitionOffsetByTime(newSourceTP, rewindStartTimestamp);
            numberOfContactedBrokers.getAndIncrement();
          } catch (Exception e) {
            // TODO: Catch more specific Exception?
            LOGGER.error(
                "Failed to reach broker {} when trying to get partitionOffsetByTime for topic {} partitions {}",
                sourceKafkaURL.toString(),
                newSourceTopicName,
                newSourceTopicPartition);
          }
          if (upstreamStartOffset != OffsetRecord.LOWEST_OFFSET) {
            upstreamStartOffset -= 1;
          }
          upstreamStartOffsetByKafkaURL.put(sourceKafkaURL.toString(), upstreamStartOffset);
        } else {
          upstreamStartOffsetByKafkaURL.put(sourceKafkaURL.toString(), OffsetRecord.LOWEST_OFFSET);
        }
      });

      if (numberOfContactedBrokers.get() == 0) {
        throw new VeniceException("Failed to query any broker for rewind!  Aborting topic switch processing!");
      }

      upstreamStartOffsetByKafkaURL.forEach((sourceKafkaURL, upstreamStartOffset) -> {
        partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(sourceKafkaURL, upstreamStartOffset);
      });
    }
    /**
     * TopicSwitch needs to be persisted locally for both servers and DaVinci clients so that ready-to-serve check
     * can make the correct decision.
     */
    syncTopicSwitchToIngestionMetadataService(topicSwitch, partitionConsumptionState, upstreamStartOffsetByKafkaURL);
    if (!isLeader(partitionConsumptionState)) {
      partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopic);
      return true;
    }
    return false;
  }

  @Override
  protected void updateLatestInMemoryProcessedOffset(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl) {
    updateOffsetsFromConsumerRecord(
        partitionConsumptionState,
        consumerRecord,
        leaderProducedRecordContext,
        partitionConsumptionState::updateLatestProcessedLocalVersionTopicOffset,
        (sourceKafkaUrl, upstreamTopic, upstreamTopicOffset) -> {
          if (upstreamTopic.isRealTime()) {
            partitionConsumptionState.updateLatestProcessedUpstreamRTOffset(sourceKafkaUrl, upstreamTopicOffset);
          } else {
            partitionConsumptionState.updateLatestProcessedUpstreamVersionTopicOffset(upstreamTopicOffset);
          }
        },
        (sourceKafkaUrl, upstreamTopic) -> upstreamTopic.isRealTime()
            ? partitionConsumptionState.getLatestProcessedUpstreamRTOffset(sourceKafkaUrl)
            : partitionConsumptionState.getLatestProcessedUpstreamVersionTopicOffset(),
        () -> getUpstreamKafkaUrl(partitionConsumptionState, consumerRecord, kafkaUrl));
  }

  /**
   * Get upstream kafka url for record.
   *   1. For leaders, it is the source where the record was consumed from
   *   2. For Followers, it is either
   *       a. The upstream kafka url populated by the leader in the record, or
   *       b. The local kafka address
   *
   * @param partitionConsumptionState The current {@link PartitionConsumptionState} for the partition
   * @param consumerRecord The record for which the upstream Kafka url needs to be computed
   * @param recordSourceKafkaUrl The Kafka URL from where the record was consumed
   * @return The computed upstream Kafka URL for the record
   */
  private String getUpstreamKafkaUrl(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      String recordSourceKafkaUrl) {
    final String upstreamKafkaURL;
    if (isLeader(partitionConsumptionState)) {
      // Wherever leader consumes from is considered as "upstream"
      upstreamKafkaURL = recordSourceKafkaUrl;
    } else {
      KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
      if (kafkaValue.leaderMetadataFooter == null) {
        /**
         * This "leaderMetadataFooter" field do not get populated in case the source fabric is the same as the
         * local fabric since the VT source will be one of the prod fabric after batch NR is fully ramped. The
         * leader replica consumes from local Kafka URL. Hence, from a follower's perspective, the upstream Kafka
         * cluster which the leader consumes from should be the local Kafka URL.
         */
        upstreamKafkaURL = localKafkaServer;
      } else {
        upstreamKafkaURL = getUpstreamKafkaUrlFromKafkaValue(kafkaValue);
      }
    }
    return upstreamKafkaURL;
  }

  @Override
  protected boolean isRealTimeBufferReplayStarted(PartitionConsumptionState partitionConsumptionState) {
    TopicSwitchWrapper topicSwitchWrapper = partitionConsumptionState.getTopicSwitch();
    if (topicSwitchWrapper == null) {
      return false;
    }
    if (topicSwitchWrapper.getTopicSwitch().sourceKafkaServers.isEmpty()) {
      throw new VeniceException("Got empty source Kafka URLs in Topic Switch.");
    }
    return topicSwitchWrapper.getNewSourceTopic().isRealTime();
  }

  /**
   * For A/A, there are multiple entries in upstreamOffsetMap during RT ingestion.
   * If the current DataReplicationPolicy is on Aggregate mode, A/A will check the upstream offset lags from all regions;
   * otherwise, only check the upstream offset lag from the local region.
   */
  @Override
  protected long getLatestPersistedUpstreamOffsetForHybridOffsetLagMeasurement(
      PartitionConsumptionState pcs,
      String upstreamKafkaUrl) {
    return pcs.getLatestProcessedUpstreamRTOffsetWithIgnoredMessages(upstreamKafkaUrl);
  }

  /**
   * Different from the persisted upstream offset map in OffsetRecord, latest consumed upstream offset map is maintained
   * for each individual Kafka url.
   */
  @Override
  protected long getLatestConsumedUpstreamOffsetForHybridOffsetLagMeasurement(
      PartitionConsumptionState pcs,
      String upstreamKafkaUrl) {
    return pcs.getLeaderConsumedUpstreamRTOffset(upstreamKafkaUrl);
  }

  @Override
  protected void updateLatestInMemoryLeaderConsumedRTOffset(
      PartitionConsumptionState pcs,
      String kafkaUrl,
      long offset) {
    pcs.updateLeaderConsumedUpstreamRTOffset(kafkaUrl, offset);
  }

  private String getUpstreamKafkaUrlFromKafkaValue(KafkaMessageEnvelope kafkaValue) {
    if (kafkaValue.leaderMetadataFooter == null) {
      throw new VeniceException("leaderMetadataFooter field in KME should have been set.");
    }
    String upstreamKafkaURL = this.kafkaClusterIdToUrlMap.get(kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId);
    if (upstreamKafkaURL == null) {
      throw new VeniceException(
          String.format(
              "No Kafka cluster ID found in the cluster ID to Kafka URL map. "
                  + "Got cluster ID %d and ID to cluster URL map %s",
              kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId,
              kafkaClusterIdToUrlMap));
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
    StoreVersionState svs = storageEngine.getStoreVersionState();
    if (svs == null) {
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

    String kafkaSourceAddress = kafkaClusterIdToUrlMap.get(regionId);
    // This storage node does not register with the given region ID.
    if (kafkaSourceAddress == null) {
      return 0;
    }

    long offsetLag = partitionConsumptionStateMap.values()
        .stream()
        .filter(LeaderFollowerStoreIngestionTask.LEADER_OFFSET_LAG_FILTER)
        // Leader consumption upstream RT offset is only available in leader subPartition
        .filter(pcs -> amplificationFactorAdapter.isLeaderSubPartition(pcs.getPartition()))
        // the lag is (latest fabric RT offset - consumed fabric RT offset)
        .mapToLong((pcs) -> {
          PubSubTopic currentLeaderTopic = pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
          if (currentLeaderTopic == null || !currentLeaderTopic.isRealTime()) {
            // Leader topic not found, indicating that it is VT topic.
            return 0;
          }

          // Consumer might not existed after the consumption state is created, but before attaching the corresponding
          // consumer.
          long offsetLagOptional =
              getPartitionOffsetLag(kafkaSourceAddress, currentLeaderTopic, pcs.getUserPartition());
          if (offsetLagOptional >= 0) {
            return offsetLagOptional;
          }

          // Fall back to calculate offset lag in the old way
          return (cachedKafkaMetadataGetter
              .getOffset(getTopicManager(kafkaSourceAddress), currentLeaderTopic, pcs.getUserPartition()) - 1)
              - pcs.getLeaderConsumedUpstreamRTOffset(kafkaSourceAddress);
        })
        .sum();

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
  protected long measureRTOffsetLagForMultiRegions(
      Set<String> sourceRealTimeTopicKafkaURLs,
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag) {
    if (this.hybridStoreConfig.get().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)) {
      long minLag = Long.MAX_VALUE;
      for (String sourceRealTimeTopicKafkaURL: sourceRealTimeTopicKafkaURLs) {
        long lag =
            measureRTOffsetLagForSingleRegion(sourceRealTimeTopicKafkaURL, partitionConsumptionState, shouldLogLag);
        if (minLag > lag) {
          minLag = lag;
        }
      }
      return minLag;
    } else if (this.hybridStoreConfig.get().getDataReplicationPolicy().equals(DataReplicationPolicy.ACTIVE_ACTIVE)) {
      long maxLag = Long.MIN_VALUE;
      int numberOfUnreachableRegions = 0;
      for (String sourceRealTimeTopicKafkaURL: sourceRealTimeTopicKafkaURLs) {
        try {
          long lag =
              measureRTOffsetLagForSingleRegion(sourceRealTimeTopicKafkaURL, partitionConsumptionState, shouldLogLag);
          maxLag = Math.max(lag, maxLag);
        } catch (Exception e) {
          LOGGER.error(
              "Failed to measure RT offset lag for topic {} partition id {} in {}",
              partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository),
              partitionConsumptionState.getPartition(),
              sourceRealTimeTopicKafkaURL,
              e);
          if (++numberOfUnreachableRegions > 1) {
            LOGGER
                .error("More than one regions are unreachable. Return {} as it is not ready-to-serve", Long.MAX_VALUE);
            return Long.MAX_VALUE;
          }
        }
      }
      return maxLag;
    } else {
      if (sourceRealTimeTopicKafkaURLs.contains(localKafkaServer)) {
        return measureRTOffsetLagForSingleRegion(localKafkaServer, partitionConsumptionState, shouldLogLag);
      } else {
        throw new VeniceException(
            String.format(
                "Expect source RT Kafka URLs contains local Kafka URL. Got local "
                    + "Kafka URL %s and RT source Kafka URLs %s",
                localKafkaServer,
                sourceRealTimeTopicKafkaURLs));
      }
    }
  }

  @Override
  public boolean isReadyToServeAnnouncedWithRTLag() {
    if (!hybridStoreConfig.isPresent() || partitionConsumptionStateMap.isEmpty()) {
      return false;
    }
    long offsetLagThreshold = hybridStoreConfig.get().getOffsetLagThresholdToGoOnline();
    for (PartitionConsumptionState pcs: partitionConsumptionStateMap.values()) {
      if (pcs.hasLagCaughtUp() && offsetLagThreshold >= 0) {
        // If pcs is marked as having caught up, but we're not ready to serve, that means we're lagging
        // after having announced that we are ready to serve.
        try {
          if (!this.isReadyToServe(pcs)) {
            return true;
          }
        } catch (Exception e) {
          // Something wasn't reachable, we'll report that something is amiss.
          return true;
        }
      }
    }
    return false;
  }

  Runnable buildRepairTask(
      String sourceKafkaUrl,
      PubSubTopicPartition sourceTopicPartition,
      long rewindStartTimestamp,
      PartitionConsumptionState pcs) {
    return () -> {
      // Calculate upstream offset
      long upstreamOffset =
          getTopicPartitionOffsetByKafkaURL(sourceKafkaUrl, sourceTopicPartition, rewindStartTimestamp);

      // Subscribe (unsubscribe should have processed correctly regardless of remote broker state)
      consumerSubscribe(sourceTopicPartition, upstreamOffset, sourceKafkaUrl);

      // syncConsumedUpstreamRTOffsetMapIfNeeded
      Map<String, Long> urlToOffsetMap = new HashMap<>();
      urlToOffsetMap.put(sourceKafkaUrl, upstreamOffset);
      syncConsumedUpstreamRTOffsetMapIfNeeded(pcs, urlToOffsetMap);

      LOGGER.info(
          "Successfully repaired consumption and subscribed to {} at offset {}",
          sourceTopicPartition,
          upstreamOffset);
    };
  }

  int getRmdProtocolVersionID() {
    return rmdProtocolVersionID;
  }

  protected BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> getProduceToTopicFunction(
      byte[] key,
      ByteBuffer updatedValueBytes,
      ByteBuffer updatedRmdBytes,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest,
      int valueSchemaId,
      boolean resultReuseInput) {
    return (callback, leaderMetadataWrapper) -> {
      if (resultReuseInput) {
        // Restore the original header so this function is eventually idempotent as the original KME ByteBuffer
        // will be recovered after producing the message to Kafka or if the production failing.
        ((ActiveActiveProducerCallback) callback).setOnCompletionFunction(
            () -> ByteUtils.prependIntHeaderToByteBuffer(
                updatedValueBytes,
                ByteUtils.getIntHeaderFromByteBuffer(updatedValueBytes),
                true));
      }
      getVeniceWriter().get()
          .put(
              key,
              ByteUtils.extractByteArray(updatedValueBytes),
              valueSchemaId,
              callback,
              leaderMetadataWrapper,
              APP_DEFAULT_LOGICAL_TS,
              new PutMetadata(getRmdProtocolVersionID(), updatedRmdBytes),
              oldValueManifest,
              oldRmdManifest);
    };
  }

  protected LeaderProducerCallback createProducerCallback(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    return new ActiveActiveProducerCallback(
        this,
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        subPartition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);
  }
}
