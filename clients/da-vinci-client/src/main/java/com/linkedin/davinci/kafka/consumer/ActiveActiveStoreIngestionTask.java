package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static com.linkedin.venice.VeniceConstants.REWIND_TIME_DECIDED_BY_SERVER;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
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
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.store.view.VeniceViewWriter;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubConstants;
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
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.ChunkAwareCallback;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.PutMetadata;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.io.IOException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
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

  private final int rmdProtocolVersionId;
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
      Optional<ObjectCacheBackend> cacheBackend,
      Function<Integer, DaVinciRecordTransformer> getRecordTransformer) {
    super(
        builder,
        store,
        version,
        kafkaConsumerProperties,
        isCurrentVersion,
        storeConfig,
        errorPartitionId,
        isIsolatedIngestion,
        cacheBackend,
        getRecordTransformer);

    this.rmdProtocolVersionId = version.getRmdVersionId();

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

    this.rmdSerDe = new RmdSerDe(
        annotatedReadOnlySchemaRepository,
        rmdProtocolVersionId,
        getServerConfig().isComputeFastAvroEnabled());
    this.mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            annotatedReadOnlySchemaRepository,
            rmdSerDe,
            getStoreName(),
            isWriteComputationEnabled,
            getServerConfig().isComputeFastAvroEnabled());
    this.remoteIngestionRepairService = builder.getRemoteIngestionRepairService();
  }

  @Override
  protected DelegateConsumerRecordResult delegateConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingPerRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs) {
    if (!consumerRecord.getTopicPartition().getPubSubTopic().isRealTime()) {
      /**
       * We don't need to lock the partition here because during VT consumption there is only one consumption source.
       */
      return super.delegateConsumerRecord(
          consumerRecord,
          partition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingPerRecordTimestampNs,
          beforeProcessingBatchRecordsTimestampMs);
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
            partition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingPerRecordTimestampNs,
            beforeProcessingBatchRecordsTimestampMs);
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
   * @param key                       Bytes of key.
   * @param partition                 The partition to fetch the replication metadata from storage engine
   * @return The object containing RMD and value schema id. If nothing is found, return null
   */
  RmdWithValueSchemaId getReplicationMetadataAndSchemaId(
      PartitionConsumptionState partitionConsumptionState,
      byte[] key,
      int partition,
      long currentTimeForMetricsMs) {
    PartitionConsumptionState.TransientRecord cachedRecord = partitionConsumptionState.getTransientRecord(key);
    if (cachedRecord != null) {
      getHostLevelIngestionStats().recordIngestionReplicationMetadataCacheHitCount(currentTimeForMetricsMs);
      return new RmdWithValueSchemaId(
          cachedRecord.getValueSchemaId(),
          getRmdProtocolVersionId(),
          cachedRecord.getReplicationMetadataRecord(),
          cachedRecord.getRmdManifest());
    }
    ChunkedValueManifestContainer rmdManifestContainer = new ChunkedValueManifestContainer();
    byte[] replicationMetadataWithValueSchemaBytes =
        getRmdWithValueSchemaByteBufferFromStorage(partition, key, rmdManifestContainer, currentTimeForMetricsMs);
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
      int partition,
      byte[] key,
      ChunkedValueManifestContainer rmdManifestContainer,
      long currentTimeForMetricsMs) {
    final long lookupStartTimeInNS = System.nanoTime();
    ValueRecord result = SingleGetChunkingAdapter
        .getReplicationMetadata(getStorageEngine(), partition, key, isChunked(), rmdManifestContainer);
    getHostLevelIngestionStats().recordIngestionReplicationMetadataLookUpLatency(
        LatencyUtils.getElapsedTimeFromNSToMS(lookupStartTimeInNS),
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
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs) {
    /**
     * With {@link BatchConflictResolutionPolicy.BATCH_WRITE_LOSES} there is no need
     * to perform DCR before EOP and L/F DIV passthrough mode should be used. If the version is going through data
     * recovery then there is no need to perform DCR until we completed data recovery and switched to consume from RT.
     * TODO. We need to refactor this logic when we support other batch conflict resolution policy.
     */
    if (!partitionConsumptionState.isEndOfPushReceived()
        || isDataRecovery && partitionConsumptionState.getTopicSwitch() != null) {
      super.processMessageAndMaybeProduceToKafka(
          consumerRecord,
          partitionConsumptionState,
          partition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs,
          beforeProcessingBatchRecordsTimestampMs);
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
            ingestionTaskName + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }
    final ChunkedValueManifestContainer valueManifestContainer = new ChunkedValueManifestContainer();
    Lazy<ByteBufferValueRecord<ByteBuffer>> oldValueProvider = Lazy.of(
        () -> getValueBytesForKey(
            partitionConsumptionState,
            keyBytes,
            consumerRecord.getTopicPartition(),
            valueManifestContainer,
            beforeProcessingBatchRecordsTimestampMs));

    final RmdWithValueSchemaId rmdWithValueSchemaID = getReplicationMetadataAndSchemaId(
        partitionConsumptionState,
        keyBytes,
        partition,
        beforeProcessingBatchRecordsTimestampMs);

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

    Lazy<ByteBuffer> oldValueByteBufferProvider = unwrapByteBufferFromOldValueProvider(oldValueProvider);

    long beforeDCRTimestampInNs = System.nanoTime();
    switch (msgType) {
      case PUT:
        mergeConflictResult = mergeConflictResolver.put(
            unwrapByteBufferFromOldValueProvider(oldValueProvider),
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
        getHostLevelIngestionStats()
            .recordIngestionActiveActivePutLatency(LatencyUtils.getElapsedTimeFromNSToMS(beforeDCRTimestampInNs));
        break;

      case DELETE:
        mergeConflictResult = mergeConflictResolver.delete(
            oldValueByteBufferProvider,
            rmdWithValueSchemaID,
            writeTimestamp,
            sourceOffset,
            kafkaClusterId,
            kafkaClusterId);
        getHostLevelIngestionStats()
            .recordIngestionActiveActiveDeleteLatency(LatencyUtils.getElapsedTimeFromNSToMS(beforeDCRTimestampInNs));
        break;

      case UPDATE:
        mergeConflictResult = mergeConflictResolver.update(
            oldValueByteBufferProvider,
            rmdWithValueSchemaID,
            ((Update) kafkaValue.payloadUnion).updateValue,
            incomingValueSchemaId,
            incomingWriteComputeSchemaId,
            writeTimestamp,
            sourceOffset,
            kafkaClusterId,
            kafkaClusterId,
            valueManifestContainer);
        getHostLevelIngestionStats()
            .recordIngestionActiveActiveUpdateLatency(LatencyUtils.getElapsedTimeFromNSToMS(beforeDCRTimestampInNs));
        break;
      default:
        throw new VeniceMessageException(
            ingestionTaskName + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }

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

      // Write to views
      if (this.viewWriters.size() > 0) {
        /**
         * The ordering guarantees we want is the following:
         *
         * 1. Write to all view topics (in parallel).
         * 2. Write to the VT only after we get the ack for all views AND the previous write to VT was queued into the
         *    producer (but not necessarily acked).
         */
        long preprocessingTime = System.currentTimeMillis();
        CompletableFuture currentVersionTopicWrite = new CompletableFuture();
        CompletableFuture[] viewWriterFutures = new CompletableFuture[this.viewWriters.size() + 1];
        int index = 0;
        // The first future is for the previous write to VT
        viewWriterFutures[index++] = partitionConsumptionState.getLastVTProduceCallFuture();
        ByteBuffer oldValueBB = oldValueByteBufferProvider.get();
        int oldValueSchemaId = oldValueBB == null ? -1 : oldValueProvider.get().writerSchemaId();
        for (VeniceViewWriter writer: viewWriters.values()) {
          viewWriterFutures[index++] = writer.processRecord(
              mergeConflictResult.getNewValue(),
              oldValueBB,
              keyBytes,
              versionNumber,
              mergeConflictResult.getValueSchemaId(),
              oldValueSchemaId,
              mergeConflictResult.getRmdRecord());
        }
        CompletableFuture.allOf(viewWriterFutures).whenCompleteAsync((value, exception) -> {
          hostLevelIngestionStats.recordViewProducerLatency(LatencyUtils.getElapsedTimeFromMsToMs(preprocessingTime));
          if (exception == null) {
            producePutOrDeleteToKafka(
                mergeConflictResult,
                partitionConsumptionState,
                keyBytes,
                consumerRecord,
                partition,
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingRecordTimestampNs,
                valueManifestContainer.getManifest(),
                rmdWithValueSchemaID == null ? null : rmdWithValueSchemaID.getRmdManifest());
            currentVersionTopicWrite.complete(null);
          } else {
            VeniceException veniceException = new VeniceException(exception);
            this.setIngestionException(partitionConsumptionState.getPartition(), veniceException);
            currentVersionTopicWrite.completeExceptionally(veniceException);
          }
        });
        partitionConsumptionState.setLastVTProduceCallFuture(currentVersionTopicWrite);
      } else {
        // This function may modify the original record in KME and it is unsafe to use the payload from KME directly
        // after
        // this call.
        producePutOrDeleteToKafka(
            mergeConflictResult,
            partitionConsumptionState,
            keyBytes,
            consumerRecord,
            partition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingRecordTimestampNs,
            valueManifestContainer.getManifest(),
            rmdWithValueSchemaID == null ? null : rmdWithValueSchemaID.getRmdManifest());
      }
    }
  }

  /**
   * Package private for testing purposes.
   */
  static Lazy<ByteBuffer> unwrapByteBufferFromOldValueProvider(
      Lazy<ByteBufferValueRecord<ByteBuffer>> oldValueProvider) {
    return Lazy.of(() -> {
      ByteBufferValueRecord<ByteBuffer> bbValueRecord = oldValueProvider.get();
      return bbValueRecord == null ? null : bbValueRecord.value();
    });
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
  private ByteBufferValueRecord<ByteBuffer> getValueBytesForKey(
      PartitionConsumptionState partitionConsumptionState,
      byte[] key,
      PubSubTopicPartition topicPartition,
      ChunkedValueManifestContainer valueManifestContainer,
      long currentTimeForMetricsMs) {
    ByteBufferValueRecord<ByteBuffer> originalValue = null;
    // Find the existing value. If a value for this key is found from the transient map then use that value, otherwise
    // get it from DB.
    PartitionConsumptionState.TransientRecord transientRecord = partitionConsumptionState.getTransientRecord(key);
    if (transientRecord == null) {
      long lookupStartTimeInNS = System.nanoTime();
      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      ByteBuffer reusedRawValue = reusableObjects.reusedByteBuffer;
      BinaryDecoder binaryDecoder = reusableObjects.binaryDecoder;
      originalValue = RawBytesChunkingAdapter.INSTANCE.getWithSchemaId(
          storageEngine,
          topicPartition.getPartitionNumber(),
          ByteBuffer.wrap(key),
          isChunked,
          reusedRawValue,
          binaryDecoder,
          RawBytesStoreDeserializerCache.getInstance(),
          compressor.get(),
          valueManifestContainer);
      hostLevelIngestionStats.recordIngestionValueBytesLookUpLatency(
          LatencyUtils.getElapsedTimeFromNSToMS(lookupStartTimeInNS),
          currentTimeForMetricsMs);
    } else {
      hostLevelIngestionStats.recordIngestionValueBytesCacheHitCount(currentTimeForMetricsMs);
      // construct originalValue from this transient record only if it's not null.
      if (transientRecord.getValue() != null) {
        if (valueManifestContainer != null) {
          valueManifestContainer.setManifest(transientRecord.getValueManifest());
        }
        originalValue = new ByteBufferValueRecord<>(
            getCurrentValueFromTransientRecord(transientRecord),
            transientRecord.getValueSchemaId());
      }
    }
    return originalValue;
  }

  ByteBuffer getCurrentValueFromTransientRecord(PartitionConsumptionState.TransientRecord transientRecord) {
    ByteBuffer compressedValue =
        ByteBuffer.wrap(transientRecord.getValue(), transientRecord.getValueOffset(), transientRecord.getValueLen());
    try {
      return getCompressionStrategy().isCompressionEnabled()
          ? getCompressor().get()
              .decompress(compressedValue.array(), compressedValue.position(), compressedValue.remaining())
          : compressedValue;
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }

  /**
   * This function parses the {@link MergeConflictResult} and decides if the update should be ignored or emit a PUT or a
   * DELETE record to VT.
   * <p>
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after
   * this function.
   *
   * @param mergeConflictResult       The result of conflict resolution.
   * @param partitionConsumptionState The {@link PartitionConsumptionState} of the current partition
   * @param key                       The key bytes of the incoming record.
   * @param consumerRecord            The {@link PubSubMessage} for the current record.
   * @param partition
   * @param kafkaUrl
   */
  private void producePutOrDeleteToKafka(
      MergeConflictResult mergeConflictResult,
      PartitionConsumptionState partitionConsumptionState,
      byte[] key,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      int partition,
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
      deletePayload.replicationMetadataVersionId = rmdProtocolVersionId;
      deletePayload.replicationMetadataPayload = updatedRmdBytes;
      BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceToTopicFunction =
          (callback, sourceTopicOffset) -> veniceWriter.get()
              .delete(
                  key,
                  callback,
                  sourceTopicOffset,
                  APP_DEFAULT_LOGICAL_TS,
                  new DeleteMetadata(valueSchemaId, rmdProtocolVersionId, updatedRmdBytes),
                  oldValueManifest,
                  oldRmdManifest);
      LeaderProducedRecordContext leaderProducedRecordContext =
          LeaderProducedRecordContext.newDeleteRecord(kafkaClusterId, consumerRecord.getOffset(), key, deletePayload);
      produceToLocalKafka(
          consumerRecord,
          partitionConsumptionState,
          leaderProducedRecordContext,
          produceToTopicFunction,
          partition,
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
      updatedPut.replicationMetadataVersionId = rmdProtocolVersionId;
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
          partition,
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
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs) {
    super.produceToLocalKafka(
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        produceFunction,
        partition,
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
  protected Map<String, Long> calculateLeaderUpstreamOffsetWithTopicSwitch(
      PartitionConsumptionState partitionConsumptionState,
      TopicSwitch topicSwitch,
      PubSubTopic newSourceTopic,
      List<CharSequence> unreachableBrokerList) {
    Map<String, Long> upstreamOffsetsByKafkaURLs = new HashMap<>(topicSwitch.sourceKafkaServers.size());
    final PubSubTopicPartition sourceTopicPartition = partitionConsumptionState.getSourceTopicPartition(newSourceTopic);
    long rewindStartTimestamp;
    // calculate the rewind start time here if controller asked to do so by using this sentinel value.
    if (topicSwitch.rewindStartTimestamp == REWIND_TIME_DECIDED_BY_SERVER) {
      rewindStartTimestamp = calculateRewindStartTime(partitionConsumptionState);
      LOGGER.info(
          "{} leader calculated rewindStartTimestamp {} for {}",
          ingestionTaskName,
          rewindStartTimestamp,
          sourceTopicPartition);
    } else {
      rewindStartTimestamp = topicSwitch.rewindStartTimestamp;
    }

    topicSwitch.sourceKafkaServers.forEach(sourceKafkaURL -> {
      Long upstreamStartOffset =
          partitionConsumptionState.getLatestProcessedUpstreamRTOffsetWithNoDefault(sourceKafkaURL.toString());
      if (upstreamStartOffset == null || upstreamStartOffset < 0) {
        if (rewindStartTimestamp > 0) {
          PubSubTopicPartition newSourceTopicPartition =
              new PubSubTopicPartitionImpl(newSourceTopic, sourceTopicPartition.getPartitionNumber());
          try {
            upstreamStartOffset =
                getTopicPartitionOffsetByKafkaURL(sourceKafkaURL, newSourceTopicPartition, rewindStartTimestamp);
            LOGGER.info(
                "{} get upstreamStartOffset: {} for source URL: {}, topic: {}, rewind timestamp: {}",
                ingestionTaskName,
                upstreamStartOffset,
                sourceKafkaURL,
                newSourceTopicPartition,
                rewindStartTimestamp);
            upstreamOffsetsByKafkaURLs.put(sourceKafkaURL.toString(), upstreamStartOffset);
          } catch (Exception e) {
            /**
             * This is actually tricky. Potentially we could return a -1 value here, but this has the gotcha that if we
             * have a non-symmetrical failure (like, region1 can't talk to the region2 broker) this will result in a remote
             * colo rewinding to a potentially non-deterministic offset when the remote becomes available again. So
             * instead, we record the context of this call and commit it to a repair queue to rewind to a
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
            /**
             *  Add to repair queue. We won't attempt to resubscribe for brokers we couldn't compute an upstream offset
             *  accurately for. We will not persist the wrong offset into OffsetRecord, we'll reattempt subscription later.
             */
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
                      sourceTopicPartition,
                      upstreamStartOffset));
            }
          }
        } else {
          LOGGER.warn(
              "{} got unexpected rewind time: {}, will start ingesting upstream from the beginning",
              ingestionTaskName,
              rewindStartTimestamp);
          upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
          upstreamOffsetsByKafkaURLs.put(sourceKafkaURL.toString(), upstreamStartOffset);
        }
      } else {
        upstreamOffsetsByKafkaURLs.put(sourceKafkaURL.toString(), upstreamStartOffset);
      }
    });
    if (unreachableBrokerList.size() >= ((topicSwitch.sourceKafkaServers.size() + 1) / 2)) {
      // We couldn't reach a quorum of brokers and that's a red flag, so throw exception and abort!
      throw new VeniceException("Couldn't reach any broker!!  Aborting topic switch triggered consumer subscription!");
    }
    return upstreamOffsetsByKafkaURLs;
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
      LOGGER
          .info("{} enabled remote consumption from topic {} partition {}", ingestionTaskName, leaderTopic, partition);
    }

    partitionConsumptionState.setLeaderFollowerState(LEADER);
    Set<String> leaderSourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    Map<String, Long> leaderOffsetByKafkaURL = new HashMap<>(leaderSourceKafkaURLs.size());
    for (String kafkaURL: leaderSourceKafkaURLs) {
      leaderOffsetByKafkaURL.put(kafkaURL, partitionConsumptionState.getLeaderOffset(kafkaURL, pubSubTopicRepository));
    }
    List<CharSequence> unreachableBrokerList = new ArrayList<>();
    if (leaderTopic.isRealTime() && leaderOffsetByKafkaURL.containsValue(OffsetRecord.LOWEST_OFFSET)) {
      TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch().getTopicSwitch();
      if (topicSwitch == null) {
        throw new VeniceException(
            "New leader does not have topic switch, unable to switch to realtime leader topic: " + leaderTopic);
      }
      leaderOffsetByKafkaURL = calculateLeaderUpstreamOffsetWithTopicSwitch(
          partitionConsumptionState,
          topicSwitch,
          leaderTopic,
          unreachableBrokerList);
    }

    if (!unreachableBrokerList.isEmpty()) {
      LOGGER.warn(
          "Failed to reach broker urls {}, will schedule retry to compute upstream offset and resubscribe!",
          unreachableBrokerList.toString());
    }
    LOGGER.info(
        "{} is promoted to leader for partition {} and it is going to start consuming from "
            + "topic {} with offset by Kafka URL mapping {}",
        ingestionTaskName,
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
        ingestionTaskName,
        leaderTopic,
        partition,
        leaderOffsetByKafkaURL);
  }

  private long calculateRewindStartTime(PartitionConsumptionState partitionConsumptionState) {
    long rewindStartTime = 0;
    long rewindTimeInMs = hybridStoreConfig.get().getRewindTimeInSeconds() * Time.MS_PER_SECOND;
    if (isDataRecovery) {
      // Override the user rewind if the version is under data recovery to avoid data loss when user have short rewind.
      rewindTimeInMs = Math.max(PubSubConstants.BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN, rewindTimeInMs);
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
    List<CharSequence> unreachableBrokerList = new ArrayList<>();
    Map<String, Long> upstreamOffsetsByKafkaURLs = calculateLeaderUpstreamOffsetWithTopicSwitch(
        partitionConsumptionState,
        topicSwitch,
        newSourceTopic,
        unreachableBrokerList);
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
          ingestionTaskName,
          newSourceTopic,
          sourceTopicPartition,
          upstreamOffsetsByKafkaURLs);
    }

    // Update leader topic.
    partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopic);
    // Sync the upstream offset calculated for each region to OffsetRecord.
    upstreamOffsetsByKafkaURLs.forEach(
        (upstreamKafkaURL, upstreamStartOffset) -> partitionConsumptionState.getOffsetRecord()
            .setLeaderUpstreamOffset(upstreamKafkaURL, upstreamStartOffset));

    if (!unreachableBrokerList.isEmpty()) {
      LOGGER.warn(
          "Failed to reach broker urls {}, will schedule retry to compute upstream offset and resubscribe!",
          unreachableBrokerList.toString());
    }

    // Subscribe new leader topic for all regions.
    upstreamOffsetsByKafkaURLs.forEach((kafkaURL, upstreamStartOffset) -> {
      consumerSubscribe(
          partitionConsumptionState.getSourceTopicPartition(newSourceTopic),
          upstreamStartOffset,
          kafkaURL);
    });

    syncConsumedUpstreamRTOffsetMapIfNeeded(partitionConsumptionState, upstreamOffsetsByKafkaURLs);
    LOGGER.info(
        "{} leader successfully switch feed topic from {} to {} on partition {} with offset by Kafka URL mapping {}",
        ingestionTaskName,
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
    TopicSwitch topicSwitch = (TopicSwitch) controlMessage.controlMessageUnion;
    ingestionNotificationDispatcher.reportTopicSwitchReceived(partitionConsumptionState);
    final String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    PubSubTopic newSourceTopic = pubSubTopicRepository.getTopic(newSourceTopicName);

    /**
     * TopicSwitch needs to be persisted locally for both servers and DaVinci clients so that ready-to-serve check
     * can make the correct decision.
     */
    syncTopicSwitchToIngestionMetadataService(topicSwitch, partitionConsumptionState);
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
      String kafkaUrl,
      boolean dryRun) {
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
        () -> getUpstreamKafkaUrl(partitionConsumptionState, consumerRecord, kafkaUrl),
        dryRun);
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
        upstreamKafkaURL =
            getUpstreamKafkaUrlFromKafkaValue(consumerRecord, recordSourceKafkaUrl, this.kafkaClusterIdToUrlMap);
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

  /**
   * N.B. package-private for testing purposes.
   */
  static String getUpstreamKafkaUrlFromKafkaValue(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      String recordSourceKafkaUrl,
      Int2ObjectMap<String> kafkaClusterIdToUrlMap) {
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    if (kafkaValue.leaderMetadataFooter == null) {
      throw new VeniceException("leaderMetadataFooter field in KME should have been set.");
    }
    String upstreamKafkaURL = kafkaClusterIdToUrlMap.get(kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId);
    if (upstreamKafkaURL == null) {
      MessageType type = MessageType.valueOf(kafkaValue.messageType);
      throw new VeniceException(
          String.format(
              "No Kafka cluster ID found in the cluster ID to Kafka URL map. "
                  + "Got cluster ID %d and ID to cluster URL map %s. Source Kafka: %s; "
                  + "%s; Offset: %d; Message type: %s; ProducerMetadata: %s; LeaderMetadataFooter: %s",
              kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId,
              kafkaClusterIdToUrlMap,
              recordSourceKafkaUrl,
              consumerRecord.getTopicPartition(),
              consumerRecord.getOffset(),
              type.toString() + (type == MessageType.CONTROL_MESSAGE
                  ? "/" + ControlMessageType.valueOf((ControlMessage) kafkaValue.getPayloadUnion())
                  : ""),
              kafkaValue.producerMetadata,
              kafkaValue.leaderMetadataFooter));
    }
    return upstreamKafkaURL;
  }

  /**
   * For Active-Active this buffer is always used.
   * @return
   */
  @Override
  public boolean isTransientRecordBufferUsed() {
    return true;
  }

  @Override
  protected boolean shouldCheckLeaderCompleteStateInFollower() {
    return getServerConfig().isLeaderCompleteStateCheckInFollowerEnabled();
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
        // the lag is (latest fabric RT offset - consumed fabric RT offset)
        .mapToLong((pcs) -> {
          PubSubTopic currentLeaderTopic = pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
          if (currentLeaderTopic == null || !currentLeaderTopic.isRealTime()) {
            // Leader topic not found, indicating that it is VT topic.
            return 0;
          }

          // Consumer might not exist after the consumption state is created, but before attaching the corresponding
          // consumer.
          long lagBasedOnMetrics =
              getPartitionOffsetLagBasedOnMetrics(kafkaSourceAddress, currentLeaderTopic, pcs.getPartition());
          if (lagBasedOnMetrics >= 0) {
            return lagBasedOnMetrics;
          }

          // Fall back to calculate offset lag in the old way
          return measureLagWithCallToPubSub(
              kafkaSourceAddress,
              currentLeaderTopic,
              pcs.getPartition(),
              pcs.getLeaderConsumedUpstreamRTOffset(kafkaSourceAddress));
        })
        .filter(VALID_LAG)
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
    long maxLag = Long.MIN_VALUE;
    int numberOfUnreachableRegions = 0;
    for (String sourceRealTimeTopicKafkaURL: sourceRealTimeTopicKafkaURLs) {
      try {
        long lag =
            measureRTOffsetLagForSingleRegion(sourceRealTimeTopicKafkaURL, partitionConsumptionState, shouldLogLag);
        maxLag = Math.max(lag, maxLag);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to measure RT offset lag for replica: {} in {}/{}",
            partitionConsumptionState.getReplicaId(),
            Utils.getReplicaId(
                partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository),
                partitionConsumptionState.getPartition()),
            sourceRealTimeTopicKafkaURL,
            e);
        if (++numberOfUnreachableRegions > 1) {
          LOGGER.error(
              "More than one regions are unreachable. Returning lag: {} as replica: {} may not be ready-to-serve.",
              Long.MAX_VALUE,
              partitionConsumptionState.getReplicaId());
          return Long.MAX_VALUE;
        }
      }
    }
    return maxLag;
  }

  /** used for metric purposes **/
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

  int getRmdProtocolVersionId() {
    return rmdProtocolVersionId;
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
              new PutMetadata(getRmdProtocolVersionId(), updatedRmdBytes),
              oldValueManifest,
              oldRmdManifest);
    };
  }

  protected LeaderProducerCallback createProducerCallback(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    return new ActiveActiveProducerCallback(
        this,
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        partition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);
  }
}
