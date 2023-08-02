package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;

import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.writer.ChunkAwareCallback;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class LeaderProducerCallback implements ChunkAwareCallback {
  private static final Logger LOGGER = LogManager.getLogger(LeaderFollowerStoreIngestionTask.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  protected static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER =
      new ChunkedValueManifestSerializer(false);
  protected static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

  protected final LeaderFollowerStoreIngestionTask ingestionTask;
  private final PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> sourceConsumerRecord;
  private final PartitionConsumptionState partitionConsumptionState;
  private final int subPartition;
  private final String kafkaUrl;
  protected final LeaderProducedRecordContext leaderProducedRecordContext;
  private final long produceTimeNs;
  private final long beforeProcessingRecordTimestampNs;

  /**
   * The mutable fields below are determined by the {@link com.linkedin.venice.writer.VeniceWriter},
   * which populates them via:
   * {@link ChunkAwareCallback#setChunkingInfo(byte[], ByteBuffer[], ChunkedValueManifest, ByteBuffer[], ChunkedValueManifest, ChunkedValueManifest, ChunkedValueManifest)}
   */
  private byte[] key = null;
  private ChunkedValueManifest chunkedValueManifest = null;
  private ByteBuffer[] valueChunks = null;
  protected ChunkedValueManifest chunkedRmdManifest = null;
  private ByteBuffer[] rmdChunks = null;

  protected ChunkedValueManifest oldValueManifest = null;
  protected ChunkedValueManifest oldRmdManifest = null;

  public LeaderProducerCallback(
      LeaderFollowerStoreIngestionTask ingestionTask,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> sourceConsumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    this.ingestionTask = ingestionTask;
    this.sourceConsumerRecord = sourceConsumerRecord;
    this.partitionConsumptionState = partitionConsumptionState;
    this.subPartition = subPartition;
    this.kafkaUrl = kafkaUrl;
    this.leaderProducedRecordContext = leaderProducedRecordContext;
    this.produceTimeNs = ingestionTask.isUserSystemStore() ? 0 : System.nanoTime();
    this.beforeProcessingRecordTimestampNs = beforeProcessingRecordTimestampNs;
  }

  @Override
  public void onCompletion(PubSubProduceResult produceResult, Exception e) {
    if (e != null) {
      ingestionTask.getVersionedDIVStats()
          .recordLeaderProducerFailure(ingestionTask.getStoreName(), ingestionTask.versionNumber);
      String message = e + " - TP: " + sourceConsumerRecord.getTopicName() + "/" + sourceConsumerRecord.getPartition();
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(message)) {
        LOGGER.error(
            "Leader failed to send out message to version topic when consuming {}",
            sourceConsumerRecord.getTopicPartition(),
            e);
      }
    } else {
      // recordMetadata.partition() represents the partition being written by VeniceWriter
      // partitionConsumptionState.getPartition() is leaderSubPartition
      // when leaderSubPartition != recordMetadata.partition(), local StorageEngine will be written by
      // followers consuming from VTs. So it is safe to skip adding the record to leader's StorageBufferService
      if (partitionConsumptionState.getLeaderFollowerState() == LEADER
          && produceResult.getPartition() != partitionConsumptionState.getPartition()) {
        leaderProducedRecordContext.completePersistedToDBFuture(null);
        return;
      }
      /**
       * performs some sanity checks for chunks.
       * key may be null in case of producing control messages with direct api's like
       * {@link VeniceWriter#SendControlMessage} or {@link VeniceWriter#asyncSendControlMessage}
       */
      if (chunkedValueManifest != null) {
        if (valueChunks == null) {
          throw new IllegalStateException("Value chunking info not initialized.");
        } else if (chunkedValueManifest.keysWithChunkIdSuffix.size() != valueChunks.length) {
          throw new IllegalStateException(
              "keysWithChunkIdSuffix in chunkedValueManifest is not in sync with value chunks.");
        }
      }
      if (chunkedRmdManifest != null) {
        if (rmdChunks == null) {
          throw new IllegalStateException("RMD chunking info not initialized.");
        } else if (chunkedRmdManifest.keysWithChunkIdSuffix.size() != rmdChunks.length) {
          throw new IllegalStateException(
              "keysWithChunkIdSuffix in chunkedRmdManifest is not in sync with RMD chunks.");
        }
      }

      long currentTimeForMetricsMs = System.currentTimeMillis();
      // record the timestamp when the writer has finished writing to the version topic
      leaderProducedRecordContext.setProducedTimestampMs(currentTimeForMetricsMs);

      // record just the time it took for this callback to be invoked before we do further processing here such as
      // queuing to drainer.
      // this indicates how much time kafka took to deliver the message to broker.
      if (!ingestionTask.isUserSystemStore()) {
        ingestionTask.getVersionedDIVStats()
            .recordLeaderProducerCompletionTime(
                ingestionTask.getStoreName(),
                ingestionTask.versionNumber,
                LatencyUtils.getLatencyInMS(produceTimeNs),
                currentTimeForMetricsMs);
        if (ingestionTask.isHybridMode() && sourceConsumerRecord.getTopicPartition().getPubSubTopic().isRealTime()
            && partitionConsumptionState.hasLagCaughtUp()) {
          ingestionTask.getVersionIngestionStats()
              .recordNearlineProducerToLocalBrokerLatency(
                  ingestionTask.getStoreName(),
                  ingestionTask.versionNumber,
                  currentTimeForMetricsMs - sourceConsumerRecord.getValue().producerMetadata.messageTimestamp,
                  currentTimeForMetricsMs);
        }
      }
      // update the keyBytes for the ProducedRecord in case it was changed due to isChunkingEnabled flag in
      // VeniceWriter.
      if (key != null) {
        leaderProducedRecordContext.setKeyBytes(key);
      }
      int producedRecordNum = 0;
      int producedRecordSize = 0;
      // produce to drainer buffer service for further processing.
      try {
        /**
         * queue the leaderProducedRecordContext to drainer service as is in case the value was not chunked.
         * Otherwise, queue the chunks and manifest individually to drainer service.
         */
        if (chunkedValueManifest == null) {
          leaderProducedRecordContext.setProducedOffset(produceResult.getOffset());
          ingestionTask.produceToStoreBufferService(
              sourceConsumerRecord,
              leaderProducedRecordContext,
              subPartition,
              kafkaUrl,
              beforeProcessingRecordTimestampNs,
              currentTimeForMetricsMs);

          producedRecordNum++;
          producedRecordSize = Math.max(0, produceResult.getSerializedSize());
        } else {
          producedRecordSize +=
              produceChunksToStoreBufferService(chunkedValueManifest, valueChunks, false, currentTimeForMetricsMs);
          producedRecordNum += chunkedValueManifest.keysWithChunkIdSuffix.size();
          if (chunkedRmdManifest != null) {
            producedRecordSize +=
                produceChunksToStoreBufferService(chunkedRmdManifest, rmdChunks, true, currentTimeForMetricsMs);
            producedRecordNum += chunkedRmdManifest.keysWithChunkIdSuffix.size();
          }
          // produce the manifest inside the top-level key
          ByteBuffer manifest = CHUNKED_VALUE_MANIFEST_SERIALIZER.serialize(chunkedValueManifest);
          /**
           * The byte[] coming out of the {@link CHUNKED_VALUE_MANIFEST_SERIALIZER} is padded in front, so
           * that the put to the storage engine can avoid a copy, but we need to set the position to skip
           * the padding in order for this trick to work.
           */
          manifest.position(ValueRecord.SCHEMA_HEADER_LENGTH);

          Put manifestPut = instantiateManifestPut();
          manifestPut.putValue = manifest;
          manifestPut.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
          LeaderProducedRecordContext producedRecordForManifest = LeaderProducedRecordContext.newPutRecordWithFuture(
              leaderProducedRecordContext.getConsumedKafkaClusterId(),
              leaderProducedRecordContext.getConsumedOffset(),
              key,
              manifestPut,
              leaderProducedRecordContext.getPersistedToDBFuture());
          producedRecordForManifest.setProducedOffset(produceResult.getOffset());
          ingestionTask.produceToStoreBufferService(
              sourceConsumerRecord,
              producedRecordForManifest,
              subPartition,
              kafkaUrl,
              beforeProcessingRecordTimestampNs,
              currentTimeForMetricsMs);
          producedRecordNum++;
          producedRecordSize += key.length + manifest.remaining();
        }
        produceDeprecatedChunkDeletionToStoreBufferService(oldValueManifest, currentTimeForMetricsMs);
        produceDeprecatedChunkDeletionToStoreBufferService(oldRmdManifest, currentTimeForMetricsMs);
        recordProducerStats(producedRecordSize, producedRecordNum);

      } catch (Exception oe) {
        boolean endOfPushReceived = partitionConsumptionState.isEndOfPushReceived();
        LOGGER.error(
            "{} received exception in kafka callback thread; EOP received: {}, {}, Offset: {}",
            ingestionTask.consumerTaskId,
            endOfPushReceived,
            sourceConsumerRecord.getTopicPartition(),
            sourceConsumerRecord.getOffset(),
            oe);
        // If EOP is not received yet, set the ingestion task exception so that ingestion will fail eventually.
        if (!endOfPushReceived) {
          try {
            ingestionTask.setIngestionException(sourceConsumerRecord.getTopicPartition().getPartitionNumber(), oe);
          } catch (VeniceException offerToQueueException) {
            ingestionTask.setLastStoreIngestionException(offerToQueueException);
          }
        }
        if (oe instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(oe);
        }
      }
    }
  }

  @Override
  public void setChunkingInfo(
      byte[] key,
      ByteBuffer[] valueChunks,
      ChunkedValueManifest chunkedValueManifest,
      ByteBuffer[] rmdChunks,
      ChunkedValueManifest chunkedRmdManifest,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {
    this.key = key;
    this.chunkedValueManifest = chunkedValueManifest;
    this.valueChunks = valueChunks;
    this.chunkedRmdManifest = chunkedRmdManifest;
    this.rmdChunks = rmdChunks;
    this.oldValueManifest = oldValueManifest;
    this.oldRmdManifest = oldRmdManifest;
    if (getPartitionConsumptionState() == null) {
      LOGGER.error("PartitionConsumptionState is missing in chunk producer callback");
      return;
    }
    // TransientRecord map is indexed by non-chunked key.
    PartitionConsumptionState.TransientRecord record =
        getPartitionConsumptionState().getTransientRecord(getSourceConsumerRecord().getKey().getKey());
    if (record != null) {
      record.setValueManifest(chunkedValueManifest);
      record.setRmdManifest(chunkedRmdManifest);
    } else {
      LOGGER.error("Transient record is missing when trying to update value/RMD manifest.");
    }
  }

  private void recordProducerStats(int producedRecordSize, int producedRecordNum) {
    ingestionTask.getVersionIngestionStats()
        .recordLeaderProduced(
            ingestionTask.getStoreName(),
            ingestionTask.versionNumber,
            producedRecordSize,
            producedRecordNum);
    ingestionTask.getHostLevelIngestionStats().recordTotalLeaderBytesProduced(producedRecordSize);
    ingestionTask.getHostLevelIngestionStats().recordTotalLeaderRecordsProduced(producedRecordNum);
  }

  protected Put instantiateValueChunkPut() {
    return new Put();
  }

  protected Put instantiateRmdChunkPut() {
    return new Put();
  }

  protected Put instantiateManifestPut() {
    return new Put();
  }

  private long produceChunksToStoreBufferService(
      ChunkedValueManifest manifest,
      ByteBuffer[] chunks,
      boolean isRmdChunks,
      long currentTimeForMetricsMs) throws InterruptedException {
    long totalChunkSize = 0;
    for (int i = 0; i < manifest.keysWithChunkIdSuffix.size(); i++) {
      ByteBuffer chunkKey = manifest.keysWithChunkIdSuffix.get(i);
      ByteBuffer chunkValue = chunks[i];
      Put chunkPut;
      if (isRmdChunks) {
        chunkPut = instantiateRmdChunkPut();
        chunkPut.replicationMetadataPayload = chunkValue;
      } else {
        chunkPut = instantiateValueChunkPut();
        chunkPut.putValue = chunkValue;
      }
      chunkPut.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
      LeaderProducedRecordContext producedRecordForChunk =
          LeaderProducedRecordContext.newChunkPutRecord(ByteUtils.extractByteArray(chunkKey), chunkPut);
      producedRecordForChunk.setProducedOffset(-1);
      ingestionTask.produceToStoreBufferService(
          sourceConsumerRecord,
          producedRecordForChunk,
          subPartition,
          kafkaUrl,
          beforeProcessingRecordTimestampNs,
          currentTimeForMetricsMs);
      totalChunkSize += chunkKey.remaining() + chunkValue.remaining();
    }
    return totalChunkSize;
  }

  void produceDeprecatedChunkDeletionToStoreBufferService(ChunkedValueManifest manifest, long currentTimeForMetricsMs)
      throws InterruptedException {
    if (manifest == null) {
      return;
    }
    for (int i = 0; i < manifest.keysWithChunkIdSuffix.size(); i++) {
      ByteBuffer chunkKey = manifest.keysWithChunkIdSuffix.get(i);
      Delete chunkDelete = new Delete();
      chunkDelete.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
      chunkDelete.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      chunkDelete.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
      LeaderProducedRecordContext producedRecordForChunk =
          LeaderProducedRecordContext.newChunkDeleteRecord(ByteUtils.extractByteArray(chunkKey), chunkDelete);
      producedRecordForChunk.setProducedOffset(-1);
      ingestionTask.produceToStoreBufferService(
          sourceConsumerRecord,
          producedRecordForChunk,
          subPartition,
          kafkaUrl,
          beforeProcessingRecordTimestampNs,
          currentTimeForMetricsMs);
    }
  }

  // Visible for VeniceWriter unit test.
  public PartitionConsumptionState getPartitionConsumptionState() {
    return partitionConsumptionState;
  }

  public PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> getSourceConsumerRecord() {
    return sourceConsumerRecord;
  }
}
