package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static com.linkedin.venice.writer.VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;

import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.writer.ChunkAwareCallback;
import java.nio.ByteBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class LeaderProducerCallback implements ChunkAwareCallback {
  private static final Logger LOGGER = LogManager.getLogger(LeaderFollowerStoreIngestionTask.class);

  protected static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER =
      new ChunkedValueManifestSerializer(false);
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

  private final LeaderFollowerStoreIngestionTask ingestionTask;
  private final ConsumerRecord<KafkaKey, KafkaMessageEnvelope> sourceConsumerRecord;
  private final PartitionConsumptionState partitionConsumptionState;
  private final String leaderTopic;
  private final String versionTopic;
  private final int partition;
  private final int subPartition;
  private final String kafkaUrl;
  private final AggVersionedDIVStats versionedDIVStats;
  private final LeaderProducedRecordContext leaderProducedRecordContext;
  private final AggVersionedIngestionStats versionedStorageIngestionStats;
  private final HostLevelIngestionStats hostLevelIngestionStats;
  private final long produceTimeNs;
  private final long beforeProcessingRecordTimestamp;
  private final boolean isActiveActiveReplication;

  /**
   * The mutable fields below are determined by the {@link com.linkedin.venice.writer.VeniceWriter},
   * which populates them via {@link ChunkAwareCallback#setChunkingInfo(byte[], ByteBuffer[],
   * ChunkedValueManifest, ByteBuffer[], ChunkedValueManifest)}.
   */
  private byte[] key = null;
  private ChunkedValueManifest chunkedValueManifest = null;
  private ByteBuffer[] valueChunks = null;
  private ChunkedValueManifest chunkedRmdManifest = null;
  private ByteBuffer[] rmdChunks = null;

  public LeaderProducerCallback(
      LeaderFollowerStoreIngestionTask ingestionTask,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> sourceConsumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String leaderTopic,
      String versionTopic,
      int partition,
      int subPartition,
      String kafkaUrl,
      AggVersionedDIVStats versionedDIVStats,
      AggVersionedIngestionStats versionedStorageIngestionStats,
      HostLevelIngestionStats hostLevelIngestionStats,
      long produceTimeNs,
      long beforeProcessingRecordTimestamp,
      boolean isActiveActiveReplication) {
    this.ingestionTask = ingestionTask;
    this.sourceConsumerRecord = sourceConsumerRecord;
    this.partitionConsumptionState = partitionConsumptionState;
    this.leaderTopic = leaderTopic;
    this.versionTopic = versionTopic;
    this.partition = partition;
    this.subPartition = subPartition;
    this.kafkaUrl = kafkaUrl;
    this.versionedDIVStats = versionedDIVStats;
    this.leaderProducedRecordContext = leaderProducedRecordContext;
    this.produceTimeNs = produceTimeNs;
    this.versionedStorageIngestionStats = versionedStorageIngestionStats;
    this.hostLevelIngestionStats = hostLevelIngestionStats;
    this.beforeProcessingRecordTimestamp = beforeProcessingRecordTimestamp;
    this.isActiveActiveReplication = isActiveActiveReplication;
  }

  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
    if (e != null) {
      LOGGER.error(
          "Leader failed to send out message to version topic when consuming " + leaderTopic + " partition "
              + partition,
          e);
      int version = Version.parseVersionFromKafkaTopicName(versionTopic);
      versionedDIVStats.recordLeaderProducerFailure(ingestionTask.getStoreName(), version);
    } else {
      // recordMetadata.partition() represents the partition being written by VeniceWriter
      // partitionConsumptionState.getPartition() is leaderSubPartition
      // when leaderSubPartition != recordMetadata.partition(), local StorageEngine will be written by
      // followers consuming from VTs. So it is safe to skip adding the record to leader's StorageBufferService
      if (partitionConsumptionState.getLeaderFollowerState() == LEADER
          && recordMetadata.partition() != partitionConsumptionState.getPartition()) {
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

      // record just the time it took for this callback to be invoked before we do further processing here such as
      // queuing to drainer.
      // this indicates how much time kafka took to deliver the message to broker.
      if (!ingestionTask.isUserSystemStore()) {
        versionedDIVStats.recordLeaderProducerCompletionTime(
            ingestionTask.getStoreName(),
            ingestionTask.versionNumber,
            LatencyUtils.getLatencyInMS(produceTimeNs));
      }

      int producedRecordNum = 0;
      int producedRecordSize = 0;
      // produce to drainer buffer service for further processing.
      try {
        /**
         * queue the leaderProducedRecordContext to drainer service as is in case the value was not chunked.
         * Otherwise queue the chunks and manifest individually to drainer service.
         */
        if (chunkedValueManifest == null) {
          LogManager.getLogger()
              .info(
                  "DEBUGGING CALLBACK NOT CHUNKED: " + leaderProducedRecordContext.getConsumedOffset() + " "
                      + leaderProducedRecordContext.getProducedOffset());
          // update the keyBytes for the ProducedRecord in case it was changed due to isChunkingEnabled flag in
          // VeniceWriter.
          if (key != null) {
            leaderProducedRecordContext.setKeyBytes(key);
          }
          leaderProducedRecordContext.setProducedOffset(recordMetadata.offset());
          ingestionTask.produceToStoreBufferService(
              sourceConsumerRecord,
              leaderProducedRecordContext,
              subPartition,
              kafkaUrl,
              beforeProcessingRecordTimestamp);

          producedRecordNum++;
          producedRecordSize =
              Math.max(0, recordMetadata.serializedKeySize()) + Math.max(0, recordMetadata.serializedValueSize());
        } else {
          int schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
          for (int i = 0; i < chunkedValueManifest.keysWithChunkIdSuffix.size(); i++) {
            ByteBuffer chunkKey = chunkedValueManifest.keysWithChunkIdSuffix.get(i);
            ByteBuffer chunkValue = valueChunks[i];

            Put chunkPut = new Put();
            chunkPut.putValue = chunkValue;
            chunkPut.schemaId = schemaId;
            if (isActiveActiveReplication) {
              chunkPut.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
              chunkPut.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
            }

            LeaderProducedRecordContext producedRecordForChunk =
                LeaderProducedRecordContext.newPutRecord(-1, -1, ByteUtils.extractByteArray(chunkKey), chunkPut);
            producedRecordForChunk.setProducedOffset(-1);
            ingestionTask.produceToStoreBufferService(
                sourceConsumerRecord,
                producedRecordForChunk,
                subPartition,
                kafkaUrl,
                beforeProcessingRecordTimestamp);
            producedRecordNum++;
            producedRecordSize += chunkKey.remaining() + chunkValue.remaining();
            LogManager.getLogger()
                .info(
                    "DEBUGGING CALLBACK CHUNKED PUT CHUNK: " + chunkValue.remaining() + " " + i + " "
                        + chunkPut.replicationMetadataPayload);
          }

          // produce the manifest inside the top-level key
          schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
          ByteBuffer manifest =
              ByteBuffer.wrap(CHUNKED_VALUE_MANIFEST_SERIALIZER.serialize(versionTopic, chunkedValueManifest));
          /**
           * The byte[] coming out of the {@link CHUNKED_VALUE_MANIFEST_SERIALIZER} is padded in front, so
           * that the put to the storage engine can avoid a copy, but we need to set the position to skip
           * the padding in order for this trick to work.
           */
          manifest.position(ValueRecord.SCHEMA_HEADER_LENGTH);

          Put manifestPut = new Put();
          manifestPut.putValue = manifest;
          manifestPut.schemaId = schemaId;
          if (isActiveActiveReplication) {
            manifestPut.replicationMetadataVersionId =
                ((Put) leaderProducedRecordContext.getValueUnion()).replicationMetadataVersionId;
            manifestPut.replicationMetadataPayload =
                ((Put) leaderProducedRecordContext.getValueUnion()).replicationMetadataPayload;
          }

          LeaderProducedRecordContext producedRecordForManifest = LeaderProducedRecordContext.newPutRecordWithFuture(
              leaderProducedRecordContext.getConsumedKafkaClusterId(),
              leaderProducedRecordContext.getConsumedOffset(),
              key,
              manifestPut,
              leaderProducedRecordContext.getPersistedToDBFuture());
          producedRecordForManifest.setProducedOffset(recordMetadata.offset());
          ingestionTask.produceToStoreBufferService(
              sourceConsumerRecord,
              producedRecordForManifest,
              subPartition,
              kafkaUrl,
              beforeProcessingRecordTimestamp);
          producedRecordNum++;
          producedRecordSize += key.length + manifest.remaining();
          LogManager.getLogger()
              .info(
                  "DEBUGGING CALLBACK CHUNKED MANIFEST: " + manifest.remaining() + " "
                      + manifestPut.replicationMetadataPayload);

        }
        recordProducerStats(producedRecordSize, producedRecordNum);

      } catch (Exception oe) {
        boolean endOfPushReceived = partitionConsumptionState.isEndOfPushReceived();
        LOGGER.error(
            ingestionTask.consumerTaskId + " received exception in kafka callback thread; EOP received: "
                + endOfPushReceived + " Topic: " + sourceConsumerRecord.topic() + " Partition: "
                + sourceConsumerRecord.partition() + ", Offset: " + sourceConsumerRecord.offset() + " exception: ",
            oe);
        // If EOP is not received yet, set the ingestion task exception so that ingestion will fail eventually.
        if (!endOfPushReceived) {
          try {
            ingestionTask.offerProducerException(oe, partition);
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
      ChunkedValueManifest chunkedRmdManifest) {
    this.key = key;
    this.chunkedValueManifest = chunkedValueManifest;
    this.valueChunks = valueChunks;
    this.chunkedRmdManifest = chunkedRmdManifest;
    this.rmdChunks = rmdChunks;
  }

  private void recordProducerStats(int producedRecordSize, int producedRecordNum) {
    versionedStorageIngestionStats.recordLeaderProduced(
        ingestionTask.getStoreName(),
        ingestionTask.versionNumber,
        producedRecordSize,
        producedRecordNum);
    hostLevelIngestionStats.recordTotalLeaderBytesProduced(producedRecordSize);
    hostLevelIngestionStats.recordTotalLeaderRecordsProduced(producedRecordNum);
  }
}
