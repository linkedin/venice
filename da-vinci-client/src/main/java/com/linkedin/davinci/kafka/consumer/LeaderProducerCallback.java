package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.AggStoreIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedStorageIngestionStats;
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
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.Logger;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.*;


class LeaderProducerCallback implements ChunkAwareCallback {
  protected static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER = new ChunkedValueManifestSerializer(false);

  private final LeaderFollowerStoreIngestionTask ingestionTask;
  private final VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> sourceConsumerRecordWrapper;
  private final PartitionConsumptionState partitionConsumptionState;
  private final String leaderTopic;
  private final String versionTopic;
  private final int partition;
  private final AggVersionedDIVStats versionedDIVStats;
  private final Logger logger;
  private final LeaderProducedRecordContext leaderProducedRecordContext;
  private final AggVersionedStorageIngestionStats versionedStorageIngestionStats;
  private final AggStoreIngestionStats storeIngestionStats;
  private final long produceTimeNs;

  /**
   * The three mutable fields below are determined by the {@link com.linkedin.venice.writer.VeniceWriter},
   * which populates them via {@link ChunkAwareCallback#setChunkingInfo(byte[], ByteBuffer[], ChunkedValueManifest)}.
   *
   */
  private byte[] key = null;
  private ChunkedValueManifest chunkedValueManifest = null;
  private ByteBuffer[] chunks = null;

  public LeaderProducerCallback(
      LeaderFollowerStoreIngestionTask ingestionTask,
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> sourceConsumerRecordWrapper,
      PartitionConsumptionState partitionConsumptionState,
      String leaderTopic,
      String versionTopic,
      int partition,
      AggVersionedDIVStats versionedDIVStats,
      Logger logger,
      LeaderProducedRecordContext leaderProducedRecordContext,
      AggVersionedStorageIngestionStats versionedStorageIngestionStats,
      AggStoreIngestionStats storeIngestionStats,
      long produceTimeNs
  ) {
    this.ingestionTask = ingestionTask;
    this.sourceConsumerRecordWrapper = sourceConsumerRecordWrapper;
    this.partitionConsumptionState = partitionConsumptionState;
    this.leaderTopic = leaderTopic;
    this.versionTopic = versionTopic;
    this.partition = partition;
    this.versionedDIVStats = versionedDIVStats;
    this.logger = logger;
    this.leaderProducedRecordContext = leaderProducedRecordContext;
    this.produceTimeNs = produceTimeNs;
    this.versionedStorageIngestionStats = versionedStorageIngestionStats;
    this.storeIngestionStats = storeIngestionStats;
  }

  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
    if (e != null) {
      logger.error("Leader failed to send out message to version topic when consuming " + leaderTopic + " partition "
          + partition, e);
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
        if (null == chunks) {
          throw new IllegalStateException("chunking info not initialized.");
        } else if (chunkedValueManifest.keysWithChunkIdSuffix.size() != chunks.length) {
          throw new IllegalStateException("chunkedValueManifest.keysWithChunkIdSuffix is not in sync with chunks.");
        }
      }

      //record just the time it took for this callback to be invoked before we do further processing here such as queuing to drainer.
      //this indicates how much time kafka took to deliver the message to broker.
      versionedDIVStats.recordLeaderProducerCompletionTime(ingestionTask.getStoreName(), ingestionTask.versionNumber, LatencyUtils.getLatencyInMS(produceTimeNs));

      int producedRecordNum = 0;
      int producedRecordSize = 0;
      //produce to drainer buffer service for further processing.
      try {
        /**
         * queue the leaderProducedRecordContext to drainer service as is in case the value was not chunked.
         * Otherwise queue the chunks and manifest individually to drainer service.
         */
        if (chunkedValueManifest == null) {
          //update the keyBytes for the ProducedRecord in case it was changed due to isChunkingEnabled flag in VeniceWriter.
          if (key != null) {
            leaderProducedRecordContext.setKeyBytes(key);
          }
          leaderProducedRecordContext.setProducedOffset(recordMetadata.offset());
          ingestionTask.produceToStoreBufferService(sourceConsumerRecordWrapper, leaderProducedRecordContext);

          producedRecordNum++;
          producedRecordSize = Math.max(0, recordMetadata.serializedKeySize()) + Math.max(0, recordMetadata.serializedValueSize());
        } else {
          int schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
          for (int i = 0; i < chunkedValueManifest.keysWithChunkIdSuffix.size(); i++) {
            ByteBuffer chunkKey = chunkedValueManifest.keysWithChunkIdSuffix.get(i);
            ByteBuffer chunkValue = chunks[i];

            Put chunkPut = new Put();
            chunkPut.putValue = chunkValue;
            chunkPut.schemaId = schemaId;

            LeaderProducedRecordContext
                producedRecordForChunk = LeaderProducedRecordContext.newPutRecord(null, -1, ByteUtils.extractByteArray(chunkKey), chunkPut);
            producedRecordForChunk.setProducedOffset(-1);
            ingestionTask.produceToStoreBufferService(sourceConsumerRecordWrapper, producedRecordForChunk);

            producedRecordNum++;
            producedRecordSize += chunkKey.remaining() + chunkValue.remaining();
          }

          //produce the manifest inside the top-level key
          schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
          ByteBuffer manifest = ByteBuffer.wrap(CHUNKED_VALUE_MANIFEST_SERIALIZER.serialize(versionTopic, chunkedValueManifest));
          /**
           * The byte[] coming out of the {@link CHUNKED_VALUE_MANIFEST_SERIALIZER} is padded in front, so
           * that the put to the storage engine can avoid a copy, but we need to set the position to skip
           * the padding in order for this trick to work.
           */
          manifest.position(ValueRecord.SCHEMA_HEADER_LENGTH);

          Put manifestPut = new Put();
          manifestPut.putValue = manifest;
          manifestPut.schemaId = schemaId;

          LeaderProducedRecordContext producedRecordForManifest = LeaderProducedRecordContext.newPutRecordWithFuture(
              leaderProducedRecordContext.getConsumedKafkaUrl(),
              leaderProducedRecordContext.getConsumedOffset(),
              key, manifestPut, leaderProducedRecordContext.getPersistedToDBFuture());
          producedRecordForManifest.setProducedOffset(recordMetadata.offset());
          ingestionTask.produceToStoreBufferService(sourceConsumerRecordWrapper, producedRecordForManifest);
          producedRecordNum++;
          producedRecordSize += key.length + manifest.remaining();
        }
        recordProducerStats(producedRecordSize, producedRecordNum);

      } catch (Exception oe) {
        boolean endOfPushReceived = partitionConsumptionState.isEndOfPushReceived();
        logger.error(ingestionTask.consumerTaskId + " received exception in kafka callback thread; EOP received: " + endOfPushReceived + " Topic: " + sourceConsumerRecordWrapper.consumerRecord().topic() + " Partition: "
            + sourceConsumerRecordWrapper.consumerRecord().partition() + ", Offset: " + sourceConsumerRecordWrapper.consumerRecord().offset() + " exception: ", oe);
        //If EOP is not received yet, set the ingestion task exception so that ingestion will fail eventually.
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
  public void setChunkingInfo(byte[] key, ByteBuffer[] chunks, ChunkedValueManifest chunkedValueManifest) {
    this.key = key;
    this.chunkedValueManifest = chunkedValueManifest;
    this.chunks = chunks;
  }

  private void recordProducerStats(int producedRecordSize, int producedRecordNum) {
    versionedStorageIngestionStats.recordLeaderBytesProduced(ingestionTask.getStoreName(), ingestionTask.versionNumber, producedRecordSize);
    versionedStorageIngestionStats.recordLeaderRecordsProduced(ingestionTask.getStoreName(), ingestionTask.versionNumber, producedRecordNum);
    storeIngestionStats.recordTotalLeaderBytesProduced(producedRecordSize);
    storeIngestionStats.recordTotalLeaderRecordsProduced(producedRecordNum);
  }
}
