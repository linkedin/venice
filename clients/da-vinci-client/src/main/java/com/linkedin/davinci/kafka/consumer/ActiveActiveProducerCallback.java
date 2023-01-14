package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class ActiveActiveProducerCallback extends LeaderProducerCallback {
  private static final Runnable NO_OP = () -> {};
  private Runnable onCompletionFunction = NO_OP;

  public ActiveActiveProducerCallback(
      LeaderFollowerStoreIngestionTask ingestionTask,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> sourceConsumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestamp) {
    super(
        ingestionTask,
        sourceConsumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        subPartition,
        kafkaUrl,
        beforeProcessingRecordTimestamp);
  }

  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
    this.onCompletionFunction.run();
    super.onCompletion(recordMetadata, exception);
  }

  @Override
  protected Put instantiateValueChunkPut() {
    Put chunkPut = new Put();
    chunkPut.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
    chunkPut.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    return chunkPut;
  }

  @Override
  protected Put instantiateRmdChunkPut() {
    Put chunkPut = new Put();
    chunkPut.putValue = EMPTY_BYTE_BUFFER;
    // This field is not used in the RMD chunk. The correct RMD version id will be stored in the RMD manifest.
    chunkPut.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    return chunkPut;
  }

  @Override
  protected Put instantiateManifestPut() {
    Put manifestPut = new Put();
    Put leaderProducedRecordContextPut = (Put) leaderProducedRecordContext.getValueUnion();
    manifestPut.replicationMetadataVersionId = leaderProducedRecordContextPut.replicationMetadataVersionId;
    if (chunkedRmdManifest == null) {
      manifestPut.replicationMetadataPayload = leaderProducedRecordContextPut.replicationMetadataPayload;
    } else {
      ByteBuffer rmdManifest = CHUNKED_VALUE_MANIFEST_SERIALIZER.serialize(chunkedRmdManifest);
      rmdManifest.position(ValueRecord.SCHEMA_HEADER_LENGTH);
      manifestPut.replicationMetadataPayload = rmdManifest;
    }

    return manifestPut;
  }

  public void setOnCompletionFunction(Runnable onCompletionFunction) {
    this.onCompletionFunction = onCompletionFunction;
  }
}
