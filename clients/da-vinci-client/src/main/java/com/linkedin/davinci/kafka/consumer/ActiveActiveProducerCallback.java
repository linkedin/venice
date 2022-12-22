package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.writer.VeniceWriter;
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
  protected Put instantiateChunkPut() {
    Put chunkPut = new Put();
    chunkPut.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
    chunkPut.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    return chunkPut;
  }

  @Override
  protected Put instantiateManifestPut() {
    Put manifestPut = new Put();
    Put leaderProducedRecordContextPut = (Put) leaderProducedRecordContext.getValueUnion();
    manifestPut.replicationMetadataVersionId = leaderProducedRecordContextPut.replicationMetadataVersionId;
    manifestPut.replicationMetadataPayload = leaderProducedRecordContextPut.replicationMetadataPayload;
    return manifestPut;
  }

  public void setOnCompletionFunction(Runnable onCompletionFunction) {
    this.onCompletionFunction = onCompletionFunction;
  }
}
