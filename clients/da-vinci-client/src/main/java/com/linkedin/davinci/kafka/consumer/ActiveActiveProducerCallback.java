package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;


public class ActiveActiveProducerCallback extends LeaderProducerCallback {
  public ActiveActiveProducerCallback(
      LeaderFollowerStoreIngestionTask ingestionTask,
      DefaultPubSubMessage sourceConsumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestamp) {
    super(
        ingestionTask,
        sourceConsumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        partition,
        kafkaUrl,
        beforeProcessingRecordTimestamp);
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
}
