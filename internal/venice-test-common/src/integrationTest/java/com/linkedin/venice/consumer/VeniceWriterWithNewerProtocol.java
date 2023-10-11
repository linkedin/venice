package com.linkedin.venice.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import org.apache.avro.Schema;


class VeniceWriterWithNewerProtocol extends VeniceWriter<String, String, byte[]> {
  protected VeniceWriterWithNewerProtocol(
      VeniceWriterOptions veniceWriterOptions,
      VeniceProperties props,
      PubSubProducerAdapter producerAdapter,
      Schema overrideProtocolSchema) {
    super(veniceWriterOptions, props, producerAdapter, overrideProtocolSchema);
  }

  @Override
  protected KafkaMessageEnvelope getKafkaMessageEnvelope(
      MessageType messageType,
      boolean isEndOfSegment,
      int partition,
      boolean incrementSequenceNumber,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs) {
    KafkaMessageEnvelope normalKME =
        super.getKafkaMessageEnvelope(messageType, isEndOfSegment, partition, true, leaderMetadataWrapper, logicalTs);

    ConsumerIntegrationTest.NewKafkaMessageEnvelopeWithExtraField newKME =
        new ConsumerIntegrationTest.NewKafkaMessageEnvelopeWithExtraField();
    for (Schema.Field normalField: normalKME.getSchema().getFields()) {
      Schema.Field newField = newKME.getSchema().getField(normalField.name());
      if (newField == null) {
        throw new IllegalStateException();
      }
      newKME.put(newField.pos(), normalKME.get(normalField.pos()));
    }
    newKME.newField = 42;

    return newKME;
  }
}
