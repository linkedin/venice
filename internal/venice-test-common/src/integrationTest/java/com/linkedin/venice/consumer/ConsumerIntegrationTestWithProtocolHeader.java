package com.linkedin.venice.consumer;

import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterOptions;
import org.apache.avro.Schema;


public class ConsumerIntegrationTestWithProtocolHeader extends ConsumerIntegrationTest {
  @Override
  VeniceWriterWithNewerProtocol getVeniceWriter(
      VeniceWriterOptions veniceWriterOptions,
      VeniceProperties props,
      PubSubProducerAdapter producerAdapter,
      Schema overrideProtocolSchema) {
    return new VeniceWriterWithNewerProtocol(veniceWriterOptions, props, producerAdapter, NEW_PROTOCOL_SCHEMA);
  }
}
