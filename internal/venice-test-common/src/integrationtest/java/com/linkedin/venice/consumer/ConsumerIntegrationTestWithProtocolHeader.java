package com.linkedin.venice.consumer;

import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.function.Supplier;
import org.apache.avro.Schema;


public class ConsumerIntegrationTestWithProtocolHeader extends ConsumerIntegrationTest {
  @Override
  VeniceWriterWithNewerProtocol getVeniceWriter(
      VeniceWriterOptions veniceWriterOptions,
      VeniceProperties props,
      Supplier<KafkaProducerWrapper> producerWrapperSupplier,
      Schema overrideProtocolSchema) {
    return new VeniceWriterWithNewerProtocol(veniceWriterOptions, props, producerWrapperSupplier, NEW_PROTOCOL_SCHEMA);
  }
}
