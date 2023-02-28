package com.linkedin.venice.consumer;

import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;


public class ConsumerIntegrationTestWithSchemaReader extends ConsumerIntegrationTest {
  @Override
  void extraBeforeClassSetUp(VeniceClusterWrapper cluster, ControllerClient controllerClient) {
    /**
     * By doing this, we emulate having started a new controller version which knows about the new protocol...
     *
     * TODO: Split this out into a separate test...
     */
    String systemStoreName = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName();
    TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
      MultiSchemaResponse response = controllerClient.getAllValueSchema(systemStoreName);
      Assert.assertFalse(response.isError());
      Assert.assertEquals(
          response.getSchemas().length,
          AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion());
    });

    ((VeniceHelixAdmin) cluster.getRandomVeniceController().getVeniceAdmin()).addValueSchema(
        cluster.getClusterName(),
        systemStoreName,
        NEW_PROTOCOL_SCHEMA.toString(),
        NEW_PROTOCOL_VERSION,
        DirectionalSchemaCompatibilityType.NONE,
        false);
  }

  @Override
  VeniceWriterWithNewerProtocol getVeniceWriter(
      VeniceWriterOptions veniceWriterOptions,
      VeniceProperties props,
      PubSubProducerAdapter producerAdapter,
      Schema overrideProtocolSchema) {
    return new VeniceWriterWithNewerProtocol(veniceWriterOptions, props, producerAdapter, null);
  }
}
