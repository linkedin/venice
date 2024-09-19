package com.linkedin.venice.consumer;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_READER_ENABLED;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


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

    cluster.getRandomVeniceController()
        .getVeniceAdmin()
        .addValueSchema(
            cluster.getClusterName(),
            systemStoreName,
            NEW_PROTOCOL_SCHEMA.toString(),
            NEW_PROTOCOL_VERSION,
            DirectionalSchemaCompatibilityType.NONE);
  }

  @Override
  Schema getOverrideProtocolSchema() {
    return null;
  }

  @Test
  public void testKIFRepushForwardCompatibility() throws Exception {
    // Write a new record to VT with new KME protocol
    String versionTopic = Version.composeKafkaTopic(store, version);
    try (VeniceWriter<String, String, byte[]> veniceWriterWithNewerProtocol =
        getVeniceWriterWithNewerProtocol(getOverrideProtocolSchema(), versionTopic)) {
      veniceWriterWithNewerProtocol.put("test_key", "test_value", 1).get();
    }

    // Run the repush job, which will fetch the new KME schema via schema reader to deserialize the new record
    Properties vpjProps = defaultVPJProps(cluster, "Ignored", store);
    vpjProps.setProperty(SOURCE_KAFKA, "true");
    vpjProps.setProperty(KAFKA_INPUT_FABRIC, "dc-0");
    vpjProps.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    vpjProps.setProperty(SYSTEM_SCHEMA_READER_ENABLED, "true");
    TestWriteUtils.runPushJob("Test push job", vpjProps);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      try {
        Object value = client.get("test_key").get();
        Assert.assertNotNull(value, "The key written with the new protocol is not in the store yet.");
        Assert.assertEquals(value.toString(), "test_value", "The key written with new protocol is not valid.");
      } catch (ExecutionException e) {
        Assert.fail("Caught exception: " + e.getMessage());
      }
    });
  }
}
