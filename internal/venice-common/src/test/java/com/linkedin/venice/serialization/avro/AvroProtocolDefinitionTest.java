package com.linkedin.venice.serialization.avro;

import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.SERVER_ADMIN_RESPONSE;

import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroProtocolDefinitionTest {
  @Test
  public void testGetSerializer() {
    Assert.assertNotNull(KAFKA_MESSAGE_ENVELOPE.getSerializer());
    Assert.assertNotNull(SERVER_ADMIN_RESPONSE.getSerializer());
  }
}
