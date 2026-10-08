package com.linkedin.venice.serialization.avro;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.status.protocol.PushJobDetails;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InternalAvroSpecificSerializerTest {
  @Test
  public void testSetSchemaReaderRejectsInvalidMaxAttempts() {
    InternalAvroSpecificSerializer<PushJobDetails> serializer = AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();

    Assert.expectThrows(IllegalArgumentException.class, () -> serializer.setSchemaReader(mock(SchemaReader.class), 0));
  }

  @Test
  public void testSchemaReaderAttemptLimit() {
    int futureProtocolVersion = AvroProtocolDefinition.PUSH_JOB_DETAILS.getCurrentProtocolVersion() + 1;
    InternalAvroSpecificSerializer<PushJobDetails> serializer = AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();
    SchemaReader schemaReader = mock(SchemaReader.class);
    serializer.setSchemaReader(schemaReader, 1);

    VeniceException exception = Assert.expectThrows(
        VeniceException.class,
        () -> serializer.deserialize(null, withProtocolVersion(serializer, futureProtocolVersion)));

    Assert.assertTrue(exception.getMessage().contains("after 1 attempts"));
    verify(schemaReader).getValueSchema(futureProtocolVersion);
  }

  @Test
  public void testSchemaReaderRetriesWithinAttemptLimit() {
    int futureProtocolVersion = AvroProtocolDefinition.PUSH_JOB_DETAILS.getCurrentProtocolVersion() + 1;
    InternalAvroSpecificSerializer<PushJobDetails> serializer = AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();
    SchemaReader schemaReader = mock(SchemaReader.class);
    when(schemaReader.getValueSchema(futureProtocolVersion))
        .thenReturn(null, AvroProtocolDefinition.PUSH_JOB_DETAILS.getCurrentProtocolVersionSchema());
    serializer.setSchemaReader(schemaReader, 2);

    PushJobDetails result = serializer.deserialize(null, withProtocolVersion(serializer, futureProtocolVersion));

    Assert.assertEquals(result.clusterName.toString(), "test-cluster");
    verify(schemaReader, times(2)).getValueSchema(futureProtocolVersion);
  }

  private byte[] withProtocolVersion(InternalAvroSpecificSerializer<PushJobDetails> serializer, int protocolVersion) {
    PushJobDetails details = new PushJobDetails();
    details.clusterName = "test-cluster";
    details.overallStatus = Collections.emptyList();
    details.pushId = "";
    details.failureDetails = "";
    byte[] bytes = serializer.serialize(null, details);
    bytes[1] = (byte) protocolVersion;
    return bytes;
  }
}
