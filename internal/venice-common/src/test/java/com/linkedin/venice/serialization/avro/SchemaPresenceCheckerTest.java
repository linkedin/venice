package com.linkedin.venice.serialization.avro;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaReader;
import java.util.Optional;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaPresenceCheckerTest {
  @Test
  public void verifySchemaPresenceSuccess() {
    SchemaReader schemaReader = mock(SchemaReader.class);
    Schema valueSchema = mock(Schema.class);
    when(schemaReader.getValueSchema(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()))
        .thenReturn(valueSchema);
    SchemaPresenceChecker schemaPresenceChecker =
        new SchemaPresenceChecker(schemaReader, AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE);
    schemaPresenceChecker.verifySchemaVersionPresentOrExit();
  }

  @Test
  public void verifySchemaPresenceFailure() {
    SchemaReader schemaReader = mock(SchemaReader.class);
    when(schemaReader.getValueSchema(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()))
        .thenReturn(null);
    SchemaPresenceChecker schemaPresenceChecker =
        new SchemaPresenceChecker(schemaReader, AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE);
    VeniceException ex = Assert.expectThrows(
        VeniceException.class,
        () -> schemaPresenceChecker.verifySchemaVersionPresentOrExit(
            Optional.of(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()),
            false));
    Assert.assertTrue(ex.getMessage().contains("SchemaVersionNotFound"));
  }
}
