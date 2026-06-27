package com.linkedin.venice.schema;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class KmeSchemaReaderTest {
  @Test
  public void testConstructorMergesNewerSchemasWithJarResources() {
    /*
     * Pass an empty newer-schemas map; the reader should still serve the jar-bundled KME schemas
     * for every protocol version baked into AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.
     */
    KmeSchemaReader reader = new KmeSchemaReader(Collections.emptyMap());
    int currentVersion = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion();
    Schema schema = reader.getValueSchema(currentVersion);
    assertNotNull(schema);
    assertEquals(reader.getLatestValueSchemaId().intValue(), currentVersion);
  }

  @Test
  public void testConstructorAcceptsNewerSchemas() {
    /*
     * The newer-schemas map carries id -> schema-JSON entries that the jar doesn't yet ship.
     * Use a deep-clone of the latest bundled schema under a synthetic future id so the merge
     * step doesn't reject duplicate ids.
     */
    int currentVersion = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion();
    int futureVersion = currentVersion + 1;
    String latestSchemaJson =
        AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersionSchema().toString();
    Map<Integer, String> newerSchemas = new HashMap<>();
    newerSchemas.put(futureVersion, latestSchemaJson);

    KmeSchemaReader reader = new KmeSchemaReader(newerSchemas);
    assertNotNull(reader.getValueSchema(futureVersion));
    assertNotNull(reader.getValueSchema(currentVersion));
    assertTrue(reader.getLatestValueSchemaId() >= futureVersion);
  }

  @Test
  public void testGetKeySchemaReturnsDefault() {
    KmeSchemaReader reader = new KmeSchemaReader(Collections.emptyMap());
    assertNotNull(reader.getKeySchema());
  }

  @Test
  public void testGetLatestValueSchemaReturnsHighestId() {
    KmeSchemaReader reader = new KmeSchemaReader(Collections.emptyMap());
    Schema latest = reader.getLatestValueSchema();
    assertNotNull(latest);
    int latestId = reader.getLatestValueSchemaId();
    assertEquals(reader.getValueSchema(latestId), latest);
  }

  @Test
  public void testUpdateSchemasUnsupported() {
    KmeSchemaReader reader = new KmeSchemaReader(Collections.emptyMap());
    assertThrows(VeniceUnsupportedOperationException.class, () -> reader.getUpdateSchema(1));
    assertThrows(VeniceUnsupportedOperationException.class, reader::getLatestUpdateSchema);
  }

  @Test
  public void testCloseIsNoOp() {
    KmeSchemaReader reader = new KmeSchemaReader(Collections.emptyMap());
    reader.close();
  }

  @Test
  public void testFromControllerClientFetchesAndMergesKmeSchemas() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    int currentVersion = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion();
    String latestSchemaJson =
        AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersionSchema().toString();

    MultiSchemaResponse.Schema schemaEntry = new MultiSchemaResponse.Schema();
    schemaEntry.setId(currentVersion);
    schemaEntry.setSchemaStr(latestSchemaJson);

    MultiSchemaResponse response = new MultiSchemaResponse();
    response.setSchemas(new MultiSchemaResponse.Schema[] { schemaEntry });
    when(controllerClient.getAllValueSchema(anyString())).thenReturn(response);

    KmeSchemaReader reader = KmeSchemaReader.fromControllerClient(controllerClient);
    assertNotNull(reader.getValueSchema(currentVersion));
  }

  @Test
  public void testFromControllerClientThrowsOnControllerError() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    MultiSchemaResponse response = new MultiSchemaResponse();
    response.setError("simulated controller error");
    when(controllerClient.getAllValueSchema(anyString())).thenReturn(response);

    VeniceException e =
        expectThrows(VeniceException.class, () -> KmeSchemaReader.fromControllerClient(controllerClient));
    assertTrue(e.getMessage().contains("simulated controller error"));
  }
}
