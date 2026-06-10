package com.linkedin.venice.hadoop.snapshot;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link SnapshotAtTSchemaRepository}: it is populated once from the controller's value / RMD /
 * derived schemas, and only the merge-path lookups are supported.
 */
public class SnapshotAtTSchemaRepositoryTest {
  private static final String STORE = "snapshot_repo_test";
  private static final String STRING_SCHEMA = "\"string\"";

  @Test
  public void testFromControllerPopulatesSchemas() {
    Schema valueSchema = new Schema.Parser().parse(STRING_SCHEMA);
    int rmdVersion = RmdSchemaGenerator.getLatestVersion();
    String rmdSchemaStr = RmdSchemaGenerator.generateMetadataSchema(valueSchema, rmdVersion).toString();

    ControllerClient client = mock(ControllerClient.class);
    when(client.getAllValueSchema(STORE)).thenReturn(valueSchemaResponse());
    when(client.getAllReplicationMetadataSchemas(STORE)).thenReturn(rmdSchemaResponse(rmdVersion, rmdSchemaStr));
    when(client.getAllValueAndDerivedSchema(STORE)).thenReturn(emptyResponse());

    SnapshotAtTSchemaRepository repository = SnapshotAtTSchemaRepository.fromController(client, STORE);
    assertNotNull(repository.getValueSchema(STORE, 1));
    assertNotNull(repository.getSupersetOrLatestValueSchema(STORE));
    assertNotNull(repository.getSupersetSchema(STORE));
    assertTrue(repository.hasValueSchema(STORE, 1));
    assertNotNull(repository.getReplicationMetadataSchema(STORE, 1, rmdVersion));
    assertEquals(repository.getRmdVersionId(), rmdVersion);
    assertNull(repository.getValueSchema(STORE, 99));
    assertEquals(repository.getValueSchemas(STORE).size(), 1);
    assertEquals(repository.getReplicationMetadataSchemas(STORE).size(), 1);
  }

  @Test
  public void testFromControllerThrowsOnValueSchemaError() {
    ControllerClient client = mock(ControllerClient.class);
    MultiSchemaResponse error = new MultiSchemaResponse();
    error.setError("boom");
    when(client.getAllValueSchema(STORE)).thenReturn(error);
    assertThrows(VeniceException.class, () -> SnapshotAtTSchemaRepository.fromController(client, STORE));
  }

  @Test
  public void testUnsupportedMethodsThrow() {
    ControllerClient client = mock(ControllerClient.class);
    when(client.getAllValueSchema(STORE)).thenReturn(valueSchemaResponse());
    when(client.getAllReplicationMetadataSchemas(STORE)).thenReturn(emptyResponse());
    when(client.getAllValueAndDerivedSchema(STORE)).thenReturn(emptyResponse());
    SnapshotAtTSchemaRepository repository = SnapshotAtTSchemaRepository.fromController(client, STORE);
    assertThrows(UnsupportedOperationException.class, () -> repository.getKeySchema(STORE));
    assertThrows(UnsupportedOperationException.class, () -> repository.getValueSchemaId(STORE, STRING_SCHEMA));
    assertThrows(UnsupportedOperationException.class, () -> repository.getLatestDerivedSchema(STORE, 1));
    repository.refresh(); // no-op
    repository.clear(); // no-op
  }

  private MultiSchemaResponse valueSchemaResponse() {
    MultiSchemaResponse response = new MultiSchemaResponse();
    MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
    schema.setId(1);
    schema.setSchemaStr(STRING_SCHEMA);
    response.setSchemas(new MultiSchemaResponse.Schema[] { schema });
    return response;
  }

  private MultiSchemaResponse rmdSchemaResponse(int rmdVersion, String rmdSchemaStr) {
    MultiSchemaResponse response = new MultiSchemaResponse();
    MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
    schema.setId(rmdVersion);
    schema.setRmdValueSchemaId(1);
    schema.setSchemaStr(rmdSchemaStr);
    response.setSchemas(new MultiSchemaResponse.Schema[] { schema });
    return response;
  }

  private MultiSchemaResponse emptyResponse() {
    MultiSchemaResponse response = new MultiSchemaResponse();
    response.setSchemas(new MultiSchemaResponse.Schema[0]);
    return response;
  }
}
