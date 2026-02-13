package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.schema.SchemaEntry;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for SchemaRequestHandler.
 */
public class SchemaRequestHandlerTest {
  private SchemaRequestHandler schemaRequestHandler;
  private Admin admin;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    ControllerRequestHandlerDependencies dependencies = mock(ControllerRequestHandlerDependencies.class);
    when(dependencies.getAdmin()).thenReturn(admin);
    schemaRequestHandler = new SchemaRequestHandler(dependencies);
  }

  @Test
  public void testGetValueSchemaSuccess() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    int schemaId = 1;

    Schema avroSchema = Schema.create(Schema.Type.STRING);
    SchemaEntry schemaEntry = new SchemaEntry(1, avroSchema);
    when(admin.getValueSchema(clusterName, storeName, schemaId)).thenReturn(schemaEntry);

    SchemaResponse response = schemaRequestHandler.getValueSchema(clusterName, storeName, schemaId);

    verify(admin, times(1)).getValueSchema(clusterName, storeName, schemaId);
    assertNotNull(response);
    assertEquals(response.getCluster(), clusterName);
    assertEquals(response.getName(), storeName);
    assertEquals(response.getId(), schemaId);
    assertEquals(response.getSchemaStr(), avroSchema.toString());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Value schema for schema id: 99 of store: testStore doesn't exist")
  public void testGetValueSchemaNotFound() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    int schemaId = 99;

    when(admin.getValueSchema(clusterName, storeName, schemaId)).thenReturn(null);

    schemaRequestHandler.getValueSchema(clusterName, storeName, schemaId);
  }

  @Test
  public void testGetKeySchemaSuccess() {
    String clusterName = "testCluster";
    String storeName = "testStore";

    Schema schema = Schema.parse("\"string\"");
    SchemaEntry schemaEntry = new SchemaEntry(1, schema);
    when(admin.getKeySchema(clusterName, storeName)).thenReturn(schemaEntry);

    SchemaResponse response = schemaRequestHandler.getKeySchema(clusterName, storeName);

    verify(admin, times(1)).getKeySchema(clusterName, storeName);
    assertNotNull(response);
    assertEquals(response.getCluster(), clusterName);
    assertEquals(response.getName(), storeName);
    assertEquals(response.getId(), 1);
    assertEquals(response.getSchemaStr(), "\"string\"");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Key schema doesn't exist for store: testStore")
  public void testGetKeySchemaWhenSchemaDoesNotExist() {
    String clusterName = "testCluster";
    String storeName = "testStore";

    when(admin.getKeySchema(clusterName, storeName)).thenReturn(null);

    schemaRequestHandler.getKeySchema(clusterName, storeName);
  }
}
