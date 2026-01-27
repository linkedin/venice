package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcResponse;
import com.linkedin.venice.schema.SchemaEntry;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


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
    GetValueSchemaGrpcRequest request = GetValueSchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .setSchemaId(1)
        .build();

    Schema avroSchema = Schema.create(Schema.Type.STRING);
    SchemaEntry schemaEntry = new SchemaEntry(1, avroSchema);
    when(admin.getValueSchema("testCluster", "testStore", 1)).thenReturn(schemaEntry);

    GetValueSchemaGrpcResponse response = schemaRequestHandler.getValueSchema(request);

    verify(admin, times(1)).getValueSchema("testCluster", "testStore", 1);
    assertEquals(response.getStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getStoreInfo().getStoreName(), "testStore");
    assertEquals(response.getSchemaId(), 1);
    assertEquals(response.getSchemaStr(), avroSchema.toString());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Value schema for schema id: 99 of store: testStore doesn't exist")
  public void testGetValueSchemaNotFound() {
    GetValueSchemaGrpcRequest request = GetValueSchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .setSchemaId(99)
        .build();

    when(admin.getValueSchema("testCluster", "testStore", 99)).thenReturn(null);

    schemaRequestHandler.getValueSchema(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cluster name is mandatory parameter")
  public void testGetValueSchemaMissingClusterName() {
    GetValueSchemaGrpcRequest request = GetValueSchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setStoreName("testStore").build())
        .setSchemaId(1)
        .build();

    schemaRequestHandler.getValueSchema(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Store name is mandatory parameter")
  public void testGetValueSchemaMissingStoreName() {
    GetValueSchemaGrpcRequest request = GetValueSchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").build())
        .setSchemaId(1)
        .build();

    schemaRequestHandler.getValueSchema(request);
  }

  @Test
  public void testGetKeySchemaSuccess() {
    GetKeySchemaGrpcRequest request = GetKeySchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .build();

    Schema schema = Schema.parse("\"string\"");
    SchemaEntry schemaEntry = new SchemaEntry(1, schema);
    when(admin.getKeySchema("testCluster", "testStore")).thenReturn(schemaEntry);

    GetKeySchemaGrpcResponse response = schemaRequestHandler.getKeySchema(request);

    verify(admin, times(1)).getKeySchema("testCluster", "testStore");
    assertEquals(response.getStoreInfo().getClusterName(), "testCluster");
    assertEquals(response.getStoreInfo().getStoreName(), "testStore");
    assertEquals(response.getSchemaId(), 1);
    assertEquals(response.getSchemaStr(), "\"string\"");
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Key schema doesn't exist for store: testStore")
  public void testGetKeySchemaWhenSchemaDoesNotExist() {
    GetKeySchemaGrpcRequest request = GetKeySchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").setStoreName("testStore").build())
        .build();

    when(admin.getKeySchema("testCluster", "testStore")).thenReturn(null);

    schemaRequestHandler.getKeySchema(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*[Cc]luster.*")
  public void testGetKeySchemaWithMissingClusterName() {
    GetKeySchemaGrpcRequest request = GetKeySchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setStoreName("testStore").build())
        .build();

    schemaRequestHandler.getKeySchema(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*[Ss]tore.*")
  public void testGetKeySchemaWithMissingStoreName() {
    GetKeySchemaGrpcRequest request = GetKeySchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName("testCluster").build())
        .build();

    schemaRequestHandler.getKeySchema(request);
  }
}
