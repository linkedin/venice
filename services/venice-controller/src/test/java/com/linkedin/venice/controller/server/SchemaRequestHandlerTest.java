package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetAllValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAllValueSchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcResponse;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.Arrays;
import java.util.Collection;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SchemaRequestHandlerTest {
  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_STORE = "testStore";

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
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build())
        .setSchemaId(1)
        .build();

    Schema avroSchema = Schema.create(Schema.Type.STRING);
    SchemaEntry schemaEntry = new SchemaEntry(1, avroSchema);
    when(admin.getValueSchema(TEST_CLUSTER, TEST_STORE, 1)).thenReturn(schemaEntry);

    GetValueSchemaGrpcResponse response = schemaRequestHandler.getValueSchema(request);

    verify(admin, times(1)).getValueSchema(TEST_CLUSTER, TEST_STORE, 1);
    assertEquals(response.getStoreInfo().getClusterName(), TEST_CLUSTER);
    assertEquals(response.getStoreInfo().getStoreName(), TEST_STORE);
    assertEquals(response.getSchemaId(), 1);
    assertEquals(response.getSchemaStr(), avroSchema.toString());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Value schema for schema id: 99 of store: testStore doesn't exist")
  public void testGetValueSchemaNotFound() {
    GetValueSchemaGrpcRequest request = GetValueSchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build())
        .setSchemaId(99)
        .build();

    when(admin.getValueSchema(TEST_CLUSTER, TEST_STORE, 99)).thenReturn(null);

    schemaRequestHandler.getValueSchema(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cluster name is mandatory parameter")
  public void testGetValueSchemaMissingClusterName() {
    GetValueSchemaGrpcRequest request = GetValueSchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setStoreName(TEST_STORE).build())
        .setSchemaId(1)
        .build();

    schemaRequestHandler.getValueSchema(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Store name is mandatory parameter")
  public void testGetValueSchemaMissingStoreName() {
    GetValueSchemaGrpcRequest request = GetValueSchemaGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).build())
        .setSchemaId(1)
        .build();

    schemaRequestHandler.getValueSchema(request);
  }

  @Test
  public void testGetAllValueSchemaSuccess() {
    Store mockStore = mock(Store.class);
    when(mockStore.getLatestSuperSetValueSchemaId()).thenReturn(2);

    SchemaEntry schema1 = new SchemaEntry(1, Schema.create(Schema.Type.STRING));
    SchemaEntry schema2 = new SchemaEntry(2, Schema.create(Schema.Type.INT));
    Collection<SchemaEntry> schemas = Arrays.asList(schema1, schema2);

    when(admin.getValueSchemas(TEST_CLUSTER, TEST_STORE)).thenReturn(schemas);
    when(admin.getStore(TEST_CLUSTER, TEST_STORE)).thenReturn(mockStore);

    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetAllValueSchemaGrpcRequest request = GetAllValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    GetAllValueSchemaGrpcResponse response = schemaRequestHandler.getAllValueSchema(request);

    assertNotNull(response);
    assertEquals(response.getStoreInfo().getClusterName(), TEST_CLUSTER);
    assertEquals(response.getStoreInfo().getStoreName(), TEST_STORE);
    assertEquals(response.getSchemasCount(), 2);
    assertEquals(response.getSchemas(0).getId(), 1);
    assertEquals(response.getSchemas(1).getId(), 2);
    assertEquals(response.getSuperSetSchemaId(), 2);
  }

  @Test
  public void testGetAllValueSchemaEmptySchemas() {
    Store mockStore = mock(Store.class);
    when(mockStore.getLatestSuperSetValueSchemaId()).thenReturn(-1);

    when(admin.getValueSchemas(TEST_CLUSTER, TEST_STORE)).thenReturn(Arrays.asList());
    when(admin.getStore(TEST_CLUSTER, TEST_STORE)).thenReturn(mockStore);

    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetAllValueSchemaGrpcRequest request = GetAllValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    GetAllValueSchemaGrpcResponse response = schemaRequestHandler.getAllValueSchema(request);

    assertNotNull(response);
    assertEquals(response.getSchemasCount(), 0);
    assertEquals(response.getSuperSetSchemaId(), -1);
  }

  @Test
  public void testGetAllValueSchemaMissingClusterName() {
    ClusterStoreGrpcInfo storeInfo = ClusterStoreGrpcInfo.newBuilder().setStoreName(TEST_STORE).build();
    GetAllValueSchemaGrpcRequest request = GetAllValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    Exception e = expectThrows(IllegalArgumentException.class, () -> schemaRequestHandler.getAllValueSchema(request));
    assertNotNull(e.getMessage());
  }

  @Test
  public void testGetAllValueSchemaMissingStoreName() {
    ClusterStoreGrpcInfo storeInfo = ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).build();
    GetAllValueSchemaGrpcRequest request = GetAllValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    Exception e = expectThrows(IllegalArgumentException.class, () -> schemaRequestHandler.getAllValueSchema(request));
    assertNotNull(e.getMessage());
  }
}
