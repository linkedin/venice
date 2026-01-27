package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SCHEMA_COMPAT_TYPE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcResponse;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class SchemaRoutesTest {
  private SchemaRequestHandler schemaRequestHandler;

  @BeforeMethod
  public void setUp() {
    schemaRequestHandler = mock(SchemaRequestHandler.class);
  }

  @Test
  public void schemaMismatchErrorMessage() {
    String cluster = "cluster_name";
    String store = "store_name";
    String schemaStr = "int";

    Admin admin = mock(Admin.class);
    when(admin.getValueSchemaId(cluster, store, schemaStr)).thenReturn(SchemaData.INVALID_VALUE_SCHEMA_ID);
    when(admin.getStore(cluster, store)).thenReturn(null);
    SchemaRoutes schemaRoutes = new SchemaRoutes(false, Optional.empty(), schemaRequestHandler);
    try {
      schemaRoutes.populateSchemaResponseForValueOrDerivedSchemaID(admin, cluster, store, schemaStr);
    } catch (VeniceNoStoreException e) {
      assertTrue(
          e.getMessage().contains("Store: store_name does not exist in cluster cluster_name"),
          "Got this instead: " + e.getMessage());
    }

    Store storeInfo = mock(Store.class);
    when(admin.getStore(cluster, store)).thenReturn(storeInfo);

    when(storeInfo.isWriteComputationEnabled()).thenReturn(false);

    try {
      schemaRoutes.populateSchemaResponseForValueOrDerivedSchemaID(admin, cluster, store, schemaStr);
    } catch (VeniceException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Can not find any registered value schema for the store store_name that matches the schema of data being pushed."),
          "Got this instead: " + e.getMessage());
    }

    when(storeInfo.isWriteComputationEnabled()).thenReturn(true);
    when(admin.getDerivedSchemaId(cluster, store, schemaStr)).thenReturn(GeneratedSchemaID.INVALID);

    try {
      schemaRoutes.populateSchemaResponseForValueOrDerivedSchemaID(admin, cluster, store, schemaStr);
    } catch (VeniceException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Can not find any registered value schema nor derived schema for the store store_name that matches the schema of data being pushed."),
          "Got this instead: " + e.getMessage());
    }
  }

  @Test
  public void testAddValueSchemaWithIdAndCompatType() throws Exception {
    String cluster = "cluster_name";
    String store = "store_name";
    String schemaStr = "\"int\"";
    int schemaId = 2;
    DirectionalSchemaCompatibilityType schemaCompatType = DirectionalSchemaCompatibilityType.BACKWARD;

    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    doReturn(cluster).when(request).queryParams(CLUSTER);
    doReturn(store).when(request).queryParams(NAME);
    doReturn(schemaStr).when(request).queryParams(VALUE_SCHEMA);
    doReturn(Integer.toString(schemaId)).when(request).queryParams(SCHEMA_ID);
    doReturn(schemaCompatType.toString()).when(request).queryParams(SCHEMA_COMPAT_TYPE);

    doReturn(true).when(admin).isLeaderControllerFor("cluster_name");
    doReturn(new SchemaEntry(schemaId, schemaStr)).when(admin)
        .addValueSchema(cluster, store, schemaStr, schemaId, schemaCompatType);

    SchemaRoutes schemaRoutes = new SchemaRoutes(false, Optional.empty(), schemaRequestHandler);
    Route route = schemaRoutes.addValueSchema(admin);
    route.handle(request, response);
    verify(response, times(0)).status(anyInt()); // no error
  }

  @Test
  public void testGetValueSchemaSuccess() throws Exception {
    String cluster = "cluster_name";
    String store = "store_name";
    int schemaId = 1;
    String schemaStr = "\"string\"";

    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    doReturn(cluster).when(request).queryParams(CLUSTER);
    doReturn(store).when(request).queryParams(NAME);
    doReturn(Integer.toString(schemaId)).when(request).queryParams(SCHEMA_ID);
    doReturn(true).when(admin).isLeaderControllerFor(cluster);

    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(cluster).setStoreName(store).build();
    GetValueSchemaGrpcResponse grpcResponse = GetValueSchemaGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .setSchemaId(schemaId)
        .setSchemaStr(schemaStr)
        .build();
    when(schemaRequestHandler.getValueSchema(any(GetValueSchemaGrpcRequest.class))).thenReturn(grpcResponse);

    SchemaRoutes schemaRoutes = new SchemaRoutes(false, Optional.empty(), schemaRequestHandler);
    Route route = schemaRoutes.getValueSchema(admin);
    String result = (String) route.handle(request, response);

    verify(schemaRequestHandler, times(1)).getValueSchema(any(GetValueSchemaGrpcRequest.class));
    verify(response, times(0)).status(anyInt()); // no error
    assertTrue(result.contains("\"schemaStr\":\"\\\"string\\\"\""), "Response should contain the schema string");
    assertTrue(result.contains("\"id\":1"), "Response should contain the schema ID");
  }

  @Test
  public void testGetValueSchemaNotFound() throws Exception {
    String cluster = "cluster_name";
    String store = "store_name";
    int schemaId = 99;

    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    doReturn(cluster).when(request).queryParams(CLUSTER);
    doReturn(store).when(request).queryParams(NAME);
    doReturn(Integer.toString(schemaId)).when(request).queryParams(SCHEMA_ID);
    doReturn(true).when(admin).isLeaderControllerFor(cluster);

    // Mock queryMap for handleError method
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    Map<String, String[]> queryMapResult = new HashMap<>();
    queryMapResult.put(CLUSTER, new String[] { cluster });
    queryMapResult.put(NAME, new String[] { store });
    queryMapResult.put(SCHEMA_ID, new String[] { Integer.toString(schemaId) });
    doReturn(queryMapResult).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(schemaRequestHandler.getValueSchema(any(GetValueSchemaGrpcRequest.class)))
        .thenThrow(new IllegalArgumentException("Value schema for schema id: 99 of store: store_name doesn't exist"));

    SchemaRoutes schemaRoutes = new SchemaRoutes(false, Optional.empty(), schemaRequestHandler);
    Route route = schemaRoutes.getValueSchema(admin);
    String result = (String) route.handle(request, response);

    verify(schemaRequestHandler, times(1)).getValueSchema(any(GetValueSchemaGrpcRequest.class));
    assertTrue(
        result.contains("Value schema for schema id: 99 of store: store_name doesn't exist"),
        "Response should contain error message");
  }

  @Test
  public void testGetKeySchemaSuccess() throws Exception {
    String cluster = "cluster_name";
    String store = "store_name";

    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    doReturn(cluster).when(request).queryParams(CLUSTER);
    doReturn(store).when(request).queryParams(NAME);
    doReturn(true).when(admin).isLeaderControllerFor(cluster);

    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(cluster).setStoreName(store).build();
    GetKeySchemaGrpcResponse grpcResponse =
        GetKeySchemaGrpcResponse.newBuilder().setStoreInfo(storeInfo).setSchemaId(1).setSchemaStr("\"string\"").build();
    when(schemaRequestHandler.getKeySchema(any(GetKeySchemaGrpcRequest.class))).thenReturn(grpcResponse);

    SchemaRoutes schemaRoutes = new SchemaRoutes(false, Optional.empty(), schemaRequestHandler);
    Route route = schemaRoutes.getKeySchema(admin);
    String result = (String) route.handle(request, response);

    verify(response, times(0)).status(anyInt()); // no error
    verify(schemaRequestHandler, times(1)).getKeySchema(any(GetKeySchemaGrpcRequest.class));

    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    SchemaResponse schemaResponse = mapper.readValue(result, SchemaResponse.class);
    assertEquals(schemaResponse.getCluster(), cluster);
    assertEquals(schemaResponse.getName(), store);
    assertEquals(schemaResponse.getId(), 1);
    assertEquals(schemaResponse.getSchemaStr(), "\"string\"");
  }

  @Test
  public void testGetKeySchemaNotFound() throws Exception {
    String cluster = "cluster_name";
    String store = "store_name";

    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    doReturn(cluster).when(request).queryParams(CLUSTER);
    doReturn(store).when(request).queryParams(NAME);

    // Mock queryMap to avoid NullPointerException in handleError
    spark.QueryParamsMap queryParamsMap = mock(spark.QueryParamsMap.class);
    when(request.queryMap()).thenReturn(queryParamsMap);
    when(queryParamsMap.toMap()).thenReturn(new java.util.HashMap<>());

    doReturn(true).when(admin).isLeaderControllerFor(cluster);

    when(schemaRequestHandler.getKeySchema(any(GetKeySchemaGrpcRequest.class)))
        .thenThrow(new VeniceException("Key schema doesn't exist for store: " + store));

    SchemaRoutes schemaRoutes = new SchemaRoutes(false, Optional.empty(), schemaRequestHandler);
    Route route = schemaRoutes.getKeySchema(admin);
    String result = (String) route.handle(request, response);

    verify(schemaRequestHandler, times(1)).getKeySchema(any(GetKeySchemaGrpcRequest.class));

    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    SchemaResponse schemaResponse = mapper.readValue(result, SchemaResponse.class);
    assertTrue(schemaResponse.isError());
    assertTrue(schemaResponse.getError().contains("Key schema doesn't exist for store"));
  }
}
