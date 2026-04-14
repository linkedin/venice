package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.DegradedDcResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.DegradedDcInfo;
import com.linkedin.venice.meta.DegradedDcStates;
import com.linkedin.venice.protocols.controller.StoreMigrationCheckGrpcRequest;
import com.linkedin.venice.protocols.controller.StoreMigrationCheckGrpcResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class ClusterRoutesTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String TEST_CLUSTER = "test_cluster";

  @Test
  public void testUpdateDarkClusterConfig() throws Exception {
    VeniceHelixAdmin mockVeniceHelixAdmin = mock(VeniceHelixAdmin.class);
    VeniceControllerClusterConfig veniceControllerClusterConfig = mock(VeniceControllerClusterConfig.class);

    doReturn(true).when(mockVeniceHelixAdmin).isLeaderControllerFor(anyString());
    when(mockVeniceHelixAdmin.getControllerConfig(anyString())).thenReturn(veniceControllerClusterConfig);
    doReturn(true).when(veniceControllerClusterConfig).isDarkCluster();

    Request request = mock(Request.class);

    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    when(request.queryMap()).thenReturn(queryParamsMap);

    Map<String, String[]> queryMapData = new HashMap<>();
    queryMapData.put(ControllerApiConstants.CLUSTER, new String[] { TEST_CLUSTER });
    queryMapData.put(ControllerApiConstants.STORES_TO_REPLICATE, new String[] { "store1,store2" });

    when(queryParamsMap.toMap()).thenReturn(queryMapData);
    when(request.queryParams(ControllerApiConstants.CLUSTER)).thenReturn(TEST_CLUSTER);
    Route updateDarkClusterConfigRoute =
        new ClusterRoutes(false, Optional.empty()).updateDarkClusterConfig(mockVeniceHelixAdmin);

    ControllerResponse response = OBJECT_MAPPER.readValue(
        updateDarkClusterConfigRoute.handle(request, mock(Response.class)).toString(),
        ControllerResponse.class);
    assertFalse(response.isError());
  }

  @Test
  public void testIsStoreMigrationAllowedReturnsTrue() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    ClusterAdminOpsRequestHandler mockRequestHandler = mock(ClusterAdminOpsRequestHandler.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));

    StoreMigrationCheckGrpcResponse grpcResponse = StoreMigrationCheckGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setStoreMigrationAllowed(true)
        .build();
    doReturn(grpcResponse).when(mockRequestHandler).isStoreMigrationAllowed(any(StoreMigrationCheckGrpcRequest.class));

    Route route = new ClusterRoutes(false, Optional.empty(), mockRequestHandler).isStoreMigrationAllowed(mockAdmin);

    StoreMigrationResponse response = ObjectMapperFactory.getInstance()
        .readValue(route.handle(request, mock(Response.class)).toString(), StoreMigrationResponse.class);
    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getCluster(), TEST_CLUSTER);
    Assert.assertTrue(response.isStoreMigrationAllowed());
  }

  @Test
  public void testIsStoreMigrationAllowedReturnsFalse() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    ClusterAdminOpsRequestHandler mockRequestHandler = mock(ClusterAdminOpsRequestHandler.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));

    StoreMigrationCheckGrpcResponse grpcResponse = StoreMigrationCheckGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setStoreMigrationAllowed(false)
        .build();
    doReturn(grpcResponse).when(mockRequestHandler).isStoreMigrationAllowed(any(StoreMigrationCheckGrpcRequest.class));

    Route route = new ClusterRoutes(false, Optional.empty(), mockRequestHandler).isStoreMigrationAllowed(mockAdmin);

    StoreMigrationResponse response = ObjectMapperFactory.getInstance()
        .readValue(route.handle(request, mock(Response.class)).toString(), StoreMigrationResponse.class);
    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getCluster(), TEST_CLUSTER);
    Assert.assertFalse(response.isStoreMigrationAllowed());
  }

  @Test
  public void testIsStoreMigrationAllowedError() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    ClusterAdminOpsRequestHandler mockRequestHandler = mock(ClusterAdminOpsRequestHandler.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));

    doThrow(new VeniceException("Error checking migration allowed")).when(mockRequestHandler)
        .isStoreMigrationAllowed(any(StoreMigrationCheckGrpcRequest.class));

    Route route = new ClusterRoutes(false, Optional.empty(), mockRequestHandler).isStoreMigrationAllowed(mockAdmin);

    StoreMigrationResponse response = ObjectMapperFactory.getInstance()
        .readValue(route.handle(request, mock(Response.class)).toString(), StoreMigrationResponse.class);
    Assert.assertTrue(response.isError());
    Assert.assertEquals(response.getCluster(), TEST_CLUSTER);
  }

  @Test
  public void testMarkDatacenterDegraded() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isParent();
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(ControllerApiConstants.CLUSTER);
    doReturn("dc-1").when(request).queryParams(ControllerApiConstants.DATACENTER_NAME);
    doReturn("60").when(request).queryParams(ControllerApiConstants.TIMEOUT_MINUTES);
    doReturn("test-operator").when(request).queryParams(ControllerApiConstants.OPERATOR_ID);

    Route route = new ClusterRoutes(false, Optional.empty()).markDatacenterDegraded(mockAdmin);
    ControllerResponse response =
        OBJECT_MAPPER.readValue(route.handle(request, mock(Response.class)).toString(), ControllerResponse.class);
    assertFalse(response.isError());
    Assert.assertEquals(response.getCluster(), TEST_CLUSTER);
  }

  @Test
  public void testMarkDatacenterDegradedNonParentRejected() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(false).when(mockAdmin).isParent();
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(ControllerApiConstants.CLUSTER);
    doReturn("dc-1").when(request).queryParams(ControllerApiConstants.DATACENTER_NAME);

    Route route = new ClusterRoutes(false, Optional.empty()).markDatacenterDegraded(mockAdmin);
    ControllerResponse response =
        OBJECT_MAPPER.readValue(route.handle(request, mock(Response.class)).toString(), ControllerResponse.class);
    Assert.assertTrue(response.isError());
    Assert.assertTrue(response.getError().contains("parent controller"));
  }

  @Test
  public void testMarkDatacenterDegradedInvalidTimeout() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isParent();
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(ControllerApiConstants.CLUSTER);
    doReturn("dc-1").when(request).queryParams(ControllerApiConstants.DATACENTER_NAME);
    doReturn("not-a-number").when(request).queryParams(ControllerApiConstants.TIMEOUT_MINUTES);

    Route route = new ClusterRoutes(false, Optional.empty()).markDatacenterDegraded(mockAdmin);
    ControllerResponse response =
        OBJECT_MAPPER.readValue(route.handle(request, mock(Response.class)).toString(), ControllerResponse.class);
    Assert.assertTrue(response.isError());
    Assert.assertTrue(response.getError().contains("Invalid timeout_minutes"));
  }

  @Test
  public void testUnmarkDatacenterDegraded() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isParent();
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(ControllerApiConstants.CLUSTER);
    doReturn("dc-1").when(request).queryParams(ControllerApiConstants.DATACENTER_NAME);

    Route route = new ClusterRoutes(false, Optional.empty()).unmarkDatacenterDegraded(mockAdmin);
    ControllerResponse response =
        OBJECT_MAPPER.readValue(route.handle(request, mock(Response.class)).toString(), ControllerResponse.class);
    assertFalse(response.isError());
  }

  @Test
  public void testGetDegradedDatacenters() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isParent();
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    DegradedDcStates states = new DegradedDcStates();
    states.addDegradedDatacenter("dc-1", new DegradedDcInfo(System.currentTimeMillis(), 120, "op"));
    doReturn(states).when(mockAdmin).getDegradedDcStates(TEST_CLUSTER);

    Request request = mock(Request.class);
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(ControllerApiConstants.CLUSTER);

    Route route = new ClusterRoutes(false, Optional.empty()).getDegradedDatacenters(mockAdmin);
    DegradedDcResponse response =
        OBJECT_MAPPER.readValue(route.handle(request, mock(Response.class)).toString(), DegradedDcResponse.class);
    assertFalse(response.isError());
    Assert.assertTrue(response.getDegradedDatacenters().containsKey("dc-1"));
  }
}
