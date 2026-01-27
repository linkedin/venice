package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.protocols.controller.IsStoreMigrationAllowedGrpcRequest;
import com.linkedin.venice.protocols.controller.IsStoreMigrationAllowedGrpcResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class ClusterRoutesTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String TEST_CLUSTER = "test_cluster";

  @Test
  public void testIsStoreMigrationAllowedReturnsTrue() throws Exception {
    VeniceHelixAdmin mockAdmin = mock(VeniceHelixAdmin.class);
    ClusterAdminOpsRequestHandler requestHandler = mock(ClusterAdminOpsRequestHandler.class);
    VeniceControllerClusterConfig controllerClusterConfig = mock(VeniceControllerClusterConfig.class);

    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    doReturn(controllerClusterConfig).when(mockAdmin).getControllerConfig(anyString());

    IsStoreMigrationAllowedGrpcResponse grpcResponse = IsStoreMigrationAllowedGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setStoreMigrationAllowed(true)
        .build();
    doReturn(grpcResponse).when(requestHandler).isStoreMigrationAllowed(any(IsStoreMigrationAllowedGrpcRequest.class));

    Request request = mock(Request.class);
    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    Map<String, String[]> queryMap = new HashMap<>();
    queryMap.put(ControllerApiConstants.CLUSTER, new String[] { TEST_CLUSTER });
    doReturn(queryMap).when(queryParamsMap).toMap();
    doReturn(queryParamsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(ControllerApiConstants.CLUSTER);

    Route route = new ClusterRoutes(false, Optional.empty()).isStoreMigrationAllowed(mockAdmin, requestHandler);
    StoreMigrationResponse response =
        OBJECT_MAPPER.readValue(route.handle(request, mock(Response.class)).toString(), StoreMigrationResponse.class);

    assertFalse(response.isError());
    assertEquals(response.getCluster(), TEST_CLUSTER);
    assertTrue(response.isStoreMigrationAllowed());
  }

  @Test
  public void testIsStoreMigrationAllowedReturnsFalse() throws Exception {
    VeniceHelixAdmin mockAdmin = mock(VeniceHelixAdmin.class);
    ClusterAdminOpsRequestHandler requestHandler = mock(ClusterAdminOpsRequestHandler.class);
    VeniceControllerClusterConfig controllerClusterConfig = mock(VeniceControllerClusterConfig.class);

    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    doReturn(controllerClusterConfig).when(mockAdmin).getControllerConfig(anyString());

    IsStoreMigrationAllowedGrpcResponse grpcResponse = IsStoreMigrationAllowedGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setStoreMigrationAllowed(false)
        .build();
    doReturn(grpcResponse).when(requestHandler).isStoreMigrationAllowed(any(IsStoreMigrationAllowedGrpcRequest.class));

    Request request = mock(Request.class);
    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    Map<String, String[]> queryMap = new HashMap<>();
    queryMap.put(ControllerApiConstants.CLUSTER, new String[] { TEST_CLUSTER });
    doReturn(queryMap).when(queryParamsMap).toMap();
    doReturn(queryParamsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(ControllerApiConstants.CLUSTER);

    Route route = new ClusterRoutes(false, Optional.empty()).isStoreMigrationAllowed(mockAdmin, requestHandler);
    StoreMigrationResponse response =
        OBJECT_MAPPER.readValue(route.handle(request, mock(Response.class)).toString(), StoreMigrationResponse.class);

    assertFalse(response.isError());
    assertEquals(response.getCluster(), TEST_CLUSTER);
    assertFalse(response.isStoreMigrationAllowed());
  }

  @Test
  public void testIsStoreMigrationAllowedHandlesException() throws Exception {
    VeniceHelixAdmin mockAdmin = mock(VeniceHelixAdmin.class);
    ClusterAdminOpsRequestHandler requestHandler = mock(ClusterAdminOpsRequestHandler.class);
    VeniceControllerClusterConfig controllerClusterConfig = mock(VeniceControllerClusterConfig.class);

    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    doReturn(controllerClusterConfig).when(mockAdmin).getControllerConfig(anyString());

    doReturn(null).when(requestHandler).isStoreMigrationAllowed(any(IsStoreMigrationAllowedGrpcRequest.class));

    Request request = mock(Request.class);
    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    Map<String, String[]> queryMap = new HashMap<>();
    queryMap.put(ControllerApiConstants.CLUSTER, new String[] { TEST_CLUSTER });
    doReturn(queryMap).when(queryParamsMap).toMap();
    doReturn(queryParamsMap).when(request).queryMap();
    doReturn(TEST_CLUSTER).when(request).queryParams(ControllerApiConstants.CLUSTER);

    Route route = new ClusterRoutes(false, Optional.empty()).isStoreMigrationAllowed(mockAdmin, requestHandler);
    StoreMigrationResponse response =
        OBJECT_MAPPER.readValue(route.handle(request, mock(Response.class)).toString(), StoreMigrationResponse.class);

    assertTrue(response.isError());
  }
}
