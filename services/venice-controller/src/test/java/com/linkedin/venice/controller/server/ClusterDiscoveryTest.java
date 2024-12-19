package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerRoute.CLUSTER_DISCOVERY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.request.ClusterDiscoveryRequest;
import com.linkedin.venice.utils.ObjectMapperFactory;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class ClusterDiscoveryTest {
  @Test
  public void testDiscoverCluster() throws Exception {
    // Case 1: Store name is not provided
    String clusterName = "test-cluster";
    String storeName = "test-store";
    String d2Service = "d2://test-service";
    String serverD2Service = "d2://test-server";

    Admin admin = mock(Admin.class);
    VeniceControllerRequestHandler requestHandler = mock(VeniceControllerRequestHandler.class);

    doAnswer(invocation -> {
      D2ServiceDiscoveryResponse response = invocation.getArgument(1);
      response.setName(storeName);
      response.setCluster(clusterName);
      response.setD2Service(d2Service);
      response.setServerD2Service(serverD2Service);
      return null;
    }).when(requestHandler).discoverCluster(any(ClusterDiscoveryRequest.class), any(D2ServiceDiscoveryResponse.class));

    Request request = mock(Request.class);
    when(request.pathInfo()).thenReturn(CLUSTER_DISCOVERY.getPath());
    when(request.queryParams(eq(ControllerApiConstants.NAME))).thenReturn(storeName);
    Response response = mock(Response.class);

    Route discoverCluster = ClusterDiscovery.discoverCluster(admin, requestHandler);
    D2ServiceDiscoveryResponse d2ServiceDiscoveryResponse = ObjectMapperFactory.getInstance()
        .readValue(discoverCluster.handle(request, response).toString(), D2ServiceDiscoveryResponse.class);
    assertNotNull(d2ServiceDiscoveryResponse, "Response should not be null");
    assertEquals(d2ServiceDiscoveryResponse.getName(), storeName, "Store name should match");
    assertEquals(d2ServiceDiscoveryResponse.getCluster(), clusterName, "Cluster name should match");
    assertEquals(d2ServiceDiscoveryResponse.getD2Service(), d2Service, "D2 service should match");
    assertEquals(d2ServiceDiscoveryResponse.getServerD2Service(), serverD2Service, "Server D2 service should match");

    // Case 2: Store name is not provided
    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    when(request.queryMap()).thenReturn(queryParamsMap);
    when(request.queryParams(eq(ControllerApiConstants.NAME))).thenReturn("");
    D2ServiceDiscoveryResponse d2ServiceDiscoveryResponse2 = ObjectMapperFactory.getInstance()
        .readValue(discoverCluster.handle(request, response).toString(), D2ServiceDiscoveryResponse.class);
    assertNotNull(d2ServiceDiscoveryResponse2, "Response should not be null");
    assertTrue(d2ServiceDiscoveryResponse2.isError(), "Error should be present in the response");
  }
}
