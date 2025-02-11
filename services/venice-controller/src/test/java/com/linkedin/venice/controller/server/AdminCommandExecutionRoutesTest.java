package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXECUTION_ID;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.AdminCommandExecutionStatus;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcRequest;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.HashMap;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class AdminCommandExecutionRoutesTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String TEST_CLUSTER = "test-cluster";
  private static final long TEST_EXECUTION_ID = 12345L;

  private Admin mockAdmin;
  private ClusterAdminOpsRequestHandler requestHandler;
  private Request request;
  private Response response;

  @BeforeMethod
  public void setUp() {
    request = mock(Request.class);
    response = mock(Response.class);
    mockAdmin = mock(Admin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);
    requestHandler = mock(ClusterAdminOpsRequestHandler.class);
  }

  @Test
  public void testGetExecutionSuccess() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);
    when(request.queryParams(EXECUTION_ID)).thenReturn(String.valueOf(TEST_EXECUTION_ID));

    AdminCommandExecutionStatusGrpcResponse grpcResponse = AdminCommandExecutionStatusGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setAdminCommandExecutionId(TEST_EXECUTION_ID)
        .setOperation("test-operation")
        .putFabricToExecutionStatusMap("fabric1", AdminCommandExecutionStatus.COMPLETED.name())
        .putFabricToExecutionStatusMap("fabric2", AdminCommandExecutionStatus.PROCESSING.name())
        .setStartTime(String.valueOf(123456789L))
        .build();

    when(requestHandler.getAdminCommandExecutionStatus(any(AdminCommandExecutionStatusGrpcRequest.class)))
        .thenReturn(grpcResponse);

    Route route = new AdminCommandExecutionRoutes(false, Optional.empty()).getExecution(mockAdmin, requestHandler);
    AdminCommandExecutionResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminCommandExecutionResponse.class);

    verify(requestHandler, times(1)).getAdminCommandExecutionStatus(any(AdminCommandExecutionStatusGrpcRequest.class));
    assertEquals(responseObject.getCluster(), TEST_CLUSTER);
    assertEquals(responseObject.getExecution().getExecutionId(), TEST_EXECUTION_ID);
    assertEquals(responseObject.getExecution().getOperation(), "test-operation");
    assertEquals(
        responseObject.getExecution().getFabricToExecutionStatusMap().get("fabric1"),
        AdminCommandExecutionStatus.COMPLETED);
  }

  @Test
  public void testGetExecutionMissingParameters() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(null); // Missing cluster
    when(request.queryParams(EXECUTION_ID)).thenReturn(String.valueOf(TEST_EXECUTION_ID));

    Route route = new AdminCommandExecutionRoutes(false, Optional.empty()).getExecution(mockAdmin, requestHandler);
    AdminCommandExecutionResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminCommandExecutionResponse.class);

    verify(requestHandler, never()).getAdminCommandExecutionStatus(any());
    assertNotNull(responseObject.getError());
    assertTrue(responseObject.getError().contains("cluster_name is a required parameter"));
  }

  @Test
  public void testGetExecutionHandlesException() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);
    when(request.queryParams(EXECUTION_ID)).thenReturn(String.valueOf(TEST_EXECUTION_ID));

    when(requestHandler.getAdminCommandExecutionStatus(any(AdminCommandExecutionStatusGrpcRequest.class)))
        .thenThrow(new RuntimeException("Internal error"));

    Route route = new AdminCommandExecutionRoutes(false, Optional.empty()).getExecution(mockAdmin, requestHandler);
    AdminCommandExecutionResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminCommandExecutionResponse.class);

    verify(requestHandler, times(1)).getAdminCommandExecutionStatus(any(AdminCommandExecutionStatusGrpcRequest.class));
    assertNotNull(responseObject.getError());
    assertTrue(responseObject.getError().contains("Internal error"));
  }

  @Test
  public void testGetLastSucceedExecutionIdSuccess() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);

    LastSuccessfulAdminCommandExecutionGrpcResponse grpcResponse =
        LastSuccessfulAdminCommandExecutionGrpcResponse.newBuilder()
            .setLastSuccessfulAdminCommandExecutionId(TEST_EXECUTION_ID)
            .build();

    when(requestHandler.getLastSucceedExecutionId(any(LastSuccessfulAdminCommandExecutionGrpcRequest.class)))
        .thenReturn(grpcResponse);

    Route route =
        new AdminCommandExecutionRoutes(false, Optional.empty()).getLastSucceedExecutionId(mockAdmin, requestHandler);
    LastSucceedExecutionIdResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), LastSucceedExecutionIdResponse.class);

    verify(requestHandler, times(1))
        .getLastSucceedExecutionId(any(LastSuccessfulAdminCommandExecutionGrpcRequest.class));
    assertEquals(responseObject.getCluster(), TEST_CLUSTER);
    assertEquals(responseObject.getLastSucceedExecutionId(), TEST_EXECUTION_ID);
    assertNull(responseObject.getError());
  }

  @Test
  public void testGetLastSucceedExecutionIdHandlesException() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);

    when(requestHandler.getLastSucceedExecutionId(any(LastSuccessfulAdminCommandExecutionGrpcRequest.class)))
        .thenThrow(new RuntimeException("Internal error"));

    Route route =
        new AdminCommandExecutionRoutes(false, Optional.empty()).getLastSucceedExecutionId(mockAdmin, requestHandler);
    LastSucceedExecutionIdResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), LastSucceedExecutionIdResponse.class);

    verify(requestHandler, times(1))
        .getLastSucceedExecutionId(any(LastSuccessfulAdminCommandExecutionGrpcRequest.class));
    assertNotNull(responseObject.getError());
    assertTrue(responseObject.getError().contains("Internal error"));
  }
}
