package com.linkedin.venice.controller.server;

import static com.linkedin.venice.VeniceConstants.CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ADMIN_OPERATION_PROTOCOL_VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXECUTION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_NAME;
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
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.protocols.controller.AdminTopicGrpcMetadata;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAdminOperationProtocolVersionGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAdminTopicMetadataGrpcRequest;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Optional;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class AdminTopicMetadataRoutesTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";
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
  public void testGetAdminTopicMetadataSuccess() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);
    when(request.queryParams(STORE_NAME)).thenReturn(null);

    AdminTopicMetadataGrpcResponse grpcResponse = AdminTopicMetadataGrpcResponse.newBuilder()
        .setMetadata(
            AdminTopicGrpcMetadata.newBuilder()
                .setClusterName(TEST_CLUSTER)
                .setExecutionId(TEST_EXECUTION_ID)
                .setOffset(100)
                .setUpstreamOffset(200))
        .build();

    when(requestHandler.getAdminTopicMetadata(any(AdminTopicMetadataGrpcRequest.class))).thenReturn(grpcResponse);

    Route route =
        new AdminTopicMetadataRoutes(false, Optional.empty()).getAdminTopicMetadata(mockAdmin, requestHandler);
    AdminTopicMetadataResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminTopicMetadataResponse.class);

    verify(requestHandler, times(1)).getAdminTopicMetadata(any(AdminTopicMetadataGrpcRequest.class));
    assertEquals(responseObject.getCluster(), TEST_CLUSTER);
    assertEquals(responseObject.getExecutionId(), TEST_EXECUTION_ID);
    assertEquals(responseObject.getOffset(), 100L);
    assertEquals(responseObject.getUpstreamOffset(), 200L);
    assertNull(responseObject.getError());

    // non-null store name
    when(request.queryParams(STORE_NAME)).thenReturn(TEST_STORE);
    grpcResponse = AdminTopicMetadataGrpcResponse.newBuilder()
        .setMetadata(
            AdminTopicGrpcMetadata.newBuilder()
                .setClusterName(TEST_CLUSTER)
                .setStoreName(TEST_STORE)
                .setExecutionId(TEST_EXECUTION_ID)
                .setOffset(100)
                .setUpstreamOffset(200))
        .build();

    when(requestHandler.getAdminTopicMetadata(any(AdminTopicMetadataGrpcRequest.class))).thenReturn(grpcResponse);

    responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminTopicMetadataResponse.class);

    verify(requestHandler, times(2)).getAdminTopicMetadata(any(AdminTopicMetadataGrpcRequest.class));
    assertEquals(responseObject.getCluster(), TEST_CLUSTER);
    assertEquals(responseObject.getName(), TEST_STORE);
    assertEquals(responseObject.getExecutionId(), TEST_EXECUTION_ID);
    assertEquals(responseObject.getOffset(), -1L);
    assertEquals(responseObject.getUpstreamOffset(), -1L);
    assertNull(responseObject.getError());
  }

  @Test
  public void testGetAdminTopicMetadataHandlesMissingParams() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(null); // Missing cluster parameter

    Route route =
        new AdminTopicMetadataRoutes(false, Optional.empty()).getAdminTopicMetadata(mockAdmin, requestHandler);
    AdminTopicMetadataResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminTopicMetadataResponse.class);

    verify(requestHandler, never()).getAdminTopicMetadata(any());
    assertNotNull(responseObject.getError());
    assertTrue(responseObject.getError().contains("cluster_name is a required parameter"));
  }

  @Test
  public void testUpdateAdminTopicMetadataSuccess() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);
    when(request.queryParams(EXECUTION_ID)).thenReturn(String.valueOf(TEST_EXECUTION_ID));
    when(request.queryParams(STORE_NAME)).thenReturn(TEST_STORE);

    AdminTopicGrpcMetadata.Builder adminTopicGrpcMetadataBuilder = AdminTopicGrpcMetadata.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setStoreName(TEST_STORE)
        .setExecutionId(TEST_EXECUTION_ID);
    AdminTopicMetadataGrpcResponse grpcResponse =
        AdminTopicMetadataGrpcResponse.newBuilder().setMetadata(adminTopicGrpcMetadataBuilder.build()).build();

    when(requestHandler.updateAdminTopicMetadata(any(UpdateAdminTopicMetadataGrpcRequest.class)))
        .thenReturn(grpcResponse);

    Route route =
        new AdminTopicMetadataRoutes(false, Optional.empty()).updateAdminTopicMetadata(mockAdmin, requestHandler);
    AdminTopicMetadataResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminTopicMetadataResponse.class);

    verify(requestHandler, times(1)).updateAdminTopicMetadata(any(UpdateAdminTopicMetadataGrpcRequest.class));
    assertEquals(responseObject.getCluster(), TEST_CLUSTER);
    assertEquals(responseObject.getName(), TEST_STORE);
    assertNull(responseObject.getError());
  }

  @Test
  public void testUpdateAdminTopicMetadataHandlesUnauthorizedAccess() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    DynamicAccessController accessController = mock(DynamicAccessController.class);
    when(accessController.isAllowlistUsers(any(), any(), any())).thenReturn(false);
    HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    when(request.raw()).thenReturn(httpServletRequest);
    X509Certificate certificate = mock(X509Certificate.class);
    X500Principal principal = new X500Principal("CN=foo");
    X509Certificate[] certificates = new X509Certificate[] { mock(X509Certificate.class) };
    when(httpServletRequest.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME)).thenReturn(certificates);
    doReturn(principal).when(certificate).getSubjectX500Principal();
    doReturn(httpServletRequest).when(request).raw();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);
    when(request.queryParams(EXECUTION_ID)).thenReturn(String.valueOf(TEST_EXECUTION_ID));
    when(request.queryParams(STORE_NAME)).thenReturn(TEST_STORE);
    Route route = new AdminTopicMetadataRoutes(false, Optional.of(accessController))
        .updateAdminTopicMetadata(mockAdmin, requestHandler);
    AdminTopicMetadataResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminTopicMetadataResponse.class);

    verify(requestHandler, never()).updateAdminTopicMetadata(any(UpdateAdminTopicMetadataGrpcRequest.class));
    assertNotNull(responseObject.getError());
    assertTrue(responseObject.getError().contains("Only admin users are allowed"));
  }

  @Test
  public void testUpdateAdminTopicMetadataHandlesException() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);
    when(request.queryParams(EXECUTION_ID)).thenReturn(String.valueOf(TEST_EXECUTION_ID));
    when(request.queryParams(STORE_NAME)).thenReturn(TEST_STORE);

    when(requestHandler.updateAdminTopicMetadata(any(UpdateAdminTopicMetadataGrpcRequest.class)))
        .thenThrow(new RuntimeException("Internal error"));

    Route route =
        new AdminTopicMetadataRoutes(false, Optional.empty()).updateAdminTopicMetadata(mockAdmin, requestHandler);
    AdminTopicMetadataResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminTopicMetadataResponse.class);

    verify(requestHandler, times(1)).updateAdminTopicMetadata(any(UpdateAdminTopicMetadataGrpcRequest.class));
    assertNotNull(responseObject.getError());
    assertTrue(responseObject.getError().contains("Internal error"));
  }

  @Test
  public void testUpdateAdminOperationProtocolVersion() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    long adminOperationProtocolVersion = 1L;
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);
    when(request.queryParams(ADMIN_OPERATION_PROTOCOL_VERSION))
        .thenReturn(String.valueOf(adminOperationProtocolVersion));

    AdminTopicGrpcMetadata.Builder adminTopicGrpcMetadataBuilder = AdminTopicGrpcMetadata.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setAdminOperationProtocolVersion(adminOperationProtocolVersion);
    AdminTopicMetadataGrpcResponse grpcResponse =
        AdminTopicMetadataGrpcResponse.newBuilder().setMetadata(adminTopicGrpcMetadataBuilder.build()).build();

    when(requestHandler.updateAdminOperationProtocolVersion(any(UpdateAdminOperationProtocolVersionGrpcRequest.class)))
        .thenReturn(grpcResponse);

    Route route = new AdminTopicMetadataRoutes(false, Optional.empty())
        .updateAdminOperationProtocolVersion(mockAdmin, requestHandler);

    AdminTopicMetadataResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminTopicMetadataResponse.class);

    verify(requestHandler, times(1))
        .updateAdminOperationProtocolVersion(any(UpdateAdminOperationProtocolVersionGrpcRequest.class));
    assertEquals(responseObject.getCluster(), TEST_CLUSTER);
    assertEquals(responseObject.getAdminOperationProtocolVersion(), adminOperationProtocolVersion);
    assertNull(responseObject.getError());
  }

  @Test
  public void testUpdateAdminOperationProtocolVersionHandlesUnauthorizedAccess() throws Exception {
    DynamicAccessController accessController = mock(DynamicAccessController.class);
    when(accessController.isAllowlistUsers(any(), any(), any())).thenReturn(false);
    HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    when(request.raw()).thenReturn(httpServletRequest);
    X509Certificate certificate = mock(X509Certificate.class);
    X500Principal principal = new X500Principal("CN=foo");
    X509Certificate[] certificates = new X509Certificate[] { mock(X509Certificate.class) };
    when(httpServletRequest.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME)).thenReturn(certificates);
    doReturn(principal).when(certificate).getSubjectX500Principal();
    doReturn(httpServletRequest).when(request).raw();

    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    String adminOperationProtocolVersion = "1";
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(TEST_CLUSTER);
    when(request.queryParams(ADMIN_OPERATION_PROTOCOL_VERSION)).thenReturn(adminOperationProtocolVersion);

    Route route = new AdminTopicMetadataRoutes(false, Optional.of(accessController))
        .updateAdminOperationProtocolVersion(mockAdmin, requestHandler);

    AdminTopicMetadataResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminTopicMetadataResponse.class);

    verify(requestHandler, never()).updateAdminOperationProtocolVersion(any());
    assertNotNull(responseObject.getError());
    assertTrue(responseObject.getError().contains("Only admin users are allowed"));
  }

  @Test
  public void testUpdateAdminOperationProtocolVersionHandlesMissingParams() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(null); // Missing cluster parameter

    Route route = new AdminTopicMetadataRoutes(false, Optional.empty())
        .updateAdminOperationProtocolVersion(mockAdmin, requestHandler);
    AdminTopicMetadataResponse responseObject =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AdminTopicMetadataResponse.class);

    verify(requestHandler, never()).updateAdminOperationProtocolVersion(any());
    assertNotNull(responseObject.getError());
    assertTrue(responseObject.getError().contains("cluster_name is a required parameter"));
  }
}
