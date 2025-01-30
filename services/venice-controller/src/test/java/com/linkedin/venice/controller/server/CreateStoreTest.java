package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACCESS_PERMISSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.NEW_STORE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Optional;
import org.apache.commons.httpclient.HttpStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class CreateStoreTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String CLUSTER_NAME = Utils.getUniqueString("test-cluster");
  private static final String STORE_NAME = Utils.getUniqueString("test-store");

  private Admin mockAdmin;
  private StoreRequestHandler requestHandler;
  private Request request;
  private Response response;

  @BeforeMethod
  public void setUp() {
    request = mock(Request.class);
    response = mock(Response.class);
    mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(CLUSTER_NAME);
    ControllerRequestHandlerDependencies dependencies = mock(ControllerRequestHandlerDependencies.class);
    doReturn(mockAdmin).when(dependencies).getAdmin();
    requestHandler = new StoreRequestHandler(dependencies);
  }

  @Test
  public void testCreateStoreWhenThrowsNPEInternally() throws Exception {
    String fakeMessage = "fake_message";

    // Throws NPE here
    doThrow(new NullPointerException(fakeMessage)).when(mockAdmin)
        .createStore(any(), any(), any(), any(), any(), anyBoolean(), any());

    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(NEW_STORE.getPath()).when(request).pathInfo();

    doReturn(CLUSTER_NAME).when(request).queryParams(CLUSTER);
    doReturn("test-store").when(request).queryParams(NAME);
    doReturn("fake-owner").when(request).queryParams(OWNER);
    doReturn("\"long\"").when(request).queryParams(KEY_SCHEMA);
    doReturn("\"string\"").when(request).queryParams(VALUE_SCHEMA);

    CreateStore createStoreRoute = new CreateStore(false, Optional.empty());
    Route createStoreRouter = createStoreRoute.createStore(mockAdmin, requestHandler);
    createStoreRouter.handle(request, response);
    verify(response).status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @Test(expectedExceptions = Error.class)
  public void testCreateStoreWhenThrowsError() throws Exception {
    String fakeMessage = "fake_message";

    // Throws NPE here
    doThrow(new Error(fakeMessage)).when(mockAdmin).createStore(any(), any(), any(), any(), any(), anyBoolean(), any());

    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(NEW_STORE.getPath()).when(request).pathInfo();

    doReturn(CLUSTER_NAME).when(request).queryParams(CLUSTER);
    doReturn("test-store").when(request).queryParams(NAME);
    doReturn("fake-owner").when(request).queryParams(OWNER);
    doReturn("\"long\"").when(request).queryParams(KEY_SCHEMA);
    doReturn("\"string\"").when(request).queryParams(VALUE_SCHEMA);

    CreateStore createStoreRoute = new CreateStore(false, Optional.empty());
    Route createStoreRouter = createStoreRoute.createStore(mockAdmin, requestHandler);
    createStoreRouter.handle(request, response);
  }

  @Test
  public void testCreateStoreWhenSomeParamNotPresent() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(NEW_STORE.getPath()).when(request).pathInfo();

    doReturn(CLUSTER_NAME).when(request).queryParams(CLUSTER);

    CreateStore createStoreRoute = new CreateStore(false, Optional.empty());
    Route createStoreRouter = createStoreRoute.createStore(mockAdmin, requestHandler);
    createStoreRouter.handle(request, response);
    verify(response).status(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void testCreateStoreWhenNotLeaderController() throws Exception {
    doReturn(false).when(mockAdmin).isLeaderControllerFor(CLUSTER_NAME);

    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(NEW_STORE.getPath()).when(request).pathInfo();

    doReturn(CLUSTER_NAME).when(request).queryParams(CLUSTER);
    doReturn("test-store").when(request).queryParams(NAME);
    doReturn("fake-owner").when(request).queryParams(OWNER);
    doReturn("\"long\"").when(request).queryParams(KEY_SCHEMA);
    doReturn("\"string\"").when(request).queryParams(VALUE_SCHEMA);

    CreateStore createStoreRoute = new CreateStore(false, Optional.empty());
    Route createStoreRouter = createStoreRoute.createStore(mockAdmin, requestHandler);
    createStoreRouter.handle(request, response);
    verify(response).status(HttpConstants.SC_MISDIRECTED_REQUEST);
  }

  @Test
  public void testUpdateAclForStoreSuccess() throws Exception {
    String accessPermissions = "read,write";

    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(CLUSTER_NAME);
    when(request.queryParams(NAME)).thenReturn(STORE_NAME);
    when(request.queryParams(ACCESS_PERMISSION)).thenReturn(accessPermissions);

    doNothing().when(mockAdmin).updateAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME), eq(accessPermissions));
    Route route = new CreateStore(false, Optional.empty()).updateAclForStore(mockAdmin, requestHandler);
    AclResponse aclResponse = OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AclResponse.class);

    verify(mockAdmin, times(1)).updateAclForStore(CLUSTER_NAME, STORE_NAME, accessPermissions);
    assertEquals(aclResponse.getCluster(), CLUSTER_NAME);
    assertEquals(aclResponse.getName(), STORE_NAME);
  }

  @Test
  public void testUpdateAclForStoreMissingParameters() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(CLUSTER_NAME);
    when(request.queryParams(NAME)).thenReturn(STORE_NAME);
    when(request.queryParams(ACCESS_PERMISSION)).thenReturn(null);

    doNothing().when(mockAdmin).updateAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME), any());
    Route route = new CreateStore(false, Optional.empty()).updateAclForStore(mockAdmin, requestHandler);

    ControllerResponse controllerResponse =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AclResponse.class);

    verify(mockAdmin, never()).updateAclForStore(anyString(), anyString(), anyString());
    verify(response).status(HttpStatus.SC_BAD_REQUEST);
    assertTrue(
        controllerResponse.getError().contains("access_permission is a required parameter"),
        "Actual:" + controllerResponse.getError());
  }

  @Test
  public void testGetAclForStoreSuccess() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    when(request.queryParams(CLUSTER)).thenReturn(CLUSTER_NAME);
    when(request.queryParams(NAME)).thenReturn(STORE_NAME);
    when(mockAdmin.getAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME))).thenReturn("read,write");
    Route route = new CreateStore(false, Optional.empty()).getAclForStore(mockAdmin, requestHandler);
    AclResponse aclResponse = OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AclResponse.class);

    verify(mockAdmin, times(1)).getAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME));
    verify(response, never()).status(HttpStatus.SC_BAD_REQUEST);

    assertEquals(aclResponse.getCluster(), CLUSTER_NAME);
    assertEquals(aclResponse.getName(), STORE_NAME);
    assertEquals(aclResponse.getAccessPermissions(), "read,write");
    assertNull(aclResponse.getError());
  }

  @Test
  public void testGetAclForStoreMissingParameters() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(null); // Missing cluster parameter
    when(request.queryParams(NAME)).thenReturn(STORE_NAME);

    Route route = new CreateStore(false, Optional.empty()).getAclForStore(mockAdmin, requestHandler);
    ControllerResponse controllerResponse =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AclResponse.class);

    verify(mockAdmin, never()).getAclForStore(anyString(), anyString());
    verify(response).status(HttpStatus.SC_BAD_REQUEST);

    assertTrue(
        controllerResponse.getError().contains("cluster_name is a required parameter"),
        "Actual:" + controllerResponse.getError());
  }

  @Test
  public void testGetAclForStoreHandlesException() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(CLUSTER_NAME);
    when(request.queryParams(NAME)).thenReturn(STORE_NAME);

    // Simulate an exception in request handler
    doThrow(new RuntimeException("Internal error")).when(mockAdmin).getAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME));

    Route route = new CreateStore(false, Optional.empty()).getAclForStore(mockAdmin, requestHandler);
    AclResponse aclResponse = OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AclResponse.class);

    verify(mockAdmin, times(1)).getAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME));
    assertNotNull(aclResponse.getError());
    assertTrue(aclResponse.getError().contains("Internal error"), "Actual:" + aclResponse.getError());
  }

  @Test
  public void testDeleteAclForStoreSuccess() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(CLUSTER_NAME);
    when(request.queryParams(NAME)).thenReturn(STORE_NAME);

    doNothing().when(mockAdmin).deleteAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME));

    Route route = new CreateStore(false, Optional.empty()).deleteAclForStore(mockAdmin, requestHandler);
    AclResponse aclResponse = OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AclResponse.class);

    verify(mockAdmin, times(1)).deleteAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME));
    verify(response, never()).status(HttpStatus.SC_BAD_REQUEST);

    assertEquals(aclResponse.getCluster(), CLUSTER_NAME);
    assertEquals(aclResponse.getName(), STORE_NAME);
    assertNull(aclResponse.getError());
  }

  @Test
  public void testDeleteAclForStoreMissingParameters() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();

    when(request.queryParams(CLUSTER)).thenReturn(CLUSTER_NAME);
    when(request.queryParams(NAME)).thenReturn(null); // Missing store name

    Route route = new CreateStore(false, Optional.empty()).deleteAclForStore(mockAdmin, requestHandler);
    ControllerResponse controllerResponse =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AclResponse.class);

    verify(mockAdmin, never()).deleteAclForStore(anyString(), anyString());
    verify(response).status(HttpStatus.SC_BAD_REQUEST);

    assertTrue(
        controllerResponse.getError().contains("name is a required parameter"),
        "Actual:" + controllerResponse.getError());
  }

  @Test
  public void testDeleteAclForStoreHandlesException() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    when(request.queryParams(CLUSTER)).thenReturn(CLUSTER_NAME);
    when(request.queryParams(NAME)).thenReturn(STORE_NAME);

    doThrow(new RuntimeException("Internal error")).when(mockAdmin).deleteAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME));

    Route route = new CreateStore(false, Optional.empty()).deleteAclForStore(mockAdmin, requestHandler);
    AclResponse aclResponse = OBJECT_MAPPER.readValue(route.handle(request, response).toString(), AclResponse.class);

    verify(mockAdmin, times(1)).deleteAclForStore(eq(CLUSTER_NAME), eq(STORE_NAME));
    assertNotNull(aclResponse.getError());
    assertTrue(aclResponse.getError().contains("Internal error"), "Actual:" + aclResponse.getError());
  }

  @Test
  public void testCheckResourceCleanupForStoreCreation() throws Exception {
    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    when(request.queryParams(CLUSTER)).thenReturn(CLUSTER_NAME);
    when(request.queryParams(NAME)).thenReturn(STORE_NAME);

    doNothing().when(mockAdmin).checkResourceCleanupBeforeStoreCreation(eq(CLUSTER_NAME), eq(STORE_NAME));

    Route route =
        new CreateStore(false, Optional.empty()).checkResourceCleanupForStoreCreation(mockAdmin, requestHandler);
    ControllerResponse controllerResponse =
        OBJECT_MAPPER.readValue(route.handle(request, response).toString(), ControllerResponse.class);

    assertEquals(controllerResponse.getCluster(), CLUSTER_NAME);
    assertEquals(controllerResponse.getName(), STORE_NAME);
    assertFalse(controllerResponse.isError());
    verify(mockAdmin, times(1)).checkResourceCleanupBeforeStoreCreation(eq(CLUSTER_NAME), eq(STORE_NAME));
    verify(response, never()).status(HttpStatus.SC_BAD_REQUEST);

    // Test when there are lingering resources
    doThrow(new RuntimeException("Lingering resources found")).when(mockAdmin)
        .checkResourceCleanupBeforeStoreCreation(eq(CLUSTER_NAME), eq(STORE_NAME));
    controllerResponse = OBJECT_MAPPER.readValue(route.handle(request, response).toString(), ControllerResponse.class);
    assertTrue(controllerResponse.isError());
    assertTrue(controllerResponse.getError().contains("Lingering resources found"));
  }
}
