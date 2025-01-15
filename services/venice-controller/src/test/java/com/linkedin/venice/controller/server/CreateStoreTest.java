package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.NEW_STORE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
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
  private static final String CLUSTER_NAME = Utils.getUniqueString("test-cluster");

  private VeniceControllerRequestHandler requestHandler;
  private Admin mockAdmin;

  @BeforeMethod
  public void setUp() {
    mockAdmin = mock(VeniceParentHelixAdmin.class);
    ControllerRequestHandlerDependencies dependencies = mock(ControllerRequestHandlerDependencies.class);
    doReturn(mockAdmin).when(dependencies).getAdmin();
    requestHandler = new VeniceControllerRequestHandler(dependencies);
  }

  @Test
  public void testCreateStoreWhenThrowsNPEInternally() throws Exception {
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    String fakeMessage = "fake_message";

    doReturn(true).when(mockAdmin).isLeaderControllerFor(CLUSTER_NAME);
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
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    String fakeMessage = "fake_message";

    doReturn(true).when(mockAdmin).isLeaderControllerFor(CLUSTER_NAME);
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
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    doReturn(true).when(mockAdmin).isLeaderControllerFor(CLUSTER_NAME);

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
    Request request = mock(Request.class);
    Response response = mock(Response.class);

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
}
