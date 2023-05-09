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
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Optional;
import org.apache.commons.httpclient.HttpStatus;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class CreateStoreTest {
  private static String clusterName = Utils.getUniqueString("test-cluster");

  @Test
  public void testCreateStoreWhenThrowsNPEInternally() throws Exception {
    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    String fakeMessage = "fake_message";

    doReturn(true).when(admin).isLeaderControllerFor(clusterName);
    // Throws NPE here
    doThrow(new NullPointerException(fakeMessage)).when(admin)
        .createStore(any(), any(), any(), any(), any(), anyBoolean(), any());

    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(NEW_STORE.getPath()).when(request).pathInfo();

    doReturn(clusterName).when(request).queryParams(CLUSTER);
    doReturn("test-store").when(request).queryParams(NAME);
    doReturn("fake-owner").when(request).queryParams(OWNER);
    doReturn("\"long\"").when(request).queryParams(KEY_SCHEMA);
    doReturn("\"string\"").when(request).queryParams(VALUE_SCHEMA);

    CreateStore createStoreRoute = new CreateStore(false, Optional.empty(), Optional.empty(), Optional.empty());
    Route createStoreRouter = createStoreRoute.createStore(admin);
    createStoreRouter.handle(request, response);
    verify(response).status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @Test(expectedExceptions = Error.class)
  public void testCreateStoreWhenThrowsError() throws Exception {
    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    String fakeMessage = "fake_message";

    doReturn(true).when(admin).isLeaderControllerFor(clusterName);
    // Throws NPE here
    doThrow(new Error(fakeMessage)).when(admin).createStore(any(), any(), any(), any(), any(), anyBoolean(), any());

    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(NEW_STORE.getPath()).when(request).pathInfo();

    doReturn(clusterName).when(request).queryParams(CLUSTER);
    doReturn("test-store").when(request).queryParams(NAME);
    doReturn("fake-owner").when(request).queryParams(OWNER);
    doReturn("\"long\"").when(request).queryParams(KEY_SCHEMA);
    doReturn("\"string\"").when(request).queryParams(VALUE_SCHEMA);

    CreateStore createStoreRoute = new CreateStore(false, Optional.empty(), Optional.empty(), Optional.empty());
    Route createStoreRouter = createStoreRoute.createStore(admin);
    createStoreRouter.handle(request, response);
  }

  @Test
  public void testCreateStoreWhenSomeParamNotPresent() throws Exception {
    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(NEW_STORE.getPath()).when(request).pathInfo();

    doReturn(clusterName).when(request).queryParams(CLUSTER);

    CreateStore createStoreRoute = new CreateStore(false, Optional.empty(), Optional.empty(), Optional.empty());
    Route createStoreRouter = createStoreRoute.createStore(admin);
    createStoreRouter.handle(request, response);
    verify(response).status(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void testCreateStoreWhenNotLeaderController() throws Exception {
    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    doReturn(false).when(admin).isLeaderControllerFor(clusterName);

    QueryParamsMap paramsMap = mock(QueryParamsMap.class);
    doReturn(new HashMap<>()).when(paramsMap).toMap();
    doReturn(paramsMap).when(request).queryMap();
    doReturn(NEW_STORE.getPath()).when(request).pathInfo();

    doReturn(clusterName).when(request).queryParams(CLUSTER);
    doReturn("test-store").when(request).queryParams(NAME);
    doReturn("fake-owner").when(request).queryParams(OWNER);
    doReturn("\"long\"").when(request).queryParams(KEY_SCHEMA);
    doReturn("\"string\"").when(request).queryParams(VALUE_SCHEMA);

    CreateStore createStoreRoute = new CreateStore(false, Optional.empty(), Optional.empty(), Optional.empty());
    Route createStoreRouter = createStoreRoute.createStore(admin);
    createStoreRouter.handle(request, response);
    verify(response).status(HttpConstants.SC_MISDIRECTED_REQUEST);
  }
}
