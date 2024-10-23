package com.linkedin.venice.controller.server;

import static com.linkedin.venice.exceptions.ErrorType.INCORRECT_CONTROLLER;
import static com.linkedin.venice.exceptions.ErrorType.STORE_NOT_FOUND;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class StoreRoutesTest {
  private static final String TEST_CLUSTER = "test_cluster";
  private static final String TEST_STORE_NAME = "test_store";

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

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
  public void testGetFutureVersion() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockAdmin).getStore(TEST_CLUSTER, TEST_STORE_NAME);

    Map<String, String> storeStatusMap = Collections.singletonMap("dc-0", "1");
    doReturn(storeStatusMap).when(mockAdmin).getFutureVersionsForMultiColos(TEST_CLUSTER, TEST_STORE_NAME);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.NAME));

    Route getFutureVersionRoute =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler).getFutureVersion(mockAdmin);
    MultiStoreStatusResponse multiStoreStatusResponse = ObjectMapperFactory.getInstance()
        .readValue(
            getFutureVersionRoute.handle(request, mock(Response.class)).toString(),
            MultiStoreStatusResponse.class);
    Assert.assertEquals(multiStoreStatusResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(multiStoreStatusResponse.getStoreStatusMap(), storeStatusMap);
  }

  @Test
  public void testRollForwardToFutureVersion() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockAdmin).getStore(TEST_CLUSTER, TEST_STORE_NAME);

    Map<String, String> storeStatusMap = Collections.singletonMap("dc-0", "1");
    doReturn(storeStatusMap).when(mockAdmin).getFutureVersionsForMultiColos(TEST_CLUSTER, TEST_STORE_NAME);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.NAME));

    Route rollForwardToFutureVersion = new StoresRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler)
        .rollForwardToFutureVersion(mockAdmin);

    MultiStoreStatusResponse multiStoreStatusResponse = ObjectMapperFactory.getInstance()
        .readValue(
            rollForwardToFutureVersion.handle(request, mock(Response.class)).toString(),
            MultiStoreStatusResponse.class);
    Assert.assertEquals(multiStoreStatusResponse.getCluster(), TEST_CLUSTER);
  }

  @Test
  public void testGetFutureVersionForChildController() throws Exception {
    Admin mockAdmin = mock(VeniceHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockAdmin).getStore(TEST_CLUSTER, TEST_STORE_NAME);

    doCallRealMethod().when(mockAdmin).getFutureVersionsForMultiColos(TEST_CLUSTER, TEST_STORE_NAME);
    doReturn(1).when(mockAdmin).getFutureVersion(TEST_CLUSTER, TEST_STORE_NAME);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.NAME));

    Route getFutureVersionRoute =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler).getFutureVersion(mockAdmin);
    MultiStoreStatusResponse multiStoreStatusResponse = ObjectMapperFactory.getInstance()
        .readValue(
            getFutureVersionRoute.handle(request, mock(Response.class)).toString(),
            MultiStoreStatusResponse.class);
    Assert.assertEquals(multiStoreStatusResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(multiStoreStatusResponse.getStoreStatusMap(), Collections.singletonMap(TEST_STORE_NAME, "1"));

    doCallRealMethod().when(mockAdmin).getBackupVersionsForMultiColos(TEST_CLUSTER, TEST_STORE_NAME);
    doReturn(2).when(mockAdmin).getBackupVersion(TEST_CLUSTER, TEST_STORE_NAME);
    Route getBackupVersionRoute =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler).getBackupVersion(mockAdmin);
    multiStoreStatusResponse = ObjectMapperFactory.getInstance()
        .readValue(
            getBackupVersionRoute.handle(request, mock(Response.class)).toString(),
            MultiStoreStatusResponse.class);
    Assert.assertEquals(multiStoreStatusResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(multiStoreStatusResponse.getStoreStatusMap(), Collections.singletonMap(TEST_STORE_NAME, "2"));
  }

  @Test
  public void testGetFutureVersionWhenNotLeaderController() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(false).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockAdmin).getStore(TEST_CLUSTER, TEST_STORE_NAME);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.NAME));

    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);

    Map<String, String[]> queryMap = new HashMap<>(2);
    queryMap.put(ControllerApiConstants.CLUSTER, new String[] { TEST_CLUSTER });
    queryMap.put(ControllerApiConstants.NAME, new String[] { TEST_STORE_NAME });

    doReturn(queryMap).when(queryParamsMap).toMap();
    doReturn(queryParamsMap).when(request).queryMap();

    Route getFutureVersionRoute =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler).getFutureVersion(mockAdmin);
    MultiStoreStatusResponse multiStoreStatusResponse = ObjectMapperFactory.getInstance()
        .readValue(
            getFutureVersionRoute.handle(request, mock(Response.class)).toString(),
            MultiStoreStatusResponse.class);
    Assert.assertTrue(multiStoreStatusResponse.isError());
    Assert.assertEquals(multiStoreStatusResponse.getErrorType(), INCORRECT_CONTROLLER);
  }

  @Test
  public void testGetFutureVersionWhenStoreNotExist() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    doReturn(null).when(mockAdmin).getStore(TEST_CLUSTER, TEST_STORE_NAME);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.NAME));

    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);

    Map<String, String[]> queryMap = new HashMap<>(2);
    queryMap.put(ControllerApiConstants.CLUSTER, new String[] { TEST_CLUSTER });
    queryMap.put(ControllerApiConstants.NAME, new String[] { TEST_STORE_NAME });

    doReturn(queryMap).when(queryParamsMap).toMap();
    doReturn(queryParamsMap).when(request).queryMap();

    Route getFutureVersionRoute =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler).getFutureVersion(mockAdmin);
    MultiStoreStatusResponse multiStoreStatusResponse = ObjectMapperFactory.getInstance()
        .readValue(
            getFutureVersionRoute.handle(request, mock(Response.class)).toString(),
            MultiStoreStatusResponse.class);
    Assert.assertTrue(multiStoreStatusResponse.isError());
    Assert.assertEquals(multiStoreStatusResponse.getErrorType(), STORE_NOT_FOUND);
  }

  /** Testing getStore API code paths for code coverage purposes */
  @Test
  public void testGetStore() throws Exception {
    final Store testStore = new ZKStore(
        TEST_STORE_NAME,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);

    final int testMaxRecordSizeBytesValue = 33333;
    final Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);
    doReturn(testStore).when(mockAdmin).getStore(TEST_CLUSTER, TEST_STORE_NAME);
    doReturn(testMaxRecordSizeBytesValue).when(mockAdmin).getDefaultMaxRecordSizeBytes();

    final Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.NAME));

    final StoresRoutes storesRoutes = new StoresRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler);
    final StoreResponse response = ObjectMapperFactory.getInstance()
        .readValue(
            storesRoutes.getStore(mockAdmin).handle(request, mock(Response.class)).toString(),
            StoreResponse.class);
    Assert.assertEquals(response.getStore().getMaxRecordSizeBytes(), testMaxRecordSizeBytesValue);
  }
}
