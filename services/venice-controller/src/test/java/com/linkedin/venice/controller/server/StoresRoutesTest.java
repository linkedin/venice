package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerRoute.LEADER_CONTROLLER;
import static com.linkedin.venice.exceptions.ErrorType.INCORRECT_CONTROLLER;
import static com.linkedin.venice.exceptions.ErrorType.INVALID_CONFIG;
import static com.linkedin.venice.exceptions.ErrorType.STORE_NOT_FOUND;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.RepushJobResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.SystemStoreHeartbeatResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
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
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class StoresRoutesTest {
  private static final String TEST_CLUSTER = "test_cluster";
  private static final String TEST_STORE_NAME = "test_store";

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

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
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).getFutureVersion(mockAdmin);
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

    Route rollForwardToFutureVersion =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).rollForwardToFutureVersion(mockAdmin);

    MultiStoreStatusResponse multiStoreStatusResponse = ObjectMapperFactory.getInstance()
        .readValue(
            rollForwardToFutureVersion.handle(request, mock(Response.class)).toString(),
            MultiStoreStatusResponse.class);
    Assert.assertEquals(multiStoreStatusResponse.getCluster(), TEST_CLUSTER);
  }

  @Test
  public void testDeleteStore() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.NAME));

    Route deleteStoreRoute = new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).deleteStore(mockAdmin);
    TrackableControllerResponse trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(
            deleteStoreRoute.handle(request, mock(Response.class)).toString(),
            TrackableControllerResponse.class);
    Assert.assertFalse(trackableControllerResponse.isError());
    Assert.assertEquals(trackableControllerResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(trackableControllerResponse.getName(), TEST_STORE_NAME);

    doReturn("true").when(request).queryParams(eq(ControllerApiConstants.IS_ABORT_MIGRATION_CLEANUP));
    String errMessage = "Store " + TEST_STORE_NAME + "'s migrating flag is false. Not safe to delete a store "
        + "that is assumed to be migrating without the migrating flag setup as true.";
    doThrow(new VeniceException(errMessage, INVALID_CONFIG)).when(mockAdmin)
        .deleteStore(TEST_CLUSTER, TEST_STORE_NAME, true, Store.IGNORE_VERSION, false);
    trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(
            deleteStoreRoute.handle(request, mock(Response.class)).toString(),
            TrackableControllerResponse.class);
    Assert.assertTrue(trackableControllerResponse.isError());
    Assert.assertEquals(trackableControllerResponse.getErrorType(), INVALID_CONFIG);
    Assert.assertEquals(trackableControllerResponse.getError(), errMessage);
    Assert.assertEquals(trackableControllerResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(trackableControllerResponse.getName(), TEST_STORE_NAME);
  }

  @Test
  public void testMigrateStore() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class, RETURNS_DEEP_STUBS);
    String DEST_CLUSTER = "dest_cluster";
    when(mockAdmin.discoverCluster(TEST_STORE_NAME).getFirst()).thenReturn(TEST_CLUSTER);
    Request request = mock(Request.class);
    doReturn(LEADER_CONTROLLER.getPath()).when(request).pathInfo();
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(DEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER_DEST));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.STORE_NAME));
    Route migrateStoreRoute = new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).migrateStore(mockAdmin);

    when(mockAdmin.getControllerConfig(TEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(true);
    when(mockAdmin.getControllerConfig(DEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(false);
    TrackableControllerResponse trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(
            migrateStoreRoute.handle(request, mock(Response.class)).toString(),
            TrackableControllerResponse.class);
    Assert.assertTrue(trackableControllerResponse.isError());

    when(mockAdmin.getControllerConfig(TEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(false);
    when(mockAdmin.getControllerConfig(DEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(true);
    trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(
            migrateStoreRoute.handle(request, mock(Response.class)).toString(),
            TrackableControllerResponse.class);
    Assert.assertTrue(trackableControllerResponse.isError());

    when(mockAdmin.getControllerConfig(TEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(true);
    when(mockAdmin.getControllerConfig(DEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(true);
    trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(
            migrateStoreRoute.handle(request, mock(Response.class)).toString(),
            TrackableControllerResponse.class);
    Assert.assertFalse(trackableControllerResponse.isError());

    when(mockAdmin.getControllerConfig(TEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(false);
    when(mockAdmin.getControllerConfig(DEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(false);
    trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(
            migrateStoreRoute.handle(request, mock(Response.class)).toString(),
            TrackableControllerResponse.class);
    Assert.assertFalse(trackableControllerResponse.isError());
  }

  @Test
  public void testCleanExecutionIds() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class, RETURNS_DEEP_STUBS);
    String DEST_CLUSTER = "dest_cluster";
    when(mockAdmin.discoverCluster(TEST_STORE_NAME).getFirst()).thenReturn(TEST_CLUSTER);
    Request request = mock(Request.class);
    doReturn(LEADER_CONTROLLER.getPath()).when(request).pathInfo();
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.STORE_NAME));
    Route cleanExecutionIdRoute =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).cleanExecutionIds(mockAdmin);

    TrackableControllerResponse trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(
            cleanExecutionIdRoute.handle(request, mock(Response.class)).toString(),
            TrackableControllerResponse.class);
    Assert.assertFalse(trackableControllerResponse.isError());
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
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).getFutureVersion(mockAdmin);
    MultiStoreStatusResponse multiStoreStatusResponse = ObjectMapperFactory.getInstance()
        .readValue(
            getFutureVersionRoute.handle(request, mock(Response.class)).toString(),
            MultiStoreStatusResponse.class);
    Assert.assertEquals(multiStoreStatusResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(multiStoreStatusResponse.getStoreStatusMap(), Collections.singletonMap(TEST_STORE_NAME, "1"));

    doCallRealMethod().when(mockAdmin).getBackupVersionsForMultiColos(TEST_CLUSTER, TEST_STORE_NAME);
    doReturn(2).when(mockAdmin).getBackupVersion(TEST_CLUSTER, TEST_STORE_NAME);
    Route getBackupVersionRoute =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).getBackupVersion(mockAdmin);
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
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).getFutureVersion(mockAdmin);
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
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).getFutureVersion(mockAdmin);
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

    final StoresRoutes storesRoutes = new StoresRoutes(false, Optional.empty(), pubSubTopicRepository);
    final StoreResponse response = ObjectMapperFactory.getInstance()
        .readValue(
            storesRoutes.getStore(mockAdmin).handle(request, mock(Response.class)).toString(),
            StoreResponse.class);
    Assert.assertEquals(response.getStore().getMaxRecordSizeBytes(), testMaxRecordSizeBytesValue);
  }

  @Test
  public void testRepushStoreWithErrorResponse() throws Exception {
    Admin mockAdmin = mock(Admin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Route repushStoreRoute = new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).repushStore(mockAdmin);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.NAME));

    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    doReturn(queryParamsMap).when(request).queryMap();

    Map<String, String[]> queryMap = new HashMap<>(1);
    queryMap.put(ControllerApiConstants.NAME, new String[] { TEST_STORE_NAME });
    doReturn(queryMap).when(queryParamsMap).toMap();

    when(mockAdmin.repushStore(any())).thenThrow(mock(Exception.class));

    RepushJobResponse repushJobResponse = ObjectMapperFactory.getInstance()
        .readValue(repushStoreRoute.handle(request, mock(Response.class)).toString(), RepushJobResponse.class);
    Assert.assertTrue(repushJobResponse.isError());
  }

  @Test
  public void testAutoMigrateStore() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class, RETURNS_DEEP_STUBS);
    String DEST_CLUSTER = "dest_cluster";
    when(mockAdmin.discoverCluster(TEST_STORE_NAME).getFirst()).thenReturn(TEST_CLUSTER);
    Request request = mock(Request.class);
    doReturn(LEADER_CONTROLLER.getPath()).when(request).pathInfo();
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(DEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER_DEST));
    doReturn(TEST_STORE_NAME).when(request).queryParams(eq(ControllerApiConstants.STORE_NAME));
    doReturn(null).when(request).queryParams(eq(ControllerApiConstants.AUTO_STORE_MIGRATION_CURRENT_STEP));
    doReturn(null).when(request).queryParams(eq(ControllerApiConstants.AUTO_STORE_MIGRATION_ABORT_ON_FAILURE));
    Route migrateStoreRoute =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).autoMigrateStore(mockAdmin);

    // Happy path
    when(mockAdmin.getControllerConfig(TEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(false);
    when(mockAdmin.getControllerConfig(DEST_CLUSTER).isRealTimeTopicVersioningEnabled()).thenReturn(false);
    StoreMigrationResponse trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(migrateStoreRoute.handle(request, mock(Response.class)).toString(), StoreMigrationResponse.class);
    Assert.assertFalse(trackableControllerResponse.isError());
    Assert.assertNull(trackableControllerResponse.getError());
    Assert.assertEquals(trackableControllerResponse.getSrcClusterName(), TEST_CLUSTER);
    Assert.assertEquals(trackableControllerResponse.getCluster(), DEST_CLUSTER);
    Assert.assertEquals(trackableControllerResponse.getName(), TEST_STORE_NAME);

    // Bad Request Path:
    // 1. Store belongs to destination cluster
    when(mockAdmin.discoverCluster(TEST_STORE_NAME).getFirst()).thenReturn(DEST_CLUSTER);

    trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(migrateStoreRoute.handle(request, mock(Response.class)).toString(), StoreMigrationResponse.class);
    Assert.assertTrue(trackableControllerResponse.isError());
    Assert.assertEquals(trackableControllerResponse.getErrorType(), ErrorType.BAD_REQUEST);
    Assert.assertEquals(
        trackableControllerResponse.getError(),
        String.format("Store %s already belongs to cluster %s.", TEST_STORE_NAME, DEST_CLUSTER));

    // 2. Store doesn't belong to the source cluster
    String EXTRA_CLUSTER = "extra_cluster";
    when(mockAdmin.discoverCluster(TEST_STORE_NAME).getFirst()).thenReturn(EXTRA_CLUSTER);
    trackableControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(migrateStoreRoute.handle(request, mock(Response.class)).toString(), StoreMigrationResponse.class);
    Assert.assertTrue(trackableControllerResponse.isError());
    Assert.assertEquals(trackableControllerResponse.getErrorType(), ErrorType.BAD_REQUEST);
    Assert.assertEquals(
        trackableControllerResponse.getError(),
        String.format(
            "Store %s belongs to cluster %s, which is different from the given src cluster name %s.",
            TEST_STORE_NAME,
            EXTRA_CLUSTER,
            TEST_CLUSTER));
  }

  @Test
  public void testGetHeartbeatFromSystemStore() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);
    String systemStore = VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(TEST_STORE_NAME);

    doReturn(100L).when(mockAdmin).getHeartbeatFromSystemStore(TEST_CLUSTER, systemStore);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(systemStore).when(request).queryParams(eq(ControllerApiConstants.NAME));

    Route getSystemStoreHeartbeatRoute =
        new StoresRoutes(false, Optional.empty(), pubSubTopicRepository).getHeartbeatFromSystemStore(mockAdmin);
    SystemStoreHeartbeatResponse multiStoreStatusResponse = ObjectMapperFactory.getInstance()
        .readValue(
            getSystemStoreHeartbeatRoute.handle(request, mock(Response.class)).toString(),
            SystemStoreHeartbeatResponse.class);
    Assert.assertEquals(multiStoreStatusResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(multiStoreStatusResponse.getHeartbeatTimestamp(), 100L);
  }
}
