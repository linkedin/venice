package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SystemStoreHeartbeatResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SystemStoreMultiColoTest {
  private static final int TEST_TIMEOUT_MS = 90_000;
  private static final int NUMBER_OF_SERVERS = 2;
  private static final int REPLICATION_FACTOR = 2;
  private VeniceClusterWrapper cluster;
  private ControllerClient parentControllerClient;
  private D2Client d2Client;
  private PushStatusStoreReader reader;
  private VeniceControllerWrapper parentController;

  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int NUMBER_OF_PARENT_CONTROLLERS = 1;
  private static final int NUMBER_OF_CHILD_CONTROLLERS = 1;
  private static final int NUMBER_OF_ROUTERS = 1;
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties extraProperties = new Properties();
    // all tests in this class will be reading incremental push status from push status store.
    extraProperties.setProperty(USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH, String.valueOf(true));

    // Enable auto materialize for meta and da-vinci push status system stores.
    extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(true));
    extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, String.valueOf(true));

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(NUMBER_OF_PARENT_CONTROLLERS)
            .numberOfChildControllers(NUMBER_OF_CHILD_CONTROLLERS)
            .numberOfServers(NUMBER_OF_SERVERS)
            .numberOfRouters(NUMBER_OF_ROUTERS)
            .replicationFactor(REPLICATION_FACTOR)
            .forkServer(false)
            .parentControllerProperties(extraProperties)
            .childControllerProperties(extraProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();

    String[] clusterNames = childDatacenters.get(0).getClusterNames();
    cluster = childDatacenters.get(0).getClusters().get(clusterNames[0]);
    parentController = parentControllers.get(0);

    parentControllerClient = new ControllerClient(cluster.getClusterName(), parentController.getControllerUrl());
    d2Client = D2TestUtils.getAndStartD2Client(cluster.getZk().getAddress());
    reader = new PushStatusStoreReader(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        TimeUnit.MINUTES.toSeconds(10));
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(reader);
    D2ClientUtils.shutdownClient(d2Client);
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testSystemStoreHeartbeat() {
    String userStoreName = Utils.getUniqueString("new-user-store");
    NewStoreResponse newStoreResponse = parentControllerClient
        .createNewStore(userStoreName, "venice-test", DEFAULT_KEY_SCHEMA, STRING_SCHEMA.toString());
    assertFalse(newStoreResponse.isError(), "Unexpected new store creation failure");
    String clusterName = "venice-cluster0";
    IntegrationTestPushUtils
        .makeSureUserSystemStoreIsOnline(multiRegionMultiClusterWrapper, clusterName, userStoreName);
    long currentTs = System.currentTimeMillis();
    for (VeniceMultiClusterWrapper datacenter: childDatacenters) {
      String childControllerUrl = datacenter.getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(clusterName, childControllerUrl)) {
        ControllerResponse response = childControllerClient
            .sendHeartbeatToSystemStore(VeniceSystemStoreUtils.getMetaStoreName(userStoreName), currentTs);
        Assert.assertFalse(response.isError());
        response = childControllerClient
            .sendHeartbeatToSystemStore(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(userStoreName), currentTs);
        Assert.assertFalse(response.isError());
      }
    }

    for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
      String childControllerUrl = childDatacenter.getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(clusterName, childControllerUrl)) {
        TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
          SystemStoreHeartbeatResponse response =
              childControllerClient.getHeartbeatFromSystemStore(VeniceSystemStoreUtils.getMetaStoreName(userStoreName));
          Assert.assertFalse(response.isError());
          Assert.assertEquals(response.getHeartbeatTimestamp(), currentTs);
          response = childControllerClient
              .getHeartbeatFromSystemStore(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(userStoreName));
          Assert.assertFalse(response.isError());
          Assert.assertEquals(response.getHeartbeatTimestamp(), currentTs);
        });
      }
    }

  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testParentControllerAutoMaterializeDaVinciPushStatusSystemStore() {
    String zkSharedDaVinciPushStatusSchemaStoreName =
        AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getSystemStoreName();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Store readOnlyStore = parentController.getVeniceAdmin()
          .getReadOnlyZKSharedSystemStoreRepository()
          .getStore(zkSharedDaVinciPushStatusSchemaStoreName);
      assertNotNull(
          readOnlyStore,
          "Store: " + zkSharedDaVinciPushStatusSchemaStoreName + " should be initialized by "
              + ClusterLeaderInitializationRoutine.class.getSimpleName());
      assertTrue(
          readOnlyStore.isHybrid(),
          "Store: " + zkSharedDaVinciPushStatusSchemaStoreName + " should be configured to hybrid");
    });
    String userStoreName = Utils.getUniqueString("new-user-store");
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(userStoreName, "venice-test", DEFAULT_KEY_SCHEMA, "\"string\"");
    assertFalse(newStoreResponse.isError(), "Unexpected new store creation failure");
    String daVinciPushStatusSystemStoreName =
        VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName);
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(daVinciPushStatusSystemStoreName, 1),
        parentControllerClient,
        30,
        TimeUnit.SECONDS);
    Store daVinciPushStatusSystemStore =
        parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
    assertEquals(daVinciPushStatusSystemStore.getLargestUsedVersionNumber(), 1);

    // Do empty pushes to increase the system store's version
    final int emptyPushAttempt = 2;
    for (int i = 0; i < emptyPushAttempt; i++) {
      final int newVersion = parentController.getVeniceAdmin()
          .incrementVersionIdempotent(
              cluster.getClusterName(),
              daVinciPushStatusSystemStoreName,
              "push job ID placeholder " + i,
              1,
              1)
          .getNumber();
      parentController.getVeniceAdmin()
          .writeEndOfPush(cluster.getClusterName(), daVinciPushStatusSystemStoreName, newVersion, true);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(daVinciPushStatusSystemStoreName, newVersion),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }
    daVinciPushStatusSystemStore =
        parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
    final int systemStoreCurrVersionBeforeBeingDeleted = daVinciPushStatusSystemStore.getLargestUsedVersionNumber();
    assertEquals(systemStoreCurrVersionBeforeBeingDeleted, 1 + emptyPushAttempt);

    TestUtils.assertCommand(parentControllerClient.disableAndDeleteStore(userStoreName));
    // Both the system store and user store should be gone at this point
    assertNull(parentController.getVeniceAdmin().getStore(cluster.getClusterName(), userStoreName));
    assertNull(parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName));

    Admin parentAdmin = parentControllers.get(0).getVeniceAdmin();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      assertEquals(
          parentAdmin.getLargestUsedVersion(cluster.getClusterName(), daVinciPushStatusSystemStoreName),
          systemStoreCurrVersionBeforeBeingDeleted);
    });

    // Create the same regular store again
    TestUtils.assertCommand(
        parentControllerClient.createNewStore(userStoreName, "venice-test", DEFAULT_KEY_SCHEMA, "\"string\""),
        "Unexpected new store creation failure");

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Store reCreatedPs3 =
          parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
      assertFalse(
          reCreatedPs3.getVersions().isEmpty(),
          "Re-created Da Vinci push status system store should have at least one version");
    });

    // The re-created/materialized per-user store system store should contain a continued version from its last life
    daVinciPushStatusSystemStore =
        parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
    assertEquals(
        daVinciPushStatusSystemStore.getLargestUsedVersionNumber(),
        systemStoreCurrVersionBeforeBeingDeleted + 1);

    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(daVinciPushStatusSystemStoreName, systemStoreCurrVersionBeforeBeingDeleted + 1),
        parentControllerClient,
        30,
        TimeUnit.SECONDS);
  }
}
