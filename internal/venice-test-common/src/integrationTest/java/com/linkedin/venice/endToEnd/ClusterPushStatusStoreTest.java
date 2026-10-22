package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_PUSH_STATUS_STORE_DUAL_WRITE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLUSTER_PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLUSTER_PUSH_STATUS_STORE_READ_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_INSTANCE_NAME_SUFFIX;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_FULL_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.SYSTEM_SCHEMA_INITIALIZATION_AT_START_TIME_ENABLED;
import static com.linkedin.venice.common.PushStatusStoreUtils.CONTROLLER_HEARTBEAT_INSTANCE_NAME;
import static com.linkedin.venice.common.PushStatusStoreUtils.getClusterPushStatusKey;
import static com.linkedin.venice.common.PushStatusStoreUtils.getHeartbeatKey;
import static com.linkedin.venice.common.PushStatusStoreUtils.getPushKey;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.init.ClusterPushStatusStoreInitializationRoutine;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class ClusterPushStatusStoreTest {
  @Test(timeOut = 180_000)
  public void testDualWriteReadAndCleanupAcrossStores() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(CLUSTER_PUSH_STATUS_STORE_ENABLED, "true");
    properties.setProperty(CLUSTER_PUSH_STATUS_STORE_DUAL_WRITE_ENABLED, "true");
    properties.setProperty(CLUSTER_PUSH_STATUS_STORE_READ_ENABLED, "true");
    // Controller leader initialization owns schemas; concurrent server registration races ZK schema creation.
    properties.setProperty(SYSTEM_SCHEMA_INITIALIZATION_AT_START_TIME_ENABLED, "false");
    String clusterName = Utils.getUniqueString("push-status-cluster");
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().clusterName(clusterName)
            .clusterToServerD2(Collections.singletonMap(clusterName, Utils.getUniqueString("server-service")))
            .numberOfServers(1)
            .replicationFactor(1)
            .maxNumberOfPartitions(200)
            .extraProperties(properties)
            .build());
        ControllerClient controller = cluster.getControllerClient()) {
      String firstStore = Utils.getUniqueString("first-store");
      String secondStore = Utils.getUniqueString("second-store");
      for (String store: new String[] { firstStore, secondStore }) {
        TestUtils.assertCommand(controller.createNewStore(store, "owner", "\"string\"", "\"string\""));
        cluster.createPushStatusSystemStore(store);
      }
      D2Client d2Client = D2TestUtils.getAndStartD2Client(cluster.getZk().getAddress());
      String clusterStore = VeniceSystemStoreUtils.getDaVinciPushStatusStoreNameForCluster(cluster.getClusterName());
      try (
          PushStatusStoreReader legacy =
              new PushStatusStoreReader(d2Client, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME, 60);
          AvroSpecificStoreClient<PushStatusKey, PushStatusValue> primary = ClientFactory.getAndStartSpecificAvroClient(
              ClientConfig.defaultSpecificClientConfig(clusterStore, PushStatusValue.class)
                  .setD2Client(d2Client)
                  .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME))) {
        VeniceHelixAdmin admin = (VeniceHelixAdmin) cluster.getLeaderVeniceController().getVeniceAdmin();
        cluster.waitVersion(clusterStore, 1, controller);
        new ClusterPushStatusStoreInitializationRoutine(admin.getMultiClusterConfigs(), admin).execute(clusterName);
        Store provisioned = admin.getStore(clusterName, clusterStore);
        assertEquals(provisioned.getPartitionCount(), 200);
        assertEquals(provisioned.getVersions().size(), 1);
        assertTrue(provisioned.isHybrid() && provisioned.isWriteComputationEnabled());
        assertEquals(provisioned.getNativeReplicationSourceFabric(), admin.getMultiClusterConfigs().getRegionName());
        assertEquals(
            admin.getDerivedSchemas(clusterName, clusterStore).size(),
            admin.getValueSchemas(clusterName, clusterStore).size());
        PushStatusStoreWriter writer = admin.getPushStatusStoreWriter();
        PushStatusStoreReader migrated = admin.getPushStatusStoreReader();
        writer.writeVersionLevelPushStatus(firstStore, 1, COMPLETED, Collections.singleton(0), Optional.empty());
        writer.writeVersionLevelPushStatus(secondStore, 1, ERROR, Collections.singleton(0), Optional.empty());
        PushStatusKey firstKey = getClusterPushStatusKey(firstStore, getPushKey(1));
        PushStatusKey secondKey = getClusterPushStatusKey(secondStore, getPushKey(1));
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          PushStatusValue first = primary.get(firstKey).get();
          PushStatusValue second = primary.get(secondKey).get();
          assertNotNull(first);
          assertNotNull(second);
          assertEquals(first.instances.values().iterator().next().intValue(), COMPLETED.getValue());
          assertEquals(second.instances.values().iterator().next().intValue(), ERROR.getValue());
          assertEquals(migrated.getVersionStatus(firstStore, 1, Optional.empty()).size(), 1);
          assertEquals(legacy.getVersionStatus(firstStore, 1, Optional.empty()).size(), 1);
        });
        writer.deletePushStatus(firstStore, 1, Optional.empty(), 1);
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          assertNull(primary.get(firstKey).get());
          assertNull(legacy.getVersionStatus(firstStore, 1, Optional.empty()));
          assertNotNull(primary.get(secondKey).get());
        });
        cluster.createMetaSystemStore(firstStore);
        TestUtils.assertCommand(controller.updateStore(firstStore, new UpdateStoreQueryParams().setPartitionCount(1)));
        TestUtils.assertCommand(controller.emptyPush(firstStore, "empty-push", 1));
        cluster.waitVersion(firstStore, 1, controller);
        try (
            CachingDaVinciClientFactory factory = DaVinciTestContext.getCachingDaVinciClientFactory(
                d2Client,
                VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
                new MetricsRepository(),
                DaVinciTestContext.getDaVinciPropertyBuilder(cluster.getZk().getAddress())
                    .put(PUSH_STATUS_STORE_ENABLED, true)
                    .put(PUSH_STATUS_INSTANCE_NAME_SUFFIX, "cluster-status-test")
                    .put(CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY, true)
                    .put(SERVER_DISK_FULL_THRESHOLD, 0.99)
                    .put(CLUSTER_PUSH_STATUS_STORE_DUAL_WRITE_ENABLED, true)
                    .build(),
                cluster);
            DaVinciClient<String, String> client =
                factory.getAndStartGenericAvroClient(firstStore, new DaVinciConfig())) {
          PushStatusKey discoveryHeartbeat =
              getClusterPushStatusKey(firstStore, getHeartbeatKey(Utils.getHostName() + "_cluster-status-test"));
          // Resolve optional cluster routing before ingestion can emit its one-shot terminal partition report.
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
            AvroGenericDaVinciClient.getBackend().getPushStatusStoreWriter().writeHeartbeat(firstStore);
            assertNotNull(primary.get(discoveryHeartbeat).get());
          });
          client.subscribeAll().get();
          PushStatusKey daVinciKey = getClusterPushStatusKey(firstStore, getPushKey(1, 0, Optional.empty()));
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
            PushStatusValue status = primary.get(daVinciKey).get();
            assertNotNull(status);
            assertEquals(status.instances.values().iterator().next().intValue(), COMPLETED.getValue());
            assertEquals(legacy.getPartitionStatus(firstStore, 1, 0, Optional.empty()).size(), 1);
          });
        }
        TestUtils.assertCommand(controller.disableAndDeleteStore(firstStore));
        writer.writeHeartbeat(secondStore, 456L);
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          assertEquals(migrated.getHeartbeat(secondStore, CONTROLLER_HEARTBEAT_INSTANCE_NAME), 456L);
          assertEquals(legacy.getHeartbeat(secondStore, CONTROLLER_HEARTBEAT_INSTANCE_NAME), 456L);
          PushStatusValue heartbeat =
              primary.get(getClusterPushStatusKey(secondStore, getHeartbeatKey(CONTROLLER_HEARTBEAT_INSTANCE_NAME)))
                  .get();
          assertNotNull(heartbeat);
          assertEquals(heartbeat.reportTimestamp.longValue(), 456L);
          assertNotNull(primary.get(secondKey).get());
        });
      } finally {
        D2ClientUtils.shutdownClient(d2Client);
      }
    }
  }
}
