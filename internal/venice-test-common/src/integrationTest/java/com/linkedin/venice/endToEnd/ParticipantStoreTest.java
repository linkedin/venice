package com.linkedin.venice.endToEnd;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.Metric;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ParticipantStoreTest {
  private static final Logger LOGGER = LogManager.getLogger(ParticipantStoreTest.class);

  private VeniceTwoLayerMultiRegionMultiClusterWrapper venice;
  private VeniceClusterWrapper veniceLocalCluster;
  private VeniceServerWrapper veniceServerWrapper;

  private ControllerClient controllerClient;
  private ControllerClient parentControllerClient;
  private String participantMessageStoreName;
  private String clusterName;

  @BeforeClass
  public void setUp() {
    venice = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .build());
    clusterName = venice.getClusterNames()[0];
    participantMessageStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName);
    veniceLocalCluster = venice.getChildRegions().get(0).getClusters().get(clusterName);
    veniceServerWrapper = veniceLocalCluster.getVeniceServers().get(0);

    controllerClient = new ControllerClient(clusterName, veniceLocalCluster.getAllControllersURLs());
    parentControllerClient = new ControllerClient(clusterName, venice.getControllerConnectString());
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(participantMessageStoreName, 1),
        controllerClient,
        2,
        TimeUnit.MINUTES);
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    IOUtils.closeQuietly(venice);
  }

  // @Test(timeOut = 60 * Time.MS_PER_SECOND)
  // TODO: killed_push_jobs_count seems to be a broken metric in L/F (at least for participant stores)
  public void testParticipantStoreKill() {
    VersionCreationResponse versionCreationResponse = getNewStoreVersion(parentControllerClient, true);
    assertFalse(versionCreationResponse.isError());
    String topicName = versionCreationResponse.getKafkaTopic();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is STARTED.
      assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());
    });
    String metricPrefix = "." + clusterName + "-participant_store_consumption_task";
    double killedPushJobCount = veniceLocalCluster.getVeniceServers()
        .iterator()
        .next()
        .getMetricsRepository()
        .metrics()
        .get(metricPrefix + "--killed_push_jobs.Count")
        .value();
    assertEquals(killedPushJobCount, 0.0);
    ControllerResponse response = parentControllerClient.killOfflinePushJob(topicName);
    assertFalse(response.isError());
    verifyKillMessageInParticipantStore(topicName, true);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Poll job status to verify the job is indeed killed
      assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.ERROR.toString());
    });
    // Verify participant store consumption stats
    String requestMetricExample =
        VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName) + "--success_request_key_count.Avg";
    Map<String, ? extends Metric> metrics =
        veniceLocalCluster.getVeniceServers().iterator().next().getMetricsRepository().metrics();
    assertEquals(metrics.get(metricPrefix + "--killed_push_jobs.Count").value(), 1.0);
    assertTrue(metrics.get(metricPrefix + "--kill_push_job_latency.Avg").value() > 0);
    // One from the server stats and the other from the client stats.
    assertTrue(metrics.get("." + requestMetricExample).value() > 0);
    // "." will be replaced by "_" in AbstractVeniceStats constructor to ensure Tahuti is able to parse metric name by
    // ".".
    assertTrue(metrics.get(".venice-client_" + requestMetricExample).value() > 0);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testParticipantStoreThrottlerRestartRouter() {
    VersionCreationResponse versionCreationResponse = getNewStoreVersion(parentControllerClient, true);
    assertFalse(versionCreationResponse.isError());
    String topicName = versionCreationResponse.getKafkaTopic();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is STARTED.
      assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());
    });
    ControllerResponse response = parentControllerClient.killOfflinePushJob(topicName);
    assertFalse(response.isError());
    verifyKillMessageInParticipantStore(topicName, true);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Poll job status to verify the job is indeed killed
      assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.ERROR.toString());
    });

    // restart routers to discard in-memory throttler info
    veniceLocalCluster.stopAndRestartAllVeniceRouters();
    // Verify still can read from participant stores.
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.resourceName = topicName;
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    try (AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(participantMessageStoreName, ParticipantMessageValue.class)
                .setVeniceURL(veniceLocalCluster.getRandomRouterURL()))) {
      try {
        client.get(key).get();
      } catch (Exception e) {
        fail("Should be able to query participant store successfully");
      }
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testKillWhenVersionIsOnline() {
    String storeName = Utils.getUniqueString("testKillWhenVersionIsOnline");
    final VersionCreationResponse versionCreationResponseForOnlineVersion =
        getNewStoreVersion(parentControllerClient, storeName, true);
    final String topicNameForOnlineVersion = versionCreationResponseForOnlineVersion.getKafkaTopic();
    parentControllerClient.writeEndOfPush(storeName, versionCreationResponseForOnlineVersion.getVersion());
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is COMPLETED and the version is online.
      StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
      assertTrue(
          storeInfo.getVersions().iterator().hasNext()
              && storeInfo.getVersions().iterator().next().getStatus().equals(VersionStatus.ONLINE),
          "Waiting for a version to become online");
    });
    parentControllerClient.killOfflinePushJob(topicNameForOnlineVersion);

    /**
     * Try to kill an ongoing push, since for the same store, all the admin messages are processed sequentially.
     * When the new version receives kill job, then it is safe to make an assertion about whether the previous
     * version receives a kill-job message or not.
     */
    final VersionCreationResponse versionCreationResponseForBootstrappingVersion =
        getNewStoreVersion(parentControllerClient, storeName, false);
    final String topicNameForBootstrappingVersion = versionCreationResponseForBootstrappingVersion.getKafkaTopic();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is STARTED.
      assertEquals(
          controllerClient.queryJobStatus(topicNameForBootstrappingVersion).getStatus(),
          ExecutionStatus.STARTED.toString());
    });
    ControllerResponse response = parentControllerClient.killOfflinePushJob(topicNameForBootstrappingVersion);
    assertFalse(response.isError());
    verifyKillMessageInParticipantStore(topicNameForBootstrappingVersion, true);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Poll job status to verify the job is indeed killed
      assertEquals(
          controllerClient.queryJobStatus(topicNameForBootstrappingVersion).getStatus(),
          ExecutionStatus.ERROR.toString());
    });
    // Then we could verify whether the previous version receives a kill-job or not.
    verifyKillMessageInParticipantStore(topicNameForOnlineVersion, false);

    veniceLocalCluster.stopVeniceServer(veniceServerWrapper.getPort());
    // Ensure the partition assignment is 0 before restarting the server
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      VeniceRouterWrapper routerWrapper = veniceLocalCluster.getRandomVeniceRouter();
      assertFalse(routerWrapper.getRoutingDataRepository().containsKafkaTopic(topicNameForOnlineVersion));

    });

    veniceLocalCluster.restartVeniceServer(veniceServerWrapper.getPort());
    int expectedOnlineReplicaCount = versionCreationResponseForOnlineVersion.getReplicas();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      for (int p = 0; p < versionCreationResponseForOnlineVersion.getPartitions(); p++) {
        try {
          assertEquals(
              veniceLocalCluster.getRandomVeniceRouter()
                  .getRoutingDataRepository()
                  .getReadyToServeInstances(topicNameForOnlineVersion, p)
                  .size(),
              expectedOnlineReplicaCount,
              "Not all replicas are ONLINE yet");
        } catch (VeniceException e) {
          fail(
              "Received exception when getting the ready to serve replica count for partition: " + p + " for resource: "
                  + topicNameForOnlineVersion);
        }
      }
    });

    // Now, try to delete the version and the corresponding kill message should be present even for an ONLINE version
    // Push a new version so the ONLINE version can be deleted to mimic retiring an old version.
    VersionCreationResponse newVersionResponse = getNewStoreVersion(parentControllerClient, storeName, false);
    assertFalse(newVersionResponse.isError(), "Controller error: " + newVersionResponse.getError());
    parentControllerClient.writeEndOfPush(storeName, newVersionResponse.getVersion());
    TestUtils.waitForNonDeterministicPushCompletion(
        newVersionResponse.getKafkaTopic(),
        parentControllerClient,
        30,
        TimeUnit.SECONDS);
    parentControllerClient
        .deleteOldVersion(storeName, Version.parseVersionFromKafkaTopicName(topicNameForOnlineVersion));
    verifyKillMessageInParticipantStore(topicNameForOnlineVersion, true);
  }

  static class TestListener implements StoreDataChangedListener {
    AtomicInteger creationCount = new AtomicInteger(0);
    AtomicInteger changeCount = new AtomicInteger(0);
    AtomicInteger deletionCount = new AtomicInteger(0);

    @Override
    public void handleStoreCreated(Store store) {
      creationCount.incrementAndGet();
    }

    @Override
    public void handleStoreDeleted(String storeName) {
      deletionCount.incrementAndGet();
    }

    @Override
    public void handleStoreChanged(Store store) {
      LOGGER.info("Received handleStoreChanged: {}", store);
      changeCount.incrementAndGet();
    }

    public int getCreationCount() {
      return creationCount.get();
    }

    public int getChangeCount() {
      return changeCount.get();
    }

    public int getDeletionCount() {
      return deletionCount.get();
    }
  }

  private void verifyKillMessageInParticipantStore(String topic, boolean shouldPresent) {
    // Verify the kill push message is in the participant message store.
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.resourceName = topic;
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    try (AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(participantMessageStoreName, ParticipantMessageValue.class)
                .setVeniceURL(veniceLocalCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        try {
          if (shouldPresent) {
            // Verify that the kill offline message has made it to the participant message store.
            assertNotNull(client.get(key).get());
          } else {
            assertNull(client.get(key).get());
          }
        } catch (Exception e) {
          fail();
        }
      });
    }
  }

  private VersionCreationResponse getNewStoreVersion(
      ControllerClient controllerClient,
      String storeName,
      boolean newStore) {
    if (newStore) {
      controllerClient.createNewStore(storeName, "test-user", "\"string\"", "\"string\"");
    }
    return parentControllerClient.requestTopicForWrites(
        storeName,
        1024,
        Version.PushType.BATCH,
        Version.guidBasedDummyPushId(),
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
  }

  private VersionCreationResponse getNewStoreVersion(ControllerClient controllerClient, boolean newStore) {
    return getNewStoreVersion(controllerClient, Utils.getUniqueString("test-kill"), newStore);
  }
}
