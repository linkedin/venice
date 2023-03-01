package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
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
import java.util.Properties;
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

  private VeniceClusterWrapper venice;
  private VeniceControllerWrapper parentController;
  private ZkServerWrapper parentZk;
  private ControllerClient controllerClient;
  private ControllerClient parentControllerClient;
  private String participantMessageStoreName;
  private VeniceServerWrapper veniceServerWrapper;
  private D2Client d2Client;

  @BeforeClass
  public void setUp() {
    Properties controllerConfig = new Properties();
    Properties serverFeatureProperties = new Properties();
    Properties serverProperties = new Properties();
    controllerConfig.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    controllerConfig.setProperty(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, "false");
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    controllerConfig
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    venice = ServiceFactory.getVeniceCluster(1, 0, 1, 1, 100000, false, false, controllerConfig);
    d2Client = D2TestUtils.getAndStartD2Client(venice.getZk().getAddress());
    serverFeatureProperties.put(
        VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER,
        ClientConfig.defaultGenericClientConfig("")
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setD2Client(d2Client));
    serverProperties.setProperty(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, Long.toString(100));
    veniceServerWrapper = venice.addVeniceServer(serverFeatureProperties, serverProperties);
    parentZk = ServiceFactory.getZkServer();
    parentController = ServiceFactory.getVeniceController(
        new VeniceControllerCreateOptions.Builder(venice.getClusterName(), parentZk, venice.getKafka())
            .childControllers(venice.getVeniceControllers().toArray(new VeniceControllerWrapper[0]))
            .extraProperties(controllerConfig)
            .build());
    participantMessageStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(venice.getClusterName());
    controllerClient = venice.getControllerClient();
    parentControllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
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
    Utils.closeQuietlyWithErrorLogged(parentController);
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    IOUtils.closeQuietly(venice);
    IOUtils.closeQuietly(parentZk);
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
    String metricPrefix = "." + venice.getClusterName() + "-participant_store_consumption_task";
    double killedPushJobCount = venice.getVeniceServers()
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
    String requestMetricExample = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(venice.getClusterName())
        + "--success_request_key_count.Avg";
    Map<String, ? extends Metric> metrics =
        venice.getVeniceServers().iterator().next().getMetricsRepository().metrics();
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
    for (VeniceRouterWrapper router: venice.getVeniceRouters()) {
      venice.stopVeniceRouter(router.getPort());
      venice.restartVeniceRouter(router.getPort());
    }
    // Verify still can read from participant stores.
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.resourceName = topicName;
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    try (AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(participantMessageStoreName, ParticipantMessageValue.class)
                .setVeniceURL(venice.getRandomRouterURL()))) {
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

    venice.stopVeniceServer(veniceServerWrapper.getPort());
    // Ensure the partition assignment is 0 before restarting the server
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      VeniceRouterWrapper routerWrapper = venice.getRandomVeniceRouter();
      assertFalse(routerWrapper.getRoutingDataRepository().containsKafkaTopic(topicNameForOnlineVersion));

    });

    venice.restartVeniceServer(veniceServerWrapper.getPort());
    int expectedOnlineReplicaCount = versionCreationResponseForOnlineVersion.getReplicas();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      for (int p = 0; p < versionCreationResponseForOnlineVersion.getPartitions(); p++) {
        assertEquals(
            venice.getRandomVeniceRouter()
                .getRoutingDataRepository()
                .getReadyToServeInstances(topicNameForOnlineVersion, p)
                .size(),
            expectedOnlineReplicaCount,
            "Not all replicas are ONLINE yet");
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
                .setVeniceURL(venice.getRandomRouterURL()))) {
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
