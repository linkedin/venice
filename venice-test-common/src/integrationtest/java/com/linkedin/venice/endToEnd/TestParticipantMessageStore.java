package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.Metric;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static org.testng.Assert.*;


public class TestParticipantMessageStore {
  private static final Logger logger = Logger.getLogger(TestParticipantMessageStore.class);
  private VeniceClusterWrapper venice;
  private VeniceControllerWrapper parentController;
  private ZkServerWrapper parentZk;
  private ControllerClient controllerClient;
  private ControllerClient parentControllerClient;
  private String participantMessageStoreName;
  private VeniceServerWrapper veniceServerWrapper;

  @BeforeClass
  public void setup() {
    Properties enableParticipantMessageStore = new Properties();
    Properties serverFeatureProperties = new Properties();
    Properties serverProperties = new Properties();
    enableParticipantMessageStore.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    enableParticipantMessageStore.setProperty(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, "false");
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    enableParticipantMessageStore.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS,
        String.valueOf(Long.MAX_VALUE));
    venice = ServiceFactory.getVeniceCluster(1, 0, 1, 1,
        100000, false, false, enableParticipantMessageStore);
    D2Client d2Client = D2TestUtils.getAndStartD2Client(venice.getZk().getAddress());
    serverFeatureProperties.put(VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER, ClientConfig.defaultGenericClientConfig("")
        .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
        .setD2Client(d2Client));
    serverProperties.setProperty(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, Long.toString(100));
    veniceServerWrapper = venice.addVeniceServer(serverFeatureProperties, serverProperties);
    parentZk = ServiceFactory.getZkServer();
    parentController =
        ServiceFactory.getVeniceParentController(venice.getClusterName(), parentZk.getAddress(), venice.getKafka(),
            new VeniceControllerWrapper[]{venice.getMasterVeniceController()},
            new VeniceProperties(enableParticipantMessageStore), false);
    participantMessageStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(venice.getClusterName());
    controllerClient = venice.getControllerClient();
    parentControllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(participantMessageStoreName, 1),
        controllerClient, 2, TimeUnit.MINUTES, Optional.of(logger));
  }

  @AfterClass
  public void cleanup() {
    controllerClient.close();
    parentControllerClient.close();
    parentController.close();
    venice.close();
    parentZk.close();
  }

  @Test
  public void testParticipantStoreKill() {
    VersionCreationResponse versionCreationResponse = getNewStoreVersion(parentControllerClient);
    assertFalse(versionCreationResponse.isError());
    String topicName = versionCreationResponse.getKafkaTopic();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is STARTED.
      assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());
    });
    String metricPrefix = "." + venice.getClusterName() + "-participant_store_consumption_task";
    double killedPushJobCount = venice.getVeniceServers().iterator().next().getMetricsRepository().metrics()
        .get(metricPrefix + "--killed_push_jobs.Count").value();
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
    Map<String, ? extends Metric> metrics = venice.getVeniceServers().iterator().next().getMetricsRepository().metrics();
    assertEquals(metrics.get(metricPrefix + "--killed_push_jobs.Count").value() - killedPushJobCount, 1.0);
    assertTrue(metrics.get(metricPrefix + "--kill_push_job_latency.Avg").value() > 0);
    // One from the server stats and the other from the client stats.
    assertTrue(metrics.get("." + requestMetricExample).value() > 0);
    assertTrue(metrics.get(".venice-client." + requestMetricExample).value() > 0);
  }

  @Test
  public void testKillWhenVersionIsOnline() {
    String storeName = TestUtils.getUniqueString("testKillWhenVersionIsOnline");
    VersionCreationResponse versionCreationResponseForOnlineVersion = getNewStoreVersion(parentControllerClient, storeName);
    String topicNameForOnlineVersion = versionCreationResponseForOnlineVersion.getKafkaTopic();
    parentControllerClient.writeEndOfPush(storeName, versionCreationResponseForOnlineVersion.getVersion());
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is COMPLETED and the version is online.
      StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
      assertTrue(storeInfo.getVersions().iterator().hasNext()
          && storeInfo.getVersions().iterator().next().getStatus().equals(VersionStatus.ONLINE),
          "Waiting for a version to become online");
    });
    parentControllerClient.killOfflinePushJob(topicNameForOnlineVersion);

    /**
     * Try to kill an ongoing push, since for the same store, all the admin messages are processed sequentially.
     * When the new version receives kill job, then it is safe to make an assertion about whether the previous
     * version receives a kill-job message or not.
     */
    VersionCreationResponse versionCreationResponseForBootstrappingVersion = getNewStoreVersion(parentControllerClient, storeName);
    String topicNameForBootstrappingVersion = versionCreationResponseForBootstrappingVersion.getKafkaTopic();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is STARTED.
      assertEquals(controllerClient.queryJobStatus(topicNameForBootstrappingVersion).getStatus(), ExecutionStatus.STARTED.toString());
    });
    ControllerResponse response = parentControllerClient.killOfflinePushJob(topicNameForBootstrappingVersion);
    assertFalse(response.isError());
    verifyKillMessageInParticipantStore(topicNameForBootstrappingVersion, true);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Poll job status to verify the job is indeed killed
      assertEquals(controllerClient.queryJobStatus(topicNameForBootstrappingVersion).getStatus(), ExecutionStatus.ERROR.toString());
    });
    // Then we could verify whether the previous version receives a kill-job or not.
    verifyKillMessageInParticipantStore(topicNameForOnlineVersion, false);

    venice.stopVeniceServer(veniceServerWrapper.getPort());
    // Ensure the partition assignment is 0 before restarting the server
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> venice.getRandomVeniceRouter()
        .getRoutingDataRepository().getPartitionAssignments(topicNameForOnlineVersion).getAssignedNumberOfPartitions() == 0);
    venice.restartVeniceServer(veniceServerWrapper.getPort());
    int expectedOnlineReplicaCount = versionCreationResponseForOnlineVersion.getReplicas();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      for (int p = 0; p < versionCreationResponseForOnlineVersion.getPartitions(); p++) {
        assertEquals(venice.getRandomVeniceRouter().getRoutingDataRepository()
                .getReadyToServeInstances(topicNameForOnlineVersion, p).size(),
            expectedOnlineReplicaCount, "Not all replicas are ONLINE yet");
      }
    });
  }

  private void verifyKillMessageInParticipantStore(String topic, boolean shouldPresent) {
    // Verify the kill push message is in the participant message store.
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.resourceName = topic;
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    try (AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(participantMessageStoreName,
                ParticipantMessageValue.class).setVeniceURL(venice.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        try {
          if (shouldPresent) {
            // Verify that the kill offline message has made it to the participant message store.
            assertNotNull(client.get(key).get());
          } else {
            assertNull(client.get(key).get());
          }
        } catch (Exception e) {
          // no op
        }
      });
    }
  }

  private VersionCreationResponse getNewStoreVersion(ControllerClient controllerClient, String storeName) {
    controllerClient.createNewStore(storeName, "test-user", "\"string\"", "\"string\"");
    return parentControllerClient.requestTopicForWrites(storeName, 1024,
        Version.PushType.BATCH, Version.guidBasedDummyPushId(), true, true, Optional.empty(), Optional.empty());
  }


  private VersionCreationResponse getNewStoreVersion(ControllerClient controllerClient) {
    return getNewStoreVersion(controllerClient, TestUtils.getUniqueString("test-kill"));
  }
}
