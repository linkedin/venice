package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStoreMigrationReattempt {
  private static final int TEST_TIMEOUT = 180 * Time.MS_PER_SECOND;
  private static final int RECORD_COUNT = 20;
  private static final String NEW_OWNER = "newtest@linkedin.com";
  private static final String FABRIC0 = "dc-0";
  private static final boolean[] ABORT_MIGRATION_PROMPTS_OVERRIDE = { false, true, true };

  private VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper;
  private VeniceMultiClusterWrapper multiClusterWrapper;
  private String srcClusterName;
  private String destClusterName;
  private String parentControllerUrl;
  private String childControllerUrl0;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProperties = new Properties();
    parentControllerProperties.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1);
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    parentControllerProperties
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    parentControllerProperties.setProperty(OFFLINE_JOB_START_TIMEOUT_MS, "180000");

    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, 10L);
    // 1 parent controller, 1 child region, 2 clusters per child region, 2 servers per cluster
    // RF=2 to test both leader and follower SNs
    twoLayerMultiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        1,
        2,
        1,
        1,
        2,
        1,
        2,
        Optional.of(parentControllerProperties),
        Optional.empty(),
        Optional.of(serverProperties),
        false);

    multiClusterWrapper = twoLayerMultiRegionMultiClusterWrapper.getChildRegions().get(0);
    String[] clusterNames = multiClusterWrapper.getClusterNames();
    Arrays.sort(clusterNames);
    srcClusterName = clusterNames[0]; // venice-cluster0
    destClusterName = clusterNames[1]; // venice-cluster1
    parentControllerUrl = twoLayerMultiRegionMultiClusterWrapper.getControllerConnectString();
    childControllerUrl0 = multiClusterWrapper.getControllerConnectString();

    for (String cluster: clusterNames) {
      try (ControllerClient controllerClient = new ControllerClient(cluster, childControllerUrl0)) {
        // Verify the participant store is up and running in child region
        String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(cluster);
        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(participantStoreName, 1),
            controllerClient,
            5,
            TimeUnit.MINUTES);
      }
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(twoLayerMultiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT, enabled = true)
  public void testStoreMigrationAfterFailedAttempt() throws Exception {
    String storeName = Utils.getUniqueString("testWithFailedAttempt");
    createAndPushStore(srcClusterName, storeName);

    try (ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
        ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
      StoreResponse storeResponse = TestUtils.assertCommand(srcParentControllerClient.getStore(storeName));
      StoreInfo storeInfo = storeResponse.getStore();
      assertNotNull(storeInfo);
      String currentVersionTopicName = Version.composeKafkaTopic(storeName, 1);

      // induce a kill message in the participant store for the current version topic in the destination cluster
      VeniceClusterWrapper destClusterWrapper = multiClusterWrapper.getClusters().get(destClusterName);
      VeniceHelixAdmin destClusterVhaDc0 = destClusterWrapper.getLeaderVeniceController().getVeniceHelixAdmin();
      assertFalse(destClusterVhaDc0.isParent());
      // add kill message to dest cluster
      destClusterVhaDc0.sendKillMessageToParticipantStore(destClusterName, currentVersionTopicName);
      // Verify the kill push message is in the participant message store.
      verifyKillMessageInParticipantStore(destClusterWrapper, currentVersionTopicName, true);

      startMigration(parentControllerUrl, storeName);
      // Ensure migration status is updated in source parent controller
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> assertTrue(srcParentControllerClient.getStore(storeName).getStore().isMigrating()));

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        // Store migration status output via closure PrintFunction
        Set<String> statusOutput = new HashSet<String>();
        AdminTool.PrintFunction printFunction = (message) -> {
          statusOutput.add(message.trim());
          System.err.println(message);
        };
        checkMigrationStatus(parentControllerUrl, storeName, printFunction);
        assertTrue(
            statusOutput
                .contains(storeName + " belongs to cluster " + srcClusterName + " according to cluster discovery"));
        assertTrue(statusOutput.contains(storeName + " exists in this cluster " + destClusterName));
      });

      verifyKillMessageInParticipantStore(destClusterWrapper, currentVersionTopicName, false);
      completeMigration(parentControllerUrl, storeName);
      endMigration(parentControllerUrl, storeName);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        // Store migration status output via closure PrintFunction
        Set<String> statusOutput = new HashSet<String>();
        AdminTool.PrintFunction printFunction = (message) -> {
          statusOutput.add(message.trim());
          System.err.println(message);
        };
        checkMigrationStatus(parentControllerUrl, storeName, printFunction);
        assertTrue(
            statusOutput
                .contains(storeName + " belongs to cluster " + destClusterName + " according to cluster discovery"));
        assertTrue(statusOutput.contains(storeName + " exists in this cluster " + destClusterName));
      });

      assertTrue(srcParentControllerClient.getStore(storeName).isError());
      StoreResponse destStoreResponse = TestUtils.assertCommand(destParentControllerClient.getStore(storeName));
      StoreInfo destStoreInfo = destStoreResponse.getStore();
      assertNotNull(destStoreInfo);
      assertFalse(destStoreInfo.isMigrating());
      assertFalse(destStoreInfo.isMigrationDuplicateStore());
    }

    try (ControllerClient childControllerClient0 = new ControllerClient(destClusterName, childControllerUrl0)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse response = childControllerClient0.getStore(storeName);
        StoreInfo storeInfo = response.getStore();
        assertNotNull(storeInfo);
        StoreResponse destStoreResponse = TestUtils.assertCommand(childControllerClient0.getStore(storeName));
        StoreInfo destStoreInfo = destStoreResponse.getStore();
        assertNotNull(destStoreInfo);
        assertFalse(destStoreInfo.isMigrating());
        assertFalse(destStoreInfo.isMigrationDuplicateStore());
        assertEquals(destStoreInfo.getCurrentVersion(), 1);
      });
    }
  }

  private void verifyKillMessageInParticipantStore(
      VeniceClusterWrapper clusterWrapper,
      String topic,
      boolean shouldPresent) {
    // Verify the kill push message is in the participant message store.
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.resourceName = topic;
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    String participantStoreName =
        VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterWrapper.getClusterName());
    try (AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(participantStoreName, ParticipantMessageValue.class)
                .setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, () -> {
        try {
          if (shouldPresent) {
            // Verify that the kill offline message has made it to the participant message store.
            assertNotNull(
                client.get(key).get(),
                "Kill message not found in participant store: " + participantStoreName + " for topic: " + topic);
          } else {
            assertNull(
                client.get(key).get(),
                "Kill message found in participant store: " + participantStoreName + " for topic: " + topic);
          }
        } catch (Exception e) {
          fail();
        }
      });
    }
  }

  private Properties createAndPushStore(String clusterName, String storeName) throws Exception {
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(twoLayerMultiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, RECORD_COUNT);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setHybridRewindSeconds(TEST_TIMEOUT)
            .setHybridOffsetLagThreshold(2L)
            .setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT);
    IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreQueryParams)
        .close();

    // Verify store is created in dc-0
    try (ControllerClient childControllerClient0 = new ControllerClient(clusterName, childControllerUrl0)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse response = childControllerClient0.getStore(storeName);
        StoreInfo storeInfo = response.getStore();
        assertNotNull(storeInfo);
      });
    }

    SystemProducer veniceProducer0 = null;
    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
      job.run();

      // Write streaming records
      veniceProducer0 =
          getSamzaProducer(multiClusterWrapper.getClusters().get(clusterName), storeName, Version.PushType.STREAM);
      for (int i = 1; i <= 10; i++) {
        sendStreamingRecord(veniceProducer0, storeName, i);
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    } finally {
      if (veniceProducer0 != null) {
        veniceProducer0.stop();
      }
    }

    return props;
  }

  private void startMigration(String controllerUrl, String storeName) throws Exception {
    String[] startMigrationArgs = { "--migrate-store", "--url", controllerUrl, "--store", storeName, "--cluster-src",
        srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.main(startMigrationArgs);
  }

  private void checkMigrationStatus(String controllerUrl, String storeName, AdminTool.PrintFunction printFunction)
      throws Exception {
    String[] checkMigrationStatusArgs = { "--migration-status", "--url", controllerUrl, "--store", storeName,
        "--cluster-src", srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.checkMigrationStatus(AdminTool.getCommandLine(checkMigrationStatusArgs), printFunction);
  }

  private void completeMigration(String controllerUrl, String storeName) {
    String[] completeMigration0 = { "--complete-migration", "--url", controllerUrl, "--store", storeName,
        "--cluster-src", srcClusterName, "--cluster-dest", destClusterName, "--fabric", FABRIC0 };

    try (ControllerClient destParentControllerClient = new ControllerClient(destClusterName, controllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        AdminTool.main(completeMigration0);
        // Store discovery should point to the new cluster after completing migration
        ControllerResponse discoveryResponse = destParentControllerClient.discoverCluster(storeName);
        Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);
      });
    }
  }

  private void endMigration(String controllerUrl, String storeName) throws Exception {
    String[] endMigration = { "--end-migration", "--url", controllerUrl, "--store", storeName, "--cluster-src",
        srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.main(endMigration);

    try (ControllerClient srcControllerClient = new ControllerClient(srcClusterName, controllerUrl);
        ControllerClient destControllerClient = new ControllerClient(destClusterName, controllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // Store should be deleted in source cluster. Store in destination cluster should not be migrating.
        StoreResponse storeResponse = srcControllerClient.getStore(storeName);
        Assert.assertNull(storeResponse.getStore());

        storeResponse = destControllerClient.getStore(storeName);
        assertNotNull(storeResponse.getStore());
        assertFalse(storeResponse.getStore().isMigrating());
        assertFalse(storeResponse.getStore().isMigrationDuplicateStore());
      });
    }
  }

  private void readFromStore(AvroGenericStoreClient<String, Object> client) {
    int key = ThreadLocalRandom.current().nextInt(RECORD_COUNT) + 1;
    client.get(Integer.toString(key));
  }

  private void abortMigration(String controllerUrl, String storeName, boolean force) {
    AdminTool.abortMigration(
        controllerUrl,
        storeName,
        srcClusterName,
        destClusterName,
        force,
        ABORT_MIGRATION_PROMPTS_OVERRIDE);
  }

  private void checkStatusAfterAbortMigration(
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      String storeName) {
    // Migration flag should be false
    // Store should be deleted in dest cluster
    // Cluster discovery should point to src cluster
    StoreResponse storeResponse = srcControllerClient.getStore(storeName);
    assertNotNull(storeResponse.getStore());
    assertFalse(storeResponse.getStore().isMigrating());
    storeResponse = destControllerClient.getStore(storeName);
    Assert.assertNull(storeResponse.getStore());
    ControllerResponse discoveryResponse = destControllerClient.discoverCluster(storeName);
    Assert.assertEquals(discoveryResponse.getCluster(), srcClusterName);
  }

  private StoreInfo getStoreConfig(String controllerUrl, String clusterName, String storeName) {
    try (ControllerClient controllerClient = new ControllerClient(clusterName, controllerUrl)) {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      if (storeResponse.isError()) {
        throw new VeniceException(
            "Failed to get store configs for store " + storeName + " from cluster " + clusterName + ". Error: "
                + storeResponse.getError());
      }
      return storeResponse.getStore();
    }
  }
}
