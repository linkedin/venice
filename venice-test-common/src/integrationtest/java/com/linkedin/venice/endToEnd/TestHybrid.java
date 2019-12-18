package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.InstanceStatus;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.samza.SamzaExitMode;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.kafka.TopicManager.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class TestHybrid {
  private static final Logger logger = Logger.getLogger(TestHybrid.class);
  private static final int STREAMING_RECORD_SIZE = 1024;

  @DataProvider(name = "isLeaderFollowerModelEnabled", parallel = true)
  public static Object[][] isLeaderFollowerModelEnabled() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "isLeaderFollowerModelEnabled")
  public void testHybridInitializationOnMultiColo(boolean isLeaderFollowerModelEnabled) {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1,2,1,1, 1000000, false, false, extraProperties);
    ZkServerWrapper parentZk = ServiceFactory.getZkServer();
    VeniceControllerWrapper parentController = ServiceFactory.getVeniceParentController(
        venice.getClusterName(), parentZk.getAddress(), venice.getKafka(), new VeniceControllerWrapper[]{venice.getMasterVeniceController()}, false);

    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;
    final String storeName = TestUtils.getUniqueString("multi-colo-hybrid-store");

    //Create store at parent, make it a hybrid store
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA);
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
            .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setHybridRewindSeconds(streamingRewindSeconds)
            .setHybridOffsetLagThreshold(streamingMessageLag)
            .setLeaderFollowerModel(isLeaderFollowerModelEnabled)
    );

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfig(streamingRewindSeconds, streamingMessageLag);
    // There should be no version on the store yet
    assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(),
        0, "The newly created store must have a current version of 0");

    // Create a new version, and do an empty push for that version
    VersionCreationResponse vcr = controllerClient.emptyPush(storeName, TestUtils.getUniqueString("empty-hybrid-push"), 1L);
    int versionNumber = vcr.getVersion();
    assertNotEquals(versionNumber, 0, "requesting a topic for a push should provide a non zero version number");

    TestUtils.waitForNonDeterministicAssertion(100, TimeUnit.SECONDS, true, () -> {
      // Now the store should have version 1
      JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(Version.composeKafkaTopic(storeName, versionNumber));
      assertEquals(jobStatus.getStatus(), "COMPLETED");
    });

    //And real-time topic should exist now.
    TopicManager topicManager = new TopicManager(venice.getZk().getAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(venice.getKafka().getAddress()));
    assertTrue(topicManager.containsTopic(Version.composeRealTimeTopic(storeName)));
    assertEquals(topicManager.getTopicRetention(Version.composeRealTimeTopic(storeName)),
        hybridStoreConfig.getRetentionTimeInMs(), "RT retention not configured properly");
    // Make sure RT retention is updated when the rewind time is updated
    long newStreamingRewindSeconds = 600;
    hybridStoreConfig.setRewindTimeInSeconds(newStreamingRewindSeconds);
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(newStreamingRewindSeconds));
    assertEquals(topicManager.getTopicRetention(Version.composeRealTimeTopic(storeName)),
        hybridStoreConfig.getRetentionTimeInMs(), "RT retention not updated properly");
    IOUtils.closeQuietly(topicManager);
    parentController.close();
    parentZk.close();
    venice.close();
  }

  /**
   * N.B.: Non-L/F does not support chunking, so this permutation is skipped.
   */
  @DataProvider(name = "testPermutations", parallel = true)
  public static Object[][] testPermutations() {
    return new Object[][]{
        {false, false, false},
        {false, true, false},
        {false, true, true},
        {true, false, false},
        {true, true, false},
        {true, true, true}
    };
  }

  /**
   * This test validates the hybrid batch + streaming semantics and verifies that configured rewind time works as expected.
   *
   * TODO: This test needs to be refactored in order to leverage {@link com.linkedin.venice.utils.MockTime},
   *       which would allow the test to run faster and more deterministically.

   * @param multiDivStream if false, rewind will happen in the middle of a DIV Segment, which was originally broken.
   *                       if true, two independent DIV Segments will be placed before and after the start of buffer replay.
   *
   *                       If this test succeeds with {@param multiDivStream} set to true, but fails with it set to false,
   *                       then there is a regression in the DIV partial segment tolerance after EOP.
   * @param isLeaderFollowerModelEnabled Whether to enable Leader/Follower state transition model.
   * @param chunkingEnabled Whether chunking should be enabled (only supported in {@param isLeaderFollowerModelEnabled} is true).
   */
  @Test(dataProvider = "testPermutations", groups="flaky")
  public void testHybridEndToEnd(boolean multiDivStream, boolean isLeaderFollowerModelEnabled, boolean chunkingEnabled) throws Exception {
    logger.info("About to create VeniceClusterWrapper");
    Properties extraProperties = new Properties();
    if (isLeaderFollowerModelEnabled) {
      extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));
    }
    if (chunkingEnabled) {
      // We exercise chunking by setting the servers' max size arbitrarily low. For now, since the RT topic
      // does not support chunking, and write compute is not merged yet, there is no other way to make the
      // store-version data bigger than the RT data and thus have chunked values produced.
      int maxMessageSizeInServer = STREAMING_RECORD_SIZE / 2;
      extraProperties.setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, Integer.toString(maxMessageSizeInServer));
    }
    // N.B.: RF 2 with 2 servers is important, in order to test both the leader and follower code paths
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1,2,1,
        2, 1000000, false, false, extraProperties);
    logger.info("Finished creating VeniceClusterWrapper");

    long streamingRewindSeconds = 10L;
    long streamingMessageLag = 2L;

    String storeName = TestUtils.getUniqueString("hybrid-store");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir); // records 1-100
    Properties h2vProperties = defaultH2VProps(venice, inputDirPath, storeName);

    ControllerClient controllerClient = createStoreForJob(venice, recordSchema, h2vProperties);

    ControllerResponse response = controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(streamingRewindSeconds)
        .setHybridOffsetLagThreshold(streamingMessageLag)
        .setLeaderFollowerModel(isLeaderFollowerModelEnabled)
        .setChunkingEnabled(chunkingEnabled)
    );

    Assert.assertFalse(response.isError());

    TopicManager topicManager = new TopicManager(venice.getZk().getAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(venice.getKafka().getAddress()));

    //Do an H2V push
    runH2V(h2vProperties, 1, controllerClient);

    // verify the topic compaction policy
    String topicForStoreVersion1 = Version.composeKafkaTopic(storeName, 1);
    Assert.assertTrue(topicManager.isTopicCompactionEnabled(topicForStoreVersion1), "topic: " + topicForStoreVersion1 + " should have compaction enabled");

    //Verify some records (note, records 1-100 have been pushed)
    AvroGenericStoreClient client =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      try {
        for (int i = 1; i < 100; i++) {
          String key = Integer.toString(i);
          Object value = client.get(key).get();
          assertNotNull(value, "Key " + i + " should not be missing!");
          assertEquals(value.toString(), "test_name_" + key);
        }
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    //write streaming records
    SystemProducer veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM);
    for (int i=1; i<=10; i++) {
      // The batch values are small, but the streaming records are "big" (i.e.: not that big, but bigger than
      // the server's max configured chunk size). In the scenario where chunking is disabled, the server's
      // max chunk size is not altered, and thus this will be under threshold.
      sendCustomSizeStreamingRecord(veniceProducer, storeName, i, STREAMING_RECORD_SIZE);
    }
    if (multiDivStream) {
      veniceProducer.stop(); //close out the DIV segment
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      try {
        checkLargeRecord(client, 2);
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    runH2V(h2vProperties, 2, controllerClient);
    // verify the topic compaction policy
    String topicForStoreVersion2 = Version.composeKafkaTopic(storeName, 2);
    Assert.assertTrue(topicManager.isTopicCompactionEnabled(topicForStoreVersion2), "topic: " + topicForStoreVersion2 + " should have compaction enabled");

    // Verify streaming record in second version
    checkLargeRecord(client, 2);
    assertEquals(client.get("19").get().toString(), "test_name_19");

    // TODO: Would be great to eliminate this wait time...
    logger.info("***** Sleeping to get outside of rewind time: " + streamingRewindSeconds + " seconds");
    Utils.sleep(TimeUnit.MILLISECONDS.convert(streamingRewindSeconds, TimeUnit.SECONDS));

    // Write more streaming records
    if (multiDivStream) {
      veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM); // new producer, new DIV segment.
    }
    for (int i=10; i<=20; i++) {
      sendCustomSizeStreamingRecord(veniceProducer, storeName, i, STREAMING_RECORD_SIZE);
    }
    TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
      try {
        checkLargeRecord(client, 19);
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    // Run H2V a third Time
    runH2V(h2vProperties, 3, controllerClient);
    // verify the topic compaction policy
    String topicForStoreVersion3 = Version.composeKafkaTopic(storeName, 3);
    Assert.assertTrue(topicManager.isTopicCompactionEnabled(topicForStoreVersion3), "topic: " + topicForStoreVersion3 + " should have compaction enabled");

    // Verify new streaming record in third version
    TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
      try {
        checkLargeRecord(client, 19);
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });
    // But not old streaming record (because we waited the rewind time)
    assertEquals(client.get("2").get().toString(),"test_name_2");

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      assertFalse(storeResponse.isError());
      List<Integer> versions =
          storeResponse.getStore().getVersions().stream().map(Version::getNumber).collect(Collectors.toList());
      assertFalse(versions.contains(1), "After version 3 comes online, version 1 should be retired");
      assertTrue(versions.contains(2));
      assertTrue(versions.contains(3));
    });

    /**
     * For L/F model, {@link com.linkedin.venice.replication.LeaderStorageNodeReplicator#doesReplicationExist(String, String)}
     * would always return false.
     */
    if (!isLeaderFollowerModelEnabled) {
      // Verify replication exists for versions 2 and 3, but not for version 1
      VeniceHelixAdmin veniceHelixAdmin = (VeniceHelixAdmin) venice.getMasterVeniceController().getVeniceAdmin();
      Field topicReplicatorField = veniceHelixAdmin.getClass().getDeclaredField("onlineOfflineTopicReplicator");
      topicReplicatorField.setAccessible(true);
      Optional<TopicReplicator> topicReplicatorOptional = (Optional<TopicReplicator>) topicReplicatorField.get(veniceHelixAdmin);
      if (topicReplicatorOptional.isPresent()) {
        TopicReplicator topicReplicator = topicReplicatorOptional.get();
        String realtimeTopic = Version.composeRealTimeTopic(storeName);
        String versionOneTopic = Version.composeKafkaTopic(storeName, 1);
        String versionTwoTopic = Version.composeKafkaTopic(storeName, 2);
        String versionThreeTopic = Version.composeKafkaTopic(storeName, 3);

        assertFalse(topicReplicator.doesReplicationExist(realtimeTopic, versionOneTopic), "Replication stream must not exist for retired version 1");
        assertTrue(topicReplicator.doesReplicationExist(realtimeTopic, versionTwoTopic), "Replication stream must still exist for backup version 2");
        assertTrue(topicReplicator.doesReplicationExist(realtimeTopic, versionThreeTopic), "Replication stream must still exist for current version 3");
      } else {
        fail("Venice cluster must have a topic replicator for hybrid to be operational"); //this shouldn't happen
      }
    }

    controllerClient.listInstancesStatuses().getInstancesStatusMap().keySet().stream()
        .forEach(s -> logger.info("Replicas for " + s + ": "
            + Arrays.toString(controllerClient.listStorageNodeReplicas(s).getReplicas())));

    /**
     * Disable the restart server test for L/F model for now until the fix in rb1753313 is merged;
     * "controllerClient.listStoresStatuses()" would eventually calling
     * {@link com.linkedin.venice.controller.kafka.StoreStatusDecider#getStoreStatues(List, ResourceAssignment, VeniceControllerClusterConfig)}
     * which uses {@link Partition#getReadyToServeInstances()} to find the online replicas for the store; however, for
     * L/F model, the "stateToInstancesMap" inside Partition only contains state like "STANDBY" and "LEADER"; we should
     * use PushMonitor inside StoreStatusDecider to get the ready to serve instances.
     *
     * TODO: Once rb1753313 is merged, let's tweak the code below so that we re-query the data after the first server is killed
     */
    if (!isLeaderFollowerModelEnabled) {
      // TODO will move this test case to a single fail-over integration test.
      //Stop one server
      int port = venice.getVeniceServers().get(0).getPort();
      venice.stopVeniceServer(port);
      TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, true, () -> {
        // Make sure Helix knows the instance is shutdown
        Map<String, String> storeStatus = controllerClient.listStoresStatuses().getStoreStatusMap();
        Assert.assertTrue(storeStatus.get(storeName).equals(StoreStatus.UNDER_REPLICATED.toString()),
            "Should be UNDER_REPLICATED");

        Map<String, String> instanceStatus = controllerClient.listInstancesStatuses().getInstancesStatusMap();
        Assert.assertTrue(instanceStatus.entrySet().stream()
                .filter(entry -> entry.getKey().contains(Integer.toString(port)))
                .map(entry -> entry.getValue())
                .allMatch(s -> s.equals(InstanceStatus.DISCONNECTED.toString())),
            "Storage Node on port " + port + " should be DISCONNECTED");
      });

      //Restart one server
      venice.restartVeniceServer(port);
      TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, true, () -> {
      // Make sure Helix knows the instance has recovered
        Map<String, String> storeStatus = controllerClient.listStoresStatuses().getStoreStatusMap();
        Assert.assertTrue(storeStatus.get(storeName).equals(StoreStatus.FULLLY_REPLICATED.toString()),
            "Should be FULLLY_REPLICATED");
      });
    }
    veniceProducer.stop();
    IOUtils.closeQuietly(topicManager);
    venice.close();
  }

  private void checkLargeRecord(AvroGenericStoreClient client, int index)
      throws ExecutionException, InterruptedException {
    String key = Integer.toString(index);
    String value = client.get(key).get().toString();
    assertEquals(value.length(), STREAMING_RECORD_SIZE);

    String expectedChar = Integer.toString(index).substring(0, 1);
    for (int j = 0; j < value.length(); j++) {
      assertEquals(value.substring(j, j + 1), expectedChar);
    }
  }

  @Test(dataProvider = "isLeaderFollowerModelEnabled")
  public void testSamzaBatchLoad(boolean isLeaderFollowerModelEnabled) throws Exception {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));
    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster(1,1,1,1, 1000000, false, false, extraProperties);
    Admin admin = veniceClusterWrapper.getMasterVeniceController().getVeniceAdmin();
    String clusterName = veniceClusterWrapper.getClusterName();
    String storeName = "test-store";
    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;

    // Create empty store
    admin.addStore(clusterName, storeName, "tester", "\"string\"", "\"string\"");
    admin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(streamingRewindSeconds)
        .setHybridOffsetLagThreshold(streamingMessageLag)
        .setLeaderFollowerModel(isLeaderFollowerModelEnabled)
    );
    Assert.assertFalse(admin.getStore(clusterName, storeName).containsVersion(1));
    Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 0);


    // Batch load from Samza
    VeniceSystemFactory factory = new VeniceSystemFactory();
    Version.PushType pushType = isLeaderFollowerModelEnabled ? Version.PushType.STREAM_REPROCESSING : Version.PushType.BATCH;
    Map<String, String> samzaConfig = getSamzaProducerConfig(veniceClusterWrapper, storeName, pushType);
    SystemProducer veniceBatchProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
    if (veniceBatchProducer instanceof VeniceSystemProducer) {
      // The default behavior would exit the process
      ((VeniceSystemProducer) veniceBatchProducer).setExitMode(SamzaExitMode.NO_OP);
    }
    for (int i=10; i>=1; i--) { // Purposefully out of order, because Samza batch jobs should be allowed to write out of order
      sendStreamingRecord(veniceBatchProducer, storeName, i);
    }

    Assert.assertTrue(admin.getStore(clusterName, storeName).containsVersion(1));
    Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 0);
    // Before EOP, the Samza batch producer should still be in active state
    Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 1);

    // Write END_OF_PUSH message
    // TODO: in the future we would like to automatically send END_OF_PUSH message after batch load from Samza
    // TODO: the SystemProducer interface is currently restricting us from sending end of push deterministically.
    Utils.sleep(500);
    veniceClusterWrapper.getControllerClient().writeEndOfPush(storeName, 1);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Assert.assertTrue(admin.getStore(clusterName, storeName).containsVersion(1));
      Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 1);
      if (isLeaderFollowerModelEnabled) {
        // After EOP, the push monitor inside the system producer would mark the producer as inactive in the factory
        Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 0);
      }
    });

    // Verify data, note only 1-10 have been pushed so far
    AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()));
    for (int i = 1; i < 10; i++){
      String key = Integer.toString(i);
      Assert.assertEquals(client.get(key).get().toString(), "stream_" + key);
    }
    Assert.assertTrue(client.get(Integer.toString(11)).get() == null, "This record should not be found");

    // Switch to stream mode and push more data
    SystemProducer veniceStreamProducer = getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
    for (int i=11; i<=20; i++) {
      sendStreamingRecord(veniceStreamProducer, storeName, i);
    }
    Assert.assertTrue(admin.getStore(clusterName, storeName).containsVersion(1));
    Assert.assertFalse(admin.getStore(clusterName, storeName).containsVersion(2));
    Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 1);

    // Verify both batch and stream data
    /**
     * Leader would wait for 5 seconds before switching to real-time topic.
     */
    long extraWaitTime = isLeaderFollowerModelEnabled
        ? TimeUnit.SECONDS.toMillis(Long.valueOf(extraProperties.getProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS)))
        : 0L;
    long normalTimeForConsuming = TimeUnit.SECONDS.toMillis(3);
    logger.info("normalTimeForConsuming:" + normalTimeForConsuming + "; extraWaitTime:" + extraWaitTime);
    Utils.sleep(normalTimeForConsuming + extraWaitTime);
    for (int i = 1; i < 20; i++){
      String key = Integer.toString(i);
      Assert.assertEquals(client.get(key).get().toString(), "stream_" + key);
    }
    Assert.assertTrue(client.get(Integer.toString(21)).get() == null, "This record should not be found");

    veniceClusterWrapper.close();
  }

  @Test
  public void testMultiStreamReprocessingSystemProducers() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));
    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster(1,1,1,1, 1000000, false, false, extraProperties);
    Admin admin = veniceClusterWrapper.getMasterVeniceController().getVeniceAdmin();
    String clusterName = veniceClusterWrapper.getClusterName();
    String storeName1 = "test-store1";
    String storeName2 = "test-store2";
    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;

    // create 2 stores
    // Create empty store
    admin.addStore(clusterName, storeName1, "tester", "\"string\"", "\"string\"");
    admin.addStore(clusterName, storeName2, "tester", "\"string\"", "\"string\"");
    admin.updateStore(clusterName, storeName1, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(streamingRewindSeconds)
        .setHybridOffsetLagThreshold(streamingMessageLag)
        .setLeaderFollowerModel(true)
    );
    admin.updateStore(clusterName, storeName2, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(streamingRewindSeconds)
        .setHybridOffsetLagThreshold(streamingMessageLag)
        .setLeaderFollowerModel(true)
    );
    Assert.assertFalse(admin.getStore(clusterName, storeName1).containsVersion(1));
    Assert.assertEquals(admin.getStore(clusterName, storeName1).getCurrentVersion(), 0);
    Assert.assertFalse(admin.getStore(clusterName, storeName2).containsVersion(1));
    Assert.assertEquals(admin.getStore(clusterName, storeName2).getCurrentVersion(), 0);

    // Batch load from Samza to both stores
    VeniceSystemFactory factory = new VeniceSystemFactory();
    Map<String, String> samzaConfig1 = getSamzaProducerConfig(veniceClusterWrapper, storeName1, Version.PushType.STREAM_REPROCESSING);
    SystemProducer veniceBatchProducer1 = factory.getProducer("venice", new MapConfig(samzaConfig1), null);
    Map<String, String> samzaConfig2 = getSamzaProducerConfig(veniceClusterWrapper, storeName2, Version.PushType.STREAM_REPROCESSING);
    SystemProducer veniceBatchProducer2 = factory.getProducer("venice", new MapConfig(samzaConfig2), null);
    if (veniceBatchProducer1 instanceof VeniceSystemProducer) {
      // The default behavior would exit the process
      ((VeniceSystemProducer) veniceBatchProducer1).setExitMode(SamzaExitMode.NO_OP);
    }
    if (veniceBatchProducer2 instanceof VeniceSystemProducer) {
      // The default behavior would exit the process
      ((VeniceSystemProducer) veniceBatchProducer2).setExitMode(SamzaExitMode.NO_OP);
    }

    for (int i=10; i>=1; i--) {
      sendStreamingRecord(veniceBatchProducer1, storeName1, i);
      sendStreamingRecord(veniceBatchProducer2, storeName2, i);
    }

    // Before EOP, there should be 2 active producers
    Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 2);
    /**
     * Send EOP to the first store, eventually the first SystemProducer will be marked as inactive
     * after push monitor poll the latest push job status from router.
     */
    Utils.sleep(500);
    veniceClusterWrapper.getControllerClient().writeEndOfPush(storeName1, 1);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Assert.assertTrue(admin.getStore(clusterName, storeName1).containsVersion(1));
      Assert.assertEquals(admin.getStore(clusterName, storeName1).getCurrentVersion(), 1);
      // The second SystemProducer should still be active
      Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 1);
    });

    veniceClusterWrapper.getControllerClient().writeEndOfPush(storeName2, 1);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Assert.assertTrue(admin.getStore(clusterName, storeName2).containsVersion(1));
      Assert.assertEquals(admin.getStore(clusterName, storeName2).getCurrentVersion(), 1);
      // There should be no active SystemProducer any more.
      Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 0);
    });
  }

  @Test(dataProvider = "isLeaderFollowerModelEnabled")
  public void testHybridWithBufferReplayDisabled(boolean isLeaderFollowerModelEnabled) throws Exception {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1,1,1,1, 1000000, false, false, extraProperties);

    List<VeniceControllerWrapper> controllers = venice.getVeniceControllers();
    Assert.assertEquals(controllers.size(), 1, "There should only be one controller");
    // Create a controller with buffer replay disabled, and remove the previous controller
    Properties controllerProps = new Properties();
    controllerProps.put(CONTROLLER_SKIP_BUFFER_REPLAY_FOR_HYBRID, true);
    venice.addVeniceController(controllerProps);
    List<VeniceControllerWrapper> newControllers = venice.getVeniceControllers();
    Assert.assertEquals(newControllers.size(), 2, "There should be two controllers now");
    // Shutdown the original controller, now there is only one controller with config: buffer replay disabled.
    controllers.get(0).close();

    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;

    String storeName = TestUtils.getUniqueString("hybrid-store");

    //Create store , make it a hybrid store
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), venice.getAllControllersURLs());
    controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA);
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setHybridRewindSeconds(streamingRewindSeconds)
        .setHybridOffsetLagThreshold(streamingMessageLag)
        .setLeaderFollowerModel(isLeaderFollowerModelEnabled)
    );

    // Create a new version, and do an empty push for that version
    VersionCreationResponse vcr = controllerClient.emptyPush(storeName, TestUtils.getUniqueString("empty-hybrid-push"), 1L);
    int versionNumber = vcr.getVersion();
    assertNotEquals(versionNumber, 0, "requesting a topic for a push should provide a non zero version number");
    int partitionCnt = vcr.getPartitions();

    // Continue to write more records to the version topic
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, venice.getKafka().getAddress());
    VeniceWriter<byte[], byte[], byte[]> writer = new VeniceWriterFactory(veniceWriterProperties).createBasicVeniceWriter(Version.composeKafkaTopic(storeName, versionNumber));

    // Mock buffer replay message
    if (!isLeaderFollowerModelEnabled) {
      List<Long> bufferReplyOffsets = new ArrayList<>();
      for (int i = 0; i < partitionCnt; ++i) {
        bufferReplyOffsets.add(1l);
      }
      writer.broadcastStartOfBufferReplay(bufferReplyOffsets, "", "", new HashMap<>());
    }

    // Write 100 records
    AvroSerializer<String> stringSerializer = new AvroSerializer(Schema.parse(STRING_SCHEMA));
    for (int i = 1; i <= 100; ++i) {
      writer.put(stringSerializer.serialize("key_" + i), stringSerializer.serialize("value_" + i), 1);
    }
    // Wait for up to 10 seconds
    TestUtils.waitForNonDeterministicAssertion(10 * 1000, TimeUnit.MILLISECONDS, () -> {
      StoreResponse store = controllerClient.getStore(storeName);
      Assert.assertEquals(store.getStore().getCurrentVersion(), 1);
    });

    //Verify some records (note, records 1-100 have been pushed)
    AvroGenericStoreClient client =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
    for (int i = 1; i <= 10; i++){
      String key = "key_" + i;
      assertEquals(client.get(key).get().toString(), "value_" + i);
    }

    // And real-time topic should not exist since buffer replay is skipped.
    TopicManager topicManager = new TopicManager(venice.getZk().getAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(venice.getKafka().getAddress()));
    assertFalse(topicManager.containsTopic(Version.composeRealTimeTopic(storeName)));
    IOUtils.closeQuietly(topicManager);

    venice.close();
  }

  @Test
  public void testLeaderHonorLastTopicSwitchMessage() throws Exception {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(10L));
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1,2,1,2, 1000000, false, false, extraProperties);

    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;

    String storeName = TestUtils.getUniqueString("hybrid-store");

    //Create store , make it a hybrid store
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), venice.getAllControllersURLs());
    controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA);
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setHybridRewindSeconds(streamingRewindSeconds)
        .setHybridOffsetLagThreshold(streamingMessageLag)
        .setLeaderFollowerModel(true)
    );

    // Keep a early rewind start time to cover all messages in any topic
    long rewindStartTime = System.currentTimeMillis();

    // Create a new version, and do an empty push for that version
    VersionCreationResponse vcr = controllerClient.emptyPush(storeName, TestUtils.getUniqueString("empty-hybrid-push"), 1L);
    int versionNumber = vcr.getVersion();
    Assert.assertEquals(versionNumber, 1, "Version number should become 1 after an empty-push");
    int partitionCnt = vcr.getPartitions();

    /**
     * Write 2 TopicSwitch messages into version topic:
     * TS1 (new topic: storeName_tmp1, startTime: {@link rewindStartTime})
     * TS2 (new topic: storeName_tmp2, startTime: {@link rewindStartTime})
     *
     * All messages in TS1 should not be replayed into VT and should not be queryable;
     * but messages in TS2 should be replayed and queryable.
     */
    String tmpTopic1 = storeName + "_tmp1";
    String tmpTopic2 = storeName + "_tmp2";
    TopicManager topicManager = venice.getMasterVeniceController().getVeniceAdmin().getTopicManager();
    topicManager.createTopic(tmpTopic1, partitionCnt, 1, true);
    topicManager.createTopic(tmpTopic2, partitionCnt, 1, true);

    /**
     *  Build a producer that writes to {@link tmpTopic1}
     */
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, venice.getKafka().getAddress());
    VeniceWriter<byte[], byte[], byte[]> tmpWriter1 = new VeniceWriterFactory(veniceWriterProperties).createBasicVeniceWriter(tmpTopic1);
    // Write 10 records
    AvroSerializer<String> stringSerializer = new AvroSerializer(Schema.parse(STRING_SCHEMA));
    for (int i = 0; i < 10; ++i) {
      tmpWriter1.put(stringSerializer.serialize("key_" + i), stringSerializer.serialize("value_" + i), 1);
    }

    /**
     *  Build a producer that writes to {@link tmpTopic2}
     */
    VeniceWriter<byte[], byte[], byte[]> tmpWriter2 = new VeniceWriterFactory(veniceWriterProperties).createBasicVeniceWriter(tmpTopic2);
    // Write 10 records
    for (int i = 10; i < 20; ++i) {
      tmpWriter2.put(stringSerializer.serialize("key_" + i), stringSerializer.serialize("value_" + i), 1);
    }

    /**
     * Wait for leader to switch over to real-time topic
     */
    TestUtils.waitForNonDeterministicAssertion(20 * 1000, TimeUnit.MILLISECONDS, () -> {
          StoreResponse store = controllerClient.getStore(storeName);
          Assert.assertEquals(store.getStore().getCurrentVersion(), 1);
        });
    // Build a producer to produce 2 TS messages into RT
    VeniceWriter<byte[], byte[], byte[]> realTimeTopicWriter = new VeniceWriterFactory(veniceWriterProperties).createBasicVeniceWriter(Version.composeRealTimeTopic(storeName));

    realTimeTopicWriter.broadcastTopicSwitch(Arrays.asList(venice.getKafka().getAddress()), tmpTopic1, rewindStartTime, null);
    realTimeTopicWriter.broadcastTopicSwitch(Arrays.asList(venice.getKafka().getAddress()), tmpTopic2, rewindStartTime, null);

    /**
     * Verify that all messages from {@link tmpTopic2} are in store and no message from {@link tmpTopic1} is in store.
     */
    AvroGenericStoreClient client =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
    TestUtils.waitForNonDeterministicAssertion(30 * 1000, TimeUnit.MILLISECONDS, () -> {
      // All messages from tmpTopic2 should exist
      try {
        for (int i = 10; i < 20; i++) {
          String key = "key_" + i;
          Assert.assertEquals(client.get(key).get().toString(), "value_" + i);
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }

      // No message from tmpTopic1 should exist
      try {
        for (int i = 0; i < 10; i++) {
          String key = "key_" + i;
          Object value = client.get(key).get();
          Assert.assertEquals(value, null);
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    });

    venice.close();
  }

  /**
   * Blocking, waits for new version to go online
   */
  private static void runH2V(Properties h2vProperties, int expectedVersionNumber, ControllerClient controllerClient) throws Exception {
    long h2vStart = System.currentTimeMillis();
    String jobName = TestUtils.getUniqueString("hybrid-job-" + expectedVersionNumber);
    KafkaPushJob job = new KafkaPushJob(jobName, h2vProperties);
    job.run();
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS,
        () -> controllerClient.getStore((String) h2vProperties.get(KafkaPushJob.VENICE_STORE_NAME_PROP))
            .getStore().getCurrentVersion() == expectedVersionNumber);
    logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
  }
}
