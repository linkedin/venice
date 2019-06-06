package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.InstanceStatus;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.kafka.TopicManager.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class TestHybrid {
  private static final Logger logger = Logger.getLogger(TestHybrid.class);

  @Test
  public void testHybridInitializationOnMultiColo(){
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1,2,1,1, 1000000, false, false);
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
            .setHybridOffsetLagThreshold(streamingMessageLag));

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
    IOUtils.closeQuietly(topicManager);

    parentController.close();
    parentZk.close();
    venice.close();
  }

  @Test
  public void testHybridEndToEndWithMultiDivStream() throws Exception {
    testHybridEndToEnd(true);
  }

  @Test
  public void testHybridEndToEnd() throws Exception {
    testHybridEndToEnd(false);
  }

  /**
   * This test validates the hybrid batch + streaming semantics and verifies that configured rewind time works as expected.
   *
   * TODO: This test needs to be refactored in order to leverage {@link com.linkedin.venice.utils.MockTime},
   *       which would allow the test to run faster and more deterministically.
   *
   * This is a slow test, so it is given priority -10 so that it starts first.

   * @param multiDivStream if false, rewind will happen in the middle of a DIV Segment, which was originally broken.
   *                       if true, two independent DIV Segments will be placed before and after the start of buffer replay.
   *
   *                       If this test succeeds with {@param multiDivStream} set to true, but fails with it set to false,
   *                       then there is a regression in the DIV partial segment tolerance after EOP.
   */
  public void testHybridEndToEnd(boolean multiDivStream) throws Exception {
    logger.info("About to create VeniceClusterWrapper");
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1,1,1,1, 1000000, false, false);
    logger.info("Finished creating VeniceClusterWrapper");

    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;

    String storeName = TestUtils.getUniqueString("hybrid-store");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir); // records 1-100
    Properties h2vProperties = defaultH2VProps(venice, inputDirPath, storeName);

    ControllerClient controllerClient = createStoreForJob(venice, recordSchema, h2vProperties);

    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(streamingRewindSeconds)
        .setHybridOffsetLagThreshold(streamingMessageLag));

    //Do an H2V push
    runH2V(h2vProperties, 1, controllerClient);

    //Verify some records (note, records 1-100 have been pushed)
    AvroGenericStoreClient client =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
    for (int i=1;i<10;i++){
      String key = Integer.toString(i);
      assertEquals(client.get(key).get().toString(), "test_name_" + key);
    }

    //write streaming records
    SystemProducer veniceProducer = getSamzaProducer(venice, storeName, ControllerApiConstants.PushType.STREAM);
    for (int i=1; i<=10; i++) {
      sendStreamingRecord(veniceProducer, storeName, i);
    }
    if (multiDivStream) {
      veniceProducer.stop(); //close out the DIV segment
    }

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      try {
        assertEquals(client.get("2").get().toString(),"stream_2");
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    runH2V(h2vProperties, 2, controllerClient);

    // Verify streaming record in second version
    assertEquals(client.get("2").get().toString(),"stream_2");
    assertEquals(client.get("19").get().toString(), "test_name_19");

    logger.info("***** Sleeping to get outside of rewind time: " + streamingRewindSeconds + " seconds");
    Utils.sleep(TimeUnit.MILLISECONDS.convert(streamingRewindSeconds, TimeUnit.SECONDS));

    // Write more streaming records
    if (multiDivStream) {
      veniceProducer = getSamzaProducer(venice, storeName, ControllerApiConstants.PushType.STREAM); // new producer, new DIV segment.
    }
    for (int i=10; i<=20; i++) {
      sendStreamingRecord(veniceProducer, storeName, i);
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      try {
        assertEquals(client.get("19").get().toString(),"stream_19");
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    // Run H2V a third Time
    runH2V(h2vProperties, 3, controllerClient);

    // Verify new streaming record in third version
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      try {
        assertEquals(client.get("19").get().toString(),"stream_19");
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });
    // But not old streaming record (because we waited the rewind time)
    assertEquals(client.get("2").get().toString(),"test_name_2");

    StoreResponse storeResponse = controllerClient.getStore(storeName);
    List<Integer> versions = storeResponse.getStore().getVersions()
        .stream().map(version -> version.getNumber()).collect(Collectors.toList());

    assertFalse(versions.contains(1), "After version 3 comes online, version 1 should be retired");
    assertTrue(versions.contains(2));
    assertTrue(versions.contains(3));

    // Verify replication exists for versions 2 and 3, but not for version 1
    VeniceHelixAdmin veniceHelixAdmin = (VeniceHelixAdmin) venice.getMasterVeniceController().getVeniceAdmin();
    Field topicReplicatorField = veniceHelixAdmin.getClass().getDeclaredField("topicReplicator");
    topicReplicatorField.setAccessible(true);
    Optional<TopicReplicator> topicReplicatorOptional = (Optional<TopicReplicator>) topicReplicatorField.get(veniceHelixAdmin);
    if (topicReplicatorOptional.isPresent()){
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

    // TODO will move this test case to a single fail-over integration test.
    //Stop one server
    int port = venice.getVeniceServers().get(0).getPort();
    venice.stopVeniceServer(port);
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, ()->{
      Map<String,String> instanceStatus = controllerClient.listInstancesStatuses().getInstancesStatusMap();
      // Make sure Helix know the instance is completed shutdown
      if(instanceStatus.values().iterator().next().equals(InstanceStatus.DISCONNECTED.toString())){
        return true;
      }
      return false;
    });

    //Restart one server
    venice.restartVeniceServer(port);
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      Map<String, String> storeStatus =
          controllerClient.listStoresStatuses().getStoreStatusMap();
      // Make sure Helix know the instance is completed shutdown
      if (storeStatus.get(storeName).equals(StoreStatus.FULLLY_REPLICATED.toString())) {
        return true;
      }
      return false;
    });
    veniceProducer.stop();
    venice.close();
  }

  @Test
  public void testSamzaBatchLoad() throws Exception {
    VeniceClusterWrapper veniceClusterWrapper = ServiceFactory.getVeniceCluster(1,1,1,1, 1000000, false, false);
    Admin admin = veniceClusterWrapper.getMasterVeniceController().getVeniceAdmin();
    String clusterName = veniceClusterWrapper.getClusterName();
    String storeName = "test-store";
    long streamingRewindSeconds = 25L;
    long streamingMessageLag = 2L;

    // Create empty store
    admin.addStore(clusterName, storeName, "tester", "\"string\"", "\"string\"");
    admin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(streamingRewindSeconds)
        .setHybridOffsetLagThreshold(streamingMessageLag));
    Assert.assertFalse(admin.getStore(clusterName, storeName).containsVersion(1));
    Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 0);


    // Batch load from Samza
    SystemProducer veniceBatchProducer = getSamzaProducer(veniceClusterWrapper, storeName, ControllerApiConstants.PushType.BATCH);
    for (int i=1; i<=10; i++) {
      sendStreamingRecord(veniceBatchProducer, storeName, i);
    }
    Assert.assertTrue(admin.getStore(clusterName, storeName).containsVersion(1));
    Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 0);

    // Write END_OF_PUSH message
    // TODO: in the future we would like to automatically send END_OF_PUSH message after batch load from Samza
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, veniceClusterWrapper.getKafka().getAddress());
    VeniceWriter<byte[], byte[]> writer = new VeniceWriterFactory(veniceWriterProperties).getBasicVeniceWriter(storeName + "_v1");
    writer.broadcastEndOfPush(new HashMap<>());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      Assert.assertTrue(admin.getStore(clusterName, storeName).containsVersion(1));
      Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 1);
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
    SystemProducer veniceStreamProducer = getSamzaProducer(veniceClusterWrapper, storeName, ControllerApiConstants.PushType.STREAM);
    for (int i=11; i<=20; i++) {
      sendStreamingRecord(veniceStreamProducer, storeName, i);
    }
    Assert.assertTrue(admin.getStore(clusterName, storeName).containsVersion(1));
    Assert.assertFalse(admin.getStore(clusterName, storeName).containsVersion(2));
    Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 1);

    // Verify both batch and stream data
    Utils.sleep(3000);
    for (int i = 1; i < 20; i++){
      String key = Integer.toString(i);
      Assert.assertEquals(client.get(key).get().toString(), "stream_" + key);
    }
    Assert.assertTrue(client.get(Integer.toString(21)).get() == null, "This record should not be found");

    veniceClusterWrapper.close();
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



  @Test
  public void testHybridWithBufferReplayDisabled() throws Exception {
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1,1,1,1, 1000000, false, false);

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
        .setHybridOffsetLagThreshold(streamingMessageLag));

    // Create a new version, and do an empty push for that version
    VersionCreationResponse vcr = controllerClient.emptyPush(storeName, TestUtils.getUniqueString("empty-hybrid-push"), 1L);
    int versionNumber = vcr.getVersion();
    assertNotEquals(versionNumber, 0, "requesting a topic for a push should provide a non zero version number");
    int partitionCnt = vcr.getPartitions();

    // Continue to write more records to the version topic
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, venice.getKafka().getAddress());
    VeniceWriter<byte[], byte[]> writer = new VeniceWriterFactory(veniceWriterProperties).getBasicVeniceWriter(Version.composeKafkaTopic(storeName, versionNumber));

    // Mock buffer replay message
    List<Long> bufferReplyOffsets = new ArrayList<>();
    for (int i = 0; i < partitionCnt; ++i) {
      bufferReplyOffsets.add(1l);
    }
    writer.broadcastStartOfBufferReplay(bufferReplyOffsets, "", "", new HashMap<>());

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
}
