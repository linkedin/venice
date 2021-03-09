package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;

import io.tehuti.Metric;

import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestMultiDataCenterPush {
  private static final Logger LOGGER = Logger.getLogger(TestMultiDataCenterPush.class);
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 2;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childClusters;
  private List<List<VeniceControllerWrapper>> childControllers;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  private final byte[] emptyKeyBytes = new byte[]{'a'};

  @BeforeClass
  public void setUp() {
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(NUMBER_OF_CHILD_DATACENTERS, NUMBER_OF_CLUSTERS, 1, 1, 1, 1);

    childClusters = multiColoMultiClusterWrapper.getClusters();
    childControllers = childClusters.stream()
        .map(veniceClusterWrapper -> veniceClusterWrapper.getControllers()
            .values()
            .stream()
            .collect(Collectors.toList()))
        .collect(Collectors.toList());
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();

    LOGGER.info("parentControllers: " + parentControllers.stream()
        .map(c -> c.getControllerUrl())
        .collect(Collectors.joining(", ")));

    int i = 0;
    for (VeniceMultiClusterWrapper multiClusterWrapper : childClusters) {
      LOGGER.info("childCluster" + i++ + " controllers: " + multiClusterWrapper.getControllers()
          .values()
          .stream()
          .map(c -> c.getControllerUrl())
          .collect(Collectors.joining(", ")));
    }
  }

  @AfterClass
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMultiDataCenterPush() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    createStoreForJob(clusterName, recordSchema, props).close();

    try (KafkaPushJob job = new KafkaPushJob("Test push job", props)) {
      job.run();
      // Verify job properties
      Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, 1));
      for (int version : parentController.getVeniceAdmin()
          .getCurrentVersionsForMultiColos(clusterName, storeName)
          .values()) {
        Assert.assertEquals(version, 1);
      }
      Assert.assertEquals(job.getInputDirectory(), inputDirPath);
      String schema =
          "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
      Assert.assertEquals(job.getFileSchemaString(), schema);
      Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getInputFileDataSize(), 3872);

      // Verify the data in Venice Store
      for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
        VeniceMultiClusterWrapper veniceCluster = childClusters.get(dataCenterIndex);
        String routerUrl = veniceCluster.getClusters().get(clusterName).getRandomRouterURL();
        try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
          for (int i = 1; i <= 100; ++i) {
            String expected = "test_name_" + i;
            String actual = client.get(Integer.toString(i)).get().toString(); /* client.get().get() returns a Utf8 object */
            Assert.assertEquals(actual, expected);
          }

          try (ControllerClient controllerClient = new ControllerClient(clusterName, routerUrl)) {
            JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(job.getKafkaTopic());
            Assert.assertFalse(jobStatus.isError(), "Error in getting JobStatusResponse: " + jobStatus.getError());
            Assert.assertEquals(jobStatus.getStatus(), ExecutionStatus.COMPLETED.toString(),
                "After job is complete, status should reflect that");
            // We won't verify progress any more here since we decided to disable this feature
          }
        }
      }
    }

    /**
     * To speed up integration test, here reuses the same test case to verify topic clean up logic.
     *
     * TODO: update service factory to allow specifying {@link com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_STORE_VERSIONS_TO_PRESERVE}
     * and {@link com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE} to reduce job run times.
     */
    for (int i = 2; i <= 3; i++) {
      try (KafkaPushJob job = new KafkaPushJob("Test push job " + i, props)) {
        job.run();
      }
    }

    String v1Topic = storeName + "_v1";
    String v2Topic = storeName + "_v2";
    String v3Topic = storeName + "_v3";

    // Verify the topics in parent controller
    TopicManager parentTopicManager = parentController.getVeniceAdmin().getTopicManager();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      Assert.assertFalse(parentTopicManager.containsTopicAndAllPartitionsAreOnline(v1Topic), "Topic: " + v1Topic + " should be deleted after push");
      Assert.assertFalse(parentTopicManager.containsTopicAndAllPartitionsAreOnline(v2Topic), "Topic: " + v2Topic + " should be deleted after push");
      Assert.assertFalse(parentTopicManager.containsTopicAndAllPartitionsAreOnline(v3Topic), "Topic: " + v3Topic + " should be deleted after push");
    });

    // Verify the topics in child controller
    TopicManager childTopicManager = childControllers.get(0).get(0).getVeniceAdmin().getTopicManager();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      Assert.assertFalse(childTopicManager.containsTopicAndAllPartitionsAreOnline(v1Topic), "Topic: " + v1Topic + " should be deleted after 3 pushes");
    });
    Assert.assertTrue(childTopicManager.containsTopicAndAllPartitionsAreOnline(v2Topic), "Topic: " + v2Topic + " should be kept after 3 pushes");
    Assert.assertTrue(childTopicManager.containsTopicAndAllPartitionsAreOnline(v3Topic), "Topic: " + v3Topic + " should be kept after 3 pushes");

    /**
     * In order to speed up integration test, reuse the multi data center cluster for hybrid store RT topic retention time testing
     */
    String hybridStoreName = TestUtils.getUniqueString("hybrid_store");
    Properties pushJobPropsForHybrid = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, hybridStoreName);
    // Create a hybrid store.
    createStoreForJob(clusterName, recordSchema, pushJobPropsForHybrid).close();
    /**
     * Set a high rewind time, higher than the default 5 days retention time.
     */
    long highRewindTimeInSecond = 30L * Time.SECONDS_PER_DAY; // Rewind time is one month.
    try (ControllerClient controllerClientToParent = new ControllerClient(clusterName, parentController.getControllerUrl())) {
      controllerClientToParent.updateStore(hybridStoreName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(highRewindTimeInSecond).setHybridOffsetLagThreshold(10));
    }
    /**
     * A batch push for hybrid store would trigger the child fabrics to create RT topic.
     */
    TestPushUtils.runPushJob("Test push for hybrid", pushJobPropsForHybrid);

    /**
     * RT topic retention time should be longer than the rewind time.
     */
    String realTimeTopic = hybridStoreName + "_rt";
    long topicRetentionTimeInSecond = TimeUnit.MILLISECONDS.toSeconds(childTopicManager.getTopicRetention(realTimeTopic));
    Assert.assertTrue(topicRetentionTimeInSecond >= highRewindTimeInSecond);
  }

  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Failed to create new store version.*", timeOut = TEST_TIMEOUT)
  public void testPushDirectlyToChildColo() throws IOException {
    // In multi-colo setup, the batch push to child controller should be disabled.
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    String childControllerUrl = childControllers.get(0).get(0).getControllerUrl();
    Properties props = defaultH2VProps(childControllerUrl, inputDirPath, storeName);
    createStoreForJob(clusterName, recordSchema, props).close();

    TestPushUtils.runPushJob("Test push job", props);
  }

  @DataProvider (name = "testEmptyPushDataProvider")
  public Object[][] testEmptyPushDataProvider() {
    return new Object[][] { {true}, {false} };
  }

  @Test (dataProvider = "testEmptyPushDataProvider", timeOut = TEST_TIMEOUT)
  public void testEmptyPush(boolean toParent) {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = TestUtils.getUniqueString("store");
    String parentControllerUrl = parentControllers.get(0).getControllerUrl();
    String childControllerUrl = childControllers.get(0).get(0).getControllerUrl();

    // Create store first
    ControllerClient controllerClientToParent = new ControllerClient(clusterName, parentControllerUrl);
    controllerClientToParent.createNewStore(storeName, "test_owner", "\"int\"", "\"int\"");

    ControllerClient controllerClient = new ControllerClient(clusterName, toParent ? parentControllerUrl : childControllerUrl);
    VersionCreationResponse response = controllerClient.emptyPush(storeName, "test_push_id", 1000);
    if (toParent) {
      Assert.assertFalse(response.isError(), "Empty push to parent colo should succeed");
    } else {
      Assert.assertTrue(response.isError(), "Empty push to child colo should be blocked");
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridConfigPartitionerConfigConflict() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = TestUtils.getUniqueString("store");
    String parentControllerUrl = parentControllers.get(0).getControllerUrl();

    // Create store first
    ControllerClient controllerClient = new ControllerClient(clusterName, parentControllerUrl);
    controllerClient.createNewStore(storeName, "test_owner", "\"int\"", "\"int\"");

    // Make store from batch -> hybrid
    ControllerResponse response = controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setHybridRewindSeconds(259200).setHybridOffsetLagThreshold(1000));
    Assert.assertFalse(response.isError(), "There is error in setting hybrid config");

    // Try to update partitioner config on hybrid store, expect to fail.
    response = controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPartitionerClass("testClassName"));
    Assert.assertTrue(response.isError(), "There should be error in setting partitioner config in hybrid store");

    // Try to make store back to non-hybrid store.
    response = controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setHybridRewindSeconds(-1).setHybridOffsetLagThreshold(-1));
    Assert.assertFalse(response.isError(), "There is error in setting hybrid config");

    // Make sure store is not hybrid.
    Assert.assertNull(controllerClient.getStore(storeName).getStore().getHybridStoreConfig());

    // Try to upate partitioner config on batch store, it should succeed now.
    response = controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPartitionerClass("testClassName"));
    Assert.assertFalse(response.isError(), "There is error in setting partitioner config in non-hybrid store");
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMultiDataCenterIncrementalPush() throws Exception {
    String clusterName = CLUSTER_NAMES[1];
    // create a batch push job
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = defaultH2VProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, CompressionStrategy.NO_OP, false, true).close();

    TestPushUtils.runPushJob("Test push job", props);

    // create an incremental push job
    writeSimpleAvroFileWithUserSchema2(inputDir);
    props.setProperty(INCREMENTAL_PUSH, "true");

    try (KafkaPushJob incrementalPushJob = new KafkaPushJob("Test incremental push job", props)) {
      incrementalPushJob.run();

      Admin.OfflinePushStatusInfo offlinePushStatusInfo = parentControllers.get(0)
          .getVeniceAdmin()
          .getOffLinePushStatus(clusterName, incrementalPushJob.getKafkaTopic(), incrementalPushJob.getIncrementalPushVersion());
      Assert.assertEquals(offlinePushStatusInfo.getExecutionStatus(), ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
    }
    // validate the client can read data
    for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
      VeniceMultiClusterWrapper veniceCluster = childClusters.get(dataCenterIndex);
      String routerUrl = veniceCluster.getClusters().get(clusterName).getRandomRouterURL();

      try (AvroGenericStoreClient<String, Utf8> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= 50; ++i) {
          Utf8 expected = new Utf8("test_name_" + i);
          Utf8 actual = client.get(Integer.toString(i)).get();
          Assert.assertEquals(actual, expected);
        }

        for (int i = 51; i <= 150; ++i) {
          Utf8 expected = new Utf8("test_name_" + (i * 2));
          Utf8 actual = client.get(Integer.toString(i)).get();
          Assert.assertEquals(actual, expected);
        }
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testFailedAdminMessages() {
    String clusterName = CLUSTER_NAMES[1];
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Admin admin = parentController.getVeniceAdmin();
    VeniceWriterFactory veniceWriterFactory = admin.getVeniceWriterFactory();
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterFactory.createBasicVeniceWriter(AdminTopicUtils.getTopicNameFromClusterName(CLUSTER_NAMES[1]));
    AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
    long executionId = parentController.getVeniceAdmin().getLastSucceedExecutionId(clusterName) + 1;
    // send a bad admin message
    veniceWriter.put(
        emptyKeyBytes,
        getStoreUpdateMessage(
            CLUSTER_NAMES[1],
            "store-not-exist",
            "store-owner",
            executionId,
            adminOperationSerializer),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    TestUtils.waitForNonDeterministicCompletion(60000, TimeUnit.MILLISECONDS, () -> {
      boolean allDataCenterReceivedFailedAdminMessage = true;
      for (List<VeniceControllerWrapper> controllers : childControllers) {
        AdminConsumerService adminConsumerService = controllers.get(0).getAdminConsumerServiceByCluster(CLUSTER_NAMES[1]);
        Map<String, ? extends Metric> metrics = adminConsumerService.getMetricsRepository().metrics();
        if (metrics.containsKey("." + clusterName + "-admin_consumption_task--failed_admin_messages.Count")) {
          allDataCenterReceivedFailedAdminMessage &=
              (metrics.get("." + clusterName + "-admin_consumption_task--failed_admin_messages.Count").value() >= 1.0);
          long failingOffset = adminConsumerService.getFailingOffset();
          if (failingOffset >= 0) {
            // Cleanup the failing admin message to reduce unneeded logging.
            adminConsumerService.setOffsetToSkip(clusterName, failingOffset, false);
          }
        } else {
          return false;
        }
      }
      return allDataCenterReceivedFailedAdminMessage;
    });

    // Currently store level isolation is not fully implemented for parent controllers yet.
    // Skipping the problematic admin message is required to proceed with the other tests while child controllers can
    // still function with the blocking admin message.
    AdminConsumerService adminConsumerService = parentController.getAdminConsumerServiceByCluster(clusterName);
    adminConsumerService.setOffsetToSkip(clusterName, adminConsumerService.getFailingOffset(), false);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      boolean allFailedMessagesSkipped = adminConsumerService.getFailingOffset() == -1;
      for (List<VeniceControllerWrapper> controllerWrappers : childControllers) {
        AdminConsumerService childAdminConsumerService =
            controllerWrappers.get(0).getAdminConsumerServiceByCluster(clusterName);
        allFailedMessagesSkipped &= childAdminConsumerService.getFailingOffset() == -1;
      }
      return allFailedMessagesSkipped;
    });
  }

  private byte[] getStoreUpdateMessage(String clusterName, String storeName, String owner, long executionId, AdminOperationSerializer adminOperationSerializer) {
    UpdateStore updateStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
    updateStore.clusterName = clusterName;
    updateStore.storeName = storeName;
    updateStore.owner = owner;
    updateStore.partitionNum = 20;
    updateStore.currentVersion = 1;
    updateStore.enableReads = true;
    updateStore.enableWrites = true;
    updateStore.replicateAllConfigs = true;
    updateStore.updatedConfigsList = Collections.emptyList();
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = executionId;
    return adminOperationSerializer.serialize(adminMessage);
  }

  private byte[] getStoreCreationMessage(String clusterName, String storeName, String owner, String keySchema, String valueSchema, long executionId, AdminOperationSerializer adminOperationSerializer) {
    StoreCreation storeCreation = (StoreCreation) AdminMessageType.STORE_CREATION.getNewInstance();
    storeCreation.clusterName = clusterName;
    storeCreation.storeName = storeName;
    storeCreation.owner = owner;
    storeCreation.keySchema = new SchemaMeta();
    storeCreation.keySchema.definition = keySchema;
    storeCreation.keySchema.schemaType = SchemaType.AVRO_1_4.getValue();
    storeCreation.valueSchema = new SchemaMeta();
    storeCreation.valueSchema.definition = valueSchema;
    storeCreation.valueSchema.schemaType = SchemaType.AVRO_1_4.getValue();
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.STORE_CREATION.getValue();
    adminMessage.payloadUnion = storeCreation;
    adminMessage.executionId = executionId;
    return adminOperationSerializer.serialize(adminMessage);
  }
}
