package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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

  @BeforeClass
  public void setUp() {
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(NUMBER_OF_CHILD_DATACENTERS, 2, 1, 1, 1, 1);

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
    createStoreForJob(clusterName, recordSchema, props);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // Verify job properties
    Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, 1));
    for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
      Assert.assertEquals(version, 1);
    }
    Assert.assertEquals(job.getInputDirectory(), inputDirPath);
    String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    Assert.assertEquals(job.getFileSchemaString(), schema);
    Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getInputFileDataSize(), 3872);

    // Verify the data in Venice Store
    for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
      VeniceMultiClusterWrapper veniceCluster = childClusters.get(dataCenterIndex);
      String routerUrl = veniceCluster.getClusters().get(clusterName).getRandomRouterURL();
      try(AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= 100; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString(); /* client.get().get() returns a Utf8 object */
          Assert.assertEquals(actual, expected);
        }

        ControllerClient controllerClient = new ControllerClient(clusterName, routerUrl);
        JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(job.getKafkaTopic());
        Assert.assertEquals(jobStatus.getStatus(), ExecutionStatus.COMPLETED.toString(),
            "After job is complete, status should reflect that");
        // In this test we are allowing the progress to not reach the full capacity, but we still want to make sure
        // that most of the progress has completed
        Assert.assertTrue(jobStatus.getMessagesConsumed()*1.5 > jobStatus.getMessagesAvailable(),
            "Complete job should have progress");
      }
    }

    /**
     * To speed up integration test, here reuses the same test case to verify topic clean up logic.
     *
     * TODO: update service factory to allow specifying {@link com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_STORE_VERSIONS_TO_PRESERVE}
     * and {@link com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE} to reduce job run times.
     */
    job.run();
    job.run();

    String v1Topic = storeName + "_v1";
    String v2Topic = storeName + "_v2";
    String v3Topic = storeName + "_v3";

    // Verify the topics in parent controller
    TopicManager parentTopicManager = parentController.getVeniceAdmin().getTopicManager();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS,() -> {
      Assert.assertFalse(parentTopicManager.containsTopic(v1Topic), "Topic: " + v1Topic + " should be deleted after push");
      Assert.assertFalse(parentTopicManager.containsTopic(v2Topic), "Topic: " + v2Topic + " should be deleted after push");
      Assert.assertFalse(parentTopicManager.containsTopic(v3Topic), "Topic: " + v3Topic + " should be deleted after push");
    });

    // Verify the topics in child controller
    TopicManager childTopicManager = childControllers.get(0).get(0).getVeniceAdmin().getTopicManager();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS,() -> {
      Assert.assertFalse(childTopicManager.containsTopic(v1Topic), "Topic: " + v1Topic + " should be deleted after 3 pushes");
    });
    Assert.assertTrue(childTopicManager.containsTopic(v2Topic), "Topic: " + v2Topic + " should be kept after 3 pushes");
    Assert.assertTrue(childTopicManager.containsTopic(v3Topic), "Topic: " + v3Topic + " should be kept after 3 pushes");
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMultiDataCenterIncrementalPush() throws Exception {
    String clusterName = CLUSTER_NAMES[1];
    //create a batch push job
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = defaultH2VProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, false, false, true);

    KafkaPushJob job = new KafkaPushJob("Test batch push job", props);
    job.run();

    //create a incremental push job
    writeSimpleAvroFileWithUserSchema2(inputDir);
    props.setProperty(INCREMENTAL_PUSH, "true");
    KafkaPushJob incrementalPushJob = new KafkaPushJob("Test incremental push job", props);
    incrementalPushJob.run();

    //validate the client can read data
    for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
      VeniceMultiClusterWrapper veniceCluster = childClusters.get(dataCenterIndex);
      String routerUrl = veniceCluster.getClusters().get(clusterName).getRandomRouterURL();
      try(AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= 50; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }

        for (int i = 51; i <= 150; ++i) {
          String expected = "test_name_" + (i * 2);
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }
    }
  }
}
