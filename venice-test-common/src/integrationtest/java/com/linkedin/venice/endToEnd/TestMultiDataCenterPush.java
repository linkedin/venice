package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestMultiDataCenterPush {
  private static final Logger LOGGER = Logger.getLogger(TestMultiDataCenterPush.class);
  private static final String CLUSTER_NAME = "multi-dc-test-cluster";
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private final List<VeniceClusterWrapper> childClusters = new ArrayList<>(NUMBER_OF_CHILD_DATACENTERS);
  private final List<MirrorMakerWrapper> mirrorMakers = new ArrayList<>(NUMBER_OF_CHILD_DATACENTERS);
  private KafkaBrokerWrapper parentKafka;
  private VeniceControllerWrapper parentController;
  private VeniceControllerWrapper[] childControllers;

  @BeforeClass
  public void setUp() {
    parentKafka = ServiceFactory.getKafkaBroker();

    childControllers = new VeniceControllerWrapper[NUMBER_OF_CHILD_DATACENTERS];
    for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
      VeniceClusterWrapper childCluster = ServiceFactory.getVeniceCluster(CLUSTER_NAME);
      childClusters.add(childCluster);
      childControllers[dataCenterIndex] = childCluster.getMasterVeniceController();

      MirrorMakerWrapper mirrorMakerWrapper = ServiceFactory.getKafkaMirrorMaker(parentKafka, childCluster.getKafka());
      mirrorMakers.add(mirrorMakerWrapper);
    }

    parentController = ServiceFactory.getVeniceParentController(
        CLUSTER_NAME,
        parentKafka.getZkAddress(),
        parentKafka,
        childControllers,
        false);

    LOGGER.info("parentController.getControllerUrl(): " + parentController.getControllerUrl());
    for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
      LOGGER.info("childControllers[" + dataCenterIndex + "].getControllerUrl(): " + childControllers[dataCenterIndex].getControllerUrl());
    }
  }

  @AfterClass
  public void cleanUp() {
    for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
      MirrorMakerWrapper mirrorMakerWrapper = mirrorMakers.get(dataCenterIndex);
      IOUtils.closeQuietly(mirrorMakerWrapper);
      VeniceClusterWrapper childCluster = childClusters.get(dataCenterIndex);
      IOUtils.closeQuietly(childCluster);
    }
    IOUtils.closeQuietly(parentController);
    IOUtils.closeQuietly(parentKafka);
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testMultiDataCenterPush() throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    createStoreForJob(CLUSTER_NAME, recordSchema, props);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // Verify job properties
    Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, 1));
    Assert.assertEquals(job.getInputDirectory(), inputDirPath);
    String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    Assert.assertEquals(job.getFileSchemaString(), schema);
    Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getInputFileDataSize(), 3872);

    // Verify the data in Venice Store
    for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
      VeniceClusterWrapper veniceCluster = childClusters.get(dataCenterIndex);
      String routerUrl = veniceCluster.getRandomRouterURL();
      try(AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= 100; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString(); /* client.get().get() returns a Utf8 object */
          Assert.assertEquals(actual, expected);
        }

        JobStatusQueryResponse jobStatus = ControllerClient.queryJobStatus(routerUrl, veniceCluster.getClusterName(), job.getKafkaTopic());
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
    TopicManager childTopicManager = childControllers[0].getVeniceAdmin().getTopicManager();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS,() -> {
      Assert.assertFalse(childTopicManager.containsTopic(v1Topic), "Topic: " + v1Topic + " should be deleted after 3 pushes");
    });
    Assert.assertTrue(childTopicManager.containsTopic(v2Topic), "Topic: " + v2Topic + " should be kept after 3 pushes");
    Assert.assertTrue(childTopicManager.containsTopic(v3Topic), "Topic: " + v3Topic + " should be kept after 3 pushes");
  }
}
