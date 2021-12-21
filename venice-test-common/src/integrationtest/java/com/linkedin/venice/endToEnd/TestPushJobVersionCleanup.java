package com.linkedin.venice.endToEnd;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestPushJobVersionCleanup {
  private static final Logger logger = Logger.getLogger(TestPushJobWithNativeReplication.class);
  private static final int TEST_TIMEOUT = 120_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
      // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, "true");
    serverProperties.setProperty(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, "60000");

    Properties controllerProps = new Properties();
    controllerProps.put(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, false);
    controllerProps.put(PARTICIPANT_MESSAGE_STORE_ENABLED, true);

    multiColoMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(NUMBER_OF_CHILD_DATACENTERS, NUMBER_OF_CLUSTERS, 1,
            1, 1, 1, 1, Optional.of(new VeniceProperties(controllerProps)), Optional.of(controllerProps),
            Optional.of(new VeniceProperties(serverProperties)), false, MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMultipleBatchPushWithVersionCleanup() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, 50);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();

    UpdateStoreQueryParams updateStoreParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA).setPartitionCount(2);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
    VeniceServer server = childDataCenter.getClusters().get(clusterName).getVeniceServers().get(0).getVeniceServer();

    /**
     * Run 3 push jobs sequentially and at the end verify the first version is cleaned up properly without any ingestion_failure metrics being reported.
     */
    try (VenicePushJob job = new VenicePushJob("Test push job 1", props)) {
      job.run();
    }
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      // Current version should become 1
      for (int version : parentController.getVeniceAdmin()
          .getCurrentVersionsForMultiColos(clusterName, storeName)
          .values()) {
        Assert.assertEquals(version, 1);
      }
    });

    try (VenicePushJob job = new VenicePushJob("Test push job 2", props)) {
      job.run();
    }
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      // Current version should become 2
      for (int version : parentController.getVeniceAdmin()
          .getCurrentVersionsForMultiColos(clusterName, storeName)
          .values()) {
        Assert.assertEquals(version, 2);
      }
    });

    try (VenicePushJob job = new VenicePushJob("Test push job 3", props)) {
      job.run();
    }
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      // Current version should become 3
      for (int version : parentController.getVeniceAdmin()
          .getCurrentVersionsForMultiColos(clusterName, storeName)
          .values()) {
        Assert.assertEquals(version, 3);
      }
    });

    //There should not be any ingestion_failure.
    Assert.assertEquals(server.getMetricsRepository().getMetric("." + storeName + "--ingestion_failure.Count").value(),
        0.0);
  }
}
