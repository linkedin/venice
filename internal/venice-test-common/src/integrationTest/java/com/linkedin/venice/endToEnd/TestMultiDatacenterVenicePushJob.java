package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMultiDatacenterVenicePushJob {
  private static final int TEST_TIMEOUT = 30_000;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  ControllerClient parentControllerClient;
  ControllerClient[] childControllerClients;
  VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass
  public void setUp() {
    String clusterName = CLUSTER_NAMES[0];
    Properties controllerProps = new Properties();
    Properties serverProperties = new Properties();

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    childControllerClients = new ControllerClient[childDatacenters.size()];
    for (int i = 0; i < childDatacenters.size(); i++) {
      childControllerClients[i] =
          new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    parentControllerClient.close();
    Arrays.stream(childControllerClients).forEach(ControllerClient::close);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  // Runs a VPJ and verifies remote consumption for compression enabled batch store in a multi-region setup
  public void testVPJWithCompressionEnabledBatchStore() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("TestVPJWithCompressionEnabledBatchStore");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    createStoreForJob(
        CLUSTER_NAMES[0],
        keySchemaStr,
        NAME_RECORD_V3_SCHEMA.toString(),
        props,
        new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT)).close();
    TestWriteUtils.runPushJob("Test push job", props);
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
      Assert.assertEquals(parentControllerClient.getStore(storeName).getStore().getVersions().size(), 1);
      for (ControllerClient childControllerClient: childControllerClients) {
        Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVPJWithCompressionEnabledHybridStore() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("TestVPJWithCompressionEnabledHybridStore");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    createStoreForJob(
        CLUSTER_NAMES[0],
        keySchemaStr,
        NAME_RECORD_V3_SCHEMA.toString(),
        props,
        new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT)
            .setHybridRewindSeconds(1000)
            .setActiveActiveReplicationEnabled(true)
            .setHybridOffsetLagThreshold(1000)).close();
    TestWriteUtils.runPushJob("Test push job", props);
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
      Assert.assertEquals(parentControllerClient.getStore(storeName).getStore().getVersions().size(), 1);
      Assert.assertNotNull(parentControllerClient.getStore(storeName).getStore().getHybridStoreConfig());
      for (ControllerClient childControllerClient: childControllerClients) {
        Assert.assertNotNull(childControllerClient.getStore(storeName).getStore().getHybridStoreConfig());
        Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
      }
    });
  }
}
