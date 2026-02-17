package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_SECONDS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
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
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestMultiDatacenterVenicePushJob {
  private static final int TEST_TIMEOUT = 90_000;
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
    IntegrationTestPushUtils.runVPJ(props);
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
      Assert.assertEquals(parentControllerClient.getStore(storeName).getStore().getVersions().size(), 1);
      for (ControllerClient childControllerClient: childControllerClients) {
        Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
      }
    });
  }

  @Test(timeOut = 3 * TEST_TIMEOUT)
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
    IntegrationTestPushUtils.runVPJ(props);
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
      Assert.assertEquals(parentControllerClient.getStore(storeName).getStore().getVersions().size(), 1);
      Assert.assertNotNull(parentControllerClient.getStore(storeName).getStore().getHybridStoreConfig());
      for (ControllerClient childControllerClient: childControllerClients) {
        Assert.assertNotNull(childControllerClient.getStore(storeName).getStore().getHybridStoreConfig());
        Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
      }
    });
  }

  @DataProvider(name = "testRepushTtlSecondsWithRepushDataProvider")
  public Object[][] testRepushTtlSecondsWithRepushDataProvider() {
    UpdateStoreQueryParams hybridProps = new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1)
        .setHybridRewindSeconds(0)
        .setActiveActiveReplicationEnabled(true)
        .setNativeReplicationEnabled(true)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true);

    UpdateStoreQueryParams hybridPropsWithInvalidRewind = new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1)
        .setHybridRewindSeconds(-1)
        .setActiveActiveReplicationEnabled(true)
        .setNativeReplicationEnabled(true)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true);

    UpdateStoreQueryParams batchProps = new UpdateStoreQueryParams().setChunkingEnabled(true);

    return new Object[][] { { "1", hybridProps, true, false }, // Hybrid + Valid TTL + TTL Repush
        { "-1", hybridPropsWithInvalidRewind, true, false }, // Hybrid + Valid TTL + TTL Repush + -1 rewind
        { "-1", hybridProps, false, false }, // Batch Push + Hybrid Store
        { "-1", batchProps, false, false }, // Batch store + Batch Push
        { "-1", batchProps, false, true }, // Batch store + Repush
    };
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "testRepushTtlSecondsWithRepushDataProvider")
  public void testRepushTtlSecondsWithRepush(
      String ttlRepushSeconds,
      UpdateStoreQueryParams additionalProps,
      boolean isTTLRepush,
      boolean isRepush) throws Exception {
    // Setup input files
    File inputDir = getTempDataDirectory();
    String storeName = Utils.getUniqueString("testRepushTtlSecondsWithRepush");
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();

    // Create store
    Properties props = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    createStoreForJob(CLUSTER_NAMES[0], recordSchema, props);

    TestUtils.assertCommand(parentControllerClient.updateStore(storeName, additionalProps));

    parentControllerClient.emptyPush(storeName, "test", 14033924);
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(storeName, 1),
        parentControllerClient,
        30,
        TimeUnit.SECONDS);

    // Enable ttl re-push
    if (isTTLRepush) {
      props.setProperty(REPUSH_TTL_ENABLE, "true");
      props.setProperty(SOURCE_KAFKA, "true");
      props.setProperty(REPUSH_TTL_SECONDS, ttlRepushSeconds);
    } else if (isRepush) {
      props.setProperty(SOURCE_KAFKA, "true");
    }

    IntegrationTestPushUtils.runVPJ(props);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      for (ControllerClient childControllerClient: childControllerClients) {
        StoreResponse response = childControllerClient.getStore(storeName);
        Assert.assertFalse(response.isError());
        Assert.assertEquals(response.getStore().getVersions().size(), 2);
        Assert.assertEquals(
            response.getStore().getVersion(2).get().getRepushTtlSeconds(),
            Integer.parseInt(ttlRepushSeconds));
      }
    });
  }
}
