package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_BATCH_REPORT_END_OF_INCREMENTAL_PUSH_STATUS_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestBatchReportIncrementalPush {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int TEST_TIMEOUT_MS = 180_000;
  private static final String CLUSTER_NAME = "venice-cluster0";
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceControllerWrapper parentController;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    serverProperties.setProperty(SERVER_BATCH_REPORT_END_OF_INCREMENTAL_PUSH_STATUS_ENABLED, Long.toString(60L));
    Properties controllerProps = new Properties();
    this.multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.of(controllerProps),
        Optional.of(controllerProps),
        Optional.of(serverProperties),
        false);
    this.childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    if (parentControllers.size() != 1) {
      throw new IllegalStateException("Expect only one parent controller. Got: " + parentControllers.size());
    }
    this.parentController = parentControllers.get(0);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testBatchReportIncrementalPush() throws IOException {
    final String storeName = Utils.getUniqueString("inc_push_update_classic_format");
    String parentControllerUrl = parentController.getControllerUrl();
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(
              storeName,
              "test_owner",
              STRING_SCHEMA.toString(),
              TestWriteUtils.NAME_RECORD_V1_SCHEMA.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true)
              .setIncrementalPushEnabled(true)
              .setHybridRewindSeconds(1000L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();

      for (int i = 0; i < 5; i++) {
        int startIndex = i * 100 + 1;
        int endIndex = (i + 1) * 100;
        runIncrementalPush(storeName, startIndex, endIndex, childControllerUrl, veniceClusterWrapper);
      }

      int port = veniceClusterWrapper.getVeniceServers().get(0).getPort();
      veniceClusterWrapper.stopVeniceServer(port);
      FileUtils.deleteDirectory(veniceClusterWrapper.getVeniceServers().get(0).getDataDirectory());
      long waitTime = TimeUnit.SECONDS.toMillis(3);
      Utils.sleep(waitTime);
      veniceClusterWrapper.restartVeniceServer(port);

      DaVinciConfig daVinciConfig = new DaVinciConfig();
      daVinciConfig.setIsolated(true);

      VeniceProperties backendConfig =
          new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
              .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
              .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
              .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
              .put(PUSH_STATUS_STORE_ENABLED, true)
              .put(SERVER_BATCH_REPORT_END_OF_INCREMENTAL_PUSH_STATUS_ENABLED, true)
              .put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 1)
              .build();

      D2Client d2Client = D2TestUtils.getAndStartD2Client(
          multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper().getAddress());
      try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
          d2Client,
          VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
          new MetricsRepository(),
          backendConfig)) {
        DaVinciClient<Object, Object> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
        client.subscribeAll().get();
        runIncrementalPush(storeName, 1001, 1100, childControllerUrl, veniceClusterWrapper);

        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1001; i < 1100; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(client, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "first_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

      } catch (ExecutionException | InterruptedException e) {
        throw new VeniceException(e);
      }

    }
  }

  private void runIncrementalPush(
      String storeName,
      int startIndex,
      int endIndex,
      String childControllerUrl,
      VeniceClusterWrapper clusterWrapper) throws IOException {
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema(inputDir, startIndex, endIndex);
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    vpjProperties.put(ENABLE_WRITE_COMPUTE, true);
    vpjProperties.put(INCREMENTAL_PUSH, true);
    // VPJ push
    try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
      runVPJ(vpjProperties, 1, childControllerClient);
    }

    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = startIndex; i < endIndex; i++) {
            String key = String.valueOf(i);
            GenericRecord value = readValue(storeReader, key);
            assertNotNull(value, "Key " + key + " should not be missing!");
            assertEquals(value.get("firstName").toString(), "first_name_" + key);
            assertEquals(value.get("lastName").toString(), "last_name_" + key);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }
  }

  private void runVPJ(Properties vpjProperties, int expectedVersionNumber, ControllerClient controllerClient) {
    String jobName = Utils.getUniqueString("VPJ-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(
          60,
          TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) vpjProperties.get(VENICE_STORE_NAME_PROP))
              .getStore()
              .getCurrentVersion() == expectedVersionNumber);
    }
  }

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

  private GenericRecord readValue(DaVinciClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

}
