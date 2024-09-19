package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_DELETION_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;

import com.linkedin.venice.controller.StoreBackupVersionCleanupService;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStoreBackupVersionDeletion {
  private static final int TEST_TIMEOUT = 120_000; // ms
  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0",

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private VeniceHelixAdmin veniceHelixAdmin;

  @BeforeClass
  public void setUp() {
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1);
    controllerProps.put(DEFAULT_PARTITION_SIZE, 10);
    controllerProps
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    controllerProps.put(CONTROLLER_BACKUP_VERSION_DELETION_SLEEP_MS, 10);
    controllerProps.put(CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED, "true");
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    StoreBackupVersionCleanupService.setWaitTimeDeleteRepushSourceVersion(10);
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        1,
        1,
        1,
        Optional.of(controllerProps),
        Optional.of(controllerProps),
        Optional.of(serverProperties));

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    veniceHelixAdmin =
        (VeniceHelixAdmin) childDatacenters.get(0).getControllers().values().iterator().next().getVeniceAdmin();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRepushBackupVersionDeletion() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();
      TestWriteUtils.runPushJob("Test push job 1", props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
      TestWriteUtils.runPushJob("Test repush job 2", props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          20,
          TimeUnit.SECONDS);
      props.put(SOURCE_KAFKA, "true");
      TestWriteUtils.runPushJob("Test repush job", props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 3),
          parentControllerClient,
          20,
          TimeUnit.SECONDS);
      // repush pushed 2 as source version, so version 2 should be deleted
      TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
        Store store = veniceHelixAdmin.getStore(CLUSTER_NAMES[0], storeName);
        return store.getVersion(2) == null && store.getVersions().size() == 2;
      });
    }
  }

}
