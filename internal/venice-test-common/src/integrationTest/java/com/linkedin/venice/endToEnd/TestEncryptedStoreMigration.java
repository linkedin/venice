package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLUSTER_ENCRYPTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.StoreMigrationTestUtil;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Verifies that migrating a store between two encryption-enabled clusters correctly propagates
 * the source store's PubSub encryption key URN to the destination, and that the destination store
 * can create a version afterward (version creation on an encryption-cluster store is otherwise
 * rejected until a key URN is persisted; see {@link com.linkedin.venice.controller.storeconfig
 * .StoreConfigUpdater}).
 */
public class TestEncryptedStoreMigration {
  private static final int TEST_TIMEOUT = 180 * Time.MS_PER_SECOND;
  private static final String FABRIC0 = "dc-0";

  private VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper;
  private VeniceMultiClusterWrapper multiClusterWrapper;
  private String srcClusterName;
  private String destClusterName;
  private String parentControllerUrl;
  private String childControllerUrl0;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProperties = new Properties();
    parentControllerProperties.setProperty(CLUSTER_ENCRYPTION_ENABLED, "true");
    parentControllerProperties
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    parentControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(false));
    parentControllerProperties
        .setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, String.valueOf(false));

    Properties childControllerProperties = new Properties();
    childControllerProperties.setProperty(CLUSTER_ENCRYPTION_ENABLED, "true");

    // 1 parent controller, 1 child region, 2 encryption-enabled clusters in that region, no servers/routers:
    // this test only exercises controller-level store migration and version-creation gating, not data serving.
    VeniceMultiRegionClusterCreateOptions options =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(2)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(0)
            .numberOfRouters(0)
            .replicationFactor(1)
            .parentControllerProperties(parentControllerProperties)
            .childControllerProperties(childControllerProperties)
            .build();
    twoLayerMultiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(options);

    multiClusterWrapper = twoLayerMultiRegionMultiClusterWrapper.getChildRegions().get(0);
    String[] clusterNames = multiClusterWrapper.getClusterNames();
    Arrays.sort(clusterNames);
    srcClusterName = clusterNames[0];
    destClusterName = clusterNames[1];
    parentControllerUrl = twoLayerMultiRegionMultiClusterWrapper.getControllerConnectString();
    childControllerUrl0 = multiClusterWrapper.getControllerConnectString();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(twoLayerMultiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEncryptionKeyUrnPropagatesDuringMigration() throws Exception {
    String storeName = Utils.getUniqueString("encrypted-migration-store");
    String keyUrn = "keyUrn:encrypted-migration-test";

    try (ControllerClient parentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
        ControllerClient childSrcControllerClient = new ControllerClient(srcClusterName, childControllerUrl0)) {
      NewStoreResponse newStoreResponse =
          parentControllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError(), "Store creation should succeed: " + newStoreResponse.getError());

      ControllerResponse keyUpdate =
          parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPubSubEncryptionKeyUrn(keyUrn));
      Assert.assertFalse(keyUpdate.isError(), "Setting the source key URN should succeed: " + keyUpdate.getError());

      // The key URN update reaches the child region asynchronously via the admin channel; the clone performed by
      // store migration reads the child region's local store state, so it must be persisted there first.
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse childSrcStoreResponse = childSrcControllerClient.getStore(storeName);
        Assert.assertFalse(childSrcStoreResponse.isError());
        Assert.assertEquals(childSrcStoreResponse.getStore().getPubSubEncryptionKeyUrn(), keyUrn);
      });
    }

    StoreMigrationTestUtil.startMigration(parentControllerUrl, storeName, srcClusterName, destClusterName);
    StoreMigrationTestUtil.completeMigration(parentControllerUrl, storeName, srcClusterName, destClusterName, FABRIC0);

    try (ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse destStoreResponse = destParentControllerClient.getStore(storeName);
        Assert.assertFalse(destStoreResponse.isError());
        Assert.assertEquals(
            destStoreResponse.getStore().getPubSubEncryptionKeyUrn(),
            keyUrn,
            "The migrated store's key URN must match the source store's key URN");
      });

      VersionCreationResponse versionCreationResponse = destParentControllerClient.requestTopicForWrites(
          storeName,
          1,
          Version.PushType.BATCH,
          Version.numberBasedDummyPushId(1),
          true,
          true,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      Assert.assertFalse(
          versionCreationResponse.isError(),
          "Version creation on the migrated store should succeed since its key URN was migrated: "
              + versionCreationResponse.getError());
    }
  }
}
