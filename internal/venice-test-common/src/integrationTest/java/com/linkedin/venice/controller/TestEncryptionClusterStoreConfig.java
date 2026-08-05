package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CLUSTER_ENCRYPTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Verifies encryption-cluster ({@code cluster.encryption.enabled=true}) store behavior: a newly
 * created store defaults to {@code encryptionEnabled=true} (via {@code configureNewStore}), and
 * update-store accepts and preserves its KMS-defined PubSub encryption key URN.
 */
public class TestEncryptionClusterStoreConfig {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;

  private VeniceClusterWrapper venice;
  private String clusterName;

  @BeforeClass
  public void setUp() {
    Properties properties = new Properties();
    properties.setProperty(LOCAL_REGION_NAME, "dc-0");
    properties.setProperty(CLUSTER_ENCRYPTION_ENABLED, "true");

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .regionName("dc-0")
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(properties)
        .build();
    venice = ServiceFactory.getVeniceCluster(options);
    clusterName = venice.getClusterName();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(venice);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreEncryptionMirrorsClusterPolicy() {
    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, venice.getLeaderVeniceController().getControllerUrl())) {
      String storeName = Utils.getUniqueString("encryption-cluster-store");
      NewStoreResponse newStoreResponse =
          controllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError(), "Store creation should succeed: " + newStoreResponse.getError());

      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      Assert.assertTrue(
          storeResponse.getStore().isEncryptionEnabled(),
          "A newly created store in an encryption cluster must default to encryptionEnabled=true");
      Assert.assertEquals(storeResponse.getStore().getPubSubEncryptionKeyUrn(), "");

      String pubSubEncryptionKeyUrn = "urn:li:kmsKeyLineage:encryption-cluster-test";
      ControllerResponse keyUpdate = controllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setPubSubEncryptionKeyUrn(pubSubEncryptionKeyUrn));
      Assert.assertFalse(
          keyUpdate.isError(),
          "Setting a KMS PubSub encryption key URN must succeed: " + keyUpdate.getError());
      Assert.assertEquals(
          controllerClient.getStore(storeName).getStore().getPubSubEncryptionKeyUrn(),
          pubSubEncryptionKeyUrn);

      ControllerResponse blankKeyUpdate =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPubSubEncryptionKeyUrn("  "));
      Assert.assertTrue(blankKeyUpdate.isError(), "Blank PubSub encryption key URNs must be rejected");

      ControllerResponse omittedUpdate =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setOwner("new-owner"));
      Assert.assertFalse(omittedUpdate.isError(), "Updates that omit encryptionEnabled must succeed");

      StoreResponse storeAfterUpdate = controllerClient.getStore(storeName);
      Assert.assertFalse(storeAfterUpdate.isError());
      Assert.assertTrue(
          storeAfterUpdate.getStore().isEncryptionEnabled(),
          "Omitting encryptionEnabled must not make metadata inconsistent with cluster policy");
      Assert.assertEquals(
          storeAfterUpdate.getStore().getPubSubEncryptionKeyUrn(),
          pubSubEncryptionKeyUrn,
          "Omitting pubSubEncryptionKeyUrn must preserve the existing KMS value");

      ControllerResponse replicateAllUpdate = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setOwner("replicated-owner").setReplicateAllConfigs(true));
      Assert.assertFalse(replicateAllUpdate.isError(), "Replicate-all updates must succeed");
      Assert.assertTrue(
          controllerClient.getStore(storeName).getStore().isEncryptionEnabled(),
          "Replicate-all updates must preserve encryption metadata");
      Assert.assertEquals(
          controllerClient.getStore(storeName).getStore().getPubSubEncryptionKeyUrn(),
          pubSubEncryptionKeyUrn);

      venice.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setEncryptionEnabled(false);
            return store;
          });
      Assert.assertFalse(
          controllerClient.getStore(storeName).getStore().isEncryptionEnabled(),
          "The test setup must simulate an existing store with stale metadata");

      ControllerResponse unencryptedKeyUpdate = controllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setPubSubEncryptionKeyUrn(pubSubEncryptionKeyUrn));
      Assert.assertTrue(
          unencryptedKeyUpdate.isError(),
          "PubSub encryption key URNs must be rejected when store metadata has encryption disabled");

      ControllerResponse staleMetadataUpdate =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setOwner("reconciled-owner"));
      Assert.assertFalse(staleMetadataUpdate.isError(), "Updates that omit encryptionEnabled must skip validation");
      Assert.assertFalse(
          controllerClient.getStore(storeName).getStore().isEncryptionEnabled(),
          "An omitted encryption value must leave existing metadata unchanged");
      Assert.assertEquals(
          controllerClient.getStore(storeName).getStore().getPubSubEncryptionKeyUrn(),
          pubSubEncryptionKeyUrn);
    }
  }

  @Test(timeOut = 4 * TEST_TIMEOUT)
  public void testParentProvisionsPubSubEncryptionKeyBeforeCreatingVersion() {
    String expectedKeyUrn = "urn:test:pub-sub-encryption-key";
    AtomicInteger providerInvocationCount = new AtomicInteger();
    PubSubEncryptionKeyProvider provider = (requestedClusterName, requestedStoreName) -> {
      providerInvocationCount.incrementAndGet();
      return expectedKeyUrn;
    };
    Properties parentControllerProperties = new Properties();
    parentControllerProperties.setProperty(CLUSTER_ENCRYPTION_ENABLED, "true");
    parentControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(false));
    parentControllerProperties
        .setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, String.valueOf(false));
    parentControllerProperties.put(VeniceControllerWrapper.PUB_SUB_ENCRYPTION_KEY_PROVIDER, provider);

    VeniceMultiRegionClusterCreateOptions options =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(0)
            .numberOfRouters(0)
            .replicationFactor(1)
            .parentControllerProperties(parentControllerProperties)
            .build();
    try (VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionVenice =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(options)) {
      String testClusterName = multiRegionVenice.getClusterNames()[0];
      String parentControllerUrl = multiRegionVenice.getControllerConnectString();
      String childControllerUrl = multiRegionVenice.getChildRegions().get(0).getControllerConnectString();
      try (ControllerClient parentControllerClient = new ControllerClient(testClusterName, parentControllerUrl);
          ControllerClient childControllerClient = new ControllerClient(testClusterName, childControllerUrl)) {
        String storeName = Utils.getUniqueString("auto-key-provisioning-store");
        NewStoreResponse newStoreResponse =
            parentControllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
        Assert.assertFalse(newStoreResponse.isError(), "Store creation should succeed: " + newStoreResponse.getError());
        Assert.assertEquals(parentControllerClient.getStore(storeName).getStore().getPubSubEncryptionKeyUrn(), "");

        VersionCreationResponse versionCreationResponse = parentControllerClient.requestTopicForWrites(
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
            "Version creation should succeed: " + versionCreationResponse.getError());
        Assert.assertEquals(providerInvocationCount.get(), 1);
        StoreResponse parentStoreResponse = parentControllerClient.getStore(storeName);
        Assert.assertEquals(parentStoreResponse.getStore().getPubSubEncryptionKeyUrn(), expectedKeyUrn);
        Assert.assertTrue(parentStoreResponse.getStore().getVersion(1).isPresent());
        waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
          Assert.assertFalse(childStoreResponse.isError());
          Assert.assertEquals(childStoreResponse.getStore().getPubSubEncryptionKeyUrn(), expectedKeyUrn);
          Assert.assertTrue(childStoreResponse.getStore().getVersion(1).isPresent());
        });
      }
    }
  }
}
