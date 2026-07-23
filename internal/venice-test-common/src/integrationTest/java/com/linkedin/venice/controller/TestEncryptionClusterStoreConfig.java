package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CLUSTER_ENCRYPTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Verifies encryption-cluster ({@code cluster.encryption.enabled=true}) store behavior: a newly
 * created store defaults to {@code encryptionEnabled=true} (via {@code configureNewStore}), and
 * encryption cannot be disabled once enabled.
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

      ControllerResponse matchingUpdate =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setEncryptionEnabled(true));
      Assert.assertFalse(matchingUpdate.isError(), "A value matching cluster policy must be accepted");

      ControllerResponse conflictingUpdate =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setEncryptionEnabled(false));
      Assert.assertTrue(conflictingUpdate.isError(), "A value conflicting with cluster policy must be rejected");

      ControllerResponse omittedUpdate =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setOwner("new-owner"));
      Assert.assertFalse(omittedUpdate.isError(), "Updates that omit encryptionEnabled must succeed");

      StoreResponse storeAfterUpdate = controllerClient.getStore(storeName);
      Assert.assertFalse(storeAfterUpdate.isError());
      Assert.assertTrue(
          storeAfterUpdate.getStore().isEncryptionEnabled(),
          "Omitting encryptionEnabled must not make metadata inconsistent with cluster policy");

      venice.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setEncryptionEnabled(false);
            return store;
          });
      Assert.assertFalse(
          controllerClient.getStore(storeName).getStore().isEncryptionEnabled(),
          "The test setup must simulate an existing store with stale metadata");

      ControllerResponse staleMetadataUpdate =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setOwner("reconciled-owner"));
      Assert.assertFalse(staleMetadataUpdate.isError(), "Updates that omit encryptionEnabled must skip validation");
      Assert.assertFalse(
          controllerClient.getStore(storeName).getStore().isEncryptionEnabled(),
          "An omitted encryption value must leave existing metadata unchanged");

      ControllerResponse reconciliationUpdate =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setEncryptionEnabled(true));
      Assert.assertFalse(
          reconciliationUpdate.isError(),
          "An explicit value matching cluster policy must repair metadata");
      Assert.assertTrue(
          controllerClient.getStore(storeName).getStore().isEncryptionEnabled(),
          "The explicit matching value must reconcile stale encryption metadata");
    }
  }
}
