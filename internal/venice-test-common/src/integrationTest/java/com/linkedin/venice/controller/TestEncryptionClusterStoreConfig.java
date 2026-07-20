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
 * created store defaults to {@code encryptionEnabled=true} (via {@code configureNewStore}), but the
 * flag is overridable — an explicit update-store request to disable it is honored. Migration-driven
 * enforcement (forcing encryption on stores cloned into an encryption cluster) is covered separately
 * by {@code StoreMigrationHelperTest}.
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
  public void testEncryptionDefaultedOnCreateAndOverridableOnUpdate() {
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

      // Encryption is overridable: an explicit request to disable it must be honored.
      ControllerResponse updateResponse =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setEncryptionEnabled(false));
      Assert.assertFalse(updateResponse.isError(), "Update store should succeed: " + updateResponse.getError());

      StoreResponse storeAfterUpdate = controllerClient.getStore(storeName);
      Assert.assertFalse(storeAfterUpdate.isError());
      Assert.assertFalse(
          storeAfterUpdate.getStore().isEncryptionEnabled(),
          "encryptionEnabled must be overridable: disabling it via update-store should take effect");
    }
  }
}
