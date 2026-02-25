package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartialUpdateClusterConfigTest extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT_MS = 180_000;

  @Override
  protected int getNumberOfRegions() {
    return 1;
  }

  @Override
  protected Properties getExtraControllerProperties() {
    Properties controllerProps = new Properties();
    controllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    controllerProps.put(ConfigKeys.ENABLE_PARTIAL_UPDATE_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES, true);
    controllerProps.put(ConfigKeys.ENABLE_PARTIAL_UPDATE_FOR_HYBRID_NON_ACTIVE_ACTIVE_USER_STORES, true);
    return controllerProps;
  }

  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testPartialUpdateAutoEnable(boolean activeActiveEnabled) {
    final String storeName = Utils.getUniqueString();
    String parentControllerUrl = getParentControllerUrl();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      ControllerClient[] controllerClients = new ControllerClient[childDatacenters.size() + 1];
      controllerClients[0] = parentControllerClient;
      for (int i = 0; i < childDatacenters.size(); i++) {
        controllerClients[i + 1] =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(i).getControllerConnectString());
      }
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), NAME_RECORD_V2_SCHEMA.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setActiveActiveReplicationEnabled(activeActiveEnabled)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        for (ControllerClient controllerClient: controllerClients) {
          StoreResponse storeResponse = controllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();

          Assert.assertNotNull(storeInfo.getHybridStoreConfig());
          Assert.assertTrue(storeInfo.isWriteComputationEnabled());
          Assert.assertTrue(storeInfo.isChunkingEnabled());
          if (activeActiveEnabled) {
            Assert.assertTrue(storeInfo.isRmdChunkingEnabled());
            Assert.assertTrue(storeInfo.isActiveActiveReplicationEnabled());
          }
        }
      });
    }
  }
}
