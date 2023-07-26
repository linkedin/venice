package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_KAFKA_PRODUCER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.createAndVerifyStoreInAllRegions;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class IncrementalPushGaE2ETest {
  private static final int TEST_TIMEOUT = 5 * Time.MS_PER_MINUTE;
  private static final int PUSH_TIMEOUT = TEST_TIMEOUT / 2;

  protected static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  protected static final int NUMBER_OF_CLUSTERS = 1;
  static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  // ["venice-cluster0", "venice-cluster1", ...];

  protected List<VeniceMultiClusterWrapper> childDatacenters;
  protected List<VeniceControllerWrapper> parentControllers;
  protected VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private D2Client d2ClientForDC0Region;
  private Properties serverProperties;
  private ControllerClient parentControllerClient;
  private ControllerClient dc0Client;
  private ControllerClient dc1Client;
  private List<ControllerClient> dcControllerClientList;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 1 second;
     * Create a testing environment with 1 parent fabric and 2 child fabrics;
     */
    serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);
    serverProperties.put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, true);
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "2");

    Properties controllerProps = new Properties();
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES, true);
    controllerProps.put(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE, true);
    controllerProps.put(PARTICIPANT_MESSAGE_STORE_ENABLED, false);
    controllerProps.put(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, false);
    controllerProps.put(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);

    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        1,
        1,
        1,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(controllerProps),
        Optional.of(new VeniceProperties(serverProperties)),
        false);
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();

    // Set up a d2 client for DC0 region
    d2ClientForDC0Region = new D2ClientBuilder().setZkHosts(childDatacenters.get(0).getZkServerWrapper().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2ClientForDC0Region);

    String clusterName = CLUSTER_NAMES[0];
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
    dcControllerClientList = Arrays.asList(dc0Client, dc1Client);
  }

  @Test
  public void testIncrementalPushIsEnabledForActiveActiveHybridUserStores() {
    String storeName = TestUtils.getUniqueTopicString("test_store_");
    createAndVerifyStoreInAllRegions(storeName, parentControllerClient, dcControllerClientList);
    verifyDCConfigs(parentControllerClient, storeName, false, false, false);
    verifyDCConfigs(dc0Client, storeName, false, false, false);
    verifyDCConfigs(dc1Client, storeName, false, false, false);

    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(10)
                .setHybridOffsetLagThreshold(2)
                .setHybridDataReplicationPolicy(DataReplicationPolicy.ACTIVE_ACTIVE)));

    verifyDCConfigs(parentControllerClient, storeName, true, true, true);
    verifyDCConfigs(dc0Client, storeName, true, true, true);
    verifyDCConfigs(dc1Client, storeName, true, true, true);
  }

  public static void verifyDCConfigs(
      ControllerClient controllerClient,
      String storeName,
      boolean expectedAAStatus,
      boolean expectedIncPushStatus,
      boolean isNonNullHybridStoreConfig) {
    waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = assertCommand(controllerClient.getStore(storeName));
      StoreInfo storeInfo = storeResponse.getStore();
      assertEquals(
          storeInfo.isActiveActiveReplicationEnabled(),
          expectedAAStatus,
          "The active active replication config does not match.");
      assertEquals(
          storeInfo.isIncrementalPushEnabled(),
          expectedIncPushStatus,
          "The incremental push config does not match.");
      if (!isNonNullHybridStoreConfig) {
        assertNull(storeInfo.getHybridStoreConfig(), "The hybrid store config is not null.");
        return;
      }
      HybridStoreConfig hybridStoreConfig = storeInfo.getHybridStoreConfig();
      assertNotNull(hybridStoreConfig, "The hybrid store config is null.");
      DataReplicationPolicy policy = hybridStoreConfig.getDataReplicationPolicy();
      assertNotNull(policy, "The data replication policy is null.");
    });
  }
}
