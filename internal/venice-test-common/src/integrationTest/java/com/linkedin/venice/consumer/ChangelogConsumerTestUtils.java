package com.linkedin.venice.consumer;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;

import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.TestUtils;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


/**
 * Shared utility methods for changelog consumer integration tests.
 */
public class ChangelogConsumerTestUtils {
  private ChangelogConsumerTestUtils() {
  }

  public static Properties buildConsumerProperties(
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionWrapper,
      PubSubBrokerWrapper localKafka,
      String clusterName,
      ZkServerWrapper localZkServer) {
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionWrapper.getPubSubClientProperties());
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafka.getAddress());
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    return consumerProperties;
  }

  public static ChangelogClientConfig buildBaseChangelogClientConfig(
      Properties consumerProperties,
      String localD2ZkHosts,
      long versionSwapDetectionIntervalSeconds) {
    return new ChangelogClientConfig().setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localD2ZkHosts)
        .setControllerRequestRetryCount(3)
        .setVersionSwapDetectionIntervalTimeInSeconds(versionSwapDetectionIntervalSeconds);
  }

  public static UpdateStoreQueryParams buildDefaultStoreParams() {
    return new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3);
  }

  public static void waitForMetaSystemStoreToBeReady(
      String storeName,
      ControllerClient controllerClient,
      VeniceClusterWrapper clusterWrapper) {
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(metaSystemStoreName, 1),
        controllerClient,
        90,
        TimeUnit.SECONDS);
    clusterWrapper.refreshAllRouterMetaData();
    String routerUrl = clusterWrapper.getRandomRouterURL();
    try (AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> metaStoreClient =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
                .setVeniceURL(routerUrl))) {
      StoreMetaKey storeClusterConfigKey =
          MetaStoreDataType.STORE_CLUSTER_CONFIG.getStoreMetaKey(Collections.singletonMap("KEY_STORE_NAME", storeName));
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, false, true, () -> {
        StoreMetaValue value = metaStoreClient.get(storeClusterConfigKey).get(30, TimeUnit.SECONDS);
        Assert.assertNotNull(value, "Meta store should return non-null value for STORE_CLUSTER_CONFIG");
        Assert.assertNotNull(value.storeClusterConfig, "storeClusterConfig should not be null");
      });
    }
  }

  public static void cleanupAfterTest(
      List<AutoCloseable> testCloseables,
      List<String> testStoresToDelete,
      ControllerClient parentControllerClient,
      Logger logger) {
    for (int i = testCloseables.size() - 1; i >= 0; i--) {
      try {
        testCloseables.get(i).close();
      } catch (Exception e) {
        logger.warn("Failed to close resource during test cleanup", e);
      }
    }
    testCloseables.clear();

    for (String storeName: testStoresToDelete) {
      try {
        parentControllerClient.disableAndDeleteStore(storeName);
      } catch (Exception e) {
        logger.warn("Failed to delete store {} during test cleanup", storeName, e);
      }
    }
    testStoresToDelete.clear();
  }
}
