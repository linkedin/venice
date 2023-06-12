package com.linkedin.venice.storagenode;

import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestEarlyTermination {
  private static final int MAX_KEY_LIMIT = 20;
  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;

  private VeniceWriter<String, String, byte[]> veniceWriter;
  private String routerAddr;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(1, 0, 1, 2, 100, true, false);

    // Create store first
    storeName = Utils.getUniqueString("test_early_termination");
    veniceCluster.getNewStore(storeName);

    // Create two servers, and one with early termination enabled, and one without
    Properties serverPropertiesWithoutEarlyTermination = new Properties();
    serverPropertiesWithoutEarlyTermination.put(ConfigKeys.PERSISTENCE_TYPE, ROCKS_DB);
    serverPropertiesWithoutEarlyTermination.put(
        ConfigKeys.SERVER_STORE_TO_EARLY_TERMINATION_THRESHOLD_MS_MAP,
        storeName + ":10000000, non_existing_store:10000000");
    Properties serverFeaturePropertiesWithoutEarlyTermination = new Properties();
    serverFeaturePropertiesWithoutEarlyTermination.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");
    veniceCluster
        .addVeniceServer(serverFeaturePropertiesWithoutEarlyTermination, serverPropertiesWithoutEarlyTermination);

    Properties serverPropertiesWithEarlyTermination = new Properties();
    serverPropertiesWithEarlyTermination
        .put(ConfigKeys.SERVER_STORE_TO_EARLY_TERMINATION_THRESHOLD_MS_MAP, storeName + ":0, non_existing_store:0");
    Properties serverFeaturePropertiesWithEarlyTermination = new Properties();
    serverFeaturePropertiesWithEarlyTermination.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");
    veniceCluster.addVeniceServer(serverFeaturePropertiesWithEarlyTermination, serverPropertiesWithEarlyTermination);

    // Create new version
    VersionCreationResponse creationResponse = veniceCluster.getNewVersion(storeName);

    storeVersionName = creationResponse.getKafkaTopic();
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // Update default quota
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
    updateStore(0, MAX_KEY_LIMIT);

    veniceWriter = veniceCluster.getVeniceWriter(storeVersionName);

    routerAddr = veniceCluster.getRandomRouterURL();
  }

  private void updateStore(long readQuota, int maxKeyLimit) {
    controllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setReadQuotaInCU(readQuota)
            .setReadComputationEnabled(true)
            .setBatchGetLimit(maxKeyLimit));
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
    IOUtils.closeQuietly(veniceWriter);
  }

  @Test(timeOut = 50000)
  public void testRead() throws Exception {
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    String keyPrefix = "key_";
    String valuePrefix = "value_";

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < 100; ++i) {

      veniceWriter.put(keyPrefix + i, valuePrefix + i, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName)
          .getStore()
          .getCurrentVersion();
      return currentVersion == pushVersion;
    });

    /**
     * Test with {@link AvroGenericStoreClient}.
     */
    AvroGenericStoreClient<String, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(routerAddr)
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
    // Run multiple rounds, and no need to verify the result since we know the requests will fail.
    int rounds = 100;
    int cur = 0;
    while (++cur <= rounds) {
      Set<String> keySet = new HashSet<>();
      for (int i = 0; i < MAX_KEY_LIMIT - 1; ++i) {
        keySet.add(keyPrefix + i);
      }
      keySet.add("unknown_key");
      try {
        storeClient.batchGet(keySet).get();
      } catch (Exception e) {
        Assert.assertEquals(e.getClass(), ExecutionException.class);
      }
      try {
        /**
         * Test simple get
         */
        storeClient.get(keyPrefix + 2).get();
      } catch (Exception e) {
        Assert.assertEquals(e.getClass(), ExecutionException.class);
      }
    }

    // Check early termination metrics
    boolean earlyTerminationMetricNonZero = false;
    List<VeniceServerWrapper> serverWrapperList = veniceCluster.getVeniceServers();
    for (VeniceServerWrapper serverWrapper: serverWrapperList) {
      MetricsRepository repository = serverWrapper.getMetricsRepository();
      Metric earlyTerminationMetric =
          repository.getMetric(".total--multiget_early_terminated_request_count.OccurrenceRate");
      if (earlyTerminationMetric.value() > 0) {
        earlyTerminationMetricNonZero = true;
      }
      Map<String, ? extends Metric> metrics = repository.metrics();
      metrics.forEach((mName, metric) -> {
        if (mName.contains("early_terminated_request_count")) {
          System.out.println(mName + " -> " + metric.value());
        }
      });
    }
    Assert.assertTrue(
        earlyTerminationMetricNonZero,
        "Early termination metric should be true when the tight timeout threshold is setup in storage node");
  }
}
