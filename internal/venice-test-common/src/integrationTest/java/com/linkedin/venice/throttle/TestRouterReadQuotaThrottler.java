package com.linkedin.venice.throttle;

import static com.linkedin.venice.ConfigKeys.ROUTER_ENABLE_READ_THROTTLING;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRouterReadQuotaThrottler {
  private VeniceClusterWrapper cluster;
  private int numberOfRouter = 2;
  private String storeName;
  private int currentVersion;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    cluster = ServiceFactory.getVeniceCluster(1, 1, numberOfRouter);

    VersionCreationResponse response = cluster.getNewStoreVersion();
    Assert.assertFalse(response.isError());
    storeName = response.getName();
    currentVersion = response.getVersion();

    String stringSchema = "\"string\"";
    VeniceWriterFactory writerFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());
    try (VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(stringSchema);
        VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(stringSchema);
        VeniceWriter<Object, Object, Object> writer = writerFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(response.getKafkaTopic()).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .build())) {

      String key = Utils.getUniqueString("key");
      String value = Utils.getUniqueString("value");
      int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

      writer.broadcastStartOfPush(new HashMap<>());
      // Insert test record and wait synchronously for it to succeed
      writer.put(key, value, valueSchemaId).get();
      // Write end of push message to make node become ONLINE from BOOTSTRAP
      writer.broadcastEndOfPush(new HashMap<String, String>());
    }

    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      if (ControllerClient.getStore(cluster.getAllControllersURLs(), cluster.getClusterName(), storeName)
          .getStore()
          .getCurrentVersion() == 0) {
        return false;
      }
      cluster.refreshAllRouterMetaData();
      return true;
    });
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    cluster.close();
  }

  @Test(groups = "flaky", timeOut = 60 * Time.MS_PER_SECOND)
  public void testReadRequestBeThrottled() throws InterruptedException {
    long timeWindowInSec = TimeUnit.MILLISECONDS.toSeconds(ReadRequestThrottler.DEFAULT_STORE_QUOTA_TIME_WINDOW);
    // Setup read quota for the store.
    long totalQuota = 10;
    cluster.getLeaderVeniceController()
        .getVeniceAdmin()
        .updateStore(cluster.getClusterName(), storeName, new UpdateStoreQueryParams().setReadQuotaInCU(totalQuota));

    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      Store store = cluster.getRandomVeniceRouter().getMetaDataRepository().getStore(storeName);
      return store.getCurrentVersion() == currentVersion && store.getReadQuotaInCU() == totalQuota;
    });

    // Get one of the router
    String routerURL = cluster.getRandomRouterURL();
    AvroGenericStoreClient<String, Object> storeClient = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerURL));
    try {
      for (int i = 0; i < totalQuota / numberOfRouter * timeWindowInSec; i++) {
        storeClient.get("empty-key").get();
      }
    } catch (ExecutionException e) {
      Assert.fail("Usage has not exceeded the quota.");
    }
    try {
      // Send more requests to avoid flaky test.
      for (int i = 0; i < 5; i++) {
        storeClient.get("empty-key").get();
      }
      Assert.fail("Usage has exceeded the quota, should get the QuotaExceededException.");
    } catch (ExecutionException e) {
      // expected
    }

    // fail one router
    cluster.stopVeniceRouter(cluster.getRandomVeniceRouter().getPort());
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> cluster.getRandomVeniceRouter().getRoutersClusterManager().getLiveRoutersCount() == 1);
    routerURL = cluster.getRandomRouterURL();
    storeClient = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerURL));
    // now one router has the entire quota.
    try {
      for (int i = 0; i < totalQuota * timeWindowInSec; i++) {
        storeClient.get("empty-key").get();
      }
    } catch (ExecutionException e) {
      Assert.fail("Usage has not exceeded the quota.");
    }
    try {
      // Send more requests to avoid flaky test.
      for (int i = 0; i < 5; i++) {
        storeClient.get("empty-key").get();
      }
      Assert.fail("Usage has exceeded the quota, should get the QuotaExceededException.");
    } catch (ExecutionException e) {
      // expected
    }
  }

  @Test(priority = 1, groups = "flaky", timeOut = 60 * Time.MS_PER_SECOND)
  public void testNoopThrottlerCanReportPerRouterStoreQuota() {
    Properties props = new Properties();
    props.put(ROUTER_ENABLE_READ_THROTTLING, "false");
    for (VeniceRouterWrapper router: cluster.getVeniceRouters()) {
      cluster.removeVeniceRouter(router.getPort());
      cluster.addVeniceRouter(props);
    }

    VeniceRouterWrapper router = cluster.getRandomVeniceRouter();
    MetricsRepository metricsRepository = router.getMetricsRepository();
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    // Get default read quota
    ControllerClient controllerClient = new ControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
    StoreResponse response = controllerClient.getStore(storeName);
    Assert.assertFalse(response.isError());
    final double expectedDefaultQuota = (double) response.getStore().getReadQuotaInCU() / numberOfRouter;

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(metrics.containsKey("." + storeName + "--read_quota_per_router.Gauge"));
      Assert.assertEquals(
          metrics.get("." + storeName + "--read_quota_per_router.Gauge").value(),
          expectedDefaultQuota,
          0.0001);
    });

    // Test that the stats change after updating store read quota config
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(1000 * 1000));
    response = controllerClient.getStore(storeName);
    Assert.assertFalse(response.isError());
    final double expectedQuota = (double) response.getStore().getReadQuotaInCU() / numberOfRouter;

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(metrics.containsKey("." + storeName + "--read_quota_per_router.Gauge"));
      Assert
          .assertEquals(metrics.get("." + storeName + "--read_quota_per_router.Gauge").value(), expectedQuota, 0.0001);
    });
  }
}
