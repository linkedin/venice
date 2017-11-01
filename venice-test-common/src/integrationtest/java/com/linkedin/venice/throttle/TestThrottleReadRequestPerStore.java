package com.linkedin.venice.throttle;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;


public class TestThrottleReadRequestPerStore {
  private VeniceClusterWrapper cluster;
  private int testTimeOutMS = 3000;
  private int numberOfRouter = 2;

  private VeniceWriter<Object, Object> veniceWriter;
  private String storeName;
  private int currentVersion;

  @BeforeClass
  public void setup()
      throws Exception {
    int numberOfController = 1;
    int numberOfServer = 1;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, numberOfServer, numberOfRouter);

    VersionCreationResponse response = cluster.getNewStoreVersion();
    Assert.assertFalse(response.isError());
    storeName = response.getName();
    currentVersion = response.getVersion();

    VeniceProperties clientProps = new PropertyBuilder().put(KAFKA_BOOTSTRAP_SERVERS, cluster.getKafka().getAddress())
        .put(ZOOKEEPER_ADDRESS, cluster.getZk().getAddress())
        .put(CLUSTER_NAME, cluster.getClusterName())
        .build();

    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroGenericSerializer(stringSchema);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroGenericSerializer(stringSchema);

    int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
    veniceWriter = new VeniceWriter<>(clientProps, response.getKafkaTopic(), keySerializer, valueSerializer);
    String key = TestUtils.getUniqueString("key");
    String value = TestUtils.getUniqueString("value");
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    veniceWriter.put(key, value, valueSchemaId).get();
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<String, String>());
  }

  @AfterClass
  public void cleanup() {
    veniceWriter.close();
    cluster.close();
  }

  @Test(enabled = false) // TODO: FIX THIS BROKEN TEST
  public void testReadRequestBeThrottled()
      throws InterruptedException {
    long timeWindowInSec = TimeUnit.MILLISECONDS.toSeconds(ReadRequestThrottler.DEFAULT_STORE_QUOTA_TIME_WINDOW);
    // Setup read quota for the store.
    long totalQuota = 10;
    cluster.getMasterVeniceController()
        .getVeniceAdmin()
        .updateStore(cluster.getClusterName(), storeName,
                     Optional.empty(),
                     Optional.empty(),
                     Optional.empty(),
                     Optional.empty(),
                     Optional.empty(),
                     Optional.of(totalQuota),
                     Optional.empty(),
                     Optional.empty(),
                     Optional.empty(),
                     Optional.empty(),
                     Optional.empty());

    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      Store store = cluster.getRandomVeniceRouter().getMetaDataRepository().getStore(storeName);
      return store.getCurrentVersion() == currentVersion && store.getReadQuotaInCU() == totalQuota;
    });

    // Get one of the router
    String routerURL = cluster.getRandomRouterURL();
    AvroGenericStoreClient<String, Object> storeClient =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerURL));
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
      //expected
    }

    // fail one router
    cluster.stopVeniceRouter(cluster.getRandomVeniceRouter().getPort());
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getRandomVeniceRouter().getRoutersClusterManager().getLiveRoutersCount() == 1);
    routerURL = cluster.getRandomRouterURL();
    storeClient =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerURL));
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
      //expected
    }
  }
}
