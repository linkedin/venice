package com.linkedin.venice.storagenode;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class KeyNotFoundMetricIntegrationTest {
  private VeniceClusterWrapper veniceCluster;
  private String storeName;
  private AvroGenericStoreClient<String, GenericRecord> client;

  @BeforeClass
  public void setUp() {
    veniceCluster = ServiceFactory.getVeniceCluster(new VeniceClusterCreateOptions.Builder().build());
    storeName = Utils.getUniqueString("test-store");
    String valueSchemaStr = "{" + "  \"type\": \"record\"," + "  \"name\": \"User\"," + "  \"fields\": ["
        + "    {\"name\": \"name\", \"type\": \"string\"}" + "  ]" + "}";
    veniceCluster.useControllerClient(controllerClient -> {
      controllerClient.createNewStore(storeName, "owner", "\"string\"", valueSchemaStr);
      controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadComputationEnabled(true));
    });

    Schema valueSchema = Schema.parse(valueSchemaStr);
    GenericRecord value1 = new GenericData.Record(valueSchema);
    value1.put("name", "value1");

    veniceCluster.createVersion(
        storeName,
        "\"string\"",
        valueSchemaStr,
        Stream.of(new AbstractMap.SimpleEntry<>("key1", value1)));

    client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()));
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(client);
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(singleThreaded = true)
  public void testKeyNotFoundMetric() {
    // Single Get - Key Found
    try {
      client.get("key1").get();
    } catch (Exception e) {
      Assert.fail("Get failed", e);
    }

    // Single Get - Key Not Found
    try {
      client.get("key2").get();
    } catch (Exception e) {
      Assert.fail("Get failed", e);
    }

    // Multi Get - 1 Key Found, 1 Key Not Found
    Set<String> keys = new HashSet<>();
    keys.add("key1");
    keys.add("key3");
    try {
      client.batchGet(keys).get();
    } catch (Exception e) {
      Assert.fail("Batch Get failed", e);
    }

    // Compute - 1 Key Found, 1 Key Not Found
    try {
      client.compute().project("name").execute(keys).get();
    } catch (Exception e) {
      Assert.fail("Compute failed", e);
    }

    String singleGetMetricName = "." + storeName + "--key_not_found.Rate";
    String multiGetMetricName = "." + storeName + "--multiget_key_not_found.Rate";
    String computeMetricName = "." + storeName + "--compute_key_not_found.Rate";
    String totalSingleGetMetricName = ".total--key_not_found.Rate";
    String totalMultiGetMetricName = ".total--multiget_key_not_found.Rate";
    String totalComputeMetricName = ".total--compute_key_not_found.Rate";

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      double singleGetNotFoundRate = MetricsUtils.getMax(singleGetMetricName, veniceCluster.getVeniceServers());
      double multiGetNotFoundRate = MetricsUtils.getMax(multiGetMetricName, veniceCluster.getVeniceServers());
      double computeNotFoundRate = MetricsUtils.getMax(computeMetricName, veniceCluster.getVeniceServers());
      double totalSingleGetNotFoundRate =
          MetricsUtils.getMax(totalSingleGetMetricName, veniceCluster.getVeniceServers());
      double totalMultiGetNotFoundRate = MetricsUtils.getMax(totalMultiGetMetricName, veniceCluster.getVeniceServers());
      double totalComputeNotFoundRate = MetricsUtils.getMax(totalComputeMetricName, veniceCluster.getVeniceServers());

      Assert.assertTrue(
          singleGetNotFoundRate > 0,
          "Single Get key_not_found metric should be positive. Metric: " + singleGetMetricName);
      Assert.assertTrue(
          multiGetNotFoundRate > 0,
          "Multi Get key_not_found metric should be positive. Metric: " + multiGetMetricName);
      Assert.assertTrue(
          computeNotFoundRate > 0,
          "Compute key_not_found metric should be positive. Metric: " + computeMetricName);
      Assert.assertTrue(
          totalSingleGetNotFoundRate > 0,
          "Total Single Get key_not_found metric should be positive. Metric: " + totalSingleGetMetricName);
      Assert.assertTrue(
          totalMultiGetNotFoundRate > 0,
          "Total Multi Get key_not_found metric should be positive. Metric: " + totalMultiGetMetricName);
      Assert.assertTrue(
          totalComputeNotFoundRate > 0,
          "Total Compute key_not_found metric should be positive. Metric: " + totalComputeMetricName);
    });
  }
}
