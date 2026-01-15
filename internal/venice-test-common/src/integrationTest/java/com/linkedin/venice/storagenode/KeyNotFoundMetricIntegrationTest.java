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
    final String singleGetMetricName = "." + storeName + "--key_not_found.Total";
    final String multiGetMetricName = "." + storeName + "--multiget_key_not_found.Total";
    final String computeMetricName = "." + storeName + "--compute_key_not_found.Total";
    final String totalSingleGetMetricName = ".total--key_not_found.Total";
    final String totalMultiGetMetricName = ".total--multiget_key_not_found.Total";
    final String totalComputeMetricName = ".total--compute_key_not_found.Total";

    // Single Get
    try {
      client.get("key1").get();
    } catch (Exception e) {
      Assert.fail("Get failed", e);
    }

    final double singleGetNotFoundTotalBefore = getMaxOrZero(singleGetMetricName, veniceCluster);
    final double totalSingleGetNotFoundTotalBefore = getMaxOrZero(totalSingleGetMetricName, veniceCluster);

    try {
      client.get("key2").get();
    } catch (Exception e) {
      Assert.fail("Get failed", e);
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      double singleGetNotFoundTotalAfter = getMaxOrZero(singleGetMetricName, veniceCluster);
      double totalSingleGetNotFoundTotalAfter = getMaxOrZero(totalSingleGetMetricName, veniceCluster);

      Assert.assertEquals(
          singleGetNotFoundTotalAfter - singleGetNotFoundTotalBefore,
          1.0,
          "Expected exactly 1 key_not_found increment for single get. Metric: " + singleGetMetricName);
      Assert.assertEquals(
          totalSingleGetNotFoundTotalAfter - totalSingleGetNotFoundTotalBefore,
          1.0,
          "Expected exactly 1 key_not_found increment for total single get. Metric: " + totalSingleGetMetricName);
    });

    // Multi Get
    Set<String> foundOnlyKeys = new HashSet<>();
    foundOnlyKeys.add("key1");
    try {
      client.batchGet(foundOnlyKeys).get();
    } catch (Exception e) {
      Assert.fail("Batch Get failed", e);
    }

    final double multiGetNotFoundTotalBefore = getMaxOrZero(multiGetMetricName, veniceCluster);
    final double totalMultiGetNotFoundTotalBefore = getMaxOrZero(totalMultiGetMetricName, veniceCluster);

    Set<String> keysWithMissing = new HashSet<>();
    keysWithMissing.add("key1");
    keysWithMissing.add("key3");
    try {
      client.batchGet(keysWithMissing).get();
    } catch (Exception e) {
      Assert.fail("Batch Get failed", e);
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      double multiGetNotFoundTotalAfter = getMaxOrZero(multiGetMetricName, veniceCluster);
      double totalMultiGetNotFoundTotalAfter = getMaxOrZero(totalMultiGetMetricName, veniceCluster);

      Assert.assertEquals(
          multiGetNotFoundTotalAfter - multiGetNotFoundTotalBefore,
          1.0,
          "Expected exactly 1 multiget_key_not_found increment for multi-get. Metric: " + multiGetMetricName);
      Assert.assertEquals(
          totalMultiGetNotFoundTotalAfter - totalMultiGetNotFoundTotalBefore,
          1.0,
          "Expected exactly 1 multiget_key_not_found increment for total multi-get. Metric: "
              + totalMultiGetMetricName);
    });

    // Compute
    try {
      client.compute().project("name").execute(foundOnlyKeys).get();
    } catch (Exception e) {
      Assert.fail("Compute failed", e);
    }

    final double computeNotFoundTotalBefore = getMaxOrZero(computeMetricName, veniceCluster);
    final double totalComputeNotFoundTotalBefore = getMaxOrZero(totalComputeMetricName, veniceCluster);

    try {
      client.compute().project("name").execute(keysWithMissing).get();
    } catch (Exception e) {
      Assert.fail("Compute failed", e);
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      double computeNotFoundTotalAfter = getMaxOrZero(computeMetricName, veniceCluster);
      double totalComputeNotFoundTotalAfter = getMaxOrZero(totalComputeMetricName, veniceCluster);

      Assert.assertEquals(
          computeNotFoundTotalAfter - computeNotFoundTotalBefore,
          1.0,
          "Expected exactly 1 compute_key_not_found increment for compute. Metric: " + computeMetricName);
      Assert.assertEquals(
          totalComputeNotFoundTotalAfter - totalComputeNotFoundTotalBefore,
          1.0,
          "Expected exactly 1 compute_key_not_found increment for total compute. Metric: " + totalComputeMetricName);
    });
  }

  private static double getMaxOrZero(String metricName, VeniceClusterWrapper veniceCluster) {
    double value = MetricsUtils.getMax(metricName, veniceCluster.getVeniceServers());
    return value == Double.MIN_VALUE ? 0.0 : value;
  }
}
