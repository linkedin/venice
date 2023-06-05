package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class FastClientServerReadQuotaTest extends AbstractClientEndToEndSetup {
  @Test(timeOut = TIME_OUT)
  public void testServerReadQuota() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(false);
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA);
    // Update the read quota to 1000 and make 500 requests, all requests should be allowed.
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils
          .assertCommand(controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(1000)));
    });
    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < recordCnt; i++) {
        String key = keyPrefix + i;
        GenericRecord value = genericFastClient.get(key).get();
        assertEquals((int) value.get(VALUE_FIELD_NAME), i);
      }
    }
    MetricsRepository serverMetric = veniceCluster.getVeniceServers().get(0).getMetricsRepository();
    String readQuotaRequestedString = "." + storeName + "--quota_rcu_requested.Count";
    String readQuotaRejectedString = "." + storeName + "--quota_rcu_rejected.Count";
    String readQuotaUsageRatio = "." + storeName + "--read_quota_usage_ratio.Gauge";
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertNotNull(serverMetric.getMetric(readQuotaRequestedString));
      assertNotNull(serverMetric.getMetric(readQuotaRejectedString));
      assertNotNull(serverMetric.getMetric(readQuotaUsageRatio));
    });
    assertTrue(
        serverMetric.getMetric(readQuotaRequestedString).value() >= 250,
        "Metric value: " + serverMetric.getMetric(readQuotaRequestedString).value());
    assertEquals(serverMetric.getMetric(readQuotaRejectedString).value(), 0d);
    assertTrue(serverMetric.getMetric(readQuotaUsageRatio).value() > 0);

    // Update the read quota to 100 and make 500 requests again.
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils
          .assertCommand(controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(50)));
    });
    try {
      // Keep making requests until it gets rejected by read quota, it may take some time for the quota update to be
      // propagated to servers.
      for (int j = 0; j < Integer.MAX_VALUE; j++) {
        for (int i = 0; i < recordCnt; i++) {
          String key = keyPrefix + i;
          GenericRecord value = genericFastClient.get(key).get();
          assertEquals((int) value.get(VALUE_FIELD_NAME), i);
        }
      }
      fail("Exception should be thrown due to read quota violation");
    } catch (Exception clientException) {
      assertTrue(clientException.getMessage().contains("VeniceClientRateExceededException"));
    }
    assertTrue(serverMetric.getMetric(readQuotaRejectedString).value() > 0);
  }
}
