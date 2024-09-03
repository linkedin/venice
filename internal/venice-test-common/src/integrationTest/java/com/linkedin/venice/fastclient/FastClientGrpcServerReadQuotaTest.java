package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class FastClientGrpcServerReadQuotaTest extends AbstractClientEndToEndSetup {
  /**
   * test is copied from {@link FastClientIndividualFeatureConfigurationTest}, we need to reset server-side stats before each test class
   * @throws Exception
   */
  @Test(timeOut = TIME_OUT)
  public void testGrpcServerReadQuota() throws Exception {
    GrpcClientConfig grpcClientConfig = new GrpcClientConfig.Builder().setR2Client(r2Client)
        .setNettyServerToGrpcAddress(veniceCluster.getNettyServerToGrpcAddress())
        .setSSLFactory(SslUtils.getVeniceLocalSslFactory())
        .build();

    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setUseGrpc(true)
            .setGrpcClientConfig(grpcClientConfig)
            .setSpeculativeQueryEnabled(false);

    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA);
    // Update the read quota to 1000 and make 500 requests, all requests should be allowed.
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setReadQuotaInCU(1000).setStorageNodeReadQuotaEnabled(true)));
    });
    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < recordCnt; i++) {
        String key = keyPrefix + i;
        GenericRecord value = genericFastClient.get(key).get();
        assertEquals((int) value.get(VALUE_FIELD_NAME), i);
      }
    }
    ArrayList<MetricsRepository> serverMetrics = new ArrayList<>();
    for (int i = 0; i < veniceCluster.getVeniceServers().size(); i++) {
      serverMetrics.add(veniceCluster.getVeniceServers().get(i).getMetricsRepository());
    }
    String readQuotaRequestedString = "." + storeName + "--quota_request.Rate";
    String readQuotaRejectedString = "." + storeName + "--quota_rejected_request.Rate";
    String readQuotaAllowedUnintentionally = "." + storeName + "--quota_unintentionally_allowed_key_count.Count";
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      for (MetricsRepository serverMetric: serverMetrics) {
        assertNotNull(serverMetric.getMetric(readQuotaRequestedString));
        assertNotNull(serverMetric.getMetric(readQuotaRejectedString));
        assertNotNull(serverMetric.getMetric(readQuotaAllowedUnintentionally));
      }
    });
    int quotaRequestedSum = 0;
    for (MetricsRepository serverMetric: serverMetrics) {
      quotaRequestedSum += serverMetric.getMetric(readQuotaRequestedString).value();
      assertEquals(serverMetric.getMetric(readQuotaRejectedString).value(), 0d);
      assertEquals(serverMetric.getMetric(readQuotaAllowedUnintentionally).value(), 0d);
    }
    assertTrue(quotaRequestedSum >= 0, "Quota requested sum: " + quotaRequestedSum);

    // Update the read quota to 50 and make as many requests needed to trigger quota rejected exception.
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
    boolean readQuotaRejected = false;
    for (MetricsRepository serverMetric: serverMetrics) {
      if (serverMetric.getMetric(readQuotaRejectedString).value() > 0) {
        readQuotaRejected = true;
        break;
      }
    }
    assertTrue(readQuotaRejected);
  }

}
