package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitorConfig;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class StoreOverloadTest extends AbstractClientEndToEndSetup {
  @Test
  public void testStoreOverload() throws IOException {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setStoreLoadControllerEnabled(true)
            .setStoreLoadControllerAcceptMultiplier(1.0)
            .setStoreLoadControllerMaxRejectionRatio(0.9)
            .setStoreLoadControllerRejectionRatioUpdateIntervalInSec(1)
            .setStoreLoadControllerWindowSizeInSec(3)
            .setR2Client(r2Client)
            .setInstanceHealthMonitor(
                new InstanceHealthMonitor(
                    new InstanceHealthMonitorConfig.Builder().setRoutingRequestDefaultTimeoutMS(10000).build()));

    // Update the store quota to be minimal
    veniceCluster.useControllerClient(
        client -> assertFalse(
            client.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(1)).isError()));

    MetricsRepository clientMetricsRepository = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        clientMetricsRepository,
        StoreMetadataFetchMode.SERVER_BASED_METADATA);
    // test single get
    boolean receivedOverloadException = false;
    for (int rounds = 0; rounds < 3; ++rounds) {
      for (int i = 0; i < recordCnt; ++i) {
        Utils.sleep(10);
        String key = keyPrefix + i;
        try {
          genericFastClient.get(key).get();
        } catch (Exception e) {
          // Exception can happen because of overload.
          if (e.getMessage().contains(LoadControlledAvroGenericStoreClient.RATE_EXCEEDED_EXCEPTION.getMessage())) {
            receivedOverloadException = true;
          }
        }
      }
    }
    assertTrue(receivedOverloadException);
    // Verify some metrics
    String overloadInstanceCountMetricName =
        "." + storeName + "--rejected_request_count_by_load_controller.OccurrenceRate";
    assertTrue(clientMetricsRepository.getMetric(overloadInstanceCountMetricName).value() > 0);
  }
}
