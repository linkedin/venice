package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitorConfig;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class ServerOverloadTest extends AbstractClientEndToEndSetup {
  @Override
  protected Properties getExtraServerProperties() {
    Properties configProps = new Properties();
    configProps.setProperty(ConfigKeys.SERVER_LOAD_CONTROLLER_ENABLED, "true");
    configProps.setProperty(ConfigKeys.SERVER_LOAD_CONTROLLER_SINGLE_GET_LATENCY_ACCEPT_THRESHOLD_IN_MS, "0");
    configProps.setProperty(ConfigKeys.SERVER_LOAD_CONTROLLER_MULTI_GET_LATENCY_ACCEPT_THRESHOLD_IN_MS, "10");
    configProps.setProperty(ConfigKeys.SERVER_LOAD_CONTROLLER_COMPUTE_LATENCY_ACCEPT_THRESHOLD_IN_MS, "10");
    return configProps;
  }

  @Test
  public void testServerOverload() throws IOException, ExecutionException, InterruptedException {
    // Create a fast client with overload detection enabled
    InstanceHealthMonitorConfig healthMonitorConfig =
        new InstanceHealthMonitorConfig.Builder().setLoadControllerEnabled(true)
            .setLoadControllerAcceptMultiplier(1.0)
            .setLoadControllerWindowSizeInSec(3)
            .setLoadControllerMaxRejectionRatio(0.9)
            .setLoadControllerRejectionRatioUpdateIntervalInSec(1)
            .build();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(healthMonitorConfig);

    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setInstanceHealthMonitor(healthMonitor);

    MetricsRepository clientMetricsRepository = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        clientMetricsRepository,
        StoreMetadataFetchMode.SERVER_BASED_METADATA);
    // test single get
    boolean receivedOverloadException = false;
    for (int rounds = 0; rounds < 10; ++rounds) {
      for (int i = 0; i < recordCnt; ++i) {
        Utils.sleep(10);
        String key = keyPrefix + i;
        try {
          genericFastClient.get(key).get();
        } catch (Exception e) {
          // Exception can happen because of overload.
          if (e.getMessage().contains("Service overloaded")) {
            receivedOverloadException = true;
          }
        }
      }
    }
    assertTrue(receivedOverloadException);
    assertEquals(healthMonitor.getOverloadedInstanceCount(), 2);

    // Verify some metrics
    String overloadInstanceCountMetricName = "." + storeName + "--overloaded_instance_count.Avg";
    assertTrue(clientMetricsRepository.getMetric(overloadInstanceCountMetricName).value() > 0);

    String nonAvailReplicaMetricName = "." + storeName + "--no_available_replica_request_count.OccurrenceRate";
    assertTrue(clientMetricsRepository.getMetric(nonAvailReplicaMetricName).value() > 0);
  }
}
