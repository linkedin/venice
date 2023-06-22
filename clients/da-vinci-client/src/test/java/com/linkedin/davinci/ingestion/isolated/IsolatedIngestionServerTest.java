package com.linkedin.davinci.ingestion.isolated;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IsolatedIngestionServerTest {
  @Test
  public void testUpdateMetric() {
    IsolatedIngestionServer isolatedIngestionServer = mock(IsolatedIngestionServer.class);
    MetricsRepository metricsRepository = new MetricsRepository();
    metricsRepository.addMetric("foo", (x, y) -> 1.0);
    when(isolatedIngestionServer.getMetricsRepository()).thenReturn(metricsRepository);
    RedundantExceptionFilter redundantExceptionFilter =
        new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(10));
    when(isolatedIngestionServer.getRedundantExceptionFilter()).thenReturn(redundantExceptionFilter);
    IsolatedIngestionRequestClient metricClient = mock(IsolatedIngestionRequestClient.class);
    when(isolatedIngestionServer.getMetricClient()).thenReturn(metricClient);

    Map<String, Double> metricMap = new HashMap<>();
    when(isolatedIngestionServer.getMetricsMap()).thenReturn(metricMap);
    doCallRealMethod().when(isolatedIngestionServer).reportMetricsUpdateToMainProcess();
    isolatedIngestionServer.reportMetricsUpdateToMainProcess();
    metricsRepository.addMetric("bar.MAX", (x, y) -> 2.0);
    isolatedIngestionServer.reportMetricsUpdateToMainProcess();
    metricsRepository.addMetric("car", (x, y) -> {
      throw new VeniceException("Metric fetching bug");
    });
    isolatedIngestionServer.reportMetricsUpdateToMainProcess();
    ArgumentCaptor<IngestionMetricsReport> argumentCaptor = ArgumentCaptor.forClass(IngestionMetricsReport.class);
    verify(metricClient, times(3)).reportMetricUpdate(argumentCaptor.capture());
    Assert.assertEquals(argumentCaptor.getAllValues().get(0).aggregatedMetrics.size(), 1);
    Assert.assertEquals(argumentCaptor.getAllValues().get(1).aggregatedMetrics.size(), 1);
    Assert.assertEquals(argumentCaptor.getAllValues().get(2).aggregatedMetrics.size(), 0);
  }

  @Test
  public void testStopConsumptionAndReport() {
    Map<String, Map<Integer, AtomicBoolean>> topicPartitionSubscriptionMap = new VeniceConcurrentHashMap<>();
    IsolatedIngestionServer isolatedIngestionServer = mock(IsolatedIngestionServer.class);
    when(isolatedIngestionServer.getTopicPartitionSubscriptionMap()).thenReturn(topicPartitionSubscriptionMap);
    ExecutorService longRunningTaskExecutor = Executors.newFixedThreadPool(10);
    when(isolatedIngestionServer.getLongRunningTaskExecutor()).thenReturn(longRunningTaskExecutor);
    ExecutorService statusReportingExecutor = Executors.newSingleThreadExecutor();
    when(isolatedIngestionServer.getStatusReportingExecutor()).thenReturn(statusReportingExecutor);
    doCallRealMethod().when(isolatedIngestionServer).stopConsumptionAndReport(any());
    doCallRealMethod().when(isolatedIngestionServer).setResourceToBeUnsubscribed(anyString(), anyInt());
    KafkaStoreIngestionService storeIngestionService = mock(KafkaStoreIngestionService.class);
    when(isolatedIngestionServer.getStoreIngestionService()).thenReturn(storeIngestionService);
    StorageService storageService = mock(StorageService.class);
    when(isolatedIngestionServer.getStorageService()).thenReturn(storageService);
    VeniceStoreVersionConfig storeVersionConfig = mock(VeniceStoreVersionConfig.class);
    when(storeVersionConfig.getStoreVersionName()).thenReturn("topic");
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    when(configLoader.getStoreConfig(anyString())).thenReturn(storeVersionConfig);
    when(isolatedIngestionServer.getConfigLoader()).thenReturn(configLoader);
    when(isolatedIngestionServer.submitStopConsumptionAndCloseStorageTask(anyString(), anyInt())).thenCallRealMethod();
    IsolatedIngestionRequestClient client = mock(IsolatedIngestionRequestClient.class);
    when(isolatedIngestionServer.getReportClient()).thenReturn(client);

    IngestionTaskReport badReport = new IngestionTaskReport();
    badReport.topicName = "topic";
    badReport.partitionId = 0;
    badReport.reportType = 0;
    when(client.reportIngestionStatus(badReport)).thenReturn(false);

    IngestionTaskReport goodReport = new IngestionTaskReport();
    goodReport.topicName = "topic";
    goodReport.partitionId = 1;
    goodReport.reportType = 0;
    when(client.reportIngestionStatus(goodReport)).thenReturn(true);

    /**
     * Case 1: Handover is completed successfully. For good report, client report was successful. For bad report, client
     * report was bad.
     */
    when(storeIngestionService.isPartitionConsuming(anyString(), anyInt())).thenReturn(true);
    isolatedIngestionServer.stopConsumptionAndReport(badReport);
    verify(isolatedIngestionServer, times(1)).setResourceToBeUnsubscribed("topic", 0);
    isolatedIngestionServer.stopConsumptionAndReport(goodReport);
    verify(isolatedIngestionServer, times(1)).setResourceToBeUnsubscribed("topic", 1);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(storeIngestionService, times(1)).isPartitionConsuming("topic", 0);
      verify(storeIngestionService, times(1))
          .waitIngestionTaskToCompleteAllPartitionPendingActions("topic", 0, 100, 300);
      verify(storeIngestionService, times(1)).isPartitionConsuming("topic", 1);
      verify(storeIngestionService, times(1))
          .waitIngestionTaskToCompleteAllPartitionPendingActions("topic", 1, 100, 300);
      verify(storageService, times(2)).closeStorePartition(any(), anyInt());
      verify(client, times(2)).reportIngestionStatus(any());
    });

    /**
     * Case 2: Handover is does not complete due to pending action stops consumption. Partition will not be closed and
     * subscription was resumed, client will not report ingestion status.
     */
    when(storeIngestionService.isPartitionConsuming(anyString(), anyInt())).thenReturn(false);
    goodReport.partitionId = 2;
    isolatedIngestionServer.stopConsumptionAndReport(goodReport);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(storeIngestionService, times(1)).isPartitionConsuming("topic", 2);
      verify(storeIngestionService, times(1))
          .waitIngestionTaskToCompleteAllPartitionPendingActions("topic", 2, 100, 300);
      verify(storageService, times(2)).closeStorePartition(any(), anyInt());
      verify(isolatedIngestionServer, times(1)).setResourceToBeUnsubscribed("topic", 2);
      verify(isolatedIngestionServer, times(1)).setResourceToBeSubscribed("topic", 2);
      verify(client, times(2)).reportIngestionStatus(any());
    });

  }
}
