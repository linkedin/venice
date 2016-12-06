package com.linkedin.venice.stats;

import com.google.common.collect.Lists;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPerStoreService;
import com.linkedin.venice.kafka.consumer.StoreConsumptionTask;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.List;

public class TestServerAggStats {
  private static final int STORE_CONSUMPTION_TASK_NUM = 3;
  private static final String STORE_FOO = "store_foo";
  private static final String STORE_BAR = "store_bar";

  private MockTehutiReporter reporter;
  private ServerAggStats stats;
  private KafkaConsumerPerStoreService mockKafkaConsumerPerStoreService;

  @BeforeSuite
  public void setup() {
    MetricsRepository metrics = new MetricsRepository();
    reporter = new MockTehutiReporter();
    metrics.addReporter(reporter);
    mockKafkaConsumerPerStoreService = Mockito.mock(KafkaConsumerPerStoreService.class);

    ServerAggStats.init(metrics, mockKafkaConsumerPerStoreService);
    stats = ServerAggStats.getInstance();

    stats.recordSuccessRequest(STORE_FOO);
    stats.recordSuccessRequest(STORE_BAR);
    stats.recordErrorRequest(STORE_FOO);
    stats.recordErrorRequest();
  }

  @Test
  public void testMetrics() {
    Assert.assertEquals(reporter.query("." + STORE_FOO + "_success_request.Count").value(), 1d);
    Assert.assertEquals(reporter.query(".total_error_request.Count").value(), 2d);
    Assert.assertEquals(reporter.query(".total_success_request_ratio.LambdaStat").value(), 0.5d);
  }

  @Test
  public void testOffsetLagStat() {
    List<StoreConsumptionTask> mockStoreConsumptionTasks = Lists.newArrayList();
    for (int i = 0; i < STORE_CONSUMPTION_TASK_NUM; i ++) {
      StoreConsumptionTask task = Mockito.mock(StoreConsumptionTask.class);
      Mockito.doReturn(Long.valueOf(i)).when(task).getOffsetLag();
      Mockito.doReturn(STORE_FOO + "_v" + String.valueOf(i)).when(task).getTopic();
      mockStoreConsumptionTasks.add(task);
    }

    Mockito.doReturn(mockStoreConsumptionTasks)
        .when(mockKafkaConsumerPerStoreService)
        .getRunningConsumptionTasksByStore(STORE_FOO);

    Assert.assertEquals(reporter.query("." + STORE_FOO + "_kafka_offset_lag.OffsetLagStat").value(), 2d);
  }

  @AfterSuite
  public void cleanup() {
    stats.close();
  }
}