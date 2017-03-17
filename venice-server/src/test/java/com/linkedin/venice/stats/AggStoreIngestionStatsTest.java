package com.linkedin.venice.stats;


import com.linkedin.venice.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AggStoreIngestionStatsTest {
  private AggStoreIngestionStats stats;
  private MetricsRepository metricsRepository;
  private MockTehutiReporter reporter;

  private static final String STORE_FOO = "store_foo";
  private static final String STORE_BAR = "store_bar";

  @BeforeTest
  public void setup() {
    metricsRepository = new MetricsRepository();
    this.reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    stats = new AggStoreIngestionStats(metricsRepository);

    StoreIngestionTask task = Mockito.mock(StoreIngestionTask.class);
    Mockito.doReturn(1l).when(task).getOffsetLag();
    Mockito.doReturn(true).when(task).isRunning();

    stats.updateStoreConsumptionTask(STORE_FOO, task);
    stats.recordPollResultNum(STORE_FOO, 1);
    stats.recordPollResultNum(STORE_BAR, 2);
  }

  @AfterTest
  public void cleanup() {
    metricsRepository.close();
  }

  @Test
  public void testMetrics() {
    Assert.assertEquals(reporter.query("." + STORE_FOO + "--kafka_poll_result_num.Total").value(), 1d);
    Assert.assertEquals(reporter.query("." + STORE_BAR + "--kafka_poll_result_num.Total").value(), 2d);
    Assert.assertEquals(reporter.query(".total--kafka_poll_result_num.Avg").value(), 1.5d);
    Assert.assertEquals(reporter.query("." + STORE_FOO + "--kafka_offset_lag.OffsetLagStat").value(), 1d);
  }
}
