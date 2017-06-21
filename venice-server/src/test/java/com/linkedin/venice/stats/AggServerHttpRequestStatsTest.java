package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AggServerHttpRequestStatsTest {
  private AggServerHttpRequestStats stats;
  protected MetricsRepository metricsRepository;
  protected MockTehutiReporter reporter;

  private static final String STORE_FOO = "store_foo";
  private static final String STORE_BAR = "store_bar";

  @BeforeTest
  public void setup() {
    metricsRepository = new MetricsRepository();
    this.reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    stats = new AggServerHttpRequestStats(metricsRepository, RequestType.SINGLE_GET);

    stats.recordSuccessRequest(STORE_FOO);
    stats.recordSuccessRequest(STORE_BAR);
    stats.recordErrorRequest(STORE_FOO);
    stats.recordErrorRequest();
  }

  @AfterTest
  public void cleanup() {
    metricsRepository.close();
  }

  @Test
  public void testMetrics() {
    Assert.assertTrue(reporter.query("." + STORE_FOO + "--success_request.OccurrenceRate").value() > 0,
        "success_request rate should be positive");
    Assert.assertTrue(reporter.query(".total--error_request.OccurrenceRate").value() > 0,
        "error_request rate should be positive");
    Assert.assertTrue(reporter.query(".total--success_request_ratio.RatioStat").value() > 0,
        "success_request_ratio should be positive");
  }
}
