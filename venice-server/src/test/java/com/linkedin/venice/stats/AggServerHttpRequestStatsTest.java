package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Percentiles;
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

  @Test
  public void testPercentileNamePattern() {
    String sensorName = "sensorName";
    String storeName = "storeName";
    Percentiles percentiles = TehutiUtils.getPercentileStatForNetworkLatency(sensorName, storeName);
    percentiles.stats().stream().map(namedMeasurable -> namedMeasurable.name()).forEach(System.out::println);
    String[] percentileStrings = new String[]{"50", "77", "90", "95", "99", "99.9"};

    for (int i = 0; i < percentileStrings.length; i++) {
      String expectedName = sensorName + "--" + storeName + "." + percentileStrings[i] + "thPercentile";
      Assert.assertTrue(percentiles.stats().stream()
              .map(namedMeasurable -> namedMeasurable.name())
              .anyMatch(s -> s.equals(expectedName)),
          "The Percentiles don't contain the expected name! Missing percentile with name: " + expectedName);
    }
  }
}
