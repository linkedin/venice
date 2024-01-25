package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.AbstractVeniceStats.getSensorFullName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.SystemTime;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class KafkaConsumerServiceStatsTest {
  private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerServiceStatsTest.class);

  @Test
  public void testParentStats() {
    MetricsRepository metricsRepository = new MetricsRepository();
    String nameWithKafkaClusterAlias = "moon-east-1";
    String getLatestOffsetIsPresentName =
        getSensorFullName(nameWithKafkaClusterAlias, "getLatestOffsetIsPresent.OccurrenceRate");
    String getLatestOffsetIsAbsentName =
        getSensorFullName(nameWithKafkaClusterAlias, "getLatestOffsetIsAbsent.OccurrenceRate");
    String getLatestOffsetName = getSensorFullName(nameWithKafkaClusterAlias, "getLatestOffset.OccurrenceRate");
    String getOffsetLagIsPresentName =
        getSensorFullName(nameWithKafkaClusterAlias, "getOffsetLagIsPresent.OccurrenceRate");
    String getOffsetLagIsAbsentName =
        getSensorFullName(nameWithKafkaClusterAlias, "getOffsetLagIsAbsent.OccurrenceRate");
    String getOffsetLagName = getSensorFullName(nameWithKafkaClusterAlias, "getOffsetLag.OccurrenceRate");
    Metric getLatestOffsetIsPresent = metricsRepository.getMetric(getLatestOffsetIsPresentName);
    Metric getLatestOffsetIsAbsent = metricsRepository.getMetric(getLatestOffsetIsAbsentName);
    Metric getLatestOffset = metricsRepository.getMetric(getLatestOffsetName);
    Metric getOffsetLagIsPresent = metricsRepository.getMetric(getOffsetLagIsPresentName);
    Metric getOffsetLagIsAbsent = metricsRepository.getMetric(getOffsetLagIsAbsentName);
    Metric getOffsetLag = metricsRepository.getMetric(getOffsetLagName);
    Metric[] allMetrics = new Metric[] { getLatestOffsetIsPresent, getLatestOffsetIsAbsent, getLatestOffset,
        getOffsetLagIsPresent, getOffsetLagIsAbsent, getOffsetLag };

    // Verify initial state
    for (Metric metric: allMetrics) {
      assertNull(metric);
    }

    // Create objects under test
    KafkaConsumerServiceStats stats =
        new KafkaConsumerServiceStats(metricsRepository, nameWithKafkaClusterAlias, () -> 1, null, SystemTime.INSTANCE);
    LOGGER.info(metricsRepository.metrics().keySet().toString().replace(", ", ",\n"));
    getLatestOffsetIsPresent = metricsRepository.getMetric(getLatestOffsetIsPresentName);
    getLatestOffsetIsAbsent = metricsRepository.getMetric(getLatestOffsetIsAbsentName);
    getLatestOffset = metricsRepository.getMetric(getLatestOffsetName);
    getOffsetLagIsPresent = metricsRepository.getMetric(getOffsetLagIsPresentName);
    getOffsetLagIsAbsent = metricsRepository.getMetric(getOffsetLagIsAbsentName);
    getOffsetLag = metricsRepository.getMetric(getOffsetLagName);
    allMetrics = new Metric[] { getLatestOffsetIsPresent, getLatestOffsetIsAbsent, getLatestOffset,
        getOffsetLagIsPresent, getOffsetLagIsAbsent, getOffsetLag };

    // Verify that metrics exist but are empty
    for (Metric metric: allMetrics) {
      assertNotNull(metric);
      assertEquals(metric.value(), 0.0);
    }

    // Record into one of two children stats
    stats.recordLatestOffsetIsPresent();
    stats.recordOffsetLagIsPresent();

    // Verify that the parent is incremented but not the other child
    assertTrue(getLatestOffsetIsPresent.value() > 0.0);
    assertEquals(getLatestOffsetIsAbsent.value(), 0.0);
    assertTrue(getLatestOffset.value() > 0.0);
    assertTrue(getOffsetLagIsPresent.value() > 0.0);
    assertEquals(getOffsetLagIsAbsent.value(), 0.0);
    assertTrue(getOffsetLag.value() > 0.0);

    // Record into the second child
    stats.recordLatestOffsetIsAbsent();
    stats.recordOffsetLagIsAbsent();

    // Verify that the parent is larger than both children
    assertTrue(getLatestOffsetIsPresent.value() > 0.0);
    assertTrue(getLatestOffsetIsAbsent.value() > 0.0);
    assertTrue(getLatestOffset.value() > getLatestOffsetIsPresent.value());
    assertTrue(getLatestOffset.value() > getLatestOffsetIsAbsent.value());
    assertTrue(getOffsetLagIsPresent.value() > 0.0);
    assertTrue(getOffsetLagIsAbsent.value() > 0.0);
    assertTrue(getOffsetLag.value() > getLatestOffsetIsPresent.value());
    assertTrue(getOffsetLag.value() > getLatestOffsetIsAbsent.value());
  }

}
