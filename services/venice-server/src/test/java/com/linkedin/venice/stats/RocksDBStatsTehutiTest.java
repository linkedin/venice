package com.linkedin.venice.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import io.tehuti.metrics.MetricsRepository;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.testng.annotations.Test;


/**
 * Tehuti baseline tests for {@link RocksDBStats}. Uses plain {@link MetricsRepository} (not
 * {@link VeniceMetricsRepository}) to avoid the static AsyncGaugeExecutor shutdown issue
 * that occurs when VeniceMetricsRepository.close() is called in other test classes.
 *
 * <p>Each test creates its own MetricsRepository to stay self-contained.
 */
public class RocksDBStatsTehutiTest {
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String STAT_NAME = "rocksdb_stat";

  @Test
  public void testTickerSensors() {
    MetricsRepository repo = new MetricsRepository();
    createAndInit(
        repo,
        mockStats(
            TickerType.BLOCK_CACHE_DATA_HIT,
            100L,
            TickerType.BLOCK_CACHE_MISS,
            50L,
            TickerType.BLOOM_FILTER_USEFUL,
            42L,
            TickerType.BLOCK_CACHE_COMPRESSION_DICT_MISS,
            7L));

    assertEquals(getGauge(repo, "rocksdb_block_cache_data_hit"), 100.0);
    assertEquals(getGauge(repo, "rocksdb_block_cache_compression_dict_miss"), 7.0);
    assertEquals(getGauge(repo, "rocksdb_bloom_filter_useful"), 42.0);
    // Hit ratio: 100 / (100 + 50) = 0.6667
    assertEquals(getGauge(repo, "rocksdb_block_cache_hit_ratio"), 100.0 / 150.0, 0.001);
  }

  @Test
  public void testReadAmplificationReturnsNegativeOneWhenStatNull() {
    MetricsRepository repo = new MetricsRepository();
    new RocksDBStats(repo, STAT_NAME, TEST_CLUSTER_NAME);
    assertEquals(getGauge(repo, "rocksdb_read_amplification_factor"), -1.0);
  }

  @Test
  public void testHitRatioReturnsNaNWhenBothZero() {
    MetricsRepository repo = new MetricsRepository();
    createAndInit(repo, mockStats(TickerType.BLOCK_CACHE_DATA_HIT, 0L, TickerType.BLOCK_CACHE_MISS, 0L));

    assertTrue(
        Double.isNaN(getGauge(repo, "rocksdb_block_cache_hit_ratio")),
        "Expected NaN when both dataHit and miss are 0");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testSetRocksDBStatThrowsOnDoubleInit() {
    MetricsRepository repo = new MetricsRepository();
    RocksDBStats stats = createAndInit(repo, mock(Statistics.class));
    stats.setRocksDBStat(mock(Statistics.class));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetRocksDBStatThrowsOnNull() {
    new RocksDBStats(new MetricsRepository(), STAT_NAME, TEST_CLUSTER_NAME).setRocksDBStat(null);
  }

  // --- Helpers ---

  private RocksDBStats createAndInit(MetricsRepository repo, Statistics mockStat) {
    RocksDBStats stats = new RocksDBStats(repo, STAT_NAME, TEST_CLUSTER_NAME);
    stats.setRocksDBStat(mockStat);
    return stats;
  }

  private static Statistics mockStats(Object... tickerValuePairs) {
    Statistics mock = mock(Statistics.class);
    for (int i = 0; i < tickerValuePairs.length; i += 2) {
      when(mock.getTickerCount((TickerType) tickerValuePairs[i])).thenReturn((Long) tickerValuePairs[i + 1]);
    }
    return mock;
  }

  private double getGauge(MetricsRepository repo, String sensorName) {
    return repo.getMetric("." + STAT_NAME + "--" + sensorName + ".Gauge").value();
  }
}
