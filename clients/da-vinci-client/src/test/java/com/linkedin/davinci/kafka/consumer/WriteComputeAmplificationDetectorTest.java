package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.davinci.utils.ByteArrayKey;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class WriteComputeAmplificationDetectorTest {
  private static final long REPORT_INTERVAL_MS = 60_000;
  private static final int LARGE_THRESHOLD = 100 * 1024; // 100KB

  @Test
  public void testRecordBelowThresholdDoesNotTriggerReport() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);
    byte[] key = { 0x01, 0x02, 0x03 };

    // Record a small result (below threshold)
    detector.record(key, 100, 50_000, LARGE_THRESHOLD);

    // Should not report even after window elapsed — no large results
    long futureTime = System.currentTimeMillis() + REPORT_INTERVAL_MS * 2;
    assertNull(detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD));
  }

  @Test
  public void testRecordAboveThresholdTriggersReport() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);
    byte[] key = { 0x01, 0x02, 0x03 };

    // Record a large result (above threshold)
    detector.record(key, 500, 200_000, LARGE_THRESHOLD);

    // Should not report yet (window not elapsed)
    long now = System.currentTimeMillis();
    assertNull(detector.tryBuildReportAndReset(now, LARGE_THRESHOLD));

    // Should report after window elapsed
    long futureTime = now + REPORT_INTERVAL_MS * 2;
    assertNotNull(detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD));
  }

  @Test
  public void testTryBuildReportAndResetClearsState() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);
    byte[] key = { 0x01 };

    detector.record(key, 500, 200_000, LARGE_THRESHOLD);

    long futureTime = System.currentTimeMillis() + REPORT_INTERVAL_MS * 2;
    WriteComputeAmplificationDetector.AmplificationReport report =
        detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD);
    assertNotNull(report);
    assertEquals(report.totalWriteComputeCount, 1);
    assertEquals(report.largeResultCount, 1);
    assertEquals(report.totalResultBytes, 200_000);

    // After reset, should not report (no new data)
    assertNull(detector.tryBuildReportAndReset(futureTime + REPORT_INTERVAL_MS * 2, LARGE_THRESHOLD));
  }

  @Test
  public void testAtomicTryBuildReportPreventsDoubleReport() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);
    byte[] key = { 0x01 };

    detector.record(key, 500, 200_000, LARGE_THRESHOLD);

    long futureTime = System.currentTimeMillis() + REPORT_INTERVAL_MS * 2;
    // First call should succeed
    assertNotNull(detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD));
    // Second call should return null (window was just reset, no new data)
    assertNull(detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD));
  }

  @Test
  public void testTopKeysOrderedByTotalResultBytes() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);

    byte[] keyA = { 0x0A };
    byte[] keyB = { 0x0B };
    byte[] keyC = { 0x0C };

    // keyB gets the most total bytes (3 x 150KB = 450KB)
    detector.record(keyA, 100, 200_000, LARGE_THRESHOLD);
    detector.record(keyB, 100, 150_000, LARGE_THRESHOLD);
    detector.record(keyB, 100, 150_000, LARGE_THRESHOLD);
    detector.record(keyB, 100, 150_000, LARGE_THRESHOLD);
    detector.record(keyC, 100, 120_000, LARGE_THRESHOLD);

    long futureTime = System.currentTimeMillis() + REPORT_INTERVAL_MS * 2;
    WriteComputeAmplificationDetector.AmplificationReport report =
        detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD);
    assertNotNull(report);

    assertEquals(report.totalWriteComputeCount, 5);
    assertEquals(report.largeResultCount, 5);

    List<Map.Entry<ByteArrayKey, WriteComputeAmplificationDetector.KeyAmplificationStats>> topKeys = report.topKeys;
    assertEquals(topKeys.size(), 3);

    // First key should be keyB (highest total result bytes: 450KB)
    assertEquals(topKeys.get(0).getKey().getContent(), keyB);
    assertEquals(topKeys.get(0).getValue().count, 3);
    assertEquals(topKeys.get(0).getValue().totalResultBytes, 450_000);

    // Second should be keyA (200KB)
    assertEquals(topKeys.get(1).getKey().getContent(), keyA);

    // Third should be keyC (120KB)
    assertEquals(topKeys.get(2).getKey().getContent(), keyC);
  }

  @Test
  public void testMaxTrackedKeysLimit() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);

    // Fill up to MAX_TRACKED_KEYS
    for (int i = 0; i < WriteComputeAmplificationDetector.MAX_TRACKED_KEYS; i++) {
      byte[] key = { (byte) i };
      detector.record(key, 100, 200_000, LARGE_THRESHOLD);
    }

    // Record one more key beyond the limit — should be silently dropped
    byte[] extraKey = { (byte) 0xFF };
    detector.record(extraKey, 100, 500_000, LARGE_THRESHOLD);

    long futureTime = System.currentTimeMillis() + REPORT_INTERVAL_MS * 2;
    WriteComputeAmplificationDetector.AmplificationReport report =
        detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD);
    assertNotNull(report);

    // Total count includes the extra key
    assertEquals(report.largeResultCount, WriteComputeAmplificationDetector.MAX_TRACKED_KEYS + 1);

    // But top keys should only have MAX_TRACKED_KEYS entries (extra key was dropped)
    assertEquals(
        report.topKeys.size(),
        Math.min(
            WriteComputeAmplificationDetector.TOP_KEYS_TO_REPORT,
            WriteComputeAmplificationDetector.MAX_TRACKED_KEYS));
  }

  @Test
  public void testExistingKeyUpdatedWhenMapFull() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);

    // Fill up to MAX_TRACKED_KEYS with unique keys
    for (int i = 0; i < WriteComputeAmplificationDetector.MAX_TRACKED_KEYS; i++) {
      byte[] key = { (byte) i };
      detector.record(key, 100, 200_000, LARGE_THRESHOLD);
    }

    // Update an existing key — should succeed even though map is full
    byte[] existingKey = { 0x00 };
    detector.record(existingKey, 100, 300_000, LARGE_THRESHOLD);

    long futureTime = System.currentTimeMillis() + REPORT_INTERVAL_MS * 2;
    WriteComputeAmplificationDetector.AmplificationReport report =
        detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD);
    assertNotNull(report);

    // Find key 0x00 in top keys — it should have count=2 and totalResultBytes=500_000
    for (Map.Entry<ByteArrayKey, WriteComputeAmplificationDetector.KeyAmplificationStats> entry: report.topKeys) {
      if (entry.getKey().getContent()[0] == 0x00) {
        assertEquals(entry.getValue().count, 2);
        assertEquals(entry.getValue().totalResultBytes, 500_000);
        return;
      }
    }
    // Key 0x00 should be in top 5 since it has the highest total (500KB vs 200KB for others)
    fail("Expected key 0x00 in top keys");
  }

  @Test
  public void testMixedBelowAndAboveThreshold() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);

    byte[] key = { 0x01 };

    // 3 below threshold, 2 above
    detector.record(key, 100, 50_000, LARGE_THRESHOLD);
    detector.record(key, 100, 60_000, LARGE_THRESHOLD);
    detector.record(key, 100, 70_000, LARGE_THRESHOLD);
    detector.record(key, 200, 200_000, LARGE_THRESHOLD);
    detector.record(key, 300, 300_000, LARGE_THRESHOLD);

    long futureTime = System.currentTimeMillis() + REPORT_INTERVAL_MS * 2;
    WriteComputeAmplificationDetector.AmplificationReport report =
        detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD);
    assertNotNull(report);

    // All 5 events counted in total
    assertEquals(report.totalWriteComputeCount, 5);
    // Only 2 large results
    assertEquals(report.largeResultCount, 2);
    // Total result bytes includes all events
    assertEquals(report.totalResultBytes, 50_000 + 60_000 + 70_000 + 200_000 + 300_000);

    // Heavy key map only tracks the 2 large-result events
    assertEquals(report.topKeys.size(), 1);
    WriteComputeAmplificationDetector.KeyAmplificationStats stats = report.topKeys.get(0).getValue();
    assertEquals(stats.count, 2);
    assertEquals(stats.totalResultBytes, 500_000);
    assertEquals(stats.totalRequestBytes, 500); // 200 + 300
    assertEquals(stats.maxResultBytes, 300_000);
  }

  @Test
  public void testKeyAmplificationStatsAvgAmplification() {
    WriteComputeAmplificationDetector.KeyAmplificationStats stats =
        new WriteComputeAmplificationDetector.KeyAmplificationStats(100, 10_000);
    assertEquals(stats.getAvgAmplification(), 100.0, 0.01);

    stats.update(200, 20_000);
    // total result = 30_000, total request = 300
    assertEquals(stats.getAvgAmplification(), 100.0, 0.01);
    assertEquals(stats.count, 2);
    assertEquals(stats.maxResultBytes, 20_000);
  }

  @Test
  public void testReportToStringFormat() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);

    byte[] key = { 0x0A, 0x0B, 0x0C, 0x0D };
    detector.record(key, 500, 200_000, LARGE_THRESHOLD);

    long futureTime = System.currentTimeMillis() + REPORT_INTERVAL_MS * 2;
    WriteComputeAmplificationDetector.AmplificationReport report =
        detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD);
    assertNotNull(report);

    String output = report.toString();
    assertTrue(output.contains("WC total: 1"));
    assertTrue(output.contains("Large (>"));
    assertTrue(output.contains("KB): 1"));
    assertTrue(output.contains("Top keys by total result bytes:"));
    assertTrue(output.contains("#1 key=0x"));
    assertTrue(output.contains("count=1"));
    assertTrue(output.contains("avgAmplification="));
  }

  @Test
  public void testReportIncludesThresholdValue() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);

    byte[] key = { 0x01 };
    detector.record(key, 100, 200_000, LARGE_THRESHOLD);

    long futureTime = System.currentTimeMillis() + REPORT_INTERVAL_MS * 2;
    WriteComputeAmplificationDetector.AmplificationReport report =
        detector.tryBuildReportAndReset(futureTime, LARGE_THRESHOLD);
    assertNotNull(report);
    assertEquals(report.largeResultThreshold, LARGE_THRESHOLD);
    // Threshold of 100KB should appear as "100.0KB" in the output
    assertTrue(report.toString().contains("Large (>100.0KB):"));
  }

  @Test
  public void testWindowDurationInReport() {
    WriteComputeAmplificationDetector detector = new WriteComputeAmplificationDetector(REPORT_INTERVAL_MS);

    byte[] key = { 0x01 };
    detector.record(key, 100, 200_000, LARGE_THRESHOLD);

    // Use a time far enough in the future that minor jitter is irrelevant
    long now = System.currentTimeMillis();
    long reportTime = now + 90_000;
    WriteComputeAmplificationDetector.AmplificationReport report =
        detector.tryBuildReportAndReset(reportTime, LARGE_THRESHOLD);
    assertNotNull(report);

    // Window duration should be approximately 90 seconds (allow 2s tolerance for clock jitter)
    assertTrue(
        report.windowDurationMs >= 88_000 && report.windowDurationMs <= 92_000,
        "Window duration should be ~90s but was " + report.windowDurationMs + "ms");
  }
}
