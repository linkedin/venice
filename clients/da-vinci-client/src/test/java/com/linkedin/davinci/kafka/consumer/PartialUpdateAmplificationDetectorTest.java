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


public class PartialUpdateAmplificationDetectorTest {
  private static final long REPORT_INTERVAL_MS = 60_000;
  private static final int LARGE_THRESHOLD = 100 * 1024; // 100KB

  @Test
  public void testRecordBelowThresholdDoesNotTriggerReport() {
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(REPORT_INTERVAL_MS);
    byte[] key = { 0x01, 0x02, 0x03 };

    // Record a small result (below threshold) — should not report even if window elapsed
    // (no large results to report)
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(key, 100, 50_000, LARGE_THRESHOLD);
    assertNull(report);
  }

  @Test
  public void testRecordAboveThresholdTriggersReportAfterWindow() {
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(REPORT_INTERVAL_MS);
    byte[] key = { 0x01, 0x02, 0x03 };

    // Record a large result — should not report yet (window not elapsed)
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);
    assertNull(report, "Should not report immediately — window hasn't elapsed");

    // After waiting for the window to elapse, the next record should trigger a report
    try {
      Thread.sleep(REPORT_INTERVAL_MS + 100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    report = detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);
    assertNotNull(report, "Should report after window elapsed");
  }

  @Test
  public void testRecordAndMaybeReportClearsState() {
    // Use a tiny interval so reports trigger quickly
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(1);
    byte[] key = { 0x01 };

    // First record — window just started, no report
    detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);

    // Sleep to ensure window elapses
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Second record triggers report
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);
    assertNotNull(report);
    assertEquals(report.totalPartialUpdateCount, 2);
    assertEquals(report.largeResultCount, 2);

    // Next record after reset — no large results accumulated yet in new window, so no report
    PartialUpdateAmplificationDetector.AmplificationReport report2 =
        detector.recordAndMaybeReport(new byte[] { 0x02 }, 100, 50_000, LARGE_THRESHOLD);
    assertNull(report2, "After reset, should not report with only below-threshold records");
  }

  @Test
  public void testAtomicRecordAndReportPreventsDoubleReport() {
    // Use a tiny interval
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(1);
    byte[] key = { 0x01 };

    detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);

    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // First call should produce report
    PartialUpdateAmplificationDetector.AmplificationReport report1 =
        detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);
    assertNotNull(report1);

    // Immediate second call — window just reset, should not report even though this is a large result
    PartialUpdateAmplificationDetector.AmplificationReport report2 =
        detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);
    assertNull(report2);
  }

  @Test
  public void testTopKeysOrderedByTotalResultBytes() {
    // Use a tiny interval
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(1);

    byte[] keyA = { 0x0A };
    byte[] keyB = { 0x0B };
    byte[] keyC = { 0x0C };

    // keyB gets the most total bytes (3 x 150KB = 450KB)
    detector.recordAndMaybeReport(keyA, 100, 200_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(keyB, 100, 150_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(keyB, 100, 150_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(keyB, 100, 150_000, LARGE_THRESHOLD);

    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // This record triggers the report
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(keyC, 100, 120_000, LARGE_THRESHOLD);
    assertNotNull(report);

    assertEquals(report.totalPartialUpdateCount, 5);
    assertEquals(report.largeResultCount, 5);

    List<Map.Entry<ByteArrayKey, PartialUpdateAmplificationDetector.KeyAmplificationStats>> topKeys = report.topKeys;
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
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(1);

    // Fill up to MAX_TRACKED_KEYS
    for (int i = 0; i < PartialUpdateAmplificationDetector.MAX_TRACKED_KEYS; i++) {
      byte[] key = { (byte) i };
      detector.recordAndMaybeReport(key, 100, 200_000, LARGE_THRESHOLD);
    }

    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Record one more key beyond the limit — should be silently dropped from heavy key map
    byte[] extraKey = { (byte) 0xFF };
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(extraKey, 100, 500_000, LARGE_THRESHOLD);
    assertNotNull(report);

    // Total count includes the extra key
    assertEquals(report.largeResultCount, PartialUpdateAmplificationDetector.MAX_TRACKED_KEYS + 1);

    // But top keys should only have MAX_TRACKED_KEYS entries (extra key was dropped)
    assertEquals(
        report.topKeys.size(),
        Math.min(
            PartialUpdateAmplificationDetector.TOP_KEYS_TO_REPORT,
            PartialUpdateAmplificationDetector.MAX_TRACKED_KEYS));
  }

  @Test
  public void testExistingKeyUpdatedWhenMapFull() {
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(1);

    // Fill up to MAX_TRACKED_KEYS with unique keys
    for (int i = 0; i < PartialUpdateAmplificationDetector.MAX_TRACKED_KEYS; i++) {
      byte[] key = { (byte) i };
      detector.recordAndMaybeReport(key, 100, 200_000, LARGE_THRESHOLD);
    }

    // Update an existing key — should succeed even though map is full
    byte[] existingKey = { 0x00 };
    detector.recordAndMaybeReport(existingKey, 100, 300_000, LARGE_THRESHOLD);

    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Trigger report with another record on existing key
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(existingKey, 100, 200_000, LARGE_THRESHOLD);
    assertNotNull(report);

    // Find key 0x00 in top keys — it should have count=3 and totalResultBytes=700_000
    for (Map.Entry<ByteArrayKey, PartialUpdateAmplificationDetector.KeyAmplificationStats> entry: report.topKeys) {
      if (entry.getKey().getContent()[0] == 0x00) {
        assertEquals(entry.getValue().count, 3);
        assertEquals(entry.getValue().totalResultBytes, 700_000);
        return;
      }
    }
    // Key 0x00 should be in top 5 since it has the highest total (700KB vs 200KB for others)
    fail("Expected key 0x00 in top keys");
  }

  @Test
  public void testMixedBelowAndAboveThreshold() {
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(1);

    byte[] key = { 0x01 };

    // 3 below threshold, 2 above
    detector.recordAndMaybeReport(key, 100, 50_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(key, 100, 60_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(key, 100, 70_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(key, 200, 200_000, LARGE_THRESHOLD);

    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(key, 300, 300_000, LARGE_THRESHOLD);
    assertNotNull(report);

    // All 5 events counted in total
    assertEquals(report.totalPartialUpdateCount, 5);
    // Only 3 large results (200K + 300K from before + the triggering 300K)
    assertEquals(report.largeResultCount, 3);
    // Total result bytes includes all events
    assertEquals(report.totalResultBytes, 50_000 + 60_000 + 70_000 + 200_000 + 300_000);

    // Heavy key map tracks the 3 large-result events for key 0x01
    assertEquals(report.topKeys.size(), 1);
    PartialUpdateAmplificationDetector.KeyAmplificationStats stats = report.topKeys.get(0).getValue();
    assertEquals(stats.count, 3);
    assertEquals(stats.totalResultBytes, 700_000);
    assertEquals(stats.totalRequestBytes, 600); // 200 + 300 + 300
    assertEquals(stats.maxResultBytes, 300_000);
  }

  @Test
  public void testKeyAmplificationStatsAvgAmplification() {
    PartialUpdateAmplificationDetector.KeyAmplificationStats stats =
        new PartialUpdateAmplificationDetector.KeyAmplificationStats(100, 10_000);
    assertEquals(stats.getAvgAmplification(), 100.0, 0.01);

    stats.update(200, 20_000);
    // total result = 30_000, total request = 300
    assertEquals(stats.getAvgAmplification(), 100.0, 0.01);
    assertEquals(stats.count, 2);
    assertEquals(stats.maxResultBytes, 20_000);
  }

  @Test
  public void testReportToStringFormat() {
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(1);

    byte[] key = { 0x0A, 0x0B, 0x0C, 0x0D };
    detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);

    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);
    assertNotNull(report);

    String output = report.toString();
    assertTrue(output.contains("PU total: 2"));
    assertTrue(output.contains("Large (>"));
    assertTrue(output.contains("KB): 2"));
    assertTrue(output.contains("Top keys by total result bytes:"));
    assertTrue(output.contains("#1 key=0x"));
    assertTrue(output.contains("count=2"));
    assertTrue(output.contains("avgAmplification="));
  }

  @Test
  public void testReportIncludesThresholdValue() {
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(1);

    byte[] key = { 0x01 };
    detector.recordAndMaybeReport(key, 100, 200_000, LARGE_THRESHOLD);

    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(key, 100, 200_000, LARGE_THRESHOLD);
    assertNotNull(report);
    assertEquals(report.largeResultThreshold, LARGE_THRESHOLD);
    // Threshold of 100KB should appear as "100.0KB" in the output
    assertTrue(report.toString().contains("Large (>100.0KB):"));
  }

  @Test
  public void testWindowDurationInReport() {
    PartialUpdateAmplificationDetector detector = new PartialUpdateAmplificationDetector(1);

    byte[] key = { 0x01 };
    detector.recordAndMaybeReport(key, 100, 200_000, LARGE_THRESHOLD);

    // Sleep ~100ms so window duration is measurable
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(key, 100, 200_000, LARGE_THRESHOLD);
    assertNotNull(report);

    // Window duration should be at least 50ms (we slept 100ms, allow some tolerance)
    assertTrue(
        report.windowDurationMs >= 50 && report.windowDurationMs <= 500,
        "Window duration should be ~100ms but was " + report.windowDurationMs + "ms");
  }
}
