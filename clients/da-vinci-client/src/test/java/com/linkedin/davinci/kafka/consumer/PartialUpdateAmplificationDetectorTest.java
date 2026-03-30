package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.davinci.utils.ByteArrayKey;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;


public class PartialUpdateAmplificationDetectorTest {
  private static final long REPORT_INTERVAL_MS = 60_000;
  private static final int LARGE_THRESHOLD = 100 * 1024; // 100KB

  private static PartialUpdateAmplificationDetector createDetector(long intervalMs, AtomicLong clock) {
    return new PartialUpdateAmplificationDetector(intervalMs, clock::get);
  }

  @Test
  public void testRecordBelowThresholdDoesNotTriggerReport() {
    AtomicLong clock = new AtomicLong(1000);
    PartialUpdateAmplificationDetector detector = createDetector(REPORT_INTERVAL_MS, clock);
    byte[] key = { 0x01, 0x02, 0x03 };

    // Small result — should not report even if window elapses (no large results to report)
    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    assertNull(detector.recordAndMaybeReport(key, 100, 50_000, LARGE_THRESHOLD));
  }

  @Test
  public void testRecordAboveThresholdTriggersReportAfterWindow() {
    AtomicLong clock = new AtomicLong(1000);
    PartialUpdateAmplificationDetector detector = createDetector(REPORT_INTERVAL_MS, clock);
    byte[] key = { 0x01, 0x02, 0x03 };

    // Large result — should not report yet (window not elapsed)
    assertNull(detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD));

    // Advance past window — next large result triggers report
    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    assertNotNull(detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD));
  }

  @Test
  public void testRecordAndMaybeReportClearsState() {
    AtomicLong clock = new AtomicLong(1000);
    PartialUpdateAmplificationDetector detector = createDetector(1, clock);
    byte[] key = { 0x01 };

    // First record
    detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);

    // Advance past window, trigger report
    clock.addAndGet(10);
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);
    assertNotNull(report);
    assertEquals(report.totalPartialUpdateCount, 2);
    assertEquals(report.largeResultCount, 2);

    // After reset — no large results accumulated, no report
    assertNull(detector.recordAndMaybeReport(new byte[] { 0x02 }, 100, 50_000, LARGE_THRESHOLD));
  }

  @Test
  public void testAtomicRecordAndReportPreventsDoubleReport() {
    AtomicLong clock = new AtomicLong(1000);
    PartialUpdateAmplificationDetector detector = createDetector(1, clock);
    byte[] key = { 0x01 };

    detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);
    clock.addAndGet(10);

    // First call produces report
    assertNotNull(detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD));
    // Immediate second call — window just reset, no report
    assertNull(detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD));
  }

  @Test
  public void testTopKeysOrderedByTotalResultBytes() {
    AtomicLong clock = new AtomicLong(1000);
    PartialUpdateAmplificationDetector detector = createDetector(1, clock);

    byte[] keyA = { 0x0A };
    byte[] keyB = { 0x0B };
    byte[] keyC = { 0x0C };

    // keyB gets the most total bytes (3 x 150KB = 450KB)
    detector.recordAndMaybeReport(keyA, 100, 200_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(keyB, 100, 150_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(keyB, 100, 150_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(keyB, 100, 150_000, LARGE_THRESHOLD);

    clock.addAndGet(10);
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(keyC, 100, 120_000, LARGE_THRESHOLD);
    assertNotNull(report);

    assertEquals(report.totalPartialUpdateCount, 5);
    assertEquals(report.largeResultCount, 5);

    List<Map.Entry<ByteArrayKey, PartialUpdateAmplificationDetector.KeyAmplificationStats>> topKeys = report.topKeys;
    assertEquals(topKeys.size(), 3);
    assertEquals(topKeys.get(0).getKey().getContent(), keyB);
    assertEquals(topKeys.get(0).getValue().count, 3);
    assertEquals(topKeys.get(0).getValue().totalResultBytes, 450_000);
    assertEquals(topKeys.get(1).getKey().getContent(), keyA);
    assertEquals(topKeys.get(2).getKey().getContent(), keyC);
  }

  @Test
  public void testMaxTrackedKeysLimit() {
    AtomicLong clock = new AtomicLong(1000);
    PartialUpdateAmplificationDetector detector = createDetector(1, clock);

    for (int i = 0; i < PartialUpdateAmplificationDetector.MAX_TRACKED_KEYS; i++) {
      detector.recordAndMaybeReport(new byte[] { (byte) i }, 100, 200_000, LARGE_THRESHOLD);
    }

    clock.addAndGet(10);
    // Extra key beyond limit — silently dropped from heavy key map
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(new byte[] { (byte) 0xFF }, 100, 500_000, LARGE_THRESHOLD);
    assertNotNull(report);
    assertEquals(report.largeResultCount, PartialUpdateAmplificationDetector.MAX_TRACKED_KEYS + 1);
    assertEquals(
        report.topKeys.size(),
        Math.min(
            PartialUpdateAmplificationDetector.TOP_KEYS_TO_REPORT,
            PartialUpdateAmplificationDetector.MAX_TRACKED_KEYS));
  }

  @Test
  public void testExistingKeyUpdatedWhenMapFull() {
    AtomicLong clock = new AtomicLong(1000);
    PartialUpdateAmplificationDetector detector = createDetector(1, clock);

    for (int i = 0; i < PartialUpdateAmplificationDetector.MAX_TRACKED_KEYS; i++) {
      detector.recordAndMaybeReport(new byte[] { (byte) i }, 100, 200_000, LARGE_THRESHOLD);
    }
    // Update existing key — should succeed even though map is full
    detector.recordAndMaybeReport(new byte[] { 0x00 }, 100, 300_000, LARGE_THRESHOLD);

    clock.addAndGet(10);
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(new byte[] { 0x00 }, 100, 200_000, LARGE_THRESHOLD);
    assertNotNull(report);

    for (Map.Entry<ByteArrayKey, PartialUpdateAmplificationDetector.KeyAmplificationStats> entry: report.topKeys) {
      if (entry.getKey().getContent()[0] == 0x00) {
        assertEquals(entry.getValue().count, 3);
        assertEquals(entry.getValue().totalResultBytes, 700_000);
        return;
      }
    }
    fail("Expected key 0x00 in top keys");
  }

  @Test
  public void testMixedBelowAndAboveThreshold() {
    AtomicLong clock = new AtomicLong(1000);
    PartialUpdateAmplificationDetector detector = createDetector(1, clock);
    byte[] key = { 0x01 };

    // 3 below threshold, 2 above
    detector.recordAndMaybeReport(key, 100, 50_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(key, 100, 60_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(key, 100, 70_000, LARGE_THRESHOLD);
    detector.recordAndMaybeReport(key, 200, 200_000, LARGE_THRESHOLD);

    clock.addAndGet(10);
    PartialUpdateAmplificationDetector.AmplificationReport report =
        detector.recordAndMaybeReport(key, 300, 300_000, LARGE_THRESHOLD);
    assertNotNull(report);

    assertEquals(report.totalPartialUpdateCount, 5);
    assertEquals(report.largeResultCount, 3);
    assertEquals(report.totalResultBytes, 50_000 + 60_000 + 70_000 + 200_000 + 300_000);

    assertEquals(report.topKeys.size(), 1);
    PartialUpdateAmplificationDetector.KeyAmplificationStats stats = report.topKeys.get(0).getValue();
    assertEquals(stats.count, 3);
    assertEquals(stats.totalResultBytes, 700_000);
    assertEquals(stats.totalRequestBytes, 600);
    assertEquals(stats.maxResultBytes, 300_000);
  }

  @Test
  public void testKeyAmplificationStatsAvgAmplification() {
    PartialUpdateAmplificationDetector.KeyAmplificationStats stats =
        new PartialUpdateAmplificationDetector.KeyAmplificationStats(100, 10_000);
    assertEquals(stats.getAvgAmplification(), 100.0, 0.01);

    stats.update(200, 20_000);
    assertEquals(stats.getAvgAmplification(), 100.0, 0.01);
    assertEquals(stats.count, 2);
    assertEquals(stats.maxResultBytes, 20_000);
  }

  @Test
  public void testReportToStringFormat() {
    AtomicLong clock = new AtomicLong(1000);
    PartialUpdateAmplificationDetector detector = createDetector(1, clock);
    byte[] key = { 0x0A, 0x0B, 0x0C, 0x0D };
    detector.recordAndMaybeReport(key, 500, 200_000, LARGE_THRESHOLD);

    clock.addAndGet(10);
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
}
