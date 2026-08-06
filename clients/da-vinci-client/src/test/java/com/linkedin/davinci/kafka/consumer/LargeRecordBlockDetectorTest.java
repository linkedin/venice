package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.utils.ByteArrayKey;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;


public class LargeRecordBlockDetectorTest {
  private static final long REPORT_INTERVAL_MS = 60_000;
  private static final int LIMIT_BYTES = 5 * 1024 * 1024;
  private static final int MAX_TRACKED_KEYS = 3;

  private static final byte[] KEY_A = { 0x01, 0x02, 0x03 };
  private static final byte[] KEY_B = { 0x04, 0x05, 0x06 };

  private static LargeRecordBlockDetector createDetector(AtomicLong clock) {
    return new LargeRecordBlockDetector(REPORT_INTERVAL_MS, MAX_TRACKED_KEYS, clock::get);
  }

  @Test
  public void testKeyIsNotBlockedUntilRecorded() {
    LargeRecordBlockDetector detector = createDetector(new AtomicLong(1000));
    assertFalse(detector.isBlocked(KEY_A));
    assertEquals(detector.getBlockedKeyCount(), 0);
  }

  @Test
  public void testRecordedKeyBecomesBlocked() {
    LargeRecordBlockDetector detector = createDetector(new AtomicLong(1000));

    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);

    assertTrue(detector.isBlocked(KEY_A));
    assertFalse(detector.isBlocked(KEY_B), "Blocking one key must not affect another");
    assertEquals(detector.getBlockedKeyCount(), 1);
  }

  @Test
  public void testBlockingIsKeyedByContentNotIdentity() {
    LargeRecordBlockDetector detector = createDetector(new AtomicLong(1000));

    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);

    assertTrue(detector.isBlocked(new byte[] { 0x01, 0x02, 0x03 }), "An equal-content key must be treated as blocked");
  }

  @Test
  public void testUnblockRestoresWrites() {
    LargeRecordBlockDetector detector = createDetector(new AtomicLong(1000));
    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);

    assertTrue(detector.unblock(KEY_A));
    assertFalse(detector.isBlocked(KEY_A));
    assertEquals(detector.getBlockedKeyCount(), 0);

    assertFalse(detector.unblock(KEY_A), "Unblocking an already-unblocked key reports no change");
  }

  @Test
  public void testFirstBlockOfAKeyArmsAReportImmediately() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);

    // Window has not elapsed, so no report yet even though the key is newly blocked.
    assertNull(detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES));

    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    LargeRecordBlockDetector.BlockReport report =
        detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);
    assertNotNull(report);
    assertEquals(report.blockedWriteCount, 2);
    assertEquals(report.topKeys.size(), 1);
  }

  @Test
  public void testWindowResetClearsCountersButKeepsKeysBlocked() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);
    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);

    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    assertNotNull(detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES));

    // The key must stay blocked across the window boundary — that is what keeps enforcement in effect.
    assertTrue(detector.isBlocked(KEY_A));
    assertEquals(detector.getBlockedKeyCount(), 1);

    // No newly blocked key, so no further report is armed even after another window elapses.
    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    assertNull(detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES));
  }

  @Test
  public void testNewlyBlockedKeyArmsAnotherReport() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);
    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);
    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    assertNotNull(detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES));

    // A different key crossing the limit is new information and must be surfaced.
    detector.recordBlockAndMaybeReport(KEY_B, LIMIT_BYTES + 1, LIMIT_BYTES);
    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    LargeRecordBlockDetector.BlockReport report =
        detector.recordBlockAndMaybeReport(KEY_B, LIMIT_BYTES + 1, LIMIT_BYTES);
    assertNotNull(report);
    assertEquals(report.topKeys.size(), 2);
  }

  @Test
  public void testTrackedKeysAreBoundedByLruEviction() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);

    for (int i = 0; i < MAX_TRACKED_KEYS + 2; i++) {
      detector.recordBlockAndMaybeReport(new byte[] { (byte) i }, LIMIT_BYTES + 1, LIMIT_BYTES);
    }

    assertEquals(detector.getBlockedKeyCount(), MAX_TRACKED_KEYS);
    assertFalse(detector.isBlocked(new byte[] { 0 }), "Least recently blocked key should have been evicted");
    assertTrue(detector.isBlocked(new byte[] { MAX_TRACKED_KEYS + 1 }), "Most recent key must be retained");
  }

  @Test
  public void testEvictionCountIsReported() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);

    for (int i = 0; i < MAX_TRACKED_KEYS + 2; i++) {
      detector.recordBlockAndMaybeReport(new byte[] { (byte) i }, LIMIT_BYTES + 1, LIMIT_BYTES);
    }
    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    LargeRecordBlockDetector.BlockReport report =
        detector.recordBlockAndMaybeReport(new byte[] { 0x7f }, LIMIT_BYTES + 1, LIMIT_BYTES);

    assertNotNull(report);
    assertEquals(report.evictedKeyCount, 3, "Two overflow inserts plus the one that armed this report");
  }

  @Test
  public void testRepeatBlocksAccumulateOnTheSameKey() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);

    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 10, LIMIT_BYTES);
    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 500, LIMIT_BYTES);
    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    LargeRecordBlockDetector.BlockReport report =
        detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 20, LIMIT_BYTES);

    assertNotNull(report);
    assertEquals(report.topKeys.size(), 1);
    LargeRecordBlockDetector.BlockedKeyStats stats = report.topKeys.get(0).getValue();
    assertEquals(stats.blockedWriteCount, 3);
    assertEquals(stats.maxObservedSizeBytes, LIMIT_BYTES + 500, "Report should surface the worst observed size");
  }

  @Test
  public void testUnknownSizeFromFastPathDoesNotLowerMaxObservedSize() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);

    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 500, LIMIT_BYTES);
    // Subsequent rejections happen on the fast path, where the value is never assembled.
    detector.recordBlockAndMaybeReport(KEY_A, LargeRecordBlockDetector.BlockedKeyStats.SIZE_UNKNOWN, LIMIT_BYTES);

    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    LargeRecordBlockDetector.BlockReport report =
        detector.recordBlockAndMaybeReport(KEY_A, LargeRecordBlockDetector.BlockedKeyStats.SIZE_UNKNOWN, LIMIT_BYTES);

    assertNotNull(report);
    assertEquals(report.topKeys.get(0).getValue().maxObservedSizeBytes, LIMIT_BYTES + 500);
  }

  @Test
  public void testTopKeysAreOrderedByRejectionCount() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);

    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);
    for (int i = 0; i < 5; i++) {
      detector.recordBlockAndMaybeReport(KEY_B, LIMIT_BYTES + 1, LIMIT_BYTES);
    }

    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    LargeRecordBlockDetector.BlockReport report =
        detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);

    assertNotNull(report);
    assertEquals(report.topKeys.get(0).getKey(), ByteArrayKey.wrap(KEY_B), "Worst offender must be listed first");
  }

  @Test
  public void testReportNamesTheOffendingKey() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);
    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);
    clock.addAndGet(REPORT_INTERVAL_MS + 1);

    String report = detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES).toString();

    assertTrue(report.contains("010203"), "Report must name the offending key in hex: " + report);
    assertTrue(report.contains("full put or a delete"), "Report must tell the user how to recover: " + report);
  }

  @Test
  public void testReportSnapshotDoesNotAliasLiveState() {
    AtomicLong clock = new AtomicLong(1000);
    LargeRecordBlockDetector detector = createDetector(clock);
    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);
    clock.addAndGet(REPORT_INTERVAL_MS + 1);
    LargeRecordBlockDetector.BlockReport report =
        detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);

    int countAtSnapshot = report.topKeys.get(0).getValue().blockedWriteCount;
    detector.recordBlockAndMaybeReport(KEY_A, LIMIT_BYTES + 1, LIMIT_BYTES);

    assertEquals(report.topKeys.get(0).getValue().blockedWriteCount, countAtSnapshot);
  }
}
