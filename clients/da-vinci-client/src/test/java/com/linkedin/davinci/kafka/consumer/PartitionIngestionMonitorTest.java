package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class PartitionIngestionMonitorTest {
  @Test
  public void testSnapshotAndResetBasic() {
    PartitionIngestionMonitor monitor = new PartitionIngestionMonitor();

    // Record some consumed data
    monitor.recordConsumed(100);
    monitor.recordConsumed(200);
    monitor.recordConsumed(150);

    // Record some leader produced data
    monitor.recordLeaderProduced(500);
    monitor.recordLeaderProduced(300);

    // Record latencies (in nanoseconds)
    monitor.recordE2EProcessingLatencyNs(2_000_000); // 2ms
    monitor.recordE2EProcessingLatencyNs(4_000_000); // 4ms

    monitor.recordStoragePutLatencyNs(1_000_000); // 1ms

    // Snapshot over 1 second
    PartitionIngestionSnapshot snapshot = monitor.snapshotAndReset(1000);

    // 3 records in 1 second = 3 rec/sec
    assertEquals(snapshot.getRecordsConsumedPerSec(), 3.0, 0.001);
    // 450 bytes in 1 second = 450 bytes/sec
    assertEquals(snapshot.getBytesConsumedPerSec(), 450.0, 0.001);
    // 2 leader records in 1 second = 2 rec/sec
    assertEquals(snapshot.getLeaderRecordsProducedPerSec(), 2.0, 0.001);
    // 800 bytes in 1 second = 800 bytes/sec
    assertEquals(snapshot.getLeaderBytesProducedPerSec(), 800.0, 0.001);
    // Average e2e latency: (2+4)/2 = 3ms
    assertEquals(snapshot.getE2eProcessingLatencyAvgMs(), 3.0, 0.001);
    // Average storage put latency: 1ms
    assertEquals(snapshot.getStoragePutLatencyAvgMs(), 1.0, 0.001);
  }

  @Test
  public void testSnapshotResetsCounters() {
    PartitionIngestionMonitor monitor = new PartitionIngestionMonitor();

    monitor.recordConsumed(100);
    monitor.recordE2EProcessingLatencyNs(2_000_000);

    // First snapshot
    PartitionIngestionSnapshot snapshot1 = monitor.snapshotAndReset(1000);
    assertEquals(snapshot1.getRecordsConsumedPerSec(), 1.0, 0.001);
    assertEquals(snapshot1.getE2eProcessingLatencyAvgMs(), 2.0, 0.001);

    // Second snapshot with no new data should be zero
    PartitionIngestionSnapshot snapshot2 = monitor.snapshotAndReset(1000);
    assertEquals(snapshot2.getRecordsConsumedPerSec(), 0.0, 0.001);
    assertEquals(snapshot2.getBytesConsumedPerSec(), 0.0, 0.001);
    assertEquals(snapshot2.getE2eProcessingLatencyAvgMs(), 0.0, 0.001);
  }

  @Test
  public void testSnapshotWithZeroElapsedTime() {
    PartitionIngestionMonitor monitor = new PartitionIngestionMonitor();

    monitor.recordConsumed(100);

    PartitionIngestionSnapshot snapshot = monitor.snapshotAndReset(0);
    assertEquals(snapshot.getRecordsConsumedPerSec(), 0.0, 0.001);
    assertEquals(snapshot.getBytesConsumedPerSec(), 0.0, 0.001);
  }

  @Test
  public void testAllLatencyTypes() {
    PartitionIngestionMonitor monitor = new PartitionIngestionMonitor();

    monitor.recordE2EProcessingLatencyNs(10_000_000);
    monitor.recordLeaderPreprocessingLatencyNs(5_000_000);
    monitor.recordLeaderProduceLatencyNs(3_000_000);
    monitor.recordLeaderCompletionLatencyNs(8_000_000);
    monitor.recordLeaderCallbackLatencyNs(2_000_000);
    monitor.recordStoragePutLatencyNs(1_000_000);
    monitor.recordValueLookupLatencyNs(4_000_000);
    monitor.recordRmdLookupLatencyNs(6_000_000);

    PartitionIngestionSnapshot snapshot = monitor.snapshotAndReset(1000);

    assertEquals(snapshot.getE2eProcessingLatencyAvgMs(), 10.0, 0.001);
    assertEquals(snapshot.getLeaderPreprocessingLatencyAvgMs(), 5.0, 0.001);
    assertEquals(snapshot.getLeaderProduceLatencyAvgMs(), 3.0, 0.001);
    assertEquals(snapshot.getLeaderCompletionLatencyAvgMs(), 8.0, 0.001);
    assertEquals(snapshot.getLeaderCallbackLatencyAvgMs(), 2.0, 0.001);
    assertEquals(snapshot.getStoragePutLatencyAvgMs(), 1.0, 0.001);
    assertEquals(snapshot.getValueLookupLatencyAvgMs(), 4.0, 0.001);
    assertEquals(snapshot.getRmdLookupLatencyAvgMs(), 6.0, 0.001);
  }

  @Test
  public void testConcurrentRecording() throws InterruptedException {
    PartitionIngestionMonitor monitor = new PartitionIngestionMonitor();
    int numThreads = 8;
    int recordsPerThread = 10000;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);

    for (int t = 0; t < numThreads; t++) {
      executor.submit(() -> {
        for (int i = 0; i < recordsPerThread; i++) {
          monitor.recordConsumed(10);
          monitor.recordLeaderProduced(20);
          monitor.recordE2EProcessingLatencyNs(1_000_000); // 1ms each
        }
        latch.countDown();
      });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for concurrent recording");
    executor.shutdown();

    PartitionIngestionSnapshot snapshot = monitor.snapshotAndReset(1000);

    long expectedRecords = (long) numThreads * recordsPerThread;
    long expectedBytes = expectedRecords * 10;

    // Due to LongAdder's eventual consistency, values should be exact
    assertEquals(snapshot.getRecordsConsumedPerSec(), expectedRecords, 0.001);
    assertEquals(snapshot.getBytesConsumedPerSec(), expectedBytes, 0.001);
    assertEquals(snapshot.getLeaderRecordsProducedPerSec(), expectedRecords, 0.001);
    assertEquals(snapshot.getLeaderBytesProducedPerSec(), expectedRecords * 20.0, 0.001);
    // Average latency should be ~1ms
    assertEquals(snapshot.getE2eProcessingLatencyAvgMs(), 1.0, 0.001);
  }

  @Test
  public void testRateCalculationWithDifferentIntervals() {
    PartitionIngestionMonitor monitor = new PartitionIngestionMonitor();

    // Record 100 records
    for (int i = 0; i < 100; i++) {
      monitor.recordConsumed(50);
    }

    // Snapshot over 5 seconds
    PartitionIngestionSnapshot snapshot = monitor.snapshotAndReset(5000);
    // 100 records over 5 seconds = 20 rec/sec
    assertEquals(snapshot.getRecordsConsumedPerSec(), 20.0, 0.001);
    // 5000 bytes over 5 seconds = 1000 bytes/sec
    assertEquals(snapshot.getBytesConsumedPerSec(), 1000.0, 0.001);
  }

  @Test
  public void testNoLatencyRecordedReturnsZero() {
    PartitionIngestionMonitor monitor = new PartitionIngestionMonitor();

    // Record only consumed data, no latencies
    monitor.recordConsumed(100);

    PartitionIngestionSnapshot snapshot = monitor.snapshotAndReset(1000);
    assertEquals(snapshot.getE2eProcessingLatencyAvgMs(), 0.0, 0.001);
    assertEquals(snapshot.getLeaderPreprocessingLatencyAvgMs(), 0.0, 0.001);
    assertEquals(snapshot.getLeaderProduceLatencyAvgMs(), 0.0, 0.001);
    assertEquals(snapshot.getStoragePutLatencyAvgMs(), 0.0, 0.001);
  }
}
