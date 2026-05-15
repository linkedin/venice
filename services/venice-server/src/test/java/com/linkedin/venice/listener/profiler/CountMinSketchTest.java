package com.linkedin.venice.listener.profiler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class CountMinSketchTest {
  @Test
  public void neverUnderEstimates() {
    CountMinSketch cms = new CountMinSketch();
    byte[] key = "hot-key".getBytes();
    for (int i = 0; i < 1000; i++) {
      cms.add(key, 1);
    }
    assertTrue(cms.estimateCount(key) >= 1000, "CMS must never under-estimate");
  }

  @Test
  public void monotonicWithRepeatedAdds() {
    CountMinSketch cms = new CountMinSketch();
    byte[] key = "k".getBytes();
    long previous = 0;
    for (int i = 0; i < 100; i++) {
      cms.add(key, 1);
      long current = cms.estimateCount(key);
      assertTrue(current >= previous, "estimate must be monotonic non-decreasing");
      previous = current;
    }
  }

  @Test
  public void overCountIsBoundedAcrossManyUniqueKeys() {
    // Error guarantee: with width=2718, depth=5 the max over-count is bounded by ~N/width per
    // key. Over a 1M-event stream the per-key over-count should be <= a few thousand on average,
    // and the *hottest* key remains identifiable.
    CountMinSketch cms = new CountMinSketch();
    int hotKeyHits = 50_000;
    byte[] hotKey = "hot".getBytes();
    for (int i = 0; i < hotKeyHits; i++) {
      cms.add(hotKey, 1);
    }
    // Add 1M unique cold keys, each once. ThreadLocalRandom avoids per-call Random allocation
    // and the SpotBugs "single-use Random" warning; this test asserts a statistical bound, so
    // the specific seed does not matter.
    ThreadLocalRandom random = ThreadLocalRandom.current();
    byte[] coldKey = new byte[16];
    for (int i = 0; i < 1_000_000; i++) {
      random.nextBytes(coldKey);
      cms.add(coldKey, 1);
    }
    long hotEstimate = cms.estimateCount(hotKey);
    assertTrue(hotEstimate >= hotKeyHits, "hot key must not under-count");
    long overCount = hotEstimate - hotKeyHits;
    // Theoretical bound is ~e * N / width = ~e * 1.05M / 2718 ≈ ~1050. Allow generous headroom.
    assertTrue(overCount <= 5_000, "over-count " + overCount + " exceeds expected bound");
  }

  @Test
  public void distinctKeysGetDistinctHashRows() {
    CountMinSketch cms = new CountMinSketch();
    byte[] a = "a".getBytes();
    byte[] b = "b".getBytes();
    cms.add(a, 100);
    // 'b' may collide on some rows but is overwhelmingly unlikely to collide on all 5 rows; min
    // across rows should remain near 0.
    long bEstimate = cms.estimateCount(b);
    assertTrue(bEstimate <= 1, "unseen key estimate should be ~0, got " + bEstimate);
  }

  @Test
  public void memoryFootprintMatchesPlan() {
    CountMinSketch cms = new CountMinSketch();
    // 2718 * 5 * 8 bytes = 108,720 bytes ≈ 106 KB (long counters)
    assertEquals(cms.memoryBytes(), 108_720L);
  }

  @Test
  public void customDimensionsHonored() {
    CountMinSketch cms = new CountMinSketch(1024, 4);
    assertEquals(cms.width(), 1024);
    assertEquals(cms.depth(), 4);
    assertEquals(cms.memoryBytes(), 1024L * 4 * Long.BYTES);
  }

  @Test
  public void countsExceedIntMaxWithoutOverflow() {
    // A single bucket should be able to absorb more than Integer.MAX_VALUE events without
    // overflowing — this is the reason we use long counters.
    CountMinSketch cms = new CountMinSketch();
    byte[] hot = "h".getBytes();
    long bigBump = 3_000_000_000L; // > Integer.MAX_VALUE
    cms.add(hot, bigBump);
    cms.add(hot, bigBump);
    assertTrue(cms.estimateCount(hot) >= bigBump * 2, "long counters must not overflow at 6B");
  }

  @Test
  public void concurrentIncrementsAreNotLost() throws Exception {
    // 8 threads each increment the same hot key 100_000 times. With atomic getAndAdd the final
    // estimate must equal exactly the total number of increments. The same test against the
    // previous plain-long implementation would routinely lose updates under contention.
    int threads = 8;
    int incrementsPerThread = 100_000;
    long expected = (long) threads * incrementsPerThread;
    CountMinSketch cms = new CountMinSketch();
    byte[] hotKey = "concurrent-hot".getBytes();

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      pool.execute(() -> {
        try {
          start.await();
          for (int i = 0; i < incrementsPerThread; i++) {
            cms.add(hotKey, 1);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    assertTrue(done.await(30, TimeUnit.SECONDS), "concurrent increments did not finish in time");
    pool.shutdown();

    assertEquals(cms.estimateCount(hotKey), expected, "CMS lost increments under contention");
  }

  @Test
  public void bytesAreContentAddressed() {
    // Two distinct byte[] instances with the same content should map to the same bucket.
    CountMinSketch cms = new CountMinSketch();
    byte[] viaArray = "same".getBytes();
    byte[] viaBuffer = ByteBuffer.wrap("same".getBytes()).array();
    cms.add(viaArray, 7);
    assertTrue(cms.estimateCount(viaBuffer) >= 7);
  }
}
