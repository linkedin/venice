package com.linkedin.alpini.base.cache;

import com.linkedin.alpini.base.misc.Pair;
import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Tests using Direct byte buffers are disabled because CRT tests have low default direct buffer size limits.
 * Tests are also using a reduced MAX_ALLOCATED_SIZE because of the low CRT test heap limits.
 * Unpooled tests are also disabled since they trigger OutOfMemoryErrors in the low heap limits, too.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestSerializedMap {
  private final static long TEST_MILLISECONDS = 30000L; // 100000L;
  private final static long COVERAGE_MILLISECONDS = 5000L; // 100000L;
  private final static long MAX_ALLOCATED_SIZE = 128 /* 640 */ * 1024 * 1024;
  private final static int BLOCK_SIZE = 4 * 1024 * 1024;

  private final ByteBufAllocator _pooledAllocator = new PooledByteBufAllocator();

  @AfterMethod(groups = "unit")
  public void performGC() {
    // System.gc() is a hint so we need to sleep in the hopes that it will do a gc sweep.
    System.gc();
    sleep(1000);
    System.gc();
    sleep(1000);
  }

  // a spinning sleep because nothing much happens when main thread is sleeping.
  private void sleep(long milliseconds) {
    long endTime = Time.currentTimeMillis() + milliseconds;
    do {
      Thread.yield();
    } while (Time.currentTimeMillis() < endTime);
  }

  @Test(groups = "unit")
  public void simpleTest() {

    SerializedMap<Long, String> map = new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), 4097);

    Assert.assertEquals(map.getBlockSize(), 8192);
    Assert.assertEquals(map.getMaxAllocatedMemory(), Long.MAX_VALUE);
    Assert.assertEquals(map.getMaxBlockAge(TimeUnit.NANOSECONDS), Long.MAX_VALUE);

    map.put(1L, "Hello World");
    map.put(42L, "Thanks for the fish!");

    Assert.assertEquals(map.get(1L), "Hello World");
    Assert.assertEquals(map.get(42L), "Thanks for the fish!");

    Assert.assertNotSame(map.get(1L), "Hello World");
    Assert.assertNotSame(map.get(42L), "Thanks for the fish!");

    Assert.assertTrue(map.containsKey(1L));
    Assert.assertTrue(map.containsKey(42L));

    Assert.assertEquals(map.size(), 2);

    map.entrySet().removeIf(entry -> {
      System.out.println("key=" + entry.getKey() + " value=" + entry.getValue());
      return true;
    });

    Assert.assertTrue(map.isEmpty());

    map.clear(); // Must clear or Netty's ByteBuf complains about memory leak.
  }

  @Test(groups = "unit", enabled = false)
  public void testDefaultByteBuf() {
    testMap(new TestRun<>(configure(new ByteBufHashMap<>(ByteBufHashMap.javaSerialization())), TEST_MILLISECONDS));
  }

  @Test(groups = "unit", enabled = false)
  public void testPooledDirectByteBuf() {
    testMap(
        new TestRun<>(
            configure(new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), _pooledAllocator::directBuffer)),
            TEST_MILLISECONDS));
  }

  @Test(groups = "unit", enabled = false)
  public void testPooledDirectByteBufMultithreaded() {
    testMultithreadMap(
        new TestRun<>(
            configure(new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), _pooledAllocator::directBuffer)),
            TEST_MILLISECONDS),
        4);
  }

  @Test(groups = "unit", enabled = false)
  public void testPooledDirectByteBufCache() {
    SerializedMap<Long, Element> map =
        configure(new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), _pooledAllocator::directBuffer));
    testMap(
        new TestRun<>(
            new PhantomHashMap<>(
                new PhantomHashCache<>(Element::new),
                map.setMaxBlockAge(1500, TimeUnit.MILLISECONDS).setIncubationAge(500, TimeUnit.MILLISECONDS)),
            TEST_MILLISECONDS));
    Assert.assertEquals(map.getMaxBlockAge(TimeUnit.MILLISECONDS), 1500);
    Assert.assertEquals(map.getIncubationAge(TimeUnit.MILLISECONDS), 500);
  }

  @Test(groups = "unit", enabled = false)
  public void testPooledDirectByteBufCacheMultithreaded() {
    SerializedMap<Long, Element> map =
        configure(new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), _pooledAllocator::directBuffer));
    testMultithreadMap(
        new TestRun<>(
            new PhantomHashMap<>(
                new PhantomHashCache<>(Element::new),
                map.setMaxBlockAge(1500, TimeUnit.MILLISECONDS).setIncubationAge(500, TimeUnit.MILLISECONDS)),
            TEST_MILLISECONDS),
        4);
    Assert.assertEquals(map.getMaxBlockAge(TimeUnit.MILLISECONDS), 1500);
    Assert.assertEquals(map.getIncubationAge(TimeUnit.MILLISECONDS), 500);
  }

  @Test(groups = { "unit", "NoCoverage" })
  public void testPooledHeapByteBuf() {
    testPooledHeapByteBuf(TEST_MILLISECONDS);
  }

  @Test(groups = { "unit", "Coverage" })
  public void testPooledHeapByteBufCoverage() {
    testPooledHeapByteBuf(COVERAGE_MILLISECONDS);
  }

  private void testPooledHeapByteBuf(long duration) {
    testMap(
        new TestRun<>(
            configure(new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), _pooledAllocator::heapBuffer)),
            duration));
  }

  @Test(groups = { "unit", "NoCoverage" })
  public void testPooledHeapByteBufMultithreaded() {
    testPooledHeapByteBufMultithreaded(TEST_MILLISECONDS);
  }

  @Test(groups = { "unit", "Coverage" })
  public void testPooledHeapByteBufMultithreadedCoverage() {
    testPooledHeapByteBufMultithreaded(COVERAGE_MILLISECONDS);
  }

  private void testPooledHeapByteBufMultithreaded(long duration) {
    testMultithreadMap(
        new TestRun<>(
            configure(new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), _pooledAllocator::heapBuffer)),
            duration),
        4);
  }

  @Test(groups = { "unit", "NoCoverage" })
  public void testPooledHeapByteBufCache() {
    testPooledHeapByteBufCache(TEST_MILLISECONDS);
  }

  @Test(groups = { "unit", "Coverage" })
  public void testPooledHeapByteBufCacheCoverage() {
    testPooledHeapByteBufCache(COVERAGE_MILLISECONDS);
  }

  private void testPooledHeapByteBufCache(long duration) {
    SerializedMap<Long, Element> map =
        configure(new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), _pooledAllocator::heapBuffer));
    testMap(
        new TestRun<>(
            new PhantomHashMap<>(new PhantomHashCache<>(Element::new), map.setMaxBlockAge(3, TimeUnit.SECONDS)),
            duration));
    Assert.assertEquals(map.getMaxBlockAge(TimeUnit.SECONDS), 3);
  }

  @Test(groups = { "unit", "NoCoverage" })
  public void testPooledHeapByteBufCacheMultithreaded() {
    testPooledHeapByteBufCacheMultithreaded(TEST_MILLISECONDS);
  }

  @Test(groups = { "unit", "Coverage" })
  public void testPooledHeapByteBufCacheMultithreadedCoverage() {
    testPooledHeapByteBufCacheMultithreaded(COVERAGE_MILLISECONDS);
  }

  private void testPooledHeapByteBufCacheMultithreaded(long duration) {
    SerializedMap<Long, Element> map =
        configure(new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), _pooledAllocator::heapBuffer));
    testMultithreadMap(
        new TestRun<>(
            new PhantomHashMap<>(new PhantomHashCache<>(Element::new), map.setMaxBlockAge(3, TimeUnit.SECONDS)),
            duration),
        4);
    Assert.assertEquals(map.getMaxBlockAge(TimeUnit.SECONDS), 3);
  }

  @Test(groups = "unit", enabled = false)
  public void testUnpooledByteBuf() {
    testMap(
        new TestRun<>(
            configure(
                new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), UnpooledByteBufAllocator.DEFAULT::heapBuffer)),
            TEST_MILLISECONDS));
  }

  @Test(groups = "unit", enabled = false)
  public void testUnpooledByteBufMultithreaded() {
    testMultithreadMap(
        new TestRun<>(
            configure(
                new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), UnpooledByteBufAllocator.DEFAULT::heapBuffer)),
            TEST_MILLISECONDS),
        4);
  }

  @Test(groups = "unit", enabled = false)
  public void testUnpooledByteBufCache() {
    SerializedMap<Long, Element> map = configure(
        new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), UnpooledByteBufAllocator.DEFAULT::heapBuffer));
    testMap(
        new TestRun<>(
            new PhantomHashMap<>(new PhantomHashCache<>(Element::new), map.setMaxBlockAge(3, TimeUnit.SECONDS)),
            TEST_MILLISECONDS));
    Assert.assertEquals(map.getMaxBlockAge(TimeUnit.SECONDS), 3);
  }

  @Test(groups = "unit", enabled = false)
  public void testUnpooledByteBufCacheMultithreaded() {
    SerializedMap<Long, Element> map = configure(
        new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), UnpooledByteBufAllocator.DEFAULT::heapBuffer));
    testMultithreadMap(
        new TestRun<>(
            new PhantomHashMap<>(new PhantomHashCache<>(Element::new), map.setMaxBlockAge(3, TimeUnit.SECONDS)),
            TEST_MILLISECONDS),
        4);
    Assert.assertEquals(map.getMaxBlockAge(TimeUnit.SECONDS), 3);
  }

  public <K, V extends Serializable> SerializedMap<K, V> configure(SerializedMap<K, V> map) {
    return map.setMaxAllocatedMemory(MAX_ALLOCATED_SIZE)
        .setBlockSize(BLOCK_SIZE)
        .setMaxBlockAge(Long.MAX_VALUE, TimeUnit.SECONDS);
  }

  public Element generate(StringBuilder builder, long keyValue) {
    Random rnd = new Random(keyValue);

    for (int i = 10 + rnd.nextInt(100); i > 0; i--) {
      builder.append(Integer.toHexString(rnd.nextInt()));
    }
    return new Element(builder.toString().getBytes(StandardCharsets.US_ASCII));
  }

  public void testMultithreadMap(final TestRun<Long, Element> run, int concurrency) {
    Thread[] threads = new Thread[concurrency - 1];
    try {
      CountDownLatch start = new CountDownLatch(1);
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Thread(() -> {
          try {
            start.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          testMap(run);
        });
      }

      for (Thread t: threads) {
        t.start();
      }

      start.countDown();
      testMap(run);
    } finally {
      for (Thread t: threads) {
        try {
          t.join();
        } catch (InterruptedException e) {
          Assert.fail("join failed", e);
        }
      }
    }
  }

  public void testMap(final TestRun<Long, Element> run) {
    final Thd thd = run.add(new Thd());
    try {
      StringBuilder builder = new StringBuilder();

      do {
        long keyValue = ThreadLocalRandom.current().nextLong() & Integer.MAX_VALUE;

        Element newValue;
        Element oldValue;

        // Do 20% puts, 5% deletes, the remainder are reads
        int choice = ThreadLocalRandom.current().nextInt(100);
        if (choice > 80) {
          newValue = generate(builder, keyValue);
          oldValue = run.map.put(keyValue, newValue);
        } else if (choice > 75) {
          oldValue = run.map.remove(keyValue);
          newValue = oldValue != null ? generate(builder, keyValue) : null;
        } else {
          oldValue = run.map.get(keyValue);
          newValue = oldValue != null ? generate(builder, keyValue) : null;
        }

        if (oldValue != null) {
          if (oldValue == newValue) {
            Assert.fail("That's strange!");
          } else {
            Assert.assertEquals(oldValue, newValue);
          }
        }

        builder.setLength(0);

        thd.count++;

      } while (run.running());
    } finally {
      run.remove(thd);
    }
  }

  private static class Thd {
    long count;
  }

  private static class TestRun<K, V> {
    final Map<K, V> map;
    final Set<Thd> thdSet = Collections.newSetFromMap(new IdentityHashMap<>());
    final List<GarbageCollectorMXBean> mbs = ManagementFactory.getGarbageCollectorMXBeans();
    final MemoryMXBean mmx = ManagementFactory.getMemoryMXBean();
    final Semaphore semaphore = new Semaphore(1);
    final Map<String, Pair<Long, Long>> initialState = new HashMap<>();
    final long endTime;
    long nextTime = Time.currentTimeMillis();
    long previousCount;
    boolean ending;

    private TestRun(Map<K, V> map, long testMillis) {
      this.map = map;
      endTime = Time.currentTimeMillis() + testMillis;
      mbs.forEach(gc -> initialState.put(gc.getName(), Pair.make(gc.getCollectionCount(), gc.getCollectionTime())));
    }

    @Override
    public void finalize() throws Throwable {
      map.clear(); // Must clear or Netty's ByteBuf complains about memory leak.
      super.finalize();
    }

    synchronized Thd add(Thd thd) {
      if (this.thdSet.add(thd)) {
        return thd;
      } else {
        throw new IllegalStateException("should not occur");
      }
    }

    synchronized long count() {
      return thdSet.stream().mapToLong(x -> x.count).sum();
    }

    synchronized boolean remove(Thd thd) {
      return this.thdSet.remove(thd);
    }

    boolean running() {
      long now = Time.currentTimeMillis();
      boolean wasEnding = ending;
      ending = now > endTime;

      if (((ending && !wasEnding) || nextTime + 1000L < now) && semaphore.tryAcquire()) {
        try {
          if (nextTime == now) {
            return !wasEnding;
          }
          long count = count();
          long delta = count - previousCount;
          System.out.println("rate: " + ((delta * 1000) / (now - nextTime)) + " map size: " + map.size());
          nextTime = now;
          previousCount = count;
          mbs.forEach(gc -> {
            Pair<Long, Long> initial = initialState.get(gc.getName());
            System.out.println(
                "name=" + gc.getName() + " collectionCount=" + (gc.getCollectionCount() - initial.getFirst())
                    + " collectionTime=" + (gc.getCollectionTime() - initial.getSecond()));
          });
          System.out.println(mmx.getHeapMemoryUsage());
        } finally {
          semaphore.release();
        }
      }

      return !ending;
    }

  }

  private static class Element implements Serializable {
    private byte[] data;

    public Element() {
    }

    public Element(@Nonnull byte[] data) {
      this.data = data;
    }

    public Element(@Nonnull Element orig) {
      this(orig.data);
    }

    @Override
    public boolean equals(Object o) {
      return this == o || (o instanceof Element && Arrays.equals(data, ((Element) o).data));
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }
  }

}
