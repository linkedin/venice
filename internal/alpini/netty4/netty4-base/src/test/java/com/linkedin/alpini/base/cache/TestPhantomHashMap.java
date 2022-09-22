package com.linkedin.alpini.base.cache;

import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestPhantomHashMap {
  public static class SerializableCharSequence implements Serializable, CharSequence {
    private final char[] _backingArray;
    private final int _start;
    private final int _length;

    public SerializableCharSequence(String source) {
      this(source.toCharArray());
    }

    public SerializableCharSequence(SerializableCharSequence source) {
      this(source._backingArray, source._start, source._length);
    }

    public SerializableCharSequence(char[] backingArray) {
      this(backingArray, 0, backingArray.length);
    }

    private SerializableCharSequence(char[] backingArray, int start, int length) {
      if (start < 0 || length < 0 || start + length > backingArray.length) {
        throw new ArrayIndexOutOfBoundsException();
      }
      _backingArray = backingArray;
      _start = start;
      _length = length;
    }

    public char[] toCharArray() {
      if (_start == 0 && _length == _backingArray.length) {
        return _backingArray;
      } else {
        return Arrays.copyOfRange(_backingArray, _start, _start + _length);
      }
    }

    @Override
    public int length() {
      return _length;
    }

    @Override
    public char charAt(int index) {
      return _backingArray[index + _start];
    }

    @Override
    public SerializableCharSequence subSequence(int start, int end) {
      return new SerializableCharSequence(_backingArray, _start + start, end - start);
    }

    @Override
    public String toString() {
      return String.valueOf(_backingArray, _start, _length);
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return toString().equals(other.toString());
    }

    private Object writeReplace() throws ObjectStreamException {
      Proxy proxy = new Proxy();
      proxy.chars = toCharArray();
      return proxy;
    }

  }

  private static class Proxy implements Serializable {
    public char[] chars;

    private Object readResolve() throws ObjectStreamException {
      return new SerializableCharSequence(chars);
    }
  }

  @Test(groups = "unit")
  public void simpleTest() throws InterruptedException {

    SerializedMap<Long, SerializableCharSequence> store =
        new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), UnpooledByteBufAllocator.DEFAULT::heapBuffer);
    PhantomHashCache<Long, SerializableCharSequence> map = new PhantomHashCache<>(SerializableCharSequence::new);

    map.put(1L, store, new SerializableCharSequence("Hello World"));
    map.put(42L, store, new SerializableCharSequence("Thanks for the fish!"));

    Assert.assertEquals(map.get(1L, store), new SerializableCharSequence("Hello World"));
    Assert.assertEquals(map.get(42L, store), new SerializableCharSequence("Thanks for the fish!"));

    Assert.assertNotSame(map.get(1L, store), "Hello World");
    Assert.assertNotSame(map.get(42L, store), "Thanks for the fish!");

    Assert.assertTrue(store.containsKey(1L));
    Assert.assertTrue(store.containsKey(42L));

    Assert.assertTrue(map.removeEntry(1L, store));

    // Do some horribly inefficient stuff, hope to cause the
    // PhantomHashCache to eject the cached entry.
    System.gc();
    for (int i = 1; i < 10000; i++) {
      String s = "" + i;
      for (int j = 1; j < 100; j++) {
        s = s + j;
      }
    }
    System.gc();

    // Because we have done some horrible inefficient stuff to the heap, getting a value from
    // the phantom cache would require retrieval from the backing store. Check that this occurs.

    // Deleted stuff should have been purged when their phantom references were collected.
    Assert.assertNull(map.get(1L, store));

    Assert.assertFalse(store.containsKey(1L));
    Assert.assertTrue(store.containsKey(42L));

    SerializedMap<Long, SerializableCharSequence> mockStore = Mockito.mock(SerializedMap.class);
    Mockito.when(mockStore.get(Mockito.any(Long.class)))
        .thenAnswer(invocation -> store.get(invocation.getArguments()[0]));

    Assert.assertEquals(map.get(42L, mockStore), new SerializableCharSequence("Thanks for the fish!"));

    Mockito.verify(mockStore).get(Mockito.eq(Long.valueOf(42L)));
    Mockito.verifyNoMoreInteractions(mockStore);

    store.clear();
  }

  @Test(groups = { "unit", "NoCoverage" })
  public void testMap() throws InterruptedException {
    testMap(30000L, Integer.MAX_VALUE); // 30 second test, should enough to see the sawtooth capacity
  }

  @Test(groups = { "unit", "Coverage" })
  public void testMapCoverage() throws InterruptedException {
    testMap(5000L, Integer.MAX_VALUE);
  }

  /** TODO: Fix test to use a predicable random seed with consistent result */
  @Test(groups = "functional", enabled = false)
  public void testMapLeak() throws InterruptedException {
    // a 5 minute test run
    LongSummaryStatistics stats = testMap(300000L, 32767);

    // Check that the number of keys is reasonable.
    Assert.assertTrue(stats.getCount() < 65536);

    // Check that the sliding window has vacated the lower value keys
    Assert.assertTrue(stats.getMin() > 65536);

    // check that most of the keys are near the max.
    Assert.assertTrue(stats.getAverage() > stats.getMax() - 65536);
  }

  public LongSummaryStatistics testMap(long duration, long modulus) throws InterruptedException {
    List<GarbageCollectorMXBean> mbs = ManagementFactory.getGarbageCollectorMXBeans();
    MemoryMXBean mmx = ManagementFactory.getMemoryMXBean();

    SerializedMap<Long, SerializableCharSequence> store =
        new ByteBufHashMap<>(ByteBufHashMap.javaSerialization(), UnpooledByteBufAllocator.DEFAULT::heapBuffer);
    store.setMaxAllocatedMemory(1024 * 1024);
    PhantomHashCache<Long, SerializableCharSequence> map = new PhantomHashCache<>(SerializableCharSequence::new);
    StringBuilder builder = new StringBuilder();
    long endTime = Time.currentTimeMillis() + duration;
    long nextTime = Time.currentTimeMillis();
    long count = 0;
    long additions = 0;
    long offset = 0; // makes a "sliding window" effect

    do {
      long keyValue = ThreadLocalRandom.current().nextLong() & modulus;
      keyValue += offset;
      Random rnd = new Random(keyValue);

      for (int i = 10 + rnd.nextInt(100); i > 0; i--) {
        builder.append(Integer.toHexString(rnd.nextInt()));
      }

      SerializableCharSequence newValue = new SerializableCharSequence(builder.toString().toCharArray());
      SerializableCharSequence oldValue;

      // Do 20% puts, 5% deletes, the remainder are reads
      int choice = ThreadLocalRandom.current().nextInt(100);
      oldValue = map.get(keyValue, store);
      if (choice > 80) {
        if (oldValue == null) {
          map.put(keyValue, store, newValue);
          additions++;
        }
      } else if (choice > 75) {
        if (!map.removeEntry(keyValue, store)) {
          oldValue = null;
        }
      } else {
        oldValue = map.get(keyValue, store);
      }

      if (oldValue != null) {
        if (oldValue == newValue) {
          Assert.fail("That's strange!");
        } else {
          Assert.assertEquals(oldValue, newValue);
        }
      }

      builder.setLength(0); // Thread.yield();

      count++;

      long now = Time.currentTimeMillis();

      if (nextTime + 1000L < now) {
        offset += 1000;
        int size = map.size();
        System.out.println(
            "rate: " + ((count * 100) / (now - nextTime)) + " map size: " + size + " offset: " + offset + " additions: "
                + additions + " purged: " + map._purgedEntriesCount);
        nextTime = now;
        count = 0;
        mbs.forEach(
            gc -> System.out.println(
                "name=" + gc.getName() + " collectionCount=" + gc.getCollectionCount() + " collectionTime="
                    + gc.getCollectionTime()));
        System.out.println(mmx.getHeapMemoryUsage());
        builder.setLength(0);
      }

    } while (Time.currentTimeMillis() < endTime);

    LongSummaryStatistics keyStatistics =
        map._phantomCache.keySet().stream().mapToLong(Long::longValue).summaryStatistics();

    System.out.println("key statistics: " + keyStatistics);

    store.clear();

    return keyStatistics;
  }
}
