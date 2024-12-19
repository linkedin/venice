package com.linkedin.davinci.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests adapted from the OpenJDK and refactored to test multiple Map implementations.
 */
public class MapTest {
  @DataProvider
  Object[][] mapImplementations() {
    return new Object[][] { { new HashMap<>() }, { new TreeMap<>() }, { new LinkedHashMap<>() },
        { new IndexedHashMap<>() } };
  }

  @DataProvider
  Object[][] hashMapImplementations() {
    return new Object[][] { { new HashMap<>() }, { new LinkedHashMap<>() }, { new IndexedHashMap<>() } };
  }

  @DataProvider
  Object[][] notEmptyMapImplementations() {
    class NotEmptyHashMap<K, V> extends HashMap<K, V> {
      private static final long serialVersionUID = 1L;

      @Override
      public int size() {
        return super.size() + 1;
      }

      @Override
      public boolean isEmpty() {
        return size() == 0;
      }
    }

    class NotEmptyLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
      private static final long serialVersionUID = 1L;

      @Override
      public int size() {
        return super.size() + 1;
      }

      @Override
      public boolean isEmpty() {
        return size() == 0;
      }
    }

    class NotEmptyIndexedHashMap<K, V> extends IndexedHashMap<K, V> {
      @Override
      public int size() {
        return super.size() + 1;
      }

      @Override
      public boolean isEmpty() {
        return size() == 0;
      }
    }

    return new Object[][] { { new NotEmptyHashMap() }, { new NotEmptyLinkedHashMap() },
        { new NotEmptyIndexedHashMap() } };
  }

  @DataProvider
  Object[][] hashMapImplementationsWithConfiguredCapacityAndLoadFactor() {
    // Initial capacity of map
    // Should be >= the map capacity for treeifying, see HashMap/ConcurrentMap.MIN_TREEIFY_CAPACITY
    final int INITIAL_CAPACITY = 64;

    // Load factor of map
    // A value 1.0 will ensure that a new threshold == capacity
    final float LOAD_FACTOR = 1.0f;

    return new Object[][] { { new HashMap(INITIAL_CAPACITY, LOAD_FACTOR) },
        { new LinkedHashMap(INITIAL_CAPACITY, LOAD_FACTOR) }, { new IndexedHashMap(INITIAL_CAPACITY, LOAD_FACTOR) } };
  }

  private static class KeyWithSameHashCode implements Comparable<KeyWithSameHashCode> {
    final int i;

    KeyWithSameHashCode(int i) {
      this.i = i;
    }

    @Override
    public int hashCode() {
      return 0;
    } // Returning same hashcode so that all keys landup to same bucket

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return ((KeyWithSameHashCode) o).i == i;
    }

    @Override
    public int compareTo(KeyWithSameHashCode x) {
      return Integer.compare(this.i, x.i);
    }

    @Override
    public String toString() {
      return "Key_" + i;
    }
  }

  private static final class CollidingHash implements Comparable<CollidingHash> {
    private final int value;

    public CollidingHash(int value) {
      this.value = value;
    }

    @Override
    public int hashCode() {
      // intentionally bad hashcode. Force into first bin.
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }

      if (o.getClass() != CollidingHash.class) {
        return false;
      }

      return value == ((CollidingHash) o).value;
    }

    @Override
    public int compareTo(CollidingHash o) {
      return value - o.value;
    }
  }

  @DataProvider
  Object[][] mapImplementationSupplier() {
    class SupplierWithToString implements Supplier<Map<?, Object>> {
      final Supplier<Map<?, Object>> supplier;

      SupplierWithToString(Supplier<Map<?, Object>> supplier) {
        this.supplier = supplier;
      }

      @Override
      public Map get() {
        return supplier.get();
      }

      @Override
      public String toString() {
        return supplier.get().getClass().getSimpleName() + "Supplier";
      }
    }
    Supplier<Map<?, Object>> hashMapSupplier = new SupplierWithToString(HashMap::new);
    Supplier<Map<?, Object>> linkedHashMapSupplier = new SupplierWithToString(LinkedHashMap::new);
    Supplier<Map<?, Object>> treeMapSupplier = new SupplierWithToString(TreeMap::new);
    Supplier<Map<?, Object>> indexedHashMapSupplier = new SupplierWithToString(IndexedHashMap::new);
    return new Object[][] { { hashMapSupplier }, { linkedHashMapSupplier }, { treeMapSupplier },
        { indexedHashMapSupplier } };
  }

  /*
   * @test
   * @bug 4286765
   * @summary HashMap and TreeMap entrySet().remove(k) spuriously returned
   *          false if the Map previously mapped k to null.
   */
  @Test(dataProvider = "mapImplementations")
  public void testKeySetRemove(Map<String, Object> m) {
    m.put("bananas", null);
    if (!m.keySet().remove("bananas"))
      Assert.fail("Yes, we have no bananas: " + m.getClass().getSimpleName());
  }

  /**
   * @test
   * @bug 8019381
   * @summary Verify that we do not get exception when we override isEmpty()
   *          in a subclass of HashMap
   * @author zhangshj@linux.vnet.ibm.com
   */
  @Test(dataProvider = "notEmptyMapImplementations")
  public void testOverrideIsEmpty(Map<Object, Object> map) {
    Object key = new Object();
    Object value = new Object();
    map.get(key);
    map.remove(key);
    map.replace(key, value, null);
    map.replace(key, value);
    map.computeIfPresent(key, new BiFunction<Object, Object, Object>() {
      public Object apply(Object key, Object oldValue) {
        return oldValue;
      }
    });
  }

  /**
   * @test
   * @bug 8046085
   * @summary Ensure that when trees are being used for collisions that null key
   * insertion still works.
   */
  @Test(dataProvider = "hashMapImplementationsWithConfiguredCapacityAndLoadFactor")
  public void testPutNullKey(Map<CollidingHash, Object> map) {
    // Maximum size of map
    // Should be > the treeify threshold, see HashMap/ConcurrentMap.TREEIFY_THRESHOLD
    final int SIZE = 256;
    IntStream.range(0, SIZE).mapToObj(value -> new CollidingHash(value)).forEach(e -> {
      map.put(e, e);
    });

    // kaboom?
    map.put(null, null);
  }

  /**
   * @test
   * @bug 8025173
   * @summary Verify that replacing the value for an existing key does not
   * corrupt active iterators, in particular due to a resize() occurring and
   * not updating modCount.
   * @run main ReplaceExisting
   */
  @Test(dataProvider = "mapImplementations")
  public void testReplaceExisting(Map<Integer, Object> map) {
    /* Number of entries required to trigger a resize for cap=16, load=0.75*/
    int ENTRIES = 13;

    for (int i = 0; i <= ENTRIES; i++) {
      // Add items to one more than the resize threshold
      for (int j = 0; j < ENTRIES; j++) {
        map.put(j * 10, j * 10);
      }

      /* Iterate hm for elemBeforePut elements, then call put() to replace value
       * for existing key.  With bug 8025173, this will also cause a resize, but
       * not increase the modCount.
       * Finish the iteration to check for a corrupt iterator.
       */
      int elemBeforePut = i;
      if (elemBeforePut > map.size()) {
        throw new IllegalArgumentException("Error in test: elemBeforePut must be <= HashMap size");
      }
      // Create a copy of the keys
      HashSet<Integer> keys = new HashSet<>(map.size());
      keys.addAll(map.keySet());

      HashSet<Integer> collected = new HashSet<>(map.size());

      // Run itr for elemBeforePut items, collecting returned elems
      Iterator<Integer> itr = map.keySet().iterator();
      for (int k = 0; k < elemBeforePut; k++) {
        Integer retVal = itr.next();
        if (!collected.add(retVal)) {
          throw new RuntimeException("Corrupt iterator: key " + retVal + " already encountered");
        }
      }

      // Do put() to replace entry (and resize table when bug present)
      if (map.put(0, 100) == null) {
        throw new RuntimeException("Error in test: expected key 0 to be in the HashMap");
      }

      // Finish itr + collecting returned elems
      while (itr.hasNext()) {
        Integer retVal = itr.next();
        if (!collected.add(retVal)) {
          throw new RuntimeException("Corrupt iterator: key " + retVal + " already encountered");
        }
      }

      // Compare returned elems to original copy of keys
      if (!keys.equals(collected)) {
        throw new RuntimeException("Collected keys do not match original set of keys");
      }
    }
  }

  /*
   * @test
   * @bug 4627516
   * @summary HashMap.Entry.setValue() returns new value (as opposed to old)
   * @author jbloch
   */
  @Test(dataProvider = "mapImplementations")
  public void testSetValue(Map map) {
    final String key = "key";
    final String oldValue = "old";
    final String newValue = "new";

    map.put(key, oldValue);
    Map.Entry e = (Map.Entry) map.entrySet().iterator().next();
    Object returnVal = e.setValue(newValue);
    if (!returnVal.equals(oldValue))
      throw new RuntimeException("Return value: " + returnVal);
  }

  /*
   * @test
   * @bug 4189821
   * @summary HashMap's entry.toString threw a null pointer exc if the HashMap
   *          contained null keys or values.
   */
  @Test(dataProvider = "hashMapImplementations")
  public void testToString(Map map) {
    map.put(null, null);
    map.entrySet().iterator().next().toString();
  }

  /**
   * @test
   * @bug 8186171
   * @run testng Bug8186171Test
   * @summary Verify the fix for scenario reported in JDK-8186171
   * @author deepak.kejriwal@oracle.com
   *
   * Tests and extends the scenario reported in
   * https://bugs.openjdk.java.net/browse/JDK-8186171
   * HashMap: Entry.setValue may not work after Iterator.remove() called for previous entries
   * Runs single time by reproducing exact scenario for issue mentioned in 8186171
   */
  @Test(dataProvider = "mapImplementationSupplier")
  static void testBug8186171(Supplier<Map<KeyWithSameHashCode, Object>> mapSupplier) {
    // HashMap have TREEIFY_THRESHOLD = 8; UNTREEIFY_THRESHOLD = 6;
    final int size = 11;
    List<KeyWithSameHashCode> keys = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      keys.add(new KeyWithSameHashCode(i));
    }

    KeyWithSameHashCode keyToFrob = keys.get(9);
    Map<KeyWithSameHashCode, Object> m = mapSupplier.get();
    for (KeyWithSameHashCode key: keys)
      m.put(key, null);

    for (Iterator<Map.Entry<KeyWithSameHashCode, Object>> it = m.entrySet().iterator(); it.hasNext();) {
      Map.Entry<KeyWithSameHashCode, Object> entry = it.next();
      if (entry.getKey() == keyToFrob) {
        entry.setValue(2);
      } else {
        it.remove();
      }
    }

    final String version = System.getProperty("java.version");
    final String mapSupplierName = mapSupplier.toString();
    final boolean isJdkMap =
        Objects.equals(mapSupplierName, "HashMapSupplier") || Objects.equals(mapSupplierName, "LinkedHashMapSupplier");

    if (version.equals("1.8.0_172") && isJdkMap) {
      String failureMessage = "Expect to see JDK Bug-8186171 with JKD version " + version + " and " + mapSupplierName;
      assertTrue(m.containsValue(null), failureMessage);
      assertFalse(m.containsValue(2), failureMessage);
    } else {
      String failureMessage =
          "Not expect to see JDK Bug-8186171 with JKD version " + version + " and " + mapSupplierName;
      assertFalse(m.containsValue(null), failureMessage);
      assertTrue(m.containsValue(2), failureMessage);
    }
    assertTrue(m.containsKey(keyToFrob));
    assertEquals(1, m.size());
  }
}
