package com.linkedin.davinci.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.utils.IndexedMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class IndexedHashMapTest {
  private static final List<Class> MAPS_WITH_EQUIVALENT_INSERTION_AND_ITERATION_ORDER =
      Arrays.asList(LinkedHashMap.class, IndexedHashMap.class);
  private static final String KEY_PREFIX = "Key_";
  private static final List[] LISTS = { new ArrayList(), new LinkedList(), new CopyOnWriteArrayList(), new Vector() };

  @DataProvider
  Object[][] mapAndListImplementations() {
    Map[] maps = { new HashMap(), new TreeMap(), new LinkedHashMap(), new IndexedHashMap() };
    Object[][] params = new Object[maps.length * LISTS.length][2];
    int permutationCount = 0;
    for (int m = 0; m < maps.length; m++) {
      for (int l = 0; l < LISTS.length; l++) {
        params[permutationCount][0] = maps[m];
        params[permutationCount++][1] = LISTS[l];
      }
    }
    return params;
  }

  @DataProvider
  Object[][] listImplementations() {
    Object[][] params = new Object[LISTS.length][1];
    int permutationCount = 0;
    for (int l = 0; l < LISTS.length; l++) {
      params[permutationCount++][0] = LISTS[l];
    }
    return params;
  }

  /**
   * Load data from various map implementations into a {@link IndexedHashMap} and
   * then test all flavors of iteration over key, values and entries.
   */
  @Test(dataProvider = "mapAndListImplementations")
  public void testIndexedMapLoad(Map<String, Integer> map, List<Map.Entry<String, Integer>> list) {
    map.clear();
    list.clear();
    for (int i = 0; i < 100; i++) {
      map.put(KEY_PREFIX + i, i);
    }

    // Load from another map
    com.linkedin.venice.utils.IndexedMap<String, Integer> indexedMap = new IndexedHashMap(map, list);

    final AtomicInteger index = new AtomicInteger(0);

    Consumer<String> keyConsumer = key -> {
      // This should work for all types of imported maps
      assertTrue(map.containsKey(key));

      // The following only works for maps where the iteration order is the same as the insertion order
      if (MAPS_WITH_EQUIVALENT_INSERTION_AND_ITERATION_ORDER.contains(map.getClass())) {
        assertEquals(key, "Key_" + index.get());
        assertEquals(indexedMap.indexOf(key), index.get());
        assertEquals(key, indexedMap.getByIndex(index.get()).getKey());
      }
    };

    Consumer<Integer> valueConsumer = value -> {
      // This should work for all types of imported maps
      assertTrue(map.containsValue(value));

      // The following only works for maps where the iteration order is the same as the insertion order
      if (MAPS_WITH_EQUIVALENT_INSERTION_AND_ITERATION_ORDER.contains(map.getClass())) {
        assertEquals((int) value, index.get());
        assertEquals(value, indexedMap.getByIndex(index.get()).getValue());
      }
    };

    Consumer<Map.Entry<String, Integer>> entryConsumer = entry -> {
      String key = entry.getKey();
      Integer value = entry.getValue();

      keyConsumer.accept(key);
      valueConsumer.accept(value);

      // This should work for all types of imported maps
      assertEquals(value, map.get(key));
      assertEquals(entry, indexedMap.getByIndex(indexedMap.indexOf(key)));

      // The following only works for maps where the iteration order is the same as the insertion order
      if (MAPS_WITH_EQUIVALENT_INSERTION_AND_ITERATION_ORDER.contains(map.getClass())) {
        assertEquals(entry, indexedMap.getByIndex(index.get()));
      }
    };

    testIteration(indexedMap, index, keyConsumer, valueConsumer, entryConsumer);

    map.clear();
    list.clear();
  }

  @Test(dataProvider = "listImplementations")
  public void testRemoveByIndex(List<Map.Entry<String, Integer>> list) {
    list.clear();
    com.linkedin.venice.utils.IndexedMap<String, Integer> indexedMap = new IndexedHashMap(16, 0.75f, list);

    final int INITIAL_NUMBER_OF_ENTRIES = 100;
    for (int i = 0; i < INITIAL_NUMBER_OF_ENTRIES; i++) {
      indexedMap.put(KEY_PREFIX + i, i);
    }
    assertEquals(indexedMap.size(), INITIAL_NUMBER_OF_ENTRIES);

    // Remove 1/10th of the entries
    for (int i = 99; i >= 0; i--) {
      if (i % 10 == 0) {
        indexedMap.removeByIndex(i);
      }
    }
    assertEquals(indexedMap.size(), 90);

    final AtomicInteger index = new AtomicInteger(0);

    Consumer<String> keyConsumer = key -> {
      int numberPortion = Integer.parseInt(key.substring(KEY_PREFIX.length()));
      assertFalse(numberPortion % 10 == 0, "Got key '" + key + "' which should have been removed!");
    };

    Consumer<Integer> valueConsumer = value -> {
      assertFalse(value % 10 == 0, "Got value '" + value + "' which should have been removed!");
    };

    Consumer<Map.Entry<String, Integer>> entryConsumer = entry -> {
      String key = entry.getKey();
      Integer value = entry.getValue();

      keyConsumer.accept(key);
      valueConsumer.accept(value);
    };

    testIteration(indexedMap, index, keyConsumer, valueConsumer, entryConsumer);

    list.clear();
  }

  @Test(dataProvider = "listImplementations")
  public void testMoveElement(List<Map.Entry<String, Integer>> list) {
    list.clear();
    com.linkedin.venice.utils.IndexedMap<String, Integer> indexedMap = new IndexedHashMap(16, 0.75f, list);

    final int INITIAL_NUMBER_OF_ENTRIES = 100;
    for (int i = 0; i < INITIAL_NUMBER_OF_ENTRIES; i++) {
      indexedMap.put(KEY_PREFIX + i, i);
    }
    assertEquals(indexedMap.size(), INITIAL_NUMBER_OF_ENTRIES);

    final AtomicInteger index = new AtomicInteger(0);

    // Move 1/10th of the entries to the beginning
    for (int i = 99; i >= 0; i--) {
      if (i % 10 == 0) {
        indexedMap.moveElement(indexedMap.indexOf(KEY_PREFIX + i), 0);
      }
      final int THRESHOLD_UNDER_WHICH_THE_MOVED_ELEMENTS_ARE = (100 - i) / 10;
      final int THRESHOLD_ABOVE_WHICH_THE_ELEMENTS_WERE_TRAVERSED = i;
      Consumer<Integer> valueConsumer = value -> {
        if (index.get() < THRESHOLD_UNDER_WHICH_THE_MOVED_ELEMENTS_ARE) {
          assertTrue(value % 10 == 0);
          if (THRESHOLD_ABOVE_WHICH_THE_ELEMENTS_WERE_TRAVERSED == 0) {
            assertEquals((int) value, index.get() * 10);
          }
        } else if (index.get() > (THRESHOLD_ABOVE_WHICH_THE_ELEMENTS_WERE_TRAVERSED
            + THRESHOLD_UNDER_WHICH_THE_MOVED_ELEMENTS_ARE)) {
          assertFalse(value % 10 == 0, "Expected not to find value " + value + " above index " + index.get());
        }
      };

      Consumer<String> keyConsumer = key -> {
        int value = Integer.parseInt(key.substring(KEY_PREFIX.length()));
        valueConsumer.accept(value);
      };

      Consumer<Map.Entry<String, Integer>> entryConsumer = entry -> {
        String key = entry.getKey();
        Integer value = entry.getValue();

        keyConsumer.accept(key);
        valueConsumer.accept(value);
      };

      testIteration(indexedMap, index, keyConsumer, valueConsumer, entryConsumer);
    }
    assertEquals(indexedMap.size(), INITIAL_NUMBER_OF_ENTRIES);

    list.clear();
  }

  @Test(dataProvider = "listImplementations")
  public void testPutByIndex(List<Map.Entry<String, Integer>> list) {
    list.clear();
    com.linkedin.venice.utils.IndexedMap<String, Integer> indexedMap = new IndexedHashMap(16, 0.75f, list);

    final int INITIAL_NUMBER_OF_ENTRIES = 90;
    final int NUMBER_OF_ADDITIONAL_ENTRIES = 10;
    for (int i = 0; i < INITIAL_NUMBER_OF_ENTRIES; i++) {
      indexedMap.put(KEY_PREFIX + i, i);
    }
    assertEquals(indexedMap.size(), INITIAL_NUMBER_OF_ENTRIES);

    final AtomicInteger index = new AtomicInteger(0);

    // Move 1/10th of the entries to the beginning
    for (int i = INITIAL_NUMBER_OF_ENTRIES,
        j = 0; i < INITIAL_NUMBER_OF_ENTRIES + NUMBER_OF_ADDITIONAL_ENTRIES; i++, j++) {
      indexedMap.putByIndex(KEY_PREFIX + i, i, j);
    }
    assertEquals(indexedMap.size(), INITIAL_NUMBER_OF_ENTRIES + NUMBER_OF_ADDITIONAL_ENTRIES);

    Consumer<Integer> valueConsumer = value -> {
      assertEquals(indexedMap.getByIndex(index.get()).getValue(), value);

      if (index.get() < NUMBER_OF_ADDITIONAL_ENTRIES) {
        assertEquals((int) value, INITIAL_NUMBER_OF_ENTRIES + index.get());
      } else {
        assertEquals((int) value, index.get() - NUMBER_OF_ADDITIONAL_ENTRIES);
      }
    };

    Consumer<String> keyConsumer = key -> {
      int value = Integer.parseInt(key.substring(KEY_PREFIX.length()));
      valueConsumer.accept(value);

      assertEquals(indexedMap.indexOf(key), index.get());
    };

    Consumer<Map.Entry<String, Integer>> entryConsumer = entry -> {
      String key = entry.getKey();
      Integer value = entry.getValue();

      keyConsumer.accept(key);
      valueConsumer.accept(value);
    };

    testIteration(indexedMap, index, keyConsumer, valueConsumer, entryConsumer);

    list.clear();
  }

  @Test(dataProvider = "listImplementations")
  public void testClear(List<Map.Entry<String, Integer>> list) {
    list.clear();
    IndexedMap<String, Integer> indexedMap = new IndexedHashMap(16, 0.75f, list);

    final int INITIAL_NUMBER_OF_ENTRIES = 100;
    for (int i = 0; i < INITIAL_NUMBER_OF_ENTRIES; i++) {
      indexedMap.put(KEY_PREFIX + i, i);
    }
    assertEquals(indexedMap.size(), INITIAL_NUMBER_OF_ENTRIES);

    indexedMap.clear();

    final AtomicInteger index = new AtomicInteger(0);

    Consumer<Integer> valueConsumer = value -> {
      assertEquals(index.get(), 0);
    };

    Consumer<String> keyConsumer = key -> {
      assertEquals(index.get(), 0);
    };

    Consumer<Map.Entry<String, Integer>> entryConsumer = entry -> {
      assertEquals(index.get(), 0);
    };

    testIteration(indexedMap, index, keyConsumer, valueConsumer, entryConsumer);

    list.clear();
  }

  private void testIteration(
      Map<String, Integer> map,
      AtomicInteger index,
      Consumer<String> keyConsumer,
      Consumer<Integer> valueConsumer,
      Consumer<Map.Entry<String, Integer>> entryConsumer) {

    // Entry iteration

    index.set(0);
    for (Map.Entry<String, Integer> entry: map.entrySet()) {
      entryConsumer.accept(entry);
      index.incrementAndGet();
    }

    index.set(0);
    Iterator<Map.Entry<String, Integer>> entryIterator = map.entrySet().iterator();
    while (entryIterator.hasNext()) {
      entryConsumer.accept(entryIterator.next());
      index.incrementAndGet();
    }

    index.set(0);
    Spliterator<Map.Entry<String, Integer>> entrySpliterator = map.entrySet().spliterator();
    while (entrySpliterator.tryAdvance(entryConsumer)) {
      index.incrementAndGet();
    }

    index.set(0);
    map.entrySet().forEach(entry -> {
      entryConsumer.accept(entry);
      index.incrementAndGet();
    });

    index.set(0);
    map.entrySet().iterator().forEachRemaining(entry -> {
      entryConsumer.accept(entry);
      index.incrementAndGet();
    });

    index.set(0);
    map.entrySet().spliterator().forEachRemaining(entry -> {
      entryConsumer.accept(entry);
      index.incrementAndGet();
    });

    // Key iteration

    index.set(0);
    for (String key: map.keySet()) {
      keyConsumer.accept(key);
      index.incrementAndGet();
    }

    index.set(0);
    Iterator<String> keyIterator = map.keySet().iterator();
    while (keyIterator.hasNext()) {
      keyConsumer.accept(keyIterator.next());
      index.incrementAndGet();
    }

    index.set(0);
    Spliterator<String> keySpliterator = map.keySet().spliterator();
    while (keySpliterator.tryAdvance(keyConsumer)) {
      index.incrementAndGet();
    }

    index.set(0);
    map.keySet().forEach(key -> {
      keyConsumer.accept(key);
      index.incrementAndGet();
    });

    index.set(0);
    map.keySet().iterator().forEachRemaining(key -> {
      keyConsumer.accept(key);
      index.incrementAndGet();
    });

    index.set(0);
    map.keySet().spliterator().forEachRemaining(key -> {
      keyConsumer.accept(key);
      index.incrementAndGet();
    });

    // Value iteration

    index.set(0);
    for (int value: map.values()) {
      valueConsumer.accept(value);
      index.incrementAndGet();
    }

    index.set(0);
    Iterator<Integer> valueIterator = map.values().iterator();
    while (valueIterator.hasNext()) {
      valueConsumer.accept(valueIterator.next());
      index.incrementAndGet();
    }

    index.set(0);
    Spliterator<Integer> valueSpliterator = map.values().spliterator();
    while (valueSpliterator.tryAdvance(valueConsumer)) {
      index.incrementAndGet();
    }

    index.set(0);
    map.values().forEach(value -> {
      valueConsumer.accept(value);
      index.incrementAndGet();
    });

    index.set(0);
    map.values().iterator().forEachRemaining(value -> {
      valueConsumer.accept(value);
      index.incrementAndGet();
    });

    index.set(0);
    map.values().spliterator().forEachRemaining(value -> {
      valueConsumer.accept(value);
      index.incrementAndGet();
    });
  }
}
