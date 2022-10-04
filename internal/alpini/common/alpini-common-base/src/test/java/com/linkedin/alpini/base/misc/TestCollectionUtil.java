package com.linkedin.alpini.base.misc;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestCollectionUtil {
  @Test(groups = "unit")
  public void testSetBuilder() {
    Set<String> set = CollectionUtil.<String>setBuilder().build();
    Assert.assertEquals(set.size(), 0);

    set = CollectionUtil.<String>setBuilder().add("TEST").build();
    Assert.assertEquals(set.size(), 1);
    Assert.assertTrue(set.contains("TEST"));

    set = CollectionUtil.<String>setBuilder().add(new String[] { "TEST", "FOO" }).build();
    Assert.assertEquals(set.size(), 2);
    Assert.assertTrue(set.contains("TEST"));
    Assert.assertTrue(set.contains("FOO"));

    Set<String> set2 = CollectionUtil.<String>setBuilder().addAll(set).build();
    Assert.assertEquals(set2, set);
    Assert.assertNotSame(set2, set);

    set2 = CollectionUtil.<String>setBuilder().addAll(set.iterator()).build();
    Assert.assertEquals(set2, set);
    Assert.assertNotSame(set2, set);
  }

  @Test(groups = "unit")
  public void testMapBuilder() {
    Map<String, Integer> map = CollectionUtil.<String, Integer>mapBuilder().build();
    Assert.assertEquals(map.size(), 0);

    map = CollectionUtil.<String, Integer>mapBuilder().add(ImmutableMapEntry.make("TEST", 1)).build();
    Assert.assertEquals(map.size(), 1);
    Assert.assertSame(map.get("TEST"), 1);

    map = CollectionUtil.<String, Integer>mapBuilder()
        .add(ImmutableMapEntry.make("TEST", 1))
        .add(ImmutableMapEntry.make("FOO", 2))
        .build();
    Assert.assertEquals(map.size(), 2);
    Assert.assertSame(map.get("TEST"), 1);
    Assert.assertSame(map.get("FOO"), 2);
  }

  @Test(groups = "unit")
  public void testSetOf0() {
    Set<String> set = CollectionUtil.setOf(Collections.emptyList());
    Assert.assertEquals(set.size(), 0);

    set = CollectionUtil.setOf(new String[0]);
    Assert.assertEquals(set.size(), 0);
  }

  @Test(groups = "unit")
  public void testSetOf1() {
    Set<String> set = CollectionUtil.setOf("TEST");
    Assert.assertEquals(set.size(), 1);
    Assert.assertTrue(set.contains("TEST"));
  }

  @Test(groups = "unit")
  public void testListOf0() {
    List<String> list = CollectionUtil.listOf();
    Assert.assertEquals(list.size(), 0);
  }

  @Test(groups = "unit")
  public void testListOf1() {
    List<String> list = CollectionUtil.listOf("TEST");
    Assert.assertEquals(list.size(), 1);
    Assert.assertTrue(list.contains("TEST"));

    list = CollectionUtil.listOf(new String[] { "TEST" });
    Assert.assertEquals(list.size(), 1);
    Assert.assertTrue(list.contains("TEST"));
  }

  @Test(groups = "unit")
  public void testListOf2() {
    List<String> list = CollectionUtil.listOf("TEST", "FOO");
    Assert.assertEquals(list.size(), 2);
    Assert.assertTrue(list.contains("TEST"));
    Assert.assertTrue(list.contains("FOO"));
  }

  @Test(groups = "unit")
  public void testMapOf0() {
    @SuppressWarnings("unchecked")
    ImmutableMapEntry<String, Integer>[] empty = new ImmutableMapEntry[0];
    Map<String, Integer> map = CollectionUtil.mapOfEntries(empty);

    Assert.assertEquals(map.size(), 0);
  }

  @Test(groups = "unit")
  public void testMapOf1() {
    Map<String, Integer> map = CollectionUtil.mapOfEntries(ImmutableMapEntry.make("ONE", 1));

    Assert.assertEquals(map.size(), 1);
    Assert.assertSame(map.get("ONE"), 1);
  }

  @Test(groups = "unit")
  public void testMapOf2() {
    Map<String, Integer> map = CollectionUtil.mapOf("ONE", 1, "TWO", 2);

    Assert.assertEquals(map.size(), 2);
    Assert.assertSame(map.get("ONE"), 1);
    Assert.assertSame(map.get("TWO"), 2);
  }

  @Test(groups = "unit")
  public void testMapOf3() {
    Map<String, Integer> map = CollectionUtil.mapOf("ONE", 1, "TWO", 2, "THREE", 3);

    Assert.assertEquals(map.size(), 3);
    Assert.assertSame(map.get("ONE"), 1);
    Assert.assertSame(map.get("TWO"), 2);
    Assert.assertSame(map.get("THREE"), 3);
  }

  @Test(groups = "unit")
  public void testMapOf4() {
    Map<String, Integer> map = CollectionUtil.mapOf("ONE", 1, "TWO", 2, "THREE", 3, "FOUR", 4);

    Assert.assertEquals(map.size(), 4);
    Assert.assertSame(map.get("ONE"), 1);
    Assert.assertSame(map.get("TWO"), 2);
    Assert.assertSame(map.get("THREE"), 3);
    Assert.assertSame(map.get("FOUR"), 4);
  }

  @Test(groups = "unit")
  public void testConcat() {
    List<Integer> list1 = CollectionUtil.listOf(1, 2, 3);
    List<Integer> list2 = CollectionUtil.listOf(4, 5, 6);

    Iterator<Integer> it = CollectionUtil.concat(list1.iterator(), list2.iterator());

    Assert.assertTrue(it.hasNext());
    Assert.assertSame(it.next(), (Integer) 1);
    Assert.assertTrue(it.hasNext());
    Assert.assertSame(it.next(), (Integer) 2);
    Assert.assertTrue(it.hasNext());
    Assert.assertSame(it.next(), (Integer) 3);
    Assert.assertTrue(it.hasNext());
    Assert.assertSame(it.next(), (Integer) 4);
    Assert.assertTrue(it.hasNext());
    Assert.assertSame(it.next(), (Integer) 5);
    Assert.assertTrue(it.hasNext());
    Assert.assertSame(it.next(), (Integer) 6);
    Assert.assertFalse(it.hasNext());
  }

  @Test(groups = "unit")
  public void testBatchOf() {
    List<Integer> original = IntStream.range(1, 101).mapToObj(Integer::valueOf).collect(Collectors.toList());
    Assert.assertEquals(original.size(), 100);

    List<List<Integer>> batches = original.stream().collect(CollectionUtil.batchOf(10));

    Assert.assertEquals(batches.size(), 10);

    int value = 1;
    for (List<Integer> batch: batches) {
      Assert.assertEquals(batch.size(), 10);
      for (Integer element: batch) {
        Assert.assertSame(element, value++);
      }
    }
    Assert.assertEquals(value, 101);
  }

  @Test(groups = "unit")
  public void testComputeIfAbsent() {
    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    CollectionUtil.computeIfAbsent(map, "Hello", k -> "World");
    Assert.assertTrue(map.containsKey("Hello"));
    Assert.assertEquals(map.get("Hello"), "World");

    CollectionUtil.computeIfAbsent(map, "Hello", k -> "World");
    Assert.assertTrue(map.containsKey("Hello"));
    Assert.assertEquals(map.get("Hello"), "World");

    CollectionUtil.computeIfAbsent(map, "Hello1", k -> "World1");
    Assert.assertTrue(map.containsKey("Hello1"));
    Assert.assertEquals(map.get("Hello1"), "World1");
  }

  @Test(groups = "unit")
  public void testDeepCloneArray() {

    String[] test1 = {};
    String[] test2 = { "a" };
    String[] test3 = { "a", "b" };

    String[] result1 = CollectionUtil.deepCloneArray(test1);
    Assert.assertNotSame(result1, test1);
    Assert.assertEquals(result1, test1);
    String[] result2 = CollectionUtil.deepCloneArray(test2);
    Assert.assertNotSame(result2, test2);
    Assert.assertEquals(result2, test2);
    String[] result3 = CollectionUtil.deepCloneArray(test3);
    Assert.assertNotSame(result3, test3);
    Assert.assertEquals(result3, test3);

    String[][] test4 = { { "a" }, { "b", null } };
    String[][] result4 = CollectionUtil.deepCloneArray(test4);
    Assert.assertNotSame(result4, test4);
    Assert.assertEquals(result4.length, test4.length);
    Assert.assertNotSame(result4[0], test4[0]);
    Assert.assertEquals(result4[0], test4[0]);
    Assert.assertNotSame(result4[1], test4[1]);
    Assert.assertEquals(result4[1], test4[1]);

    // check that we can't trick it into an infinite loop and that
    // the structure will be identical
    Object[] test5 = { "foo", new String[] { "bar " }, new Object[] { null }, new int[] { 1, 2 } };
    ((Object[]) test5[2])[0] = test5;
    Object[] result5 = CollectionUtil.deepCloneArray(test5);
    Assert.assertNotSame(result5, test5);
    Assert.assertEquals(result5.length, test5.length);
    Assert.assertSame(result5[0], test5[0]);
    Assert.assertNotSame(result5[1], test5[1]);
    Assert.assertEquals(result5[1], test5[1]);
    Assert.assertNotSame(result5[2], test5[2]);
    Assert.assertEquals(((Object[]) result5[2]).length, 1);
    Assert.assertSame(((Object[]) result5[2])[0], result5);
    Assert.assertNotSame(result5[3], test5[3]);
    Assert.assertEquals(result5[3], test5[3]);

    Object[] test6 = { new boolean[] { true }, new byte[] { 1 }, new short[] { 2 }, new char[] { 'C' }, new int[] { 5 },
        new float[] { 6.0f }, new long[] { 7L }, new double[] { 8.0 }, null, null };
    test6[9] = test6[2];
    Object[] result6 = CollectionUtil.deepCloneArray(test6);
    Assert.assertNotSame(result6, test6);
    Assert.assertEquals(result6.length, test6.length);
    Assert.assertSame(result6[9], result6[2]);
    Assert.assertNull(result6[8]);
    for (int i = 0; i < 8; i++) {
      Assert.assertNotSame(result6[i], test6[i]);
      Assert.assertEquals(result6[i], test6[i]);
    }

  }
}
