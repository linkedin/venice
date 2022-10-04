package com.linkedin.alpini.base.misc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestIteratorUtil {
  public void testEmpty() {
    Assert.assertSame(IteratorUtil.empty(), Collections.emptyIterator());
  }

  public void testSingleton() {
    Iterator<Integer> it = IteratorUtil.singleton(42);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(it.next(), (Integer) 42);
    Assert.assertFalse(it.hasNext());
    Assert.assertThrows(NoSuchElementException.class, it::next);
  }

  public void testOf() {
    Iterator<Integer> it = IteratorUtil.of(42, 11);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(it.next(), (Integer) 42);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(it.next(), (Integer) 11);
    Assert.assertFalse(it.hasNext());
    Assert.assertThrows(NoSuchElementException.class, it::next);
  }

  public void testCount() {
    Assert.assertEquals(IteratorUtil.count(IteratorUtil.empty()), 0);
    Assert.assertEquals(IteratorUtil.count(IteratorUtil.singleton(42)), 1);
    Assert.assertEquals(IteratorUtil.count(IteratorUtil.of(42, 11)), 2);
  }

  public void testMap() {
    ArrayList<Integer> list = new ArrayList<>();
    list.add(41);
    list.add(10);

    Iterator<Long> it = IteratorUtil.map(list.iterator(), value -> 1L + value);

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(it.next(), (Long) 42L);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(it.next(), (Long) 11L);
    it.remove();
    Assert.assertFalse(it.hasNext());
    Assert.assertThrows(NoSuchElementException.class, it::next);

    Assert.assertEquals(list.size(), 1);
    Assert.assertEquals(list.get(0), (Integer) 41);
  }

  public void testFlatMap() {
    ArrayList<Integer> list = new ArrayList<>();
    list.add(42);
    list.add(10);

    Iterator<Long> it = IteratorUtil
        .flatMap(list.iterator(), value -> value == 42 ? IteratorUtil.of(6L, 7L) : IteratorUtil.singleton(1L + value));

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(it.next(), (Long) 6L);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(it.next(), (Long) 7L);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(it.next(), (Long) 11L);
    Assert.assertThrows(UnsupportedOperationException.class, it::remove);
    Assert.assertFalse(it.hasNext());
    Assert.assertThrows(NoSuchElementException.class, it::next);

    Assert.assertEquals(list.toArray(new Integer[0]), new Integer[] { 42, 10 });
  }

  public void testFilter() {
    ArrayList<Integer> list = new ArrayList<>();
    list.add(88);
    list.add(42);
    list.add(10);

    Iterator<Integer> it = IteratorUtil.filter(list.iterator(), value -> value != 42);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(it.next(), (Integer) 88);
    Assert.assertTrue(it.hasNext());
    Assert.assertThrows(UnsupportedOperationException.class, it::remove);
    Assert.assertEquals(it.next(), (Integer) 10);
    it.remove();
    Assert.assertFalse(it.hasNext());
    Assert.assertThrows(NoSuchElementException.class, it::next);

    Assert.assertEquals(list.toArray(new Integer[0]), new Integer[] { 88, 42 });
  }

  public void testStream() {
    Integer[] arr = IteratorUtil.stream(IteratorUtil.singleton(42)).toArray(Integer[]::new);
    Assert.assertEquals(arr, new Integer[] { 42 });
  }

  public void testToList() {
    ArrayList<Integer> list = new ArrayList<>();
    list.add(88);
    list.add(42);
    list.add(10);

    Assert.assertEquals(
        IteratorUtil.toList(IteratorUtil.filter(list.iterator(), value -> value != 42)).toArray(new Integer[0]),
        new Integer[] { 88, 10 });

    Assert.assertEquals(
        IteratorUtil.toList(IteratorUtil.map(list.iterator(), value -> (long) value)).toArray(new Long[0]),
        new Long[] { 88L, 42L, 10L });

    Assert.assertEquals(
        IteratorUtil
            .toList(
                IteratorUtil.flatMap(
                    list.iterator(),
                    value -> value == 42 ? IteratorUtil.of(6L, 7L) : IteratorUtil.singleton(1L + value)))
            .toArray(new Long[0]),
        new Long[] { 89L, 6L, 7L, 11L });
  }
}
