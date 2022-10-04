package com.linkedin.alpini.base.concurrency;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestArraySortedSet {
  public void basicTest() {

    SortedSet<Integer> set = new ArraySortedSet<>(Comparator.naturalOrder(), 100);

    Assert.assertTrue(set.isEmpty());
    Assert.assertTrue(set.add(10));
    Assert.assertFalse(set.isEmpty());
    Assert.assertEquals(set.first(), (Integer) 10);
    Assert.assertEquals(set.last(), (Integer) 10);

    Assert.assertTrue(set.add(1));
    Assert.assertEquals(set.first(), (Integer) 1);
    Assert.assertEquals(set.last(), (Integer) 10);

    Assert.assertFalse(set.contains(3));

    Assert.assertTrue(set.add(5));
    Assert.assertTrue(set.add(2));
    Assert.assertTrue(set.addAll(Arrays.asList(8, 3, 7)));
    Assert.assertFalse(set.addAll(Arrays.asList(8, 3, 7)));

    Assert.assertFalse(set.add(5));

    Assert.assertTrue(set.contains(3));

    Assert.assertEquals(set.size(), 7);

    Assert.assertFalse(set.remove(9));
    Assert.assertTrue(set.remove(1));

    Assert.assertEquals(set.size(), 6);

    Assert.assertEquals(set.toArray(), new Object[] { 2, 3, 5, 7, 8, 10 });

    int i = set.hashCode();
    Assert.assertTrue(set.remove(7));
    Assert.assertNotEquals(set.hashCode(), i);

    Iterator<Integer> it = set.iterator();
    Assert.assertEquals(it.next(), (Integer) 2);
    Assert.assertEquals(it.next(), (Integer) 3);
    Assert.assertEquals(it.next(), (Integer) 5);
    Assert.assertEquals(it.next(), (Integer) 8);
    Assert.assertEquals(it.next(), (Integer) 10);
    Assert.assertThrows(NoSuchElementException.class, it::next);

    SortedSet<Integer> set2 = ((ArraySortedSet<Integer>) set).clone();
    Assert.assertNotSame(set2, set);

    Assert.assertEquals(((ArraySortedSet<Integer>) set).floor(6), (Integer) 8);
    Assert.assertEquals(((ArraySortedSet<Integer>) set).floor(5), (Integer) 5);

    Assert.assertEquals(set2, set);

    Integer[] ia = new Integer[0];
    Assert.assertEquals(set.toArray(ia), new Integer[] { 2, 3, 5, 8, 10 });

    Assert.assertTrue(set.containsAll(Collections.singleton(5)));
    Assert.assertTrue(set.removeAll(Collections.singleton(5)));

    Assert.assertEquals(set.toArray(new Integer[10]), new Integer[] { 2, 3, 8, 10 });

    SortedSet<Integer> sub = set.subSet(3, 10);
    Assert.assertEquals(sub.size(), 2);
    Assert.assertSame(sub.subSet(1, 11), sub);
    StringBuilder sb = new StringBuilder();
    sub.forEach(sb::append);
    Assert.assertEquals(sb.toString(), "38");
    Assert.assertTrue(sub.containsAll(Arrays.asList(3, 8)));
    Assert.assertFalse(sub.containsAll(Arrays.asList(3, 8, 20)));
    Assert.assertFalse(sub.contains(0));

    SortedSet<Integer> tail = set.tailSet(5);
    Assert.assertFalse(tail.isEmpty());
    Assert.assertEquals(new ArrayList<>(tail).toArray(), new Object[] { 8, 10 });
    Assert.assertSame(tail.tailSet(4), tail);
    Assert.assertEquals(tail.tailSet(9).toArray(), new Object[] { 10 });

    SortedSet<Integer> head = set.headSet(5);
    Assert.assertEquals(new ArrayList<>(head).toArray(), new Object[] { 2, 3 });
    Assert.assertSame(head.headSet(6), head);
    Assert.assertEquals(head.headSet(3).toArray(ia), new Integer[] { 2 });

    head.clear();
    Assert.assertEquals(set.size(), 2);
    Assert.assertTrue(head.isEmpty());
    Assert.assertEquals(set.toArray(ia), new Integer[] { 8, 10 });

    Assert.assertFalse(tail.isEmpty());
    set.clear();
    Assert.assertTrue(tail.isEmpty());

    sb.setLength(0);
    set2.forEach(sb::append);
    Assert.assertEquals(sb.toString(), "235810");

  }

}
