package com.linkedin.venice.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IteratorUtilsTest {
  @Test
  public void testMapIterator() {
    Iterator<Integer> iterator = Arrays.asList(1, 2, 3, 4, 5).iterator();
    Iterator<String> mappedIterator = IteratorUtils.mapIterator(iterator, Object::toString);

    Arrays.asList("1", "2", "3", "4", "5").forEach(expected -> {
      Assert.assertTrue(mappedIterator.hasNext());
      Assert.assertEquals(mappedIterator.next(), expected);
    });

    Assert.assertFalse(mappedIterator.hasNext());
  }

  @Test
  public void testMapEmptyIterator() {
    List<Integer> list = Collections.emptyList();
    Iterator<Integer> iterator = list.iterator();
    Iterator<String> mappedIterator = IteratorUtils.mapIterator(iterator, Object::toString);

    Assert.assertFalse(mappedIterator.hasNext());
  }

  @Test
  public void testMapIteratorWithNullMapper() {
    Iterator<Integer> iterator = Arrays.asList(1, 2, 3, 4, 5).iterator();
    Assert.assertThrows(NullPointerException.class, () -> IteratorUtils.mapIterator(iterator, null));
  }

  @Test
  public void testMapIteratorWithNullIterator() {
    Assert.assertNull(IteratorUtils.mapIterator(null, Object::toString));
  }
}
