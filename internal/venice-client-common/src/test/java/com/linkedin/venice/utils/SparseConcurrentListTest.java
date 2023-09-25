package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SparseConcurrentListTest {
  @Test
  public void testNonNullSize() {
    SparseConcurrentList<Object> scl = new SparseConcurrentList<>();
    assertEquals(scl.size(), 0);
    assertEquals(scl.nonNullSize(), 0);
    assertEquals(scl.values().size(), 0);

    scl.set(5, new Object());
    Assert.assertNotNull(5);
    assertEquals(scl.size(), 6);
    assertEquals(scl.nonNullSize(), 1);
    assertEquals(scl.values().size(), 1);

    scl.set(2, new Object());
    Assert.assertNotNull(2);
    assertEquals(scl.size(), 6);
    assertEquals(scl.nonNullSize(), 2);
    assertEquals(scl.values().size(), 2);

    scl.remove(3);
    Assert.assertNull(scl.get(3));
    assertEquals(scl.size(), 6);
    assertEquals(scl.nonNullSize(), 2);
    assertEquals(scl.values().size(), 2);

    scl.remove(2);
    Assert.assertNull(scl.get(2));
    assertEquals(scl.size(), 6);
    assertEquals(scl.nonNullSize(), 1);
    assertEquals(scl.values().size(), 1);

    Collection<Object> objectsToAppend = new ArrayList<>();
    objectsToAppend.add(new Object());
    objectsToAppend.add(new Object());
    scl.addAll(objectsToAppend);
    Assert.assertNotNull(6);
    Assert.assertNotNull(7);
    assertEquals(scl.size(), 8);
    assertEquals(scl.nonNullSize(), 3);
    assertEquals(scl.values().size(), 3);

    scl.clear();
    assertEquals(scl.size(), 0);
    assertEquals(scl.nonNullSize(), 0);
    assertEquals(scl.values().size(), 0);
  }
}
