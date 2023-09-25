package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SparseConcurrentListTest {
  @Test
  public void testNonNullSize() {
    // Initial state: empty list.
    SparseConcurrentList<Object> scl = new SparseConcurrentList<>();
    assertNull(scl.get(1));
    assertThrows(() -> scl.get(-1));
    assertEquals(scl.size(), 0);
    assertEquals(scl.nonNullSize(), 0);
    assertEquals(scl.values().size(), scl.nonNullSize()); // This assertion should always be true.
    Assert.assertTrue(scl.isEmpty());

    // Add a new item at index 5.
    scl.set(5, new Object());
    Assert.assertNotNull(5);
    assertEquals(scl.size(), 6);
    assertEquals(scl.nonNullSize(), 1);
    assertEquals(scl.values().size(), scl.nonNullSize());
    Assert.assertFalse(scl.isEmpty());

    // Add a new item at an earlier index than the already present item. No resizing expected.
    scl.set(2, new Object());
    Assert.assertNotNull(2);
    assertEquals(scl.size(), 6);
    assertEquals(scl.nonNullSize(), 2);
    assertEquals(scl.values().size(), scl.nonNullSize());
    Assert.assertFalse(scl.isEmpty());

    // Remove the element at an index that already had nothing.
    scl.remove(3);
    Assert.assertNull(scl.get(3));
    assertEquals(scl.size(), 6);
    assertEquals(scl.nonNullSize(), 2);
    assertEquals(scl.values().size(), scl.nonNullSize());
    Assert.assertFalse(scl.isEmpty());

    // Remove the element at an index that does contain something.
    scl.remove(2);
    Assert.assertNull(scl.get(2));
    assertEquals(scl.size(), 6);
    assertEquals(scl.nonNullSize(), 1);
    assertEquals(scl.values().size(), scl.nonNullSize());
    Assert.assertFalse(scl.isEmpty());

    // Append a couple of elements to the end of the list.
    Collection<Object> objectsToAppend = new ArrayList<>();
    objectsToAppend.add(new Object());
    objectsToAppend.add(new Object());
    scl.addAll(objectsToAppend);
    Assert.assertNotNull(6);
    Assert.assertNotNull(7);
    assertEquals(scl.size(), 8);
    assertEquals(scl.nonNullSize(), 3);
    assertEquals(scl.values().size(), scl.nonNullSize());
    Assert.assertFalse(scl.isEmpty());

    // Compute if absent for an already populated index.
    Object newObject = new Object();
    scl.computeIfAbsent(5, k -> newObject);
    Assert.assertNotEquals(scl.get(5), newObject);
    assertEquals(scl.size(), 8);
    assertEquals(scl.nonNullSize(), 3);
    assertEquals(scl.values().size(), scl.nonNullSize());
    Assert.assertFalse(scl.isEmpty());

    // Compute if absent for an unpopulated index.
    scl.computeIfAbsent(4, k -> newObject);
    Assert.assertEquals(scl.get(4), newObject);
    assertEquals(scl.size(), 8);
    assertEquals(scl.nonNullSize(), 4);
    assertEquals(scl.values().size(), scl.nonNullSize());
    Assert.assertFalse(scl.isEmpty());

    // Go back to the initial state...
    scl.clear();
    assertEquals(scl.size(), 0);
    assertEquals(scl.nonNullSize(), 0);
    assertEquals(scl.values().size(), 0);
    Assert.assertTrue(scl.isEmpty());
  }
}
