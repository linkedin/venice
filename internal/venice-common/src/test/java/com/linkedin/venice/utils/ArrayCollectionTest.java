package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.testng.annotations.Test;


public class ArrayCollectionTest {
  @Test
  public void test() {
    Integer[] array = new Integer[10];
    ArrayCollection<Integer> arrayCollection = new ArrayCollection<>(array);

    // Empty
    assertTrue(arrayCollection.isEmpty());
    assertEquals(arrayCollection.size(), 0);
    assertFalse(arrayCollection.contains(1));
    assertFalse(arrayCollection.contains(2));
    assertFalse(arrayCollection.contains(100));
    assertFalse(arrayCollection.contains(null));

    // Add one item in the middle
    array[5] = 1;
    assertFalse(arrayCollection.isEmpty());
    assertEquals(arrayCollection.size(), 1);
    assertTrue(arrayCollection.contains(1));
    assertFalse(arrayCollection.contains(2));
    assertFalse(arrayCollection.contains(100));
    assertFalse(arrayCollection.contains(null));
    int numberOfIteration = 0;
    for (Integer item: arrayCollection) {
      numberOfIteration++;
      assertEquals(item.intValue(), 1);
    }
    assertEquals(numberOfIteration, 1);
    Iterator<Integer> iterator = arrayCollection.iterator();
    Integer firstItem = iterator.next();
    assertEquals(firstItem.intValue(), 1);
    assertThrows(NoSuchElementException.class, () -> iterator.next());

    // Add another before that
    array[2] = 100;
    assertFalse(arrayCollection.isEmpty());
    assertEquals(arrayCollection.size(), 2);
    assertTrue(arrayCollection.contains(1));
    assertFalse(arrayCollection.contains(2));
    assertTrue(arrayCollection.contains(100));
    assertFalse(arrayCollection.contains(null));
    numberOfIteration = 0;
    boolean foundItem1 = false;
    boolean foundItem100 = false;
    for (Integer item: arrayCollection) {
      numberOfIteration++;
      if (item.equals(1)) {
        // Should have seen the other already
        assertTrue(foundItem100);
        foundItem1 = true;
      } else if (item.equals(100)) {
        // Should not have seen this one yet
        assertFalse(foundItem1);
        foundItem100 = true;
      }
    }
    assertEquals(numberOfIteration, 2);
    Iterator<Integer> iterator2 = arrayCollection.iterator();
    firstItem = iterator2.next();
    assertEquals(firstItem.intValue(), 100);
    Integer secondItem = iterator2.next();
    assertEquals(secondItem.intValue(), 1);
    assertThrows(NoSuchElementException.class, () -> iterator2.next());
  }
}
