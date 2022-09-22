/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.alpini.base.misc;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Based upon Apache Harmony unit test for LinkedList
 */
public class TestDoublyLinkedList {
  DoublyLinkedList<TestEntry> ll;

  static TestEntry[] objArray;

  @BeforeMethod(groups = "unit")
  @SuppressWarnings("unchecked")
  protected void setUp() throws Exception {
    objArray = new TestEntry[100];
    for (int i = 0; i < objArray.length; i++)
      objArray[i] = new TestEntry(i);

    ll = new DoublyLinkedList<TestEntry>();
    for (TestEntry i: objArray) {
      ll.add(i);
    }
  }

  static class TestEntry extends DoublyLinkedList.Entry<TestEntry> {
    final Object value;

    public TestEntry(Object value) {
      this.value = value instanceof TestEntry ? ((TestEntry) value).value : value;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return this == o || value == o || (value != null && value.equals(o)) || o instanceof TestEntry
          && (value == ((TestEntry) o).value || value != null && value.equals(((TestEntry) o).value));
    }

    @Override
    public String toString() {
      return value.toString();
    }

  }

  /**
   * @tests java.util.LinkedList#LinkedList()
   */
  @Test(groups = "unit")
  public void test_Constructor() {
    // Test for method java.util.LinkedList()
    (new Support_ListTest(ll) {
      @Override
      public Object newInteger(int i) {
        return new TestEntry(i);
      }
    }).runTest();

    DoublyLinkedList subList = new DoublyLinkedList();

    for (int i = -50; i < 150; i++)
      subList.add(new TestEntry(i));

    (new Support_ListTest(subList.subList(50, 150)) {
      @Override
      public Object newInteger(int i) {
        return new TestEntry(i);
      }
    }).runTest();
  }

  @Test(groups = "unit")
  public void test_ConstructorLjava_util_Collection() {
    List<TestEntry> src = new ArrayList<TestEntry>();
    for (TestEntry e: ll) {
      src.add(new TestEntry(e.value));
    }

    // Test for method java.util.LinkedList(java.util.Collection)
    assertTrue(new DoublyLinkedList<TestEntry>(src).equals(ll), "Incorrect LinkedList constructed");
  }

  /**
   * @tests java.util.LinkedList#add(int, java.lang.Object)
   */
  @Test(groups = "unit")
  public void test_addILjava_lang_Object() {
    // Test for method void java.util.LinkedList.add(int, java.lang.Object)
    TestEntry o;
    ll.add(50, o = new TestEntry("Test"));
    assertTrue(ll.get(50) == o, "Failed to add Object>: " + ll.get(50).toString());
    assertTrue(ll.get(51) == objArray[50] && (ll.get(52) == objArray[51]), "Failed to fix up list after insert");
    ll.add(50, new TestEntry(null));
    assertTrue(ll.get(50).equals(null), "Did not add null correctly");

    try {
      ll.add(-1, new TestEntry("Test"));
      fail("Should throw IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // Excepted
    }

    try {
      ll.add(-1, new TestEntry(null));
      fail("Should throw IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // Excepted
    }

    try {
      ll.add(ll.size() + 1, new TestEntry("Test"));
      fail("Should throw IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // Excepted
    }

    try {
      ll.add(ll.size() + 1, null);
      fail("Should throw IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // Excepted
    }
  }

  /**
   * @tests java.util.LinkedList#add(java.lang.Object)
   */
  @Test(groups = "unit")
  public void test_addLjava_lang_Object() {
    // Test for method boolean java.util.LinkedList.add(java.lang.Object)
    TestEntry o;
    ll.add(o = new TestEntry(new Object()));
    assertTrue(ll.getLast() == o, "Failed to add Object");
    // ll.add(TestEntry.instanceOf(null));
    // assertNull(ll.get(ll.size() - 1), "Did not add null correctly");
  }

  /**
   * @tests java.util.LinkedList#addAll(int, java.util.Collection)
   */
  @Test(groups = "unit")
  public void test_addAllILjava_util_Collection_2() {
    // Regression for HARMONY-467
    DoublyLinkedList obj = new DoublyLinkedList();
    try {
      obj.addAll(-1, (Collection) null);
      fail("IndexOutOfBoundsException expected");
    } catch (IndexOutOfBoundsException e) {
    }
  }

  /**
   * @tests java.util.LinkedList#addFirst(java.lang.Object)
   */
  @Test(groups = "unit")
  public void test_addFirstLjava_lang_Object() {
    // Test for method void java.util.LinkedList.addFirst(java.lang.Object)
    TestEntry o;
    ll.addFirst(o = new TestEntry(new Object()));
    assertTrue(ll.getFirst() == o, "Failed to add Object");
    // ll.addFirst(null);
    // assertNull("Failed to add null", ll.getFirst());
  }

  /**
   * @tests java.util.LinkedList#addFirst(java.lang.Object)
   */
  @Test(groups = "unit")
  public void testPush() {
    // Test for method void java.util.LinkedList.addFirst(java.lang.Object)
    TestEntry o;
    ll.push(o = new TestEntry(new Object()));
    assertTrue(ll.getFirst() == o, "Failed to add Object");
  }

  /**
   * @tests java.util.LinkedList#addLast(java.lang.Object)
   */
  @Test(groups = "unit")
  public void test_addLastLjava_lang_Object() {
    // Test for method void java.util.LinkedList.addLast(java.lang.Object)
    TestEntry o;
    ll.addLast(o = new TestEntry(new Object()));
    assertTrue(ll.getLast() == o, "Failed to add Object");
    // ll.addLast(null);
    // assertNull("Failed to add null", ll.getLast());
  }

  /**
   * @tests java.util.LinkedList#clear()
   */
  @Test(groups = "unit")
  public void test_clear() {
    // Test for method void java.util.LinkedList.clear()
    ll.clear();
    for (int i = 0; i < ll.size(); i++)
      assertNull(ll.get(i), "Failed to clear list");
  }

  /**
   * @tests java.util.LinkedList#contains(java.lang.Object)
   */
  @Test(groups = "unit")
  public void test_containsLjava_lang_Object() {
    // Test for method boolean
    // java.util.LinkedList.contains(java.lang.Object)
    assertTrue(ll.contains(objArray[99]), "Returned false for valid element");
    assertTrue(ll.contains(new TestEntry(8)), "Returned false for equal element");
    assertTrue(!ll.contains(new Object()), "Returned true for invalid element");
    assertTrue(!ll.contains(null), "Should not contain null");
    ll.add(25, new TestEntry(null));
    assertTrue(ll.contains(new TestEntry(null)), "Should contain null");
  }

  /**
   * @tests java.util.LinkedList#get(int)
   */
  @Test(groups = "unit")
  public void test_getI() {
    // Test for method java.lang.Object java.util.LinkedList.get(int)
    // assertTrue(ll.get(22) == objArray[22], "Returned incorrect element");
    assertEquals(ll.get(22), objArray[22], "Returned incorrect element");
    try {
      ll.get(8765);
      fail("Failed to throw expected exception for index > size");
    } catch (IndexOutOfBoundsException e) {
    }
  }

  /**
   * @tests {@link java.util.LinkedList#peek()}
   */
  @Test(groups = "unit")
  public void test_peek() {
    DoublyLinkedList list = new DoublyLinkedList();

    assertNull(list.peek(), "Should return null if this list is empty");

    assertEquals(ll.peek(), objArray[0], "Returned incorrect first element");
    assertEquals(ll.getFirst(), objArray[0], "Peek remove the head (first element) of this list");
  }

  /**
   * @tests java.util.LinkedList#getFirst()
   */
  @Test(groups = "unit")
  public void test_getFirst() {
    // Test for method java.lang.Object java.util.LinkedList.getFirst()
    assertTrue(ll.getFirst().equals(objArray[0]), "Returned incorrect first element");

    DoublyLinkedList list = new DoublyLinkedList();
    try {
      list.getFirst();
      fail("Should throw NoSuchElementException");
    } catch (NoSuchElementException e) {
      // Excepted
    }
  }

  /**
   * @tests java.util.LinkedList#getLast()
   */
  @Test(groups = "unit")
  public void test_getLast() {
    // Test for method java.lang.Object java.util.LinkedList.getLast()
    assertTrue(ll.getLast().equals(objArray[objArray.length - 1]), "Returned incorrect first element");

    DoublyLinkedList list = new DoublyLinkedList();
    try {
      list.getLast();
      fail("Should throw NoSuchElementException");
    } catch (NoSuchElementException e) {
      // Excepted
    }
  }

  /**
   * @tests java.util.LinkedList#indexOf(java.lang.Object)
   */
  @Test(groups = "unit")
  public void test_indexOfLjava_lang_Object() {
    // Test for method int java.util.LinkedList.indexOf(java.lang.Object)
    assertEquals(87, ll.indexOf(objArray[87]), "Returned incorrect index");
    assertEquals(-1, ll.indexOf(new Object()), "Returned index for invalid Object");
    ll.add(20, new TestEntry(null));
    ll.add(24, new TestEntry(null));
    assertTrue(
        ll.indexOf(new TestEntry(null)) == 20,
        "Index of null should be 20, but got: " + ll.indexOf(new TestEntry(null)));
  }

  /**
   * @tests java.util.LinkedList#lastIndexOf(java.lang.Object)
   */
  @Test(groups = "unit")
  public void test_lastIndexOfLjava_lang_Object() {
    // Test for method int
    // java.util.LinkedList.lastIndexOf(java.lang.Object)
    ll.add(new TestEntry(99));
    assertEquals(100, ll.lastIndexOf(objArray[99]), "Returned incorrect index");
    assertEquals(-1, ll.lastIndexOf(new Object()), "Returned index for invalid Object");
    ll.add(20, new TestEntry(null));
    ll.add(24, new TestEntry(null));
    assertTrue(
        ll.lastIndexOf(new TestEntry(null)) == 24,
        "Last index of null should be 20, but got: " + ll.lastIndexOf(new TestEntry(null)));
  }

  /**
   * @tests java.util.LinkedList#listIterator(int)
   */
  @Test(groups = "unit")
  public void test_listIteratorI() {
    // Test for method java.util.ListIterator
    // java.util.LinkedList.listIterator(int)
    ListIterator i = ll.listIterator();
    Object elm;
    int n = 0;
    while (i.hasNext()) {
      if (n == 0 || n == objArray.length - 1) {
        if (n == 0)
          assertTrue(!i.hasPrevious(), "First element claimed to have a previous");
        if (n == objArray.length)
          assertTrue(!i.hasNext(), "Last element claimed to have next");
      }
      elm = i.next();
      assertTrue(elm == objArray[n], "Iterator returned elements in wrong order");
      if (n > 0 && n < objArray.length - 1) {
        assertTrue(i.nextIndex() == n + 1, "Next index returned incorrect value");
        assertTrue(
            i.previousIndex() == n,
            "previousIndex returned incorrect value : " + i.previousIndex() + ", n val: " + n);
      }
      ++n;
    }
    List myList = new DoublyLinkedList();
    myList.add(new TestEntry(null));
    myList.add(new TestEntry("Blah"));
    myList.add(new TestEntry(null));
    myList.add(new TestEntry("Booga"));
    myList.add(new TestEntry(null));
    ListIterator li = myList.listIterator();
    assertTrue(!li.hasPrevious(), "li.hasPrevious() should be false");
    assertTrue(li.next().equals(null), "li.next() should be null");
    assertTrue(li.hasPrevious(), "li.hasPrevious() should be true");
    assertTrue(li.previous().equals(null), "li.prev() should be null");
    assertTrue(li.next().equals(null), "li.next() should be null");
    assertEquals("Blah", String.valueOf(li.next()), "li.next() should be Blah");
    assertTrue(li.next().equals(null), "li.next() should be null");
    assertEquals("Booga", String.valueOf(li.next()), "li.next() should be Booga");
    assertTrue(li.hasNext(), "li.hasNext() should be true");
    assertTrue(li.next().equals(null), "li.next() should be null");
    assertTrue(!li.hasNext(), "li.hasNext() should be false");
  }

  /**
   * @tests java.util.LinkedList#remove(int)
   */
  @Test(groups = "unit")
  public void test_removeI() {
    // Test for method java.lang.Object java.util.LinkedList.remove(int)
    ll.remove(10);
    assertEquals(-1, ll.indexOf(objArray[10]), "Failed to remove element");
    try {
      ll.remove(999);
      fail("Failed to throw expected exception when index out of range");
    } catch (IndexOutOfBoundsException e) {
      // Correct
    }

    ll.add(20, new TestEntry(null));
    ll.remove(20);
    assertNotNull(ll.get(20), "Should have removed null");
  }

  /**
   * @tests java.util.LinkedList#remove(java.lang.Object)
   */
  @Test(groups = "unit")
  public void test_removeLjava_lang_Object() {
    // Test for method boolean java.util.LinkedList.remove(java.lang.Object)
    assertTrue(ll.remove(objArray[87]), "Failed to remove valid Object");
    assertTrue(!ll.remove(new Object()), "Removed invalid object");
    assertEquals(-1, ll.indexOf(objArray[87]), "Found Object after removal");
    // ll.add(null);
    // ll.remove(null);
    // assertTrue(!ll.contains(null), "Should not contain null afrer removal");
  }

  /**
   * @tests java.util.LinkedList#removeFirst()
   */
  @Test(groups = "unit")
  public void test_removeFirst() {
    // Test for method java.lang.Object java.util.LinkedList.removeFirst()
    ll.removeFirst();
    assertTrue(ll.getFirst() != objArray[0], "Failed to remove first element");

    DoublyLinkedList list = new DoublyLinkedList();
    try {
      list.removeFirst();
      fail("Should throw NoSuchElementException");
    } catch (NoSuchElementException e) {
      // Excepted
    }
  }

  @Test(groups = "unit")
  public void testPop() {
    // Test for method java.lang.Object java.util.LinkedList.removeFirst()
    ll.pop();
    assertTrue(ll.getFirst() != objArray[0], "Failed to remove first element");

    DoublyLinkedList list = new DoublyLinkedList();
    try {
      list.pop();
      fail("Should throw NoSuchElementException");
    } catch (NoSuchElementException e) {
      // Excepted
    }
  }

  /**
   * @tests java.util.LinkedList#removeLast()
   */
  @Test(groups = "unit")
  public void test_removeLast() {
    // Test for method java.lang.Object java.util.LinkedList.removeLast()
    ll.removeLast();
    assertTrue(ll.getLast() != objArray[objArray.length - 1], "Failed to remove last element");

    DoublyLinkedList list = new DoublyLinkedList();
    try {
      list.removeLast();
      fail("Should throw NoSuchElementException");
    } catch (NoSuchElementException e) {
      // Excepted
    }
  }

  /**
   * @tests java.util.LinkedList#set(int, java.lang.Object)
   */
  @Test(groups = "unit")
  public void test_setILjava_lang_Object() {
    // Test for method java.lang.Object java.util.LinkedList.set(int,
    // java.lang.Object)
    TestEntry obj;
    ll.set(65, obj = new TestEntry(new Object()));
    assertTrue(ll.get(65) == obj, "Failed to set object");
  }

  /**
   * @tests java.util.LinkedList#size()
   */
  @Test(groups = "unit")
  public void test_size() {
    // Test for method int java.util.LinkedList.size()
    assertTrue(ll.size() == objArray.length, "Returned incorrect size");
    ll.removeFirst();
    assertTrue(ll.size() == objArray.length - 1, "Returned incorrect size");
  }

  /**
   * @tests java.util.LinkedList#toArray()
   */
  @Test(groups = "unit")
  public void test_toArray() {
    // Test for method java.lang.Object [] java.util.LinkedList.toArray()
    ll.add(new TestEntry(null));
    Object[] obj = ll.toArray();
    assertEquals(objArray.length + 1, obj.length, "Returned array of incorrect size");

    for (int i = 0; i < obj.length - 1; i++)
      assertTrue(obj[i] == objArray[i], "Returned incorrect array: " + i);
    assertTrue(obj[obj.length - 1].equals(null), "Returned incorrect array--end isn't null");
  }

  /**
   * @tests java.util.LinkedList#toArray(java.lang.Object[])
   */
  @Test(groups = "unit")
  public void test_toArray$Ljava_lang_Object() {
    // Test for method java.lang.Object []
    // java.util.LinkedList.toArray(java.lang.Object [])
    Object[] argArray = new Object[100];
    Object[] retArray;
    retArray = ll.toArray(argArray);
    assertTrue(retArray == argArray, "Returned different array than passed");
    for (int i = 0; i < retArray.length; i++)
      retArray[i] = new TestEntry(retArray[i]);
    List retList = new DoublyLinkedList(Arrays.asList(retArray));
    Iterator li = ll.iterator();
    Iterator ri = retList.iterator();
    while (li.hasNext())
      assertEquals(li.next(), ri.next(), "Lists are not equal");
    argArray = new Object[1000];
    retArray = ll.toArray(argArray);
    assertNull(argArray[ll.size()], "Failed to set first extra element to null");
    for (int i = 0; i < ll.size(); i++)
      assertTrue(retArray[i] == objArray[i], "Returned incorrect array: " + i);
    ll.add(50, new TestEntry(null));
    argArray = new Object[101];
    retArray = ll.toArray(argArray);
    assertTrue(retArray == argArray, "Returned different array than passed");
    retArray = ll.toArray(argArray);
    assertTrue(retArray == argArray, "Returned different array than passed");
    for (int i = 0; i < retArray.length; i++)
      retArray[i] = new TestEntry(retArray[i]);
    List srcList = Arrays.asList(retArray);
    retList = new DoublyLinkedList(srcList);
    li = srcList.iterator();
    ri = retList.iterator();
    while (li.hasNext())
      assertEquals(li.next(), ri.next(), "Lists are not equal");
  }

  @Test(groups = "unit")
  public void test_offer() {
    int origSize = ll.size();
    TestEntry e = new TestEntry(objArray[0]);
    assertTrue(ll.offer(e), "offer() should return true'");
    assertEquals(origSize, ll.lastIndexOf(e), "offer() should add an element as the last one");
  }

  @Test(groups = "unit")
  public void test_poll() {
    assertEquals(objArray[0], ll.poll(), "should return the head");
    DoublyLinkedList<TestEntry> list = new DoublyLinkedList<TestEntry>();
    assertNull(list.poll(), "should return 'null' if list is empty");
  }

  @Test(groups = "unit")
  public void testPollLast() {
    assertSame(ll.pollLast(), objArray[99], "should return the tail");
    DoublyLinkedList<TestEntry> list = new DoublyLinkedList<TestEntry>();
    assertNull(list.pollLast(), "should return 'null' if list is empty");
  }

  @Test(groups = "unit")
  public void testPeekLast() {
    assertSame(ll.peekLast(), objArray[99], "should return the tail");
    DoublyLinkedList<TestEntry> list = new DoublyLinkedList<TestEntry>();
    assertNull(list.peekLast(), "should return 'null' if list is empty");
  }

  @Test(groups = "unit")
  public void test_remove() {
    for (int i = 0; i < objArray.length; i++) {
      assertEquals(objArray[i], ll.remove(), "should remove the head");
    }
    assertEquals(0, ll.size(), "should be empty");
    try {
      ll.remove();
      fail("NoSuchElementException is expected when removing from the empty list");
    } catch (NoSuchElementException e) {
      // -- expected
    }
  }

  @Test(groups = "unit")
  public void test_element() {
    assertEquals(objArray[0], ll.element(), "ll.element() should return the head");
    assertEquals(objArray.length, ll.size(), "element() should remove nothing");
    try {
      new DoublyLinkedList().remove();
      fail("NoSuchElementException is expected when the list is empty");
    } catch (NoSuchElementException e) {
      // -- expected
    }
  }

  @Test(groups = "unit")
  public void testDecendingIterator() {
    Iterator<TestEntry> it = ll.descendingIterator();
    for (int i = objArray.length - 1; i >= 0; i--) {
      assertTrue(it.hasNext());
      assertSame(it.next(), objArray[i]);
      it.remove();
    }
    assertFalse(it.hasNext());
    assertEquals(ll.size(), 0, "should be empty");
  }

  @Test(groups = "unit")
  public void testRemoveFirstOccurence() {
    assertFalse(ll.removeFirstOccurrence(new TestEntry(new Object())));
    for (int i = objArray.length - 1; i >= 0; i--) {
      assertTrue(ll.removeFirstOccurrence(i));
    }
    assertEquals(0, ll.size(), "should be empty");
    assertFalse(ll.removeFirstOccurrence(50));
  }

  @Test(groups = "unit")
  public void testRemoveLastOccurence() {
    assertFalse(ll.removeLastOccurrence(new TestEntry(new Object())));
    for (int i = 0; i < objArray.length; i++) {
      assertTrue(ll.removeLastOccurrence(i));
    }
    assertEquals(0, ll.size(), "should be empty");
    assertFalse(ll.removeLastOccurrence(50));
  }

  @Test(groups = "unit")
  public void testIteratorSet() {
    DoublyLinkedList<TestEntry> foo = new DoublyLinkedList<>();
    foo.addFirst(new TestEntry("other"));

    ListIterator<TestEntry> it = ll.listIterator(20);
    assertSame(it.next(), objArray[20]);
    it.set(objArray[99]);
    assertSame(it.next(), objArray[21]);
    assertEquals(it.nextIndex(), 22);
    assertEquals(ll.size(), objArray.length - 1);
    it.set(objArray[0]);
    assertSame(it.previous(), objArray[99]);
    assertEquals(it.previousIndex(), 18);
    assertEquals(ll.size(), objArray.length - 2);

    it.add(objArray[20]);
    assertEquals(it.nextIndex(), 20);
    assertEquals(ll.size(), objArray.length - 1);

    it.add(objArray[21]);
    assertEquals(it.nextIndex(), 21);
    assertEquals(ll.size(), objArray.length);

    assertSame(it.next(), objArray[99]);
    try {
      it.add(foo.peekFirst());
      fail();
    } catch (IllegalArgumentException ex) {
      // expected
    }
    try {
      it.set(foo.peekFirst());
      fail();
    } catch (IllegalStateException ex) {
      // expected
    }
    assertEquals(it.nextIndex(), 22);
    assertEquals(ll.size(), objArray.length);
  }
}
