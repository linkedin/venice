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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.LinkedList;
import java.util.ListIterator;
import java.util.NoSuchElementException;


/**
 * Based upon Apache Harmony unit test for LinkedList
 */
public class Support_ListTest<List extends java.util.List> {
  List list; // must contain the Integers 0 to 99 in order

  public Support_ListTest() {
  }

  public Support_ListTest(List l) {
    list = l;
  }

  public Object newInteger(int i) {
    return i;
  }

  public void runTest() {
    int hashCode = 1;
    for (int counter = 0; counter < 100; counter++) {
      Object elem;
      elem = list.get(counter);
      hashCode = 31 * hashCode + elem.hashCode();
      assertTrue(elem.equals(newInteger(counter)), "ListTest - get failed");
    }
    assertTrue(hashCode == list.hashCode(), "ListTest - hashCode failed");

    list.add(50, newInteger(1000));
    assertTrue(list.get(50).equals(newInteger(1000)), "ListTest - a) add with index failed--did not insert");
    assertTrue(
        list.get(51).equals(newInteger(50)),
        "ListTest - b) add with index failed--did not move following elements");
    assertTrue(list.get(49).equals(newInteger(49)), "ListTest - c) add with index failed--affected previous elements");

    list.set(50, newInteger(2000));
    assertTrue(list.get(50).equals(newInteger(2000)), "ListTest - a) set failed--did not set");
    assertTrue(list.get(51).equals(newInteger(50)), "ListTest - b) set failed--affected following elements");
    assertTrue(list.get(49).equals(newInteger(49)), "ListTest - c) set failed--affected previous elements");

    list.remove(50);
    assertTrue(list.get(50).equals(newInteger(50)), "ListTest - a) remove with index failed--did not remove");
    assertTrue(
        list.get(51).equals(newInteger(51)),
        "ListTest - b) remove with index failed--did not move following elements");
    assertTrue(
        list.get(49).equals(newInteger(49)),
        "ListTest - c) remove with index failed--affected previous elements");

    java.util.List myList = new LinkedList();
    myList.add(newInteger(500));
    myList.add(newInteger(501));
    myList.add(newInteger(502));

    list.addAll(50, myList);
    assertTrue(list.get(50).equals(newInteger(500)), "ListTest - a) addAll with index failed--did not insert");
    assertTrue(list.get(51).equals(newInteger(501)), "ListTest - b) addAll with index failed--did not insert");
    assertTrue(list.get(52).equals(newInteger(502)), "ListTest - c) addAll with index failed--did not insert");
    assertTrue(
        list.get(53).equals(newInteger(50)),
        "ListTest - d) addAll with index failed--did not move following elements");
    assertTrue(
        list.get(49).equals(newInteger(49)),
        "ListTest - e) addAll with index failed--affected previous elements");

    java.util.List mySubList = list.subList(50, 53);
    assertEquals(3, mySubList.size());
    assertTrue(
        mySubList.get(0).equals(newInteger(500)),
        "ListTest - a) sublist Failed--does not contain correct elements");
    assertTrue(
        mySubList.get(1).equals(newInteger(501)),
        "ListTest - b) sublist Failed--does not contain correct elements");
    assertTrue(
        mySubList.get(2).equals(newInteger(502)),
        "ListTest - c) sublist Failed--does not contain correct elements");

    t_listIterator(mySubList);

    mySubList.clear();
    assertEquals(
        100,
        list.size(),
        "ListTest - Clearing the sublist did not remove the appropriate elements from the original list");

    t_listIterator(list);
    ListIterator li = list.listIterator();
    for (int counter = 0; li.hasNext(); counter++) {
      Object elem;
      elem = li.next();
      assertTrue(elem.equals(newInteger(counter)), "ListTest - listIterator failed");
    }

    // new Support_CollectionTest(list).runTest();

  }

  public void t_listIterator(java.util.List list) {
    ListIterator li = list.listIterator(1);
    assertTrue(li.next() == list.get(1), "listIterator(1)");

    int orgSize = list.size();
    li = list.listIterator();
    for (int i = 0; i <= orgSize; i++) {
      if (i == 0) {
        assertTrue(!li.hasPrevious(), "list iterator hasPrevious(): " + i);
      } else {
        assertTrue(li.hasPrevious(), "list iterator hasPrevious(): " + i);
      }
      if (i == list.size()) {
        assertTrue(!li.hasNext(), "list iterator hasNext(): " + i);
      } else {
        assertTrue(li.hasNext(), "list iterator hasNext(): " + i);
      }
      assertTrue(li.nextIndex() == i, "list iterator nextIndex(): " + i);
      assertTrue(li.previousIndex() == i - 1, "list iterator previousIndex(): " + i);
      boolean exception = false;
      try {
        assertTrue(li.next() == list.get(i), "list iterator next(): " + i);
      } catch (NoSuchElementException e) {
        exception = true;
      }
      if (i == list.size()) {
        assertTrue(exception, "list iterator next() exception: " + i);
      } else {
        assertTrue(!exception, "list iterator next() exception: " + i);
      }
    }

    for (int i = orgSize - 1; i >= 0; i--) {
      assertTrue(li.previous() == list.get(i), "list iterator previous(): " + i);
      assertTrue(li.nextIndex() == i, "list iterator nextIndex()2: " + i);
      assertTrue(li.previousIndex() == i - 1, "list iterator previousIndex()2: " + i);
      if (i == 0) {
        assertTrue(!li.hasPrevious(), "list iterator hasPrevious()2: " + i);
      } else {
        assertTrue(li.hasPrevious(), "list iterator hasPrevious()2: " + i);
      }
      assertTrue(li.hasNext(), "list iterator hasNext()2: " + i);
    }
    boolean exception = false;
    try {
      li.previous();
    } catch (NoSuchElementException e) {
      exception = true;
    }
    assertTrue(exception, "list iterator previous() exception");

    Object add1 = newInteger(600);
    Object add2 = newInteger(601);
    li.add(add1);
    assertTrue(list.size() == (orgSize + 1), "list iterator add(), size()");
    assertEquals(1, li.nextIndex(), "list iterator add(), nextIndex()");
    assertEquals(0, li.previousIndex(), "list iterator add(), previousIndex()");
    Object next = li.next();
    assertTrue(next == list.get(1), "list iterator add(), next(): " + next);
    li.add(add2);
    Object previous = li.previous();
    assertTrue(previous == add2, "list iterator add(), previous(): " + previous);
    assertEquals(2, li.nextIndex(), "list iterator add(), nextIndex()2");
    assertEquals(1, li.previousIndex(), "list iterator add(), previousIndex()2");

    li.remove();
    assertTrue(list.size() == (orgSize + 1), "list iterator remove(), size()");
    assertEquals(2, li.nextIndex(), "list iterator remove(), nextIndex()");
    assertEquals(1, li.previousIndex(), "list iterator remove(), previousIndex()");
    assertTrue(li.previous() == list.get(1), "list iterator previous()2");
    assertTrue(li.previous() == list.get(0), "list iterator previous()3");
    assertTrue(li.next() == list.get(0), "list iterator next()2");
    li.remove();
    assertTrue(!li.hasPrevious(), "list iterator hasPrevious()3");
    assertTrue(li.hasNext(), "list iterator hasNext()3");
    assertTrue(list.size() == orgSize, "list iterator size()");
    assertEquals(0, li.nextIndex(), "list iterator nextIndex()3");
    assertEquals(-1, li.previousIndex(), "list iterator previousIndex()3");
  }
}
