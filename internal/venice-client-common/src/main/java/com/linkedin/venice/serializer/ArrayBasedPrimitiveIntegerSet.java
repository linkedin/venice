package com.linkedin.venice.serializer;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;


/**
 * This class is not thread-safe; it's optimized for GC by maintaining a primitive int array without boxing,
 * but it's slow in add(), contains(), remove(), etc.
 */
public class ArrayBasedPrimitiveIntegerSet extends AbstractSet<Integer> implements Set<Integer> {
  private static final int[] EMPTY = new int[0];
  private int size;
  private int[] elements = EMPTY;

  public ArrayBasedPrimitiveIntegerSet() {
    super();
  }

  public ArrayBasedPrimitiveIntegerSet(int initialCapacity) {
    elements = new int[initialCapacity];
    size = 0;
  }

  public ArrayBasedPrimitiveIntegerSet(int[] backingArray, int initialSize) {
    if (initialSize > backingArray.length) {
      throw new IllegalArgumentException(
          "initialSize (" + initialSize + ") can not be bigger than the actual size of" + " the backingArray ("
              + backingArray.length + ")");
    }
    elements = backingArray;
    size = initialSize;
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public boolean contains(Object o) {
    if (!(o instanceof Integer)) {
      throw new ClassCastException("Input object is not an integer");
    }
    Integer other = (Integer) o;
    return contains((int) other);
  }

  /**
   * To explicitly use this GC optimized API, declare variable type as ArrayBasedPrimitiveIntegerSet instead of Set.
   */
  public boolean contains(int other) {
    int s = size();
    for (int i = 0; i < s; i++) {
      if (elements[i] == other) {
        return true;
      }
    }
    return false;
  }

  public Iterator<Integer> iterator() {
    return new PrimitiveIntegerSetIterator(this);
  }

  // Modification Operations
  public boolean add(Integer e) {
    if (e == null) {
      throw new NullPointerException();
    }
    return add((int) e);
  }

  /**
   * To explicitly use this GC optimized API, declare variable type as ArrayBasedPrimitiveIntegerSet instead of Set.
   */
  public boolean add(int e) {
    int s = size();
    for (int i = 0; i < s; i++) {
      if (elements[i] == e) {
        return false;
      }
    }
    // new element
    if (size == elements.length) {
      int[] newElements = new int[(size * 3) / 2 + 1];
      System.arraycopy(elements, 0, newElements, 0, size);
      elements = newElements;
    }
    elements[size++] = e;
    return true;
  }

  public boolean remove(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
    if (!(o instanceof Integer)) {
      throw new ClassCastException("Input object is not an integer");
    }
    Integer e = (Integer) o;
    return remove((int) e);
  }

  /**
   * To explicitly use this GC optimized API, declare variable type as ArrayBasedPrimitiveIntegerSet instead of Set.
   */
  public boolean remove(int e) {
    int s = size();
    if (s == 0) {
      return false;
    }
    for (int i = 0; i < s; i++) {
      if (elements[i] == e) {
        // O(1) swap
        elements[i] = elements[s - 1];
        this.size -= 1;
        return true;
      }
    }
    return false;
  }

  /**
   * Removes all of the elements from this set (optional operation).
   * The set will be empty after this call returns.
   *
   * @throws UnsupportedOperationException if the <tt>clear</tt> method
   *         is not supported by this set
   */
  public void clear() {
    // keep the backing array
    this.size = 0;
  }

  // Comparison and hashing

  @Override
  public int hashCode() {
    int hashCode = 1;
    for (int i = 0; i < this.size; i++) {
      hashCode = 31 * hashCode + Integer.hashCode(elements[i]);
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof ArrayBasedPrimitiveIntegerSet)) {
      return false;
    }
    ArrayBasedPrimitiveIntegerSet other = (ArrayBasedPrimitiveIntegerSet) obj;
    return Arrays.equals(other.elements, this.elements) && other.size == this.size;
  }

  private int[] getBackingArray() {
    return this.elements;
  }

  public static class PrimitiveIntegerSetIterator implements Iterator<Integer> {
    final private ArrayBasedPrimitiveIntegerSet set;
    final private int[] backingArray;
    final private int size;
    private int index;
    private int lastReturnedElement;

    public PrimitiveIntegerSetIterator(ArrayBasedPrimitiveIntegerSet set) {
      this.set = set;
      this.backingArray = set.getBackingArray();
      this.size = set.size();
      this.index = 0;
    }

    public boolean hasNext() {
      return index < size;
    }

    public Integer next() {
      return nextPrimitiveInteger();
    }

    public int nextPrimitiveInteger() {
      if (index >= size) {
        throw new NoSuchElementException("No more elements in this iterator");
      }
      lastReturnedElement = backingArray[index++];
      return lastReturnedElement;
    }

    public void remove() {
      if (index == 0) {
        throw new IllegalStateException("next method has not been called");
      }
      set.remove(lastReturnedElement);
    }
  }
}
