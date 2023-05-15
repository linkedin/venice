package com.linkedin.venice.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.IntSupplier;


/**
 * Expose an array of {@link E} as an immutable {@link java.util.Collection<E>}
 *
 * Changes to the array are reflected in the collection.
 */
public class ArrayCollection<E> implements Collection<E> {
  private final E[] array;
  private final IntSupplier populatedSizeSupplier;

  public ArrayCollection(E[] array) {
    this.array = array;
    this.populatedSizeSupplier = () -> {
      int populatedSize = 0;
      for (E item: array) {
        if (item != null) {
          populatedSize++;
        }
      }
      return populatedSize;
    };
  }

  /**
   * @param array backing the collection
   * @param populatedSizeSupplier for when the caller has a more efficient way of knowing the populated size of the array
   */
  public ArrayCollection(E[] array, IntSupplier populatedSizeSupplier) {
    this.array = array;
    this.populatedSizeSupplier = populatedSizeSupplier;
  }

  /**
   * @return the number of non-null elements in the backing array.
   */
  @Override
  public int size() {
    return populatedSizeSupplier.getAsInt();
  }

  /**
   * @return true if the backing array is does not contain any non-null elements.
   */
  @Override
  public boolean isEmpty() {
    return populatedSizeSupplier.getAsInt() == 0;
  }

  @Override
  public boolean contains(Object o) {
    if (o == null) {
      return false;
    }
    for (int i = 0; i < array.length; i++) {
      if (o.equals(array[i])) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Iterator<E> iterator() {
    return new ArrayCollectionIterator();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("toArray is not supported.");
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("toArray is not supported.");
  }

  @Override
  public boolean add(E E) {
    throw new UnsupportedOperationException("This collection is immutable.");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("This collection is immutable.");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object item: c) {
      if (!contains(item)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    throw new UnsupportedOperationException("This collection is immutable.");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("This collection is immutable.");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("This collection is immutable.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("This collection is immutable.");
  }

  class ArrayCollectionIterator implements Iterator<E> {
    private int index = 0;

    @Override
    public boolean hasNext() {
      while (this.index < array.length) {
        if (array[this.index] == null) {
          this.index++;
        } else {
          return true;
        }
      }
      return false;
    }

    @Override
    public E next() {
      if (this.index == array.length) {
        throw new NoSuchElementException();
      }
      return array[this.index++];
    }
  }
}
