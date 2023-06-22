package com.linkedin.venice.utils.collections;

import java.util.Collection;
import java.util.Iterator;
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
    if (array.length < populatedSizeSupplier.getAsInt()) {
      throw new IllegalArgumentException(
          "The populatedSizeSupplier cannot return a larger result than the array's length");
    }

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
    return new ArrayBackedNullSkippingIterator<>(this.array);
  }

  @Override
  public Object[] toArray() {
    int size = size();
    Object[] arrayToReturn = new Object[size];
    Iterator<E> it = iterator();
    for (int i = 0; i < size; i++) {
      if (!it.hasNext()) {
        throw new IllegalStateException("The iterator is not in sync with the size...");
      }
      arrayToReturn[i] = it.next();
    }
    return arrayToReturn;
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

  @Override
  public String toString() {
    Iterator<E> it = iterator();
    if (!it.hasNext())
      return "[]";

    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (;;) {
      E e = it.next();
      sb.append(e == this ? "(this Collection)" : e);
      if (!it.hasNext())
        return sb.append(']').toString();
      sb.append(',').append(' ');
    }
  }
}
