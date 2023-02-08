package com.linkedin.venice.utils;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;


/**
 * A very simple subclass of {@link SparseConcurrentList} which adds some immutable offset to all index.
 *
 * Useful for cases where the list needs to contain negative indices.
 *
 * A more fancy version where the offset adjusts dynamically based on the indices it needs to contain would be
 * interesting, but more complex. It would likely require starting from scratch, as the facilities provided by
 * {@link java.util.concurrent.CopyOnWriteArrayList} may be insufficient to achieve correct/efficient synchronization.
 */
public class SparseConcurrentListWithOffset<E> extends SparseConcurrentList<E> {
  private static final long serialVersionUID = 2L;

  private final int offset;

  public SparseConcurrentListWithOffset(int offset) {
    this.offset = offset;
  }

  private int offset(int index) {
    return index + offset;
  }

  public synchronized void add(int index, E element) {
    super.add(offset(index), element);
  }

  public synchronized boolean addAll(int index, Collection<? extends E> c) {
    return super.addAll(offset(index), c);
  }

  public E get(int index) {
    return super.get(offset(index));
  }

  public int indexOf(E e, int index) {
    return super.indexOf(e, offset(index));
  }

  public int lastIndexOf(E e, int index) {
    return super.lastIndexOf(e, offset(index));
  }

  public ListIterator<E> listIterator(int index) {
    return super.listIterator(offset(index));
  }

  public E remove(int index) {
    return super.remove(offset(index));
  }

  public synchronized E set(int index, E item) {
    return super.set(offset(index), item);
  }

  public synchronized List<E> subList(int fromIndex, int toIndex) {
    return super.subList(offset(fromIndex), offset(toIndex));
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof SparseConcurrentListWithOffset))
      return false;

    SparseConcurrentListWithOffset<?> list = (SparseConcurrentListWithOffset<?>) o;
    int size = size();
    if (size != list.size()) {
      return false;
    }
    Object thisElement, otherElement;
    for (int i = 0; i < size; i++) {
      thisElement = get(i);
      otherElement = list.get(i);
      if (!Objects.equals(thisElement, otherElement)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
