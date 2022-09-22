package com.linkedin.alpini.base.concurrency;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * A thread-safe {@link SortedSet} implementation which uses an array as a backing store.
 * Mutations to the backing store are guarded by a read-write lock.
 *
 * The purpose of this is to be a more garbage-collector friendly collection than ConcurrentSkipListSet at the cost
 * of slightly more CPU cost at insert and deletion.
 *
 * When used as a backing store for AbstractQuantileEstimation, 5 million samples per second can be performed at the
 * average invocation time of 3 microseconds.
 *
 * @param <E>
 */
public class ArraySortedSet<E> extends AbstractSet<E> implements SortedSet<E>, Cloneable {
  private final Comparator<Object> _comparator;
  private final Comparator<Object> _comparatorNullsFirst;
  private final Comparator<Object> _comparatorNullsLast;

  private /*final*/ ReadWriteLock _lock = new ReentrantReadWriteLock();
  private Object[] _objects;
  private int _size;

  private /*final*/ View _view = new View(null, null);

  public ArraySortedSet(Comparator<? super E> comparator, int initialSize) {
    _comparator = comparator != null ? (Comparator) comparator : (Comparator) Comparator.naturalOrder();
    _comparatorNullsFirst = Comparator.nullsFirst(_comparator);
    _comparatorNullsLast = Comparator.nullsLast(_comparator);
    _objects = new Object[Math.max(16, initialSize)];
  }

  private ArraySortedSet<E> fixup() {
    _lock = new ReentrantReadWriteLock();
    _view = new View(null, null);
    return this;
  }

  public ArraySortedSet<E> clone() {
    try {
      @SuppressWarnings("unchecked")
      ArraySortedSet<E> clone = ((ArraySortedSet<E>) super.clone()).fixup();

      Lock lock = _lock.readLock();
      lock.lock();
      try {
        clone._objects = _objects.clone();
        clone._size = _size;
        return clone;
      } finally {
        lock.unlock();
      }
    } catch (CloneNotSupportedException e) {
      throw new Error(e);
    }
  }

  @SuppressWarnings("unchecked")
  private E at(int index) {
    return (E) _objects[index];
  }

  @Nonnull
  @Override
  public Iterator<E> iterator() {
    return _view.iterator();
  }

  @Override
  public void forEach(Consumer<? super E> action) {
    _view.forEach(action);
  }

  @Override
  public int size() {
    return _size;
  }

  @Override
  public boolean isEmpty() {
    return _size == 0;
  }

  @Override
  public boolean contains(Object o) {
    return _view.contains(o);
  }

  @Nonnull
  @Override
  public Object[] toArray() {
    Lock lock = _lock.readLock();
    lock.lock();
    try {
      return Arrays.copyOf(_objects, _size, Object[].class);
    } finally {
      lock.unlock();
    }
  }

  @Nonnull
  @Override
  public <T> T[] toArray(@Nonnull T[] a) {
    Lock lock = _lock.readLock();
    lock.lock();
    try {
      System.arraycopy(_objects, 0, a, 0, Math.min(a.length, _size));
      if (a.length < _size) {
        int oldSize = a.length;
        a = Arrays.copyOf(a, _size);
        System.arraycopy(_objects, oldSize, a, oldSize, _size - oldSize);
      }
      if (a.length > _size) {
        return Arrays.copyOf(a, _size);
      }
      return a;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean add(E e) {
    return _view.add(e);
  }

  @Override
  public boolean remove(Object o) {
    return _view.remove(o);
  }

  @Override
  public boolean containsAll(@Nonnull Collection<?> c) {
    return _view.containsAll(c);
  }

  @Override
  public boolean addAll(@Nonnull Collection<? extends E> c) {
    return _view.addAll(c);
  }

  @Override
  public Comparator<? super E> comparator() {
    // noinspection rawtypes
    return (Comparator) Comparator.naturalOrder() != _comparator ? _comparator : null;
  }

  @Nonnull
  @Override
  public SortedSet<E> subSet(E fromElement, E toElement) {
    return _view.subSet(fromElement, toElement);
  }

  @Nonnull
  @Override
  public SortedSet<E> headSet(E toElement) {
    return _view.headSet(toElement);
  }

  @Nonnull
  @Override
  public SortedSet<E> tailSet(E fromElement) {
    return _view.tailSet(fromElement);
  }

  @Override
  public E first() {
    return _view.first();
  }

  @Override
  public E last() {
    return _view.last();
  }

  @Override
  public Spliterator<E> spliterator() {
    return _view.spliterator();
  }

  @Override
  public boolean equals(Object o) {
    return this == o || _view.equals(o);
  }

  @Override
  public int hashCode() {
    return _view.hashCode();
  }

  @Override
  public boolean removeAll(@Nonnull Collection<?> c) {
    return _view.removeAll(c);
  }

  @Override
  public boolean retainAll(@Nonnull Collection<?> c) {
    return _view.retainAll(c);
  }

  @Override
  public void clear() {
    Lock lock = _lock.writeLock();
    lock.lock();
    try {
      Arrays.fill(_objects, null);
      _size = 0;
    } finally {
      lock.unlock();
    }
  }

  public E floor(E v) {
    return _view.floor(v);
  }

  private class View extends AbstractSet<E> implements SortedSet<E> {
    private final E _fromElement;
    private final E _toElement;

    private View(E fromElement, E toElement) {
      _fromElement = fromElement;
      _toElement = toElement;
    }

    @Override
    public Comparator<? super E> comparator() {
      return ArraySortedSet.this.comparator();
    }

    @Override
    public boolean contains(Object o) {
      try {
        int cmpFromElement = _comparatorNullsFirst.compare(_fromElement, o);
        int cmpToElement = _comparatorNullsLast.compare(_toElement, o);

        if (cmpFromElement <= 0 && cmpToElement > 0) {
          Lock lock = _lock.readLock();
          lock.lock();
          try {
            return Arrays.binarySearch(_objects, 0, _size, o, _comparator) >= 0;
          } finally {
            lock.unlock();
          }
        } else {
          return false;
        }
      } catch (ClassCastException cse) {
        return false;
      }
    }

    public E floor(E v) {
      return apply(_lock.readLock(), (startIndex, endIndex) -> {
        int index = Arrays.binarySearch(_objects, startIndex, endIndex, v, _comparator);
        if (index >= 0) {
          return at(index);
        }

        index = Math.negateExact(index) - 1;
        if (index >= startIndex && index < endIndex) {
          return at(index);
        }

        return null;
      });
    }

    @Nonnull
    @Override
    public Object[] toArray() {
      return apply(
          _lock.readLock(),
          (startIndex, endIndex) -> Arrays.copyOfRange(_objects, startIndex, endIndex, Object[].class));
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] a) {
      return apply(_lock.readLock(), (startIndex, endIndex) -> {
        final Object[] arr = a;
        System.arraycopy(_objects, startIndex, arr, 0, Math.min(a.length, endIndex - startIndex));
        if (a.length < endIndex - startIndex) {
          T[] result = Arrays.copyOf(a, endIndex - startIndex);
          Object[] res = result;
          System.arraycopy(_objects, startIndex + arr.length, res, arr.length, endIndex - startIndex - arr.length);
          return result;
        }
        if (a.length > endIndex - startIndex) {
          return Arrays.copyOf(a, endIndex - startIndex);
        }
        return a;
      });
    }

    @Override
    public boolean add(E e) {
      int cmpFromElement = _comparatorNullsFirst.compare(_fromElement, e);
      int cmpToElement = _comparatorNullsLast.compare(_toElement, e);

      if (cmpFromElement <= 0 && cmpToElement > 0) {
        Lock lock = _lock.writeLock();
        lock.lock();
        try {
          int index = Arrays.binarySearch(_objects, 0, _size, e, _comparator);

          if (index < 0) {
            index = Math.negateExact(index) - 1; // index now equals insert position

            Object[] objects;

            if (_size + 1 > _objects.length) {
              objects = Arrays.copyOf(_objects, _size + Math.max(32, _size / 4));
            } else {
              objects = _objects;
            }

            System.arraycopy(objects, index, objects, index + 1, _size - index);
            objects[index] = e;

            _objects = objects;
            _size++;
            return true;
          }
        } finally {
          lock.unlock();
        }
      }
      return false;
    }

    @Override
    public boolean remove(Object o) {
      int cmpFromElement = _comparatorNullsFirst.compare(_fromElement, o);
      int cmpToElement = _comparatorNullsLast.compare(_toElement, o);

      if (cmpFromElement <= 0 && cmpToElement > 0) {
        Lock lock = _lock.writeLock();
        lock.lock();
        try {
          int index = Arrays.binarySearch(_objects, 0, _size, o, _comparator);

          if (index >= 0) {
            System.arraycopy(_objects, index + 1, _objects, index, _size - index);
            _objects[_size - 1] = null;
            _size--;
            return true;
          }
        } finally {
          lock.unlock();
        }
      }
      return false;
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
      try {
        if (_fromElement != null || _toElement != null) {
          for (Object o: c) {
            if (_fromElement != null && _comparatorNullsFirst.compare(_fromElement, o) > 0) {
              return false;
            }
            if (_toElement != null && _comparatorNullsLast.compare(_toElement, o) <= 0) {
              return false;
            }
          }
        }
        return apply(
            _lock.readLock(),
            (startIndex, endIndex) -> c.stream()
                .allMatch(o -> Arrays.binarySearch(_objects, startIndex, endIndex, o, _comparator) >= 0));
      } catch (ClassCastException cse) {
        return false;
      }
    }

    private List<? extends E> filterInRange(Collection<? extends E> c) {
      return c.stream()
          .filter(_fromElement == null ? e -> true : e -> _comparatorNullsLast.compare(_fromElement, e) <= 0)
          .filter(_toElement == null ? e -> true : e -> _comparatorNullsFirst.compare(_toElement, e) > 0)
          .collect(Collectors.toList());
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends E> c) {
      List<? extends E> inRange = filterInRange(c);
      if (!inRange.isEmpty()) {
        Lock lock = _lock.writeLock();
        lock.lock();
        try {
          inRange.removeIf(e -> Arrays.binarySearch(_objects, 0, _size, e, _comparator) >= 0);
          if (!inRange.isEmpty()) {
            Object[] objects;
            if (_size + inRange.size() >= _objects.length) {
              objects = Arrays.copyOf(_objects, _objects.length + inRange.size());
            } else {
              objects = _objects;
            }

            System.arraycopy(inRange.toArray(), 0, objects, _size, inRange.size());
            _size += inRange.size();
            Arrays.sort(objects, 0, _size, _comparator);
            _objects = objects;
            return true;
          }
        } finally {
          lock.unlock();
        }
      }
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof SortedSet && ((SortedSet) o).comparator() == ArraySortedSet.this.comparator()) {
        Iterator other = ((SortedSet) o).iterator();
        Iterator self = iterator();

        while (other.hasNext() && self.hasNext()) {
          if (_comparator.compare(other.next(), self.next()) != 0) {
            return false;
          }
        }

        return other.hasNext() == self.hasNext();
      }
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return apply(
          _lock.readLock(),
          ((startIndex, endIndex) -> Objects.hash(Arrays.copyOfRange(_objects, startIndex, endIndex))));
    }

    @Override
    public void forEach(Consumer<? super E> action) {
      apply(_lock.readLock(), (startIndex, endIndex) -> {
        for (int i = startIndex; i < endIndex; i++) {
          action.accept(at(i));
        }
        return null;
      });
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      return removeIf(c::contains);
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
      return apply(_lock.writeLock(), (startIndex, endIndex) -> {
        Object[] objects = new Object[_objects.length];

        System.arraycopy(_objects, 0, objects, 0, startIndex);

        int i = startIndex;
        int k = i;

        for (; i < endIndex; i++) {
          @SuppressWarnings("unchecked")
          E e = at(i);
          if (!filter.test(e)) {
            objects[k++] = e;
          }
        }

        if (k == i) {
          return false;
        }

        System.arraycopy(_objects, endIndex, objects, k, _size - endIndex);
        _size -= endIndex - k;
        _objects = objects;
        return true;
      });
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      return removeIf(((Predicate<? super E>) c::contains).negate());
    }

    @Override
    public void clear() {
      apply(_lock.writeLock(), (startIndex, endIndex) -> {
        if (startIndex < endIndex) {
          int size = _size;
          System.arraycopy(_objects, endIndex, _objects, startIndex, size - endIndex);
          _size = size - endIndex + startIndex;
          Arrays.fill(_objects, _size, size, null);
          return true;
        }
        return false;
      });
    }

    private int headCmpToElement(E toElement) {
      return _toElement != null ? _comparatorNullsLast.compare(_toElement, toElement) : toElement == null ? 0 : 1;
    }

    private int tailCmpFromElement(E fromElement) {
      return _fromElement != null
          ? _comparatorNullsFirst.compare(_fromElement, fromElement)
          : fromElement == null ? 0 : -1;
    }

    @Nonnull
    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
      int cmpToElement = headCmpToElement(toElement);
      int cmpFromElement = tailCmpFromElement(fromElement);

      if (cmpFromElement >= 0 && cmpToElement <= 0) {
        return this;
      } else {
        return new View(cmpFromElement < 0 ? fromElement : _fromElement, cmpToElement > 0 ? toElement : _toElement);
      }
    }

    @Nonnull
    @Override
    public SortedSet<E> headSet(E toElement) {
      int cmpToElement = headCmpToElement(toElement);
      if (cmpToElement <= 0) {
        return this;
      } else {
        return new View(_fromElement, toElement);
      }
    }

    @Nonnull
    @Override
    public SortedSet<E> tailSet(E fromElement) {
      int cmpFromElement = tailCmpFromElement(fromElement);
      if (cmpFromElement >= 0) {
        return this;
      } else {
        return new View(fromElement, _toElement);
      }
    }

    @Override
    public E first() {
      Lock lock = _lock.readLock();
      lock.lock();
      try {
        int index = _fromElement != null ? Arrays.binarySearch(_objects, 0, _size, _fromElement, _comparator) : 0;

        if (index < 0) {
          index = Math.negateExact(index) - 1; // index now equals insert position of _fromIndex;
        }

        if (index < _size) {
          E first = at(index);

          if (first != null && _comparatorNullsLast.compare(first, _toElement) < 0) {
            return first;
          }
        }

        throw new NoSuchElementException();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public E last() {
      Lock lock = _lock.readLock();
      lock.lock();
      try {
        int index = _toElement != null ? Arrays.binarySearch(_objects, 0, _size, _toElement, _comparator) : _size;

        if (index < 0) {
          index = Math.negateExact(index); // index now equals after insert position of _toElement;
        }
        if (index > 0 && index <= _size) {
          E last = at(index - 1);

          if (last != null && _comparatorNullsFirst.compare(last, _fromElement) >= 0) {
            return last;
          }
        }

        throw new NoSuchElementException();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Spliterator<E> spliterator() {
      return apply(_lock.readLock(), this::spliterator);
    }

    private Spliterator<E> spliterator(int startIndex, int endIndex) {
      return Spliterators.spliterator(
          _objects.clone(),
          startIndex,
          endIndex,
          Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.SIZED);
    }

    @Nonnull
    @Override
    public Iterator<E> iterator() {
      return Spliterators.iterator(spliterator());
    }

    @Override
    public int size() {
      return apply(_lock.readLock(), (startIndex, endIndex) -> endIndex - startIndex);
    }

    @Override
    public boolean isEmpty() {
      return _size == 0 || size() == 0;
    }

    private <T> T apply(Lock lock, BiIntFunction<T> fn) {
      lock.lock();
      try {
        int startIndex = _fromElement != null ? Arrays.binarySearch(_objects, 0, _size, _fromElement, _comparator) : 0;

        if (startIndex < 0) {
          startIndex = Math.negateExact(startIndex) - 1; // index now equals insert position of _fromIndex;
        }

        int endIndex =
            _toElement != null ? Arrays.binarySearch(_objects, startIndex, _size, _toElement, _comparator) : _size;

        if (endIndex < 0) {
          endIndex = Math.negateExact(endIndex) - 1; // index now equals after insert position of _toElement;
        }

        return fn.apply(startIndex, endIndex);

      } finally {
        lock.unlock();
      }

    }
  }

  private interface BiIntFunction<T> {
    T apply(int startIndex, int endIndex);
  }
}
