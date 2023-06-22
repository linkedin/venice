package com.linkedin.venice.utils;

import com.linkedin.venice.utils.collections.NullSkippingIteratorWrapper;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;


/**
 * A {@link java.util.List} implementation with some usability improvements around resizing. In particular,
 * the list provides {@link Map} semantics in some cases where the regular {@link List} behavior would be
 * throw {@link ArrayIndexOutOfBoundsException}.
 *
 * Note on concurrency and performance characteristics:
 *
 * This class extends {@link CopyOnWriteArrayList} and thus mimics its general characteristics: it is
 * threadsafe, very efficient for read operations, but it incurs a locking overhead on mutation operations.
 *
 * Unfortunately, the locking overhead may be up t double that of the parent class, because we cannot get
 * access to {@link CopyOnWriteArrayList#lock} since it is package private and Java doesn't allow us to
 * define new classes in the java.* package. So instead we are making every mutation operation synchronized.
 * The end result should be that the inner lock inside the parent class never has any contention, since the
 * locking is effectively external. In any case, even if this double-locking has any overhead, it should not
 * be a big concern, since the read operations are still lock-free, and (at the time of this writing), only
 * the read operations are used on the hot path.
 */
public class SparseConcurrentList<E> extends CopyOnWriteArrayList<E> {
  private static final long serialVersionUID = 1L;

  /**
   * A function which behaves like {@link Map#put(Object, Object)}, rather than {@link List#set(int, Object)}.
   *
   * @param index
   * @param item
   * @return
   */
  public synchronized E set(int index, E item) {
    if (size() <= index) {
      /**
       * Our list is too small, so we will add to it (mostly nulls) to make its internal array large enough...
       *
       * This could be done by calling {@link CopyOnWriteArrayList#add(Object)} repeatedly, but that would
       * cause many array resizing/copy operations in a row, so instead, we create a temporary list and call
       * {@link CopyOnWriteArrayList#addAll(Collection)} so that there is just one resizing/copy.
       */
      int capacityOfTemporaryArray = index + 1 - size(); // how many slots are missing from the list?
      List<E> temporaryList = new ArrayList<>(capacityOfTemporaryArray);
      for (int i = 0; i < capacityOfTemporaryArray - 1; i++) {
        temporaryList.add(null);
      }
      temporaryList.add(item);
      super.addAll(temporaryList);
      return null;
    } else {
      /**
       * No risk of {@link ArrayIndexOutOfBoundsException}, so we directly go ahead and call
       * {@link List#set(int, Object)}
       */
      return super.set(index, item);
    }
  }

  /**
   * @param index of the item to retrieve
   * @return the item at this index, or null
   * @throws IllegalArgumentException if the index is < 0, but NOT if the index is > the capacity of the list
   */
  public E get(int index) {
    if (size() <= index) {
      return null;
    } else if (index < 0) {
      throw new IllegalArgumentException("Index cannot be negative.");
    }
    return super.get(index);
  }

  /**
   * A function which behaves like {@link Map#remove(Object)}, rather than {@link List#remove(int)}, in the
   * sense that it removes the item from the collection, returns the previous value (if any), but *does not*
   * shift subsequent items to the left (as the regular {@link List#remove(int)} would.
   *
   * @param index of the item to nullify
   * @return the previous item at that {@param index}
   */
  public E remove(int index) {
    /**
     * It's important to use {@link #set(int, Object)} rather than {@link #remove(int)} in order to avoid
     * altering the subsequent items in the list.
     */
    return set(index, null);
  }

  public void forEach(Consumer<? super E> itemConsumer) {
    for (int partitionId = 0; partitionId < size(); partitionId++) {
      E item = get(partitionId);
      if (item == null) {
        continue;
      }
      itemConsumer.accept(item);
    }
  }

  // Boilerplate code just to add synchronization to mutation operations:

  public synchronized boolean add(E e) {
    return super.add(e);
  }

  public synchronized void add(int index, E element) {
    super.add(index, element);
  }

  public synchronized boolean removeAll(Collection<?> c) {
    return super.removeAll(c);
  }

  public synchronized boolean retainAll(Collection<?> c) {
    return super.retainAll(c);
  }

  public synchronized int addAllAbsent(Collection<? extends E> c) {
    if (c == null) {
      return 0;
    }
    return super.addAllAbsent(c);
  }

  public synchronized void clear() {
    super.clear();
  }

  public synchronized boolean addAll(Collection<? extends E> c) {
    return super.addAll(c);
  }

  public synchronized boolean addAll(int index, Collection<? extends E> c) {
    return super.addAll(index, c);
  }

  public synchronized boolean removeIf(Predicate<? super E> filter) {
    return super.removeIf(filter);
  }

  public synchronized void replaceAll(UnaryOperator<E> operator) {
    super.replaceAll(operator);
  }

  public synchronized void sort(Comparator<? super E> c) {
    super.sort(c);
  }

  public synchronized List<E> subList(int fromIndex, int toIndex) {
    return super.subList(fromIndex, toIndex);
  }

  public E computeIfAbsent(int index, IntFunction<? extends E> mappingFunction) {
    E element = get(index);
    if (element == null) {
      synchronized (this) {
        element = get(index);
        if (element == null) {
          element = mappingFunction.apply(index);
          set(index, element);
        }
      }
    }
    return element;
  }

  public Collection<E> values() {
    return new ValueCollection<>(this::iterator);
  }

  static class ValueCollection<E> extends AbstractCollection<E> {
    private final Supplier<Iterator<E>> iteratorSupplier;

    ValueCollection(Supplier<Iterator<E>> iteratorSupplier) {
      this.iteratorSupplier = iteratorSupplier;
    }

    @Override
    public Iterator<E> iterator() {
      return new NullSkippingIteratorWrapper<>(iteratorSupplier.get());
    }

    @Override
    public int size() {
      Iterator<E> iterator = new NullSkippingIteratorWrapper(iteratorSupplier.get());
      int populatedSize = 0;
      while (iterator.hasNext()) {
        iterator.next();
        populatedSize++;
      }
      return populatedSize;
    }
  }
}
