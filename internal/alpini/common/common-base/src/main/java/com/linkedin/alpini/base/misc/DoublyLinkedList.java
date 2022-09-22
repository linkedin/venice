package com.linkedin.alpini.base.misc;

import java.util.AbstractSequentialList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;


/**
 * Linked list implementation of the List interface.
 * Implements all optional list operations, but only permits elements of the DoublyLinkedList.Entry class.
 * In addition to implementing the List interface, the LinkedList class provides uniformly named methods to get,
 * remove and insert an element at the beginning and end of the list.
 * These operations allow linked lists to be used as a stack, queue, or double-ended queue.
 *
 * The class implements the Deque interface, providing first-in-first-out queue operations for add, poll,
 * along with other stack and deque operations.
 *
 * All of the operations perform as could be expected for a doubly-linked list.
 * Operations that index into the list will traverse the list from the beginning or the end,
 * whichever is closer to the specified index.
 *
 * Note that this implementation is not synchronized. If multiple threads access a linked list concurrently,
 * and at least one of the threads modifies the list structurally, it must be synchronized externally.
 * (A structural modification is any operation that adds or deletes one or more elements;
 * merely setting the value of an element is not a structural modification.)
 * This is typically accomplished by synchronizing on some object that naturally encapsulates the list.
 *
 *
 *
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public class DoublyLinkedList<E extends DoublyLinkedList.Entry<E>> extends AbstractSequentialList<E>
    implements Deque<E> {
  private final Entry<E> _start = new Terminator();
  private final Entry<E> _finish = new Terminator();
  private int _count;

  /**
   * Constructs an empty list.
   */
  public DoublyLinkedList() {
    _start._next = _finish;
    _finish._previous = _start;
  }

  /**
   * Constructs a list containing the elements of the specified collection, in the order they are returned by the collection's iterator.
   * @param source the collection whose elements are to be placed into this list
   * @throws NullPointerException if the specified collection is null
   */
  public DoublyLinkedList(Collection<? extends E> source) {
    this();
    addAll(source);
  }

  /**
   * In order to maintain a doubly-linked list, each element needs to establish links to its adjacent elements.
   * To do this, we are storing those references in the element. This does mean that each element may only
   * be contained within one list.
   * @param <E>
   */
  public static class Entry<E extends Entry<E>> {
    DoublyLinkedList<E> _owner;
    Entry<E> _previous;
    Entry<E> _next;

    /**
     * Removes this element from its list. Equivalent to calling list.remove(this)
     * @return true if successful
     */
    public boolean unlink() {
      DoublyLinkedList<E> owner = _owner;
      return owner != null && owner.unlink(this);
    }

    @SuppressWarnings("unchecked")
    private E get() {
      return (E) this;
    }
  }

  private static final class PlaceHolder<E extends Entry<E>> extends Entry<E> {
  }

  private final class Terminator extends Entry<E> {
  }

  /**
   * Called to remove children from the list.
   * @param entry child entry.
   * @return true if successful.
   */
  protected boolean unlink(Entry<E> entry) {
    if (entry instanceof PlaceHolder || entry._owner == null) {
      return false;
    }

    Preconditions.checkState(this == entry._owner);

    entry._owner = null;
    entry._previous._next = entry._next;
    entry._next._previous = entry._previous;
    entry._next = null;
    entry._previous = null;
    _count--;

    return true;
  }

  /**
   * Returns the number of elements in this list.
   * @return the number of elements in this list
   */
  @Override
  public int size() {
    return _count;
  }

  /**
   * Inserts the specified element at the beginning of this list.
   * @param e the element to add
   */
  @Override
  public void addFirst(E e) {
    listIterator(0).add(e);
  }

  /**
   * Appends the specified element to the end of this list.
   * @param e the element to add
   */
  @Override
  public void addLast(E e) {
    listIterator(_count).add(e);
  }

  /**
   * Returns true if this list contains the specified element.
   * @param o element whose presence in this list is to be tested
   * @return if this list contains the specified element
   */
  @Override
  public boolean contains(Object o) {
    return (o instanceof Entry && this == ((Entry) o)._owner) || super.contains(o);
  }

  /**
   * Returns the first element in this list.
   * @return the first element in this list
   * @throws NoSuchElementException if this list is empty
   */
  @Override
  public E element() {
    return getFirst();
  }

  /**
   * Returns the first element in this list.
   * @return the first element in this list
   * @throws NoSuchElementException if this list is empty
   */
  @Override
  public E getFirst() {
    return listIterator(0).next();
  }

  /**
   * Returns the last element in this list.
   * @return the last element in this list
   * @throws NoSuchElementException if this list is empty
   */
  @Override
  public E getLast() {
    if (_count == 0) {
      throw new NoSuchElementException();
    }
    return listIterator(_count - 1).next();
  }

  /**
   * Adds the specified element as the tail (last element) of this list.
   * @param e the element to add
   * @return true
   */
  @Override
  public boolean offer(E e) {
    return offerLast(e);
  }

  /**
   * Inserts the specified element at the front of this list.
   * @param e the element to insert
   * @return true
   */
  @Override
  public boolean offerFirst(E e) {
    listIterator(0).add(e);
    return true;
  }

  /**
   * Inserts the specified element at the end of this list.
   * @param e the element to insert
   * @return true
   */
  @Override
  public boolean offerLast(E e) {
    listIterator(_count).add(e);
    return true;
  }

  @Override
  public E peek() {
    return peekFirst();
  }

  /**
   * Retrieves, but does not remove, the first element of this list, or returns null if this list is empty.
   * @return the first element of this list, or null
   */
  @Override
  public E peekFirst() {
    ListIterator<E> it = listIterator(0);
    if (it.hasNext()) {
      return it.next();
    }
    return null;
  }

  /**
   * Retrieves, but does not remove, the last element of this list, or returns null if this list is empty.
   * @return the last element of this list, or null
   */
  @Override
  public E peekLast() {
    ListIterator<E> it = listIterator(Math.max(0, _count - 1));
    if (it.hasNext()) {
      return it.next();
    }
    return null;
  }

  /**
   * Retrieves and removes the head (first element) of this list
   * @return the head of this list, or null
   */
  @Override
  public E poll() {
    return pollFirst();
  }

  /**
   * Retrieves and removes the first element of this list, or returns null if this list is empty.
   * @return the first element of this list, or null
   */
  @Override
  public E pollFirst() {
    ListIterator<E> it = listIterator(0);
    if (it.hasNext()) {
      try {
        return it.next();
      } finally {
        it.remove();
      }
    }
    return null;
  }

  /**
   * Retrieves and removes the last element of this list, or returns null if this list is empty.
   * @return the last element of this list, or null
   */
  @Override
  public E pollLast() {
    ListIterator<E> it = listIterator(Math.max(0, _count - 1));
    if (it.hasNext()) {
      try {
        return it.next();
      } finally {
        it.remove();
      }
    }
    return null;
  }

  /**
   * Pops an element from the stack represented by this list.
   * @return the element at the front of this list
   * @throws NoSuchElementException if this list is empty.
   * @see java.util.LinkedList#pop
   */
  @Override
  public E pop() {
    return removeFirst();
  }

  /**
   * Pushes an element onto the stack represented by this list.
   * @param e the element to push
   * @see java.util.LinkedList#push(Object)
   */
  @Override
  public void push(E e) {
    addFirst(e);
  }

  /**
   * Retrieves and removes the head (first element) of this list.
   * @return the head of this list
   * @throws NoSuchElementException if this list is empty
   * @see java.util.LinkedList#remove()
   */
  @Override
  public E remove() {
    return removeFirst();
  }

  /**
   * Removes and returns the first element from this list.
   * @return the first element from this list
   * @throws NoSuchElementException if this list is empty
   * @see java.util.LinkedList#removeFirst()
   */
  @Override
  public E removeFirst() {
    if (_count == 0) {
      throw new NoSuchElementException();
    }
    ListIterator<E> it = listIterator(0);
    try {
      return it.next();
    } finally {
      it.remove();
    }
  }

  /**
   * Removes the first occurrence of the specified element from this list, if it is present.
   * @param o element to be removed from this list,
   * @return true if this list contained the specified element
   * @see java.util.LinkedList#remove(Object)
   */
  @Override
  public boolean remove(Object o) {
    if (o instanceof Entry) {
      Entry<?> e = (Entry<?>) o;
      return e._owner == this && e.unlink();
    }
    return super.remove(o);
  }

  /**
   * Removes the first occurrence of the specified element in this list.
   * @param o element to be removed from this list, if present
   * @return true if the list contained the specified element
   * @see java.util.LinkedList#removeFirstOccurrence(Object)
   */
  @Override
  public boolean removeFirstOccurrence(Object o) {
    if (o instanceof Entry) {
      Entry<?> e = (Entry<?>) o;
      return e._owner == this && e.unlink();
    }
    ListIterator<E> it = listIterator(0);
    while (it.hasNext()) {
      if (it.next().equals(o)) {
        it.remove();
        return true;
      }
    }
    return false;
  }

  /**
   * Removes the last occurrence of the specified element in this list
   * @param o element to be removed from this list, if present
   * @return true if the list contained the specified element
   * @see java.util.LinkedList#removeLastOccurrence(Object)
   */
  @Override
  public boolean removeLastOccurrence(Object o) {
    if (o instanceof Entry) {
      Entry<?> e = (Entry<?>) o;
      return e._owner == this && e.unlink();
    }
    ListIterator<E> it = listIterator(_count);
    while (it.hasPrevious()) {
      if (it.previous().equals(o)) {
        it.remove();
        return true;
      }
    }
    return false;
  }

  /**
   * Removes and returns the last element from this list.
   * @return the last element from this list
   * @throws NoSuchElementException if this list is empty
   * @see java.util.LinkedList#removeLast()
   */
  @Override
  public E removeLast() {
    if (_count == 0) {
      throw new NoSuchElementException();
    }
    ListIterator<E> it = listIterator(_count - 1);
    try {
      return it.next();
    } finally {
      it.remove();
    }
  }

  /**
   * Returns a list-iterator of the elements in this list (in proper sequence),
   * starting at the specified position in the list.
   * @param i index of the first element to be returned from the list-iterator (by a call to next)
   * @return a ListIterator of the elements in this list
   * @throws IndexOutOfBoundsException if the index is out of range.
   * @see java.util.List#listIterator(int)
   */
  @Override
  @Nonnull
  public ListIterator<E> listIterator(int i) {
    if (i != 0 && (i < 0 || i > _count)) {
      throw new IndexOutOfBoundsException();
    }

    Entry<E> current;
    int pos;

    if (i <= 1 + _count / 2) {
      current = _start;
      pos = -1;
      while (i > pos && current != _finish) {
        current = current._next;
        pos++;
      }
    } else {
      current = _finish;
      pos = _count;
      while (i < pos && current != _start) {
        current = current._previous;
        pos--;
      }
    }
    return new DoublyLinkedListIterator(current, pos);
  }

  /**
   * Representation of a ListIterator for Doubly-linked lists.
   */
  private class DoublyLinkedListIterator implements ListIterator<E> {
    private Entry<E> _prev;
    private Entry<E> _next;
    private Integer _nextIndex;
    private E _last;

    private DoublyLinkedListIterator(Entry<E> current, int index) {
      _next = current;
      _prev = current._previous;
      _nextIndex = index;
    }

    /**
     * Returns true if this list iterator has more elements when traversing the list in the forward direction.
     * @return true if the list iterator has more elements when traversing the list in the forward direction.
     * @see java.util.ListIterator#hasNext()
     */
    @Override
    public boolean hasNext() {
      if (_next != _finish && _next._owner != DoublyLinkedList.this) {
        throw new ConcurrentModificationException();
      }
      return _next != _finish;
    }

    /**
     * Returns the next element in the list
     * @return the next element in the list.
     * @throws NoSuchElementException if the iteration has no next element.
     * @see java.util.ListIterator#next()
     */
    @Override
    public E next() {
      if (hasNext()) {
        try {
          E last = _next.get();
          _last = last;
          return last;
        } finally {
          _prev = _next;
          _next = _next._next;
          if (_nextIndex != null) {
            _nextIndex++;
          }
        }
      }
      throw new NoSuchElementException();
    }

    /**
     * Returns true if this list iterator has more elements when traversing the list in the reverse direction.
     * @return if the list iterator has more elements when traversing the list in the reverse direction.
     * @see java.util.ListIterator#hasPrevious()
     */
    @Override
    public boolean hasPrevious() {
      if (_prev != _start && _prev._owner != DoublyLinkedList.this) {
        throw new ConcurrentModificationException();
      }
      return _prev != _start;
    }

    /**
     * Returns the previous element in the list.
     * @return the previous element in the list.
     * @throws NoSuchElementException if the iteration has no previous element
     * @see java.util.ListIterator#previous()
     */
    @Override
    public E previous() {
      if (hasPrevious()) {
        try {
          E last = _prev.get();
          _last = last;
          return last;
        } finally {
          _next = _prev;
          _prev = _prev._previous;
          if (_nextIndex != null) {
            _nextIndex--;
          }
        }
      }
      throw new NoSuchElementException();
    }

    /**
     * Returns the index of the element that would be returned by a subsequent call to next.
     * (Returns list size if the list iterator is at the end of the list.)
     * @return the index of the element that would be returned by a subsequent call to next,
     *         or list size if list iterator is at end of list.
     */
    @Override
    public int nextIndex() {
      if (_nextIndex != null) {
        return _nextIndex;
      }

      int index = -1;
      Entry<E> pos = _start;
      while (pos != _next) {
        index++;
        pos = pos._next;

        if (pos != _finish && pos._owner != DoublyLinkedList.this) {
          throw new ConcurrentModificationException();
        }
      }
      _nextIndex = index;
      return index;
    }

    /**
     * Returns the index of the element that would be returned by a subsequent call to previous.
     * (Returns -1 if the list iterator is at the beginning of the list.)
     * @return the index of the element that would be returned by a subsequent call to previous,
     *         or -1 if list iterator is at beginning of list.
     */
    @Override
    public int previousIndex() {
      if (_nextIndex != null) {
        return _nextIndex - 1;
      }

      int index = -1;
      Entry<E> pos = _start;
      while (pos != _prev) {
        index++;
        pos = pos._next;

        if (pos != _finish && pos._owner != DoublyLinkedList.this) {
          throw new ConcurrentModificationException();
        }
      }
      _nextIndex = index + 1;
      return index;
    }

    /**
     * Removes from the list the last element that was returned by next or previous
     * @throws IllegalStateException if neither next nor previous have been called,
     *         or remove or add have been called after the last call to next or previous.
     * @see java.util.ListIterator#remove()
     */
    @Override
    public void remove() {
      Preconditions.checkState(null != _last);

      if (_last._owner != DoublyLinkedList.this) {
        throw new ConcurrentModificationException();
      }

      if (_prev != _start && _prev._owner != DoublyLinkedList.this) {
        throw new ConcurrentModificationException();
      }

      if (_next != _finish && _next._owner != DoublyLinkedList.this) {
        throw new ConcurrentModificationException();
      }

      if (_prev == _last) {
        if (unlink(_last) && _nextIndex != null) {
          _nextIndex--;
        }
        _prev = _next._previous;
      } else if (_next == _last) {
        unlink(_last);
        _next = _prev._next;
      } else {
        throw new ConcurrentModificationException();
      }

      _last = null;
    }

    /**
     * Replaces the last element returned by next or previous with the specified element
     * @param e the element with which to replace the last element returned by next or previous
     * @throws IllegalStateException if neither next nor previous have been called,
     *         or remove or add have been called after the last call to next or previous.
     * @see ListIterator#set(Object)
     */
    @Override
    public void set(E e) {
      Preconditions.checkState(null != _last);

      if (_last._owner != DoublyLinkedList.this) {
        throw new ConcurrentModificationException();
      }

      if (_last == e) {
        return;
      }

      if (_next == e) {
        if (_next != _finish && _next._owner != DoublyLinkedList.this) {
          throw new ConcurrentModificationException();
        }

        // chomp the next item!
        if (unlink(_next)) {
          if (_nextIndex != null) {
            _nextIndex--;
          }
        }
        _last = e;
        _next = e._next;
        return;
      }

      if (_prev == e) {
        if (_prev != _start && _prev._owner != DoublyLinkedList.this) {
          throw new ConcurrentModificationException();
        }

        // chomp the previous item!
        if (unlink(_prev)) {
          if (_nextIndex != null) {
            _nextIndex--;
          }
        }
        _last = e;
        _prev = e._previous;
        return;
      }

      if (e._owner != null && e._owner != DoublyLinkedList.this) {
        throw new IllegalArgumentException("Element already owned by another list");
      }

      if (unlink(e)) {
        // We don't know where in the list that this entry came from so we invalidate the _nextIndex value.
        _nextIndex = null;
      }

      e._owner = DoublyLinkedList.this;
      e._previous = _last._previous;
      e._next = _last._next;
      unlink(_last);

      e._previous._next = e;
      e._next._previous = e;
      _last = e;
      _prev = e._previous;
      _next = e._next;
      _count++;
    }

    /**
     * Appends the specified element into the list.
     * @param e element to insert
     * @throws IllegalArgumentException if the element is already owned by another list
     * @see ListIterator#add(Object)
     */
    @Override
    public void add(E e) {
      _last = null;

      if (_next == e) {
        return;
      }

      if (e._owner != null && e._owner != DoublyLinkedList.this) {
        throw new IllegalArgumentException("Element already owned by another list");
      }

      if (unlink(e)) {
        // We don't know where in the list that this entry came from so we invalidate the _nextIndex value.
        _nextIndex = null;
      }

      if (_next != _finish && _next._owner != DoublyLinkedList.this) {
        throw new ConcurrentModificationException();
      }

      e._owner = DoublyLinkedList.this;
      e._next = _next;
      e._previous = _next._previous;
      _next._previous = e;
      e._previous._next = e;
      _prev = e;

      if (_nextIndex != null) {
        _nextIndex++;
      }

      _count++;
    }
  }

  /**
   * Returns an iterator over the elements in reverse sequential order.
   * The elements will be returned in order from last (tail) to first (head).
   * @return an iterator over the elements in reverse sequence
   * @see java.util.LinkedList#descendingIterator()
   */
  @Override
  @Nonnull
  public Iterator<E> descendingIterator() {
    return new Iterator<E>() {
      private ListIterator<E> _iterator = listIterator(_count);

      @Override
      public boolean hasNext() {
        return _iterator.hasPrevious();
      }

      @Override
      public E next() {
        return _iterator.previous();
      }

      @Override
      public void remove() {
        _iterator.remove();
      }
    };
  }

}
