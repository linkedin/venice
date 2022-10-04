package com.linkedin.alpini.base.queuing;

import javax.annotation.Nonnull;


/**
 * Simplified Queue Interface that supports basic operations like add(), poll() etc akin to the operations on
 * {@linkplain java.util.Queue}.
 *
 * @author aauradka
 *
 * @param <T>
 */
public interface SimpleQueue<T> {
  /**
   * @see java.util.Queue#add(Object)
   */
  boolean add(@Nonnull T e);

  /**
   * @see java.util.Queue#poll()
   */
  T poll();

  /**
   * @see java.util.Queue#size()
   */
  int size();

  /**
   * Checks if queue is empty.
   *
   * @return {@code true} if Queue contains no elements, {@code false} otherwise.
   */
  boolean isEmpty();
}
