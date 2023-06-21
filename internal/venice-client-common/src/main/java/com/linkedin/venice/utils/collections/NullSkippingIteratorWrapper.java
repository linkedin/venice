package com.linkedin.venice.utils.collections;

import java.util.Iterator;


/**
 * This iterator traverses the entire backing iterator, while skipping over null entries.
 */
public class NullSkippingIteratorWrapper<E> extends AbstractNullSkippingIterator<E> {
  private final Iterator<E> iterator;

  public NullSkippingIteratorWrapper(Iterator<E> iterator) {
    this.iterator = iterator;
  }

  @Override
  protected boolean internalHasNext() {
    return iterator.hasNext();
  }

  @Override
  protected E internalNext() {
    return iterator.next();
  }
}
