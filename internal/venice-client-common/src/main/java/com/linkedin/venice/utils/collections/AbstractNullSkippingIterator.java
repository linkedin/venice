package com.linkedin.venice.utils.collections;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * This abstract iterator traverses another backing data structure (provided by concrete subclasses), while skipping
 * over null entries.
 *
 * The null skipping is handled both in {@link #hasNext()} and {@link #next()}, so that various usages of
 * iterators work as expected.
 */
abstract class AbstractNullSkippingIterator<E> implements Iterator<E> {
  private E nextElement = null;

  @Override
  public boolean hasNext() {
    populateNext();
    return this.nextElement != null;
  }

  @Override
  public E next() {
    populateNext();
    E elementToReturn = this.nextElement;
    if (elementToReturn == null) {
      // We've reached the end of the backing array.
      throw new NoSuchElementException();
    }
    this.nextElement = null;
    return elementToReturn;
  }

  private void populateNext() {
    while (this.nextElement == null && internalHasNext()) {
      this.nextElement = internalNext();
    }
  }

  protected abstract boolean internalHasNext();

  protected abstract E internalNext();
}
