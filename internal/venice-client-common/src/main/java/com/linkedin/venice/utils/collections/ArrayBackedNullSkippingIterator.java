package com.linkedin.venice.utils.collections;

/**
 * This iterator traverses the entire backing array, while skipping over null entries.
 */
public class ArrayBackedNullSkippingIterator<E> extends AbstractNullSkippingIterator<E> {
  private int index = 0;
  private final E[] array;

  public ArrayBackedNullSkippingIterator(E[] array) {
    this.array = array;
  }

  @Override
  protected boolean internalHasNext() {
    return this.index < this.array.length;
  }

  @Override
  protected E internalNext() {
    return this.array[this.index++];
  }
}
