package com.linkedin.venice.utils;

import java.util.Iterator;
import java.util.function.Function;


public final class IteratorUtils {
  private IteratorUtils() {
  }

  public static <T, O> Iterator<O> mapIterator(Iterator<T> iterator, Function<T, O> mapper) {
    if (iterator == null) {
      return null;
    }

    if (mapper == null) {
      throw new NullPointerException("Expected 'mapper' to not be null");
    }

    return new Iterator<O>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public O next() {
        return mapper.apply(iterator.next());
      }
    };
  }
}
