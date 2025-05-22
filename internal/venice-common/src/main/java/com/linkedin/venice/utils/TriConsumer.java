package com.linkedin.venice.utils;

/**
 * A functional interface that takes three arguments and returns no result.
 *
 * @param <K> the first argument type
 * @param <V> the second argument type
 * @param <S> the third argument type
 */
public interface TriConsumer<K, V, S> {
  /**
   * Performs this operation on the given arguments.
   * @param k the first argument
   * @param v the second argument
   * @param s the third argument
   */
  void accept(K k, V v, S s);
}
