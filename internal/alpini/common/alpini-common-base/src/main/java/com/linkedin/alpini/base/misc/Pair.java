package com.linkedin.alpini.base.misc;

/**
 * A simple container for a pair of (2) values.
 *
 * Adapted from Jemiah's code in com.linkedin.util.
 * This class lets us hold a pair of elements
 */
public class Pair<F, S> {
  private final F _first;
  private final S _second;

  /**
   * Constructor for a pair. A simpler alternative is to use {@link #make(Object, Object)} instead.
   *
   * @param first First value.
   * @param second Second value.
   */
  public Pair(F first, S second) {
    _first = first;
    _second = second;
  }

  /**
   * Make a Pair.
   *
   * @param first First value.
   * @param second Second value.
   * @param <F> Type of first argument, usually inferred automatically.
   * @param <S> Type of seconnd argument, usually inferred automatically.
   * @return newly constructed {@linkplain Pair} instance.
   */
  public static <F, S> Pair<F, S> make(F first, S second) {
    return new Pair<>(first, second);
  }

  /**
   * Return an array of Pair.
   *
   * @param array Comma seperated list of Pair objects.
   * @param <F> Type of first argument, usually inferred automatically.
   * @param <S> Type of first argument, usually inferred automatically.
   * @return array of Pair objects.
   */
  @SafeVarargs
  public static <P extends Pair<F, S>, F, S> P[] array(P... array) {
    return array;
  }

  /**
   * Return the first value.
   * @return first value.
   */
  public final F getFirst() {
    return _first;
  }

  /**
   * Return the second value.
   * @return second value.
   */
  public final S getSecond() {
    return _second;
  }

  /**
   * Test for equality between this and {@code obj}.
   * @param obj Value to test against.
   * @return {@code true} if equals.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Pair) {
      Pair<?, ?> other = (Pair<?, ?>) obj;
      return equals(_first, other._first) && equals(_second, other._second);
    }
    return false;
  }

  /**
   * Compute a hash value for the Pair.
   * @return hash value.
   */
  @Override
  public int hashCode() {
    int h1 = _first != null ? _first.hashCode() : 0;
    int h2 = _second != null ? _second.hashCode() : 0;
    return 31 * h1 + h2;
  }

  private static boolean equals(Object o1, Object o2) {
    return o1 != null ? o1.equals(o2) : o2 == null;
  }

  /**
   * Return a string representation of the Pair.
   * @return string.
   */
  @Override
  public String toString() {
    return "Pair{" + "_first=" + _first + ", _second=" + _second + '}';
  }
}
