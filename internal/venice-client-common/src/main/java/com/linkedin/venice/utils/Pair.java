package com.linkedin.venice.utils;

import java.io.Serializable;
import java.util.Objects;


/**
 * Represents a pair of items
 * @param <F> The type of the first item
 * @param <S> The type of the second item
 *
 * @deprecated Instead of this, please create a dedicated class with well-named, non-generic (potentially primitive) properties and getters.
 */
@Deprecated
public class Pair<F, S> implements Serializable {
  private static final long serialVersionUID = 1L;

  private final F first;

  private final S second;

  /**
   * Static factory method that, unlike the constructor, performs generic
   * inference saving some typing. Use in the following way (for a pair of
   * Strings):
   *
   * <p>
   * <code>
   * Pair<String, String> pair = Pair.create("first", "second");
   * </code>
   * </p>
   *
   * @param first  The first thing
   * @param second The second thing
   * @param <F>   The type of the first thing.
   * @param <S>   The type of the second thing
   * @return The pair (first,second)
   */
  public static final <F, S> Pair<F, S> create(F first, S second) {
    return new Pair<F, S>(first, second);
  }

  /**
   * Use the static factory method {@link #create(Object, Object)} instead of
   * this where possible.
   * @param first
   * @param second
   */
  public Pair(F first, S second) {
    this.first = first;
    this.second = second;
  }

  public final F getFirst() {
    return first;
  }

  public final S getSecond() {
    return second;
  }

  @Override
  public final int hashCode() {
    return calculateHashCode(first, second);
  }

  public static <F, S> int calculateHashCode(F first, S second) {
    final int PRIME = 31;
    int result = 1;
    result = PRIME * result + ((first == null) ? 0 : first.hashCode());
    result = PRIME * result + ((second == null) ? 0 : second.hashCode());
    return result;
  }

  @Override
  public final boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Pair<?, ?>)) {
      return false;
    }

    final Pair<?, ?> other = (Pair<?, ?>) (obj);
    return Objects.equals(first, other.first) && Objects.equals(second, other.second);
  }

  @Override
  public final String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("[ " + first + ", " + second + " ]");
    return builder.toString();
  }
}
