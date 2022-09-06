package com.linkedin.venice.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


/**
 * A set that uses complement representation, which is useful when universal set cardinality is unknown.
 *
 * If "isComplement" is set to false, the ComplementSet is the same as a regular Set;
 * if "isComplement" is set to true, the "elements" inside ComplementSet represents the elements that the Set doesn't have.
*/
public class ComplementSet<T> {
  private boolean isComplement;
  private Set<T> elements;

  protected ComplementSet(boolean isComplement, Set<T> elements) {
    this.isComplement = isComplement;
    this.elements = elements;
  }

  public static <T> ComplementSet<T> of(T... elements) {
    return newSet(Arrays.asList(elements));
  }

  /**
   * This API will reuse the input "elements" which cannot be unmodifiable.
   */
  public static <T> ComplementSet<T> wrap(Set<T> elements) {
    return new ComplementSet<>(false, elements);
  }

  /**
   * This API will result in copying the data set, which is not performant or GC friendly if it's
   * in critical path.
   */
  public static <T> ComplementSet<T> newSet(Collection<T> elements) {
    return wrap(new HashSet<>(elements));
  }

  public static <T> ComplementSet<T> newSet(ComplementSet<T> other) {
    return new ComplementSet<>(other.isComplement, new HashSet<>(other.elements));
  }

  public static <T> ComplementSet<T> emptySet() {
    return new ComplementSet<>(false, new HashSet<>());
  }

  /**
   * Returns a set that contains all possible elements and is represented as a complement of an empty set.
   */
  public static <T> ComplementSet<T> universalSet() {
    return new ComplementSet<>(true, new HashSet<>());
  }

  @Override
  public String toString() {
    if (isComplement) {
      return elements.isEmpty() ? "ALL" : "ALL EXCEPT " + elements;
    }
    return elements.toString();
  }

  public boolean isEmpty() {
    return !isComplement && elements.isEmpty();
  }

  public void clear() {
    isComplement = false;
    elements.clear();
  }

  public boolean contains(T element) {
    return isComplement ^ elements.contains(element);
  }

  public void addAll(ComplementSet<T> other) {
    if (isComplement) {
      if (other.isComplement) {
        // (1 - X) + (1 - Y) = 1 - (X & Y)
        elements.retainAll(other.elements);
      } else {
        // (1 - X) + Y = 1 - (X - Y)
        elements.removeAll(other.elements);
      }
    } else {
      if (other.isComplement) {
        // X + (1 - Y) = 1 - (Y - X)
        Set<T> savedElements = elements;
        isComplement = true;
        elements = new HashSet<>(other.elements);
        elements.removeAll(savedElements);
      } else {
        // X + Y
        elements.addAll(other.elements);
      }
    }
  }

  public void removeAll(ComplementSet<T> other) {
    if (isComplement) {
      if (other.isComplement) {
        // (1 - X) - (1 - Y) = Y - X
        Set<T> savedElements = elements;
        isComplement = false;
        elements = new HashSet<>(other.elements);
        elements.removeAll(savedElements);
      } else {
        // (1 - X) - Y = 1 - (X + Y)
        elements.addAll(other.elements);
      }
    } else {
      if (other.isComplement) {
        // X - (1 - Y) = X & Y
        elements.retainAll(other.elements);
      } else {
        // X - Y
        elements.removeAll(other.elements);
      }
    }
  }
}
