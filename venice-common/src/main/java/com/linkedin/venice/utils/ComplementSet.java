package com.linkedin.venice.utils;

import java.util.Collection;
import java.util.HashSet;

/**
 * A set that uses complement representation, which is useful when universal set cardinality is unknown.
*/
public class ComplementSet<T> {
  private boolean isComplement;
  private Collection<T> elements;

  protected ComplementSet(boolean isComplement, Collection<T> elements) {
    this.isComplement = isComplement;
    this.elements = elements;
  }

  public static <T> ComplementSet<T> wrap(Collection<T> elements) {
    return new ComplementSet<>(false, elements);
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
        Collection<T> temp = elements;
        isComplement = true;
        elements = new HashSet<>(other.elements);
        elements.removeAll(temp);
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
        Collection<T> temp = elements;
        isComplement = false;
        elements = new HashSet<>(other.elements);
        elements.removeAll(temp);
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
