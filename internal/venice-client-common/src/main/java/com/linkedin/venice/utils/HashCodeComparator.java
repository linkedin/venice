package com.linkedin.venice.utils;

import java.util.Comparator;


public final class HashCodeComparator<T> implements Comparator<T> {
  @Override
  public int compare(T o1, T o2) {
    int h1 = o1.hashCode();
    int h2 = o2.hashCode();
    if (h1 == h2) {
      return 0;
    } else if (h1 > h2) {
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    return obj.hashCode() == this.hashCode();
  }
}
