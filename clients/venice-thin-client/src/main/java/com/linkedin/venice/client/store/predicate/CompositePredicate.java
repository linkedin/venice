package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.utils.collections.ArrayBackedNullSkippingIterator;
import java.util.Iterator;


public abstract class CompositePredicate<T> implements Predicate<T>, Iterable<Predicate<T>> {
  protected final Predicate[] predicates;

  CompositePredicate(Predicate<T>... predicates) {
    this.predicates = predicates;
  }

  @Override
  public Iterator<Predicate<T>> iterator() {
    return new ArrayBackedNullSkippingIterator<>(predicates);
  }
}
