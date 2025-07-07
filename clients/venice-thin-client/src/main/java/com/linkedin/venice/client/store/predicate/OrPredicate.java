package com.linkedin.venice.client.store.predicate;

public class OrPredicate<T> extends CompositePredicate<T> {
  OrPredicate(Predicate<T>... predicates) {
    super(predicates);
  }

  @Override
  public boolean evaluate(T value) {
    for (Predicate<T> predicate: predicates) {
      if (predicate.evaluate(value)) {
        return true;
      }
    }

    return false;
  }
}
