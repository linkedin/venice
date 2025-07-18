package com.linkedin.venice.client.store.predicate;

public class AndPredicate<T> extends CompositePredicate<T> {
  AndPredicate(Predicate<T>... predicates) {
    super(predicates);
  }

  @Override
  public boolean evaluate(T value) {
    for (Predicate<T> predicate: predicates) {
      if (!predicate.evaluate(value)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    return "AndPredicate{predicates=" + java.util.Arrays.toString(predicates) + "}";
  }
}
