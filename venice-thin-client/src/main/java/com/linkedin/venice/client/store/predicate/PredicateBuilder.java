package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;


public class PredicateBuilder {
  @Experimental
  public static Predicate and(Predicate... predicates) {
    return new AndPredicate(predicates);
  }

  @Experimental
  public static Predicate equalTo(String fieldName, Object expectedValue) {
    return new EqualsRelationalOperator(fieldName, expectedValue);
  }
}
