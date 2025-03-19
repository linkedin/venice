package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;
import org.apache.avro.generic.GenericRecord;


@Experimental
public interface Predicate<T> {
  @Experimental
  boolean evaluate(T value);

  // Generic predicates

  @Experimental
  static <T> Predicate<T> and(Predicate<T>... predicates) {
    return new AndPredicate<>(predicates);
  }

  @Experimental
  static <T> Predicate<T> or(Predicate<T>... predicates) {
    return new OrPredicate<>(predicates);
  }

  @Experimental
  static <T> Predicate<T> anyOf(T... expectedValues) {
    return new AnyOfPredicate<>(expectedValues);
  }

  @Experimental
  static <O> Predicate<O> equalTo(O expectedValue) {
    return new EqualsPredicate<>(expectedValue);
  }

  // Record predicates

  @Experimental
  static Predicate<GenericRecord> equalTo(String fieldName, Object expectedValue) {
    return new RecordFieldProjectionEqualsPredicate(fieldName, expectedValue);
  }
}
