package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


@Experimental
public interface Predicate<T> {
  @Experimental
  boolean evaluate(T value);

  /**
   * Checks if this predicate is compatible with the given Avro schema type.
   * This method should be implemented by each predicate type to provide
   * type-specific compatibility logic.
   * 
   * @param schema The Avro schema to check compatibility against
   * @return true if this predicate is compatible with the schema type
   */
  @Experimental
  default boolean isCompatibleWithSchema(Schema schema) {
    // Default implementation allows any type for generic predicates
    return true;
  }

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
