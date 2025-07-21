package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;
import org.apache.avro.Schema;


public interface IntPredicate extends Predicate<Integer> {
  @Experimental
  boolean evaluate(int value);

  @Override
  default boolean evaluate(Integer value) {
    return value != null && evaluate(value.intValue());
  }

  @Override
  default boolean isCompatibleWithSchema(Schema schema) {
    return schema.getType() == Schema.Type.INT;
  }

  @Experimental
  static IntPredicate equalTo(int expectedValue) {
    return new IntEqualsPredicate(expectedValue);
  }

  @Experimental
  static IntPredicate greaterThan(int threshold) {
    return new IntGreaterThanPredicate(threshold);
  }

  @Experimental
  static IntPredicate greaterOrEquals(int threshold) {
    return new IntGreaterOrEqualsPredicate(threshold);
  }

  @Experimental
  static IntPredicate lowerThan(int threshold) {
    return new IntLowerThanPredicate(threshold);
  }

  @Experimental
  static IntPredicate lowerOrEquals(int threshold) {
    return new IntLowerOrEqualsPredicate(threshold);
  }

  @Experimental
  static IntPredicate anyOf(int... expectedValues) {
    return new IntAnyOfPredicate(expectedValues);
  }
}
