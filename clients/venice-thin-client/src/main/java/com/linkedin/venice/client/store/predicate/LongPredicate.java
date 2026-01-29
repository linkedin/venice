package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;
import org.apache.avro.Schema;


public interface LongPredicate extends Predicate<Long> {
  @Experimental
  boolean evaluate(long value);

  @Override
  default boolean evaluate(Long value) {
    return value != null && evaluate(value.longValue());
  }

  @Override
  default boolean isCompatibleWithSchema(Schema schema) {
    return schema.getType() == Schema.Type.LONG;
  }

  @Experimental
  static LongPredicate equalTo(long expectedValue) {
    return new LongEqualsPredicate(expectedValue);
  }

  @Experimental
  static LongPredicate greaterThan(long threshold) {
    return new LongGreaterThanPredicate(threshold);
  }

  @Experimental
  static LongPredicate greaterOrEquals(long threshold) {
    return new LongGreaterOrEqualsPredicate(threshold);
  }

  @Experimental
  static LongPredicate lowerThan(long threshold) {
    return new LongLowerThanPredicate(threshold);
  }

  @Experimental
  static LongPredicate lowerOrEquals(long threshold) {
    return new LongLowerOrEqualsPredicate(threshold);
  }

  @Experimental
  static LongPredicate anyOf(long... expectedValues) {
    return new LongAnyOfPredicate(expectedValues);
  }
}
