package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;
import org.apache.avro.Schema;


@Experimental
public interface FloatPredicate extends Predicate<Float> {
  @Experimental
  boolean evaluate(float value);

  @Override
  default boolean evaluate(Float value) {
    return value != null && evaluate(value.floatValue());
  }

  @Override
  default boolean isCompatibleWithSchema(Schema schema) {
    return schema.getType() == Schema.Type.FLOAT;
  }

  @Experimental
  static FloatPredicate equalTo(float expectedValue, float epsilon) {
    return new FloatEqualsPredicate(expectedValue, epsilon);
  }

  @Experimental
  static FloatPredicate greaterThan(float threshold) {
    return new FloatGreaterThanPredicate(threshold);
  }

  @Experimental
  static FloatPredicate greaterOrEquals(float threshold) {
    return new FloatGreaterOrEqualsPredicate(threshold);
  }

  @Experimental
  static FloatPredicate lowerThan(float threshold) {
    return new FloatLowerThanPredicate(threshold);
  }

  @Experimental
  static FloatPredicate lowerOrEquals(float threshold) {
    return new FloatLowerOrEqualsPredicate(threshold);
  }

  @Experimental
  static FloatPredicate anyOf(float... expectedValues) {
    return new FloatAnyOfPredicate(expectedValues);
  }
}
