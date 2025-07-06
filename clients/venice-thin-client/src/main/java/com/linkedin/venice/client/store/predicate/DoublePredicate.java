package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;
import org.apache.avro.Schema;


@Experimental
public interface DoublePredicate extends Predicate<Double> {
  @Experimental
  boolean evaluate(double value);

  @Override
  default boolean evaluate(Double value) {
    return value != null && evaluate(value.doubleValue());
  }

  @Override
  default boolean isCompatibleWithSchema(Schema schema) {
    return schema.getType() == Schema.Type.DOUBLE;
  }

  @Experimental
  static DoublePredicate equalTo(double expectedValue, double epsilon) {
    return new DoubleEqualsPredicate(expectedValue, epsilon);
  }

  @Experimental
  static DoublePredicate greaterThan(double threshold) {
    return new DoubleGreaterThanPredicate(threshold);
  }

  @Experimental
  static DoublePredicate greaterOrEquals(double threshold) {
    return new DoubleGreaterOrEqualsPredicate(threshold);
  }

  @Experimental
  static DoublePredicate lowerThan(double threshold) {
    return new DoubleLowerThanPredicate(threshold);
  }

  @Experimental
  static DoublePredicate lowerOrEquals(double threshold) {
    return new DoubleLowerOrEqualsPredicate(threshold);
  }

  @Experimental
  static DoublePredicate anyOf(double... expectedValues) {
    return new DoubleAnyOfPredicate(expectedValues);
  }
}
