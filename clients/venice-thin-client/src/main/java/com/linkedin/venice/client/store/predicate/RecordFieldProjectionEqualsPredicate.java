package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class RecordFieldProjectionEqualsPredicate implements Predicate<GenericRecord> {
  private final String fieldName;
  private final Object expectedValue;

  RecordFieldProjectionEqualsPredicate(String fieldName, Object expectedValue) {
    this.fieldName = Objects.requireNonNull(fieldName);
    this.expectedValue = expectedValue;
  }

  @Override
  public boolean evaluate(GenericRecord dataRecord) {
    if (dataRecord == null) {
      return false;
    } else {
      /**
       * TODO: Since key schemas are immutable, we don't need to handle evolution, and therefore we could preload the
       *       {@link org.apache.avro.Schema.Field} at construction-time and keep using it. So far, this predicate is
       *       only used for partial key matching, so this would work. However, if we would like to apply predicates to
       *       value schemas as well, then the current implementation is necessary, in order to handle evolution.
       */
      Schema.Field field = dataRecord.getSchema().getField(fieldName);
      if (field == null) {
        return this.expectedValue == null;
      }
      Object actualValue = dataRecord.get(field.pos());
      if (expectedValue instanceof Predicate) {
        return ((Predicate) expectedValue).evaluate(actualValue);
      }
      return Objects.equals(actualValue, expectedValue);
    }
  }

  @Experimental
  public String getFieldName() {
    return fieldName;
  }

  @Experimental
  public Object getExpectedValue() {
    return expectedValue;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{fieldName: '" + fieldName + "', expectedValue: " + expectedValue + "}";
  }
}
