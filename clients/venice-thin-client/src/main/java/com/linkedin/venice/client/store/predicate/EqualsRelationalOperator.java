package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;


public class EqualsRelationalOperator implements Predicate {
  private final String fieldName;
  private final Object expectedValue;

  EqualsRelationalOperator(String fieldName, Object expectedValue) {
    if (fieldName == null) {
      throw new VeniceClientException("fieldName cannot be null.");
    }
    this.fieldName = fieldName;
    this.expectedValue = expectedValue;
  }

  @Override
  public boolean evaluate(GenericRecord dataRecord) {
    if (dataRecord == null) {
      return false;
    } else {
      return Objects.deepEquals(dataRecord.get(fieldName), expectedValue);
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
}
