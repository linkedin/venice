package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public interface ReadComputeOperator {
  void compute(
      ComputeOperation op,
      Schema.Field operatorField,
      Schema.Field resultField,
      GenericRecord valueRecord,
      GenericRecord resultRecord,
      Map<String, String> computationErrorMap,
      Map<String, Object> context);

  default void putResult(GenericRecord record, Schema.Field field, Object result) {
    record.put(field.pos(), result);
  }

  default void putDefaultResult(GenericRecord record, Schema.Field field) {
    putResult(record, field, 0.0f);
  }

  String getResultFieldName(ComputeOperation op);

  String getOperatorFieldName(ComputeOperation op);

  /**
   * Whether the extracted value of the field in the read value record is allowed to be null.
   * @return True if the extracted value is nullable and vice versa
   */
  boolean allowFieldValueToBeNull();
}
