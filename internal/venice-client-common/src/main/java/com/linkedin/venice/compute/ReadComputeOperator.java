package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public interface ReadComputeOperator {
  void compute(
      int computeRequestVersion,
      ComputeOperation op,
      GenericRecord valueRecord,
      GenericRecord resultRecord,
      Map<String, String> computationErrorMap,
      Map<String, Object> context);

  default void putResult(GenericRecord record, String field, Object result) {
    record.put(field, result);
  }

  default void putDefaultResult(GenericRecord record, String field) {
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
