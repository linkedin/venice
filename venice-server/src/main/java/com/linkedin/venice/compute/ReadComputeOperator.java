package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public interface ReadComputeOperator {
  void compute(int computeRequestVersion, ComputeOperation op, GenericRecord valueRecord, GenericRecord resultRecord, Map<String, String> computationErrorMap, Map<String, Object> context);

  default void putResult(GenericRecord record, String field, boolean useV1, Object legacyResult, Object result) {
    /**
     * The following logic cannot be replaced by record.put(field, useV1 ? legacyResult : result) because of
     * the numeric conversion rules in the conditional operator (?:); for example, in expression (a?b:c), if b is
     * double and c is float, conditional operator will convert the result to double type, so the type of the object
     * being put in the result record will be indeterminate.
     */
    if (useV1) {
      record.put(field, legacyResult);
    } else {
      record.put(field, result);
    }
  }
}
