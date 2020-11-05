package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.listener.response.ComputeResponseWrapper;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public interface ReadComputeOperator {
  void compute(int computeRequestVersion, ComputeOperation op, GenericRecord valueRecord, GenericRecord resultRecord,
      Map<String, String> computationErrorMap, Map<String, Object> context, ComputeResponseWrapper responseWrapper);

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

  default void putDefaultResult(GenericRecord record, String field, boolean useV1) {
    putResult(record, field, useV1, 0.0d, 0.0f);
  }

  String getResultFieldName(ComputeOperation op);

  String getOperatorFieldName(ComputeOperation op);
}
