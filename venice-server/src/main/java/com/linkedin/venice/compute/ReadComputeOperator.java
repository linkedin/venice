package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public interface ReadComputeOperator {
  void compute(ComputeOperation op, GenericRecord valueRecord, GenericRecord resultRecord, Map<String, String> computationErrorMap, Map<String, Object> context);
}
