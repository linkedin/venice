package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public class DotProductOperator implements ReadComputeOperator {
  @Override
  public void compute(ComputeOperation op, GenericRecord valueRecord, GenericRecord resultRecord,
      Map<String, String> computationErrorMap, Map<String, Object> context) {
    DotProduct dotProduct = (DotProduct) op.operation;
    try {
      List<Float> valueVector =  (List<Float>)valueRecord.get(dotProduct.field.toString());
      List<Float> dotProductParam = dotProduct.dotProductParam;

      if (valueVector.size() == 0 || dotProductParam.size() == 0) {
        resultRecord.put(dotProduct.resultFieldName.toString(), 0.0d);
        return;
      } else if (valueVector.size() != dotProductParam.size()) {
        resultRecord.put(dotProduct.resultFieldName.toString(), 0.0d);
        computationErrorMap.put(dotProduct.resultFieldName.toString(),
            "Failed to compute because size of dot product parameter is: " + dotProduct.dotProductParam.size() +
                " while the size of value vector(" + dotProduct.field.toString() + ") is: " + valueVector.size());
        return;
      }

      // client will make sure that the result field is double type
      resultRecord.put(dotProduct.resultFieldName.toString(), ComputeOperationUtils.dotProduct(dotProductParam, valueVector));
    } catch (Exception e) {
      resultRecord.put(dotProduct.resultFieldName.toString(), 0.0d);
      computationErrorMap.put(dotProduct.resultFieldName.toString(), e.getMessage());
    }
  }
}
