package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public class DotProductOperator implements ReadComputeOperator {
  @Override
  public void compute(
      int computeRequestVersion,
      ComputeOperation op,
      GenericRecord valueRecord,
      GenericRecord resultRecord,
      Map<String, String> computationErrorMap,
      Map<String, Object> context) {
    DotProduct dotProduct = (DotProduct) op.operation;
    try {
      List<Float> valueVector = ComputeUtils.getNullableFieldValueAsList(valueRecord, dotProduct.field.toString());
      List<Float> dotProductParam = dotProduct.dotProductParam;

      if (valueVector.size() == 0 || dotProductParam.size() == 0) {
        putResult(resultRecord, dotProduct.resultFieldName.toString(), null);
        return;
      } else if (valueVector.size() != dotProductParam.size()) {
        putResult(resultRecord, dotProduct.resultFieldName.toString(), 0.0f);
        computationErrorMap.put(
            dotProduct.resultFieldName.toString(),
            "Failed to compute because size of dot product parameter is: " + dotProduct.dotProductParam.size()
                + " while the size of value vector(" + dotProduct.field.toString() + ") is: " + valueVector.size());
        return;
      }

      float dotProductResult = ComputeUtils.dotProduct(dotProductParam, valueVector);
      /**
       * Up-casting float to double for V1 users because of backward-compatibility support;
       * V1 users don't require the extra precision in double and it's on purpose that
       * backend only generates float result.
       */
      putResult(resultRecord, dotProduct.resultFieldName.toString(), dotProductResult);
    } catch (Exception e) {
      putResult(resultRecord, dotProduct.resultFieldName.toString(), 0.0f);
      String msg = e.getClass().getSimpleName() + " : "
          + (e.getMessage() == null ? "Failed to execute dot-product operator." : e.getMessage());
      computationErrorMap.put(dotProduct.resultFieldName.toString(), msg);
    }
  }

  @Override
  public boolean allowFieldValueToBeNull() {
    return true;
  }

  @Override
  public String toString() {
    return "read-compute dot product operator";
  }

  @Override
  public String getOperatorFieldName(ComputeOperation op) {
    DotProduct operation = (DotProduct) op.operation;
    return operation.field.toString();
  }

  @Override
  public String getResultFieldName(ComputeOperation op) {
    DotProduct operation = (DotProduct) op.operation;
    return operation.resultFieldName.toString();
  }
}
