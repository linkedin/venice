package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.HadamardProduct;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public class HadamardProductOperator implements ReadComputeOperator {
  @Override
  public void compute(
      int computeRequestVersion,
      ComputeOperation op,
      GenericRecord valueRecord,
      GenericRecord resultRecord,
      Map<String, String> computationErrorMap,
      Map<String, Object> context) {
    HadamardProduct hadamardProduct = (HadamardProduct) op.operation;
    try {
      List<Float> valueVector = ComputeUtils.getNullableFieldValueAsList(valueRecord, hadamardProduct.field.toString());
      List<Float> dotProductParam = hadamardProduct.hadamardProductParam;

      if (valueVector.size() == 0 || dotProductParam.size() == 0) {
        resultRecord.put(hadamardProduct.resultFieldName.toString(), null);
        return;
      } else if (valueVector.size() != dotProductParam.size()) {
        resultRecord.put(hadamardProduct.resultFieldName.toString(), null);
        computationErrorMap.put(
            hadamardProduct.resultFieldName.toString(),
            "Failed to compute because size of hadamard product parameter is: "
                + hadamardProduct.hadamardProductParam.size() + " while the size of value vector("
                + hadamardProduct.field.toString() + ") is: " + valueVector.size());
        return;
      }

      List<Float> hadamardProductResult = ComputeUtils.hadamardProduct(dotProductParam, valueVector);
      resultRecord.put(hadamardProduct.resultFieldName.toString(), hadamardProductResult);
    } catch (Exception e) {
      resultRecord.put(hadamardProduct.resultFieldName.toString(), null);
      String msg = e.getClass().getSimpleName() + " : "
          + (e.getMessage() == null ? "Failed to execute hadamard product operator." : e.getMessage());
      computationErrorMap.put(hadamardProduct.resultFieldName.toString(), msg);
    }
  }

  @Override
  public boolean allowFieldValueToBeNull() {
    return true;
  }

  @Override
  public String toString() {
    return "read-compute hadamard product operator";
  }

  @Override
  public String getOperatorFieldName(ComputeOperation op) {
    HadamardProduct operation = (HadamardProduct) op.operation;
    return operation.field.toString();
  }

  @Override
  public String getResultFieldName(ComputeOperation op) {
    HadamardProduct operation = (HadamardProduct) op.operation;
    return operation.resultFieldName.toString();
  }
}
