package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.HadamardProduct;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class HadamardProductOperator implements ReadComputeOperator {
  @Override
  public void compute(
      ComputeOperation op,
      Schema.Field operatorInputField,
      Schema.Field resultField,
      GenericRecord inputValueRecord,
      GenericRecord resultRecord,
      Map<String, String> computationErrorMap,
      Map<String, Object> context) {
    HadamardProduct hadamardProduct = (HadamardProduct) op.operation;
    try {
      List<Float> valueVector = ComputeUtils.getNullableFieldValueAsList(inputValueRecord, operatorInputField);
      List<Float> dotProductParam = hadamardProduct.hadamardProductParam;

      if (valueVector.size() == 0 || dotProductParam.size() == 0) {
        putResult(resultRecord, resultField, null);
        return;
      } else if (valueVector.size() != dotProductParam.size()) {
        putResult(resultRecord, resultField, null);
        computationErrorMap.put(
            resultField.name(),
            "Failed to compute because size of hadamard product parameter is: "
                + hadamardProduct.hadamardProductParam.size() + " while the size of value vector("
                + hadamardProduct.field.toString() + ") is: " + valueVector.size());
        return;
      }

      List<Float> hadamardProductResult = ComputeUtils.hadamardProduct(dotProductParam, valueVector);
      putResult(resultRecord, resultField, hadamardProductResult);
    } catch (Exception e) {
      putResult(resultRecord, resultField, null);
      String msg = e.getClass().getSimpleName() + " : "
          + (e.getMessage() == null ? "Failed to execute hadamard product operator." : e.getMessage());
      computationErrorMap.put(resultField.name(), msg);
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
