package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.listener.response.ComputeResponseWrapper;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;

import static com.linkedin.venice.VeniceConstants.*;


public class DotProductOperator implements ReadComputeOperator {
  @Override
  public void compute(int computeRequestVersion, ComputeOperation op, GenericRecord valueRecord, GenericRecord resultRecord,
      Map<String, String> computationErrorMap, Map<String, Object> context, ComputeResponseWrapper responseWrapper) {
    responseWrapper.incrementDotProductCount();
    DotProduct dotProduct = (DotProduct) op.operation;
    boolean useV1 = computeRequestVersion == COMPUTE_REQUEST_VERSION_V1;
    try {
      List<Float> valueVector =  (List<Float>)valueRecord.get(dotProduct.field.toString());
      List<Float> dotProductParam = dotProduct.dotProductParam;

      if (valueVector.size() == 0 || dotProductParam.size() == 0) {
        putResult(resultRecord, dotProduct.resultFieldName.toString(), useV1, 0.0d, null);
        return;
      } else if (valueVector.size() != dotProductParam.size()) {
        putResult(resultRecord, dotProduct.resultFieldName.toString(), useV1, 0.0d, 0.0f);
        computationErrorMap.put(dotProduct.resultFieldName.toString(),
            "Failed to compute because size of dot product parameter is: " + dotProduct.dotProductParam.size() +
                " while the size of value vector(" + dotProduct.field.toString() + ") is: " + valueVector.size());
        return;
      }

      float dotProductResult = ComputeOperationUtils.dotProduct(dotProductParam, valueVector);
      /**
       * Up-casting float to double for V1 users because of backward-compatibility support;
       * V1 users don't require the extra precision in double and it's on purpose that
       * backend only generates float result.
       */
      putResult(resultRecord, dotProduct.resultFieldName.toString(), useV1, (double)dotProductResult, dotProductResult);
    } catch (Exception e) {
      putResult(resultRecord, dotProduct.resultFieldName.toString(), useV1, 0.0d, 0.0f);
      computationErrorMap.put(dotProduct.resultFieldName.toString(), e.getMessage());
    }
  }

  public String getOperatorFieldName(ComputeOperation op) {
    DotProduct operation = (DotProduct) op.operation;
    return operation.field.toString();
  }

  public String getResultFieldName(ComputeOperation op) {
    DotProduct operation = (DotProduct) op.operation;
    return operation.resultFieldName.toString();
  }

}
