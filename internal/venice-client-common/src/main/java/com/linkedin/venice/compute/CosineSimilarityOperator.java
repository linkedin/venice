package com.linkedin.venice.compute;

import static com.linkedin.venice.compute.ComputeUtils.CACHED_SQUARED_L2_NORM_KEY;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public class CosineSimilarityOperator implements ReadComputeOperator {
  @Override
  public void compute(
      int computeRequestVersion,
      ComputeOperation op,
      GenericRecord valueRecord,
      GenericRecord resultRecord,
      Map<String, String> computationErrorMap,
      Map<String, Object> context) {
    CosineSimilarity cosineSimilarity = (CosineSimilarity) op.operation;
    try {
      List<Float> valueVector =
          ComputeUtils.getNullableFieldValueAsList(valueRecord, cosineSimilarity.field.toString());
      List<Float> cosSimilarityParam = cosineSimilarity.cosSimilarityParam;

      if (valueVector.size() == 0 || cosSimilarityParam.size() == 0) {
        putResult(resultRecord, cosineSimilarity.resultFieldName.toString(), null);
        return;
      } else if (valueVector.size() != cosSimilarityParam.size()) {
        putResult(resultRecord, cosineSimilarity.resultFieldName.toString(), 0.0f);
        computationErrorMap.put(
            cosineSimilarity.resultFieldName.toString(),
            "Failed to compute because size of dot product parameter is: " + cosineSimilarity.cosSimilarityParam.size()
                + " while the size of value vector(" + cosineSimilarity.field.toString() + ") is: "
                + valueVector.size());
        return;
      }

      float dotProductResult = ComputeUtils.dotProduct(cosSimilarityParam, valueVector);
      float valueVectorSquaredL2Norm = ComputeUtils.squaredL2Norm(valueVector);
      float cosSimilarityParamSquaredL2Norm;
      // Build the context as we go though all the computations
      // The following caching is assuming the float vector is immutable, which is the case for compute.
      IdentityHashMap<List<Float>, Float> cachedSquareL2Norm =
          (IdentityHashMap<List<Float>, Float>) context.get(CACHED_SQUARED_L2_NORM_KEY);
      if (cachedSquareL2Norm == null) {
        // Build the cached identity map
        cachedSquareL2Norm = new IdentityHashMap<>();
        context.put(CACHED_SQUARED_L2_NORM_KEY, cachedSquareL2Norm);
      }
      Float cachedResult = cachedSquareL2Norm.get(cosSimilarityParam);
      if (cachedResult != null) {
        cosSimilarityParamSquaredL2Norm = cachedResult;
      } else {
        // Cache the computed result
        cosSimilarityParamSquaredL2Norm = ComputeUtils.squaredL2Norm(cosSimilarityParam);
        cachedSquareL2Norm.put(cosSimilarityParam, cosSimilarityParamSquaredL2Norm);
      }

      // write to result record
      double cosineSimilarityResult =
          dotProductResult / Math.sqrt(valueVectorSquaredL2Norm * cosSimilarityParamSquaredL2Norm);
      putResult(resultRecord, cosineSimilarity.resultFieldName.toString(), (float) cosineSimilarityResult);
    } catch (Exception e) {
      putResult(resultRecord, cosineSimilarity.resultFieldName.toString(), 0.0f);
      String msg = e.getClass().getSimpleName() + " : "
          + (e.getMessage() == null ? "Failed to execute cosine similarity operator." : e.getMessage());
      computationErrorMap.put(cosineSimilarity.resultFieldName.toString(), msg);
    }
  }

  @Override
  public boolean allowFieldValueToBeNull() {
    return true;
  }

  @Override
  public String toString() {
    return "read-compute cosine similarity operator";
  }

  @Override
  public String getOperatorFieldName(ComputeOperation op) {
    CosineSimilarity operation = (CosineSimilarity) op.operation;
    return operation.field.toString();
  }

  @Override
  public String getResultFieldName(ComputeOperation op) {
    CosineSimilarity operation = (CosineSimilarity) op.operation;
    return operation.resultFieldName.toString();
  }
}
