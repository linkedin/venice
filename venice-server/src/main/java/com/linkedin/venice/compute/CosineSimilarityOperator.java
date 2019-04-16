package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;

import static com.linkedin.venice.compute.ComputeOperationUtils.*;


public class CosineSimilarityOperator implements ReadComputeOperator {
  @Override
  public void compute(ComputeOperation op, GenericRecord valueRecord, GenericRecord resultRecord,
      Map<String, String> computationErrorMap, Map<String, Object> context) {
    CosineSimilarity cosineSimilarity = (CosineSimilarity) op.operation;
    try {
      List<Float> valueVector = (List<Float>) valueRecord.get(cosineSimilarity.field.toString());
      List<Float> cosSimilarityParam = cosineSimilarity.cosSimilarityParam;

      if (valueVector.size() == 0 || cosSimilarityParam.size() == 0) {
        resultRecord.put(cosineSimilarity.resultFieldName.toString(), 0.0d);
        return;
      } else if (valueVector.size() != cosSimilarityParam.size()) {
        resultRecord.put(cosineSimilarity.resultFieldName.toString(), 0.0d);
        computationErrorMap.put(cosineSimilarity.resultFieldName.toString(),
            "Failed to compute because size of dot product parameter is: " + cosineSimilarity.cosSimilarityParam.size() +
                " while the size of value vector(" + cosineSimilarity.field.toString() + ") is: " + valueVector.size());
        return;
      }

      double dotProductResult = ComputeOperationUtils.dotProduct(cosSimilarityParam, valueVector);
      double valueVectorSquaredL2Norm = ComputeOperationUtils.squaredL2Norm(valueVector);
      double cosSimilarityParamSquaredL2Norm;
      // Build the context as we go though all the computations
      // The following caching is assuming the float vector is immutable, which is the case for compute.
      IdentityHashMap<List<Float>, Double> cachedSquareL2Norm = (IdentityHashMap<List<Float>, Double>)context.get(
          CACHED_SQUARED_L2_NORM_KEY);
      if (cachedSquareL2Norm == null) {
        // Build the cached identity map
        cachedSquareL2Norm = new IdentityHashMap<>();
        context.put(CACHED_SQUARED_L2_NORM_KEY, cachedSquareL2Norm);
      }
      Double cachedResult = cachedSquareL2Norm.get(cosSimilarityParam);
      if (cachedResult != null) {
        cosSimilarityParamSquaredL2Norm = cachedResult;
      } else {
        // Cache the computed result
        cosSimilarityParamSquaredL2Norm = ComputeOperationUtils.squaredL2Norm(cosSimilarityParam);
        cachedSquareL2Norm.put(cosSimilarityParam, cosSimilarityParamSquaredL2Norm);
      }

      // write to result record
      resultRecord.put(cosineSimilarity.resultFieldName.toString(), dotProductResult / Math.sqrt(valueVectorSquaredL2Norm * cosSimilarityParamSquaredL2Norm));
    } catch (Exception e) {
      resultRecord.put(cosineSimilarity.resultFieldName.toString(), 0.0d);
      computationErrorMap.put(cosineSimilarity.resultFieldName.toString(), e.getMessage());
    }
  }
}
