package com.linkedin.venice.compute;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import com.linkedin.venice.schema.avro.ComputablePrimitiveFloatList;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public class CosineSimilarityOperator implements ReadComputeOperator {
  @Override
  public void compute(ComputeOperation op, GenericRecord valueRecord, GenericRecord resultRecord, Map<String, String> computationErrorMap) {
    CosineSimilarity cosineSimilarity = (CosineSimilarity) op.operation;
    try {
      ComputablePrimitiveFloatList valueVector = (ComputablePrimitiveFloatList) valueRecord.get(cosineSimilarity.field.toString());
      ComputablePrimitiveFloatList cosSimilarityParam = (ComputablePrimitiveFloatList) cosineSimilarity.cosSimilarityParam;

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

      double dotProductResult = 0.0;
      for (int i = 0; i < cosSimilarityParam.size(); i++) {
        dotProductResult += cosSimilarityParam.getPrimitive(i) * valueVector.getPrimitive(i);
      }

      // write to result record
      resultRecord.put(cosineSimilarity.resultFieldName.toString(), dotProductResult / Math.sqrt(valueVector.squaredL2Norm() * cosSimilarityParam.squaredL2Norm()));
    } catch (Exception e) {
      resultRecord.put(cosineSimilarity.resultFieldName.toString(), 0.0d);
      computationErrorMap.put(cosineSimilarity.resultFieldName.toString(), e.getMessage());
    }
  }
}
