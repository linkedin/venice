package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


/**
 * Transforms int values to strings
 */
public class TestIntToStringRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, String> {
  public TestIntToStringRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema originalValueSchema,
      Schema outputValueSchema,
      boolean storeRecordsInDaVinci) {
    super(storeVersion, keySchema, originalValueSchema, outputValueSchema, storeRecordsInDaVinci);
  }

  @Override
  public DaVinciRecordTransformerResult<String> transform(Lazy<Integer> key, Lazy<Integer> value) {
    String valueStr = value.get().toString();
    String transformedValue = valueStr + "Transformed";
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, transformedValue);
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<String> value) {
    return;
  }
}
