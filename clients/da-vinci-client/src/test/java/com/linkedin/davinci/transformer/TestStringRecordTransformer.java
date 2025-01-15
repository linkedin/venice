package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;


public class TestStringRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public TestStringRecordTransformer(
      int storeVersion,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      boolean storeRecordsInDaVinci) {
    super(storeVersion, recordTransformerConfig, storeRecordsInDaVinci);
  }

  @Override
  public DaVinciRecordTransformerResult<String> transform(Lazy<Integer> key, Lazy<String> value) {
    Object valueObj = value.get();
    String valueStr = valueObj.toString();

    // if (valueObj instanceof Utf8) {
    // valueStr = valueObj.toString();
    // } else {
    // valueStr = (String) valueObj;
    // }

    String transformedValue = valueStr + "Transformed";

    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, transformedValue);
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<String> value) {
    return;
  }
}
