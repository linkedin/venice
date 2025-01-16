package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestUnchangedResultRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public TestUnchangedResultRecordTransformer(
      int storeVersion,

      Schema keySchema,
      Schema originalValueSchema,
      Schema outputValueSchema,
      boolean storeRecordsInDaVinci) {
    super(storeVersion, keySchema, originalValueSchema, outputValueSchema, storeRecordsInDaVinci);
  }

  @Override
  public DaVinciRecordTransformerResult<String> transform(Lazy<Integer> key, Lazy<String> value) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<String> value) {
    return;
  }

}
