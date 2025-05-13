package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import org.apache.avro.Schema;


public class TestSpecificRecordTransformer
    extends DaVinciRecordTransformer<Integer, TestSpecificValue, TestSpecificValue> {
  public TestSpecificRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    super(storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
  }

  @Override
  public DaVinciRecordTransformerResult<TestSpecificValue> transform(
      Lazy<Integer> key,
      Lazy<TestSpecificValue> lazyValue,
      int partitionId) {
    TestSpecificValue value = lazyValue.get();
    value.firstName = value.firstName.toString().toUpperCase();
    value.lastName = value.lastName.toString().toUpperCase();

    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, value);
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<TestSpecificValue> value, int partitionId) {
    return;
  }

  @Override
  public void close() throws IOException {

  }
}
