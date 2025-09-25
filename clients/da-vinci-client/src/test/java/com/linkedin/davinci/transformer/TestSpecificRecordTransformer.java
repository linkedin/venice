package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import org.apache.avro.Schema;


public class TestSpecificRecordTransformer
    extends DaVinciRecordTransformer<TestSpecificKey, TestSpecificValue, TestSpecificValue> {
  public TestSpecificRecordTransformer(
      String storeName,
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    super(storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
  }

  @Override
  public DaVinciRecordTransformerResult<TestSpecificValue> transform(
      Lazy<TestSpecificKey> lazyKey,
      Lazy<TestSpecificValue> lazyValue,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    TestSpecificKey key = lazyKey.get();
    int id = key.id;

    TestSpecificValue value = lazyValue.get();

    value.firstName = value.firstName.toString() + id;
    value.lastName = value.lastName.toString() + id;

    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, value);
  }

  @Override
  public void processPut(
      Lazy<TestSpecificKey> key,
      Lazy<TestSpecificValue> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    return;
  }

  @Override
  public void close() throws IOException {

  }
}
