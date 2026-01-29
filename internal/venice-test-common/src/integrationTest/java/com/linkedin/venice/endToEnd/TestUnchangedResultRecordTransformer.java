package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import org.apache.avro.Schema;


public class TestUnchangedResultRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public TestUnchangedResultRecordTransformer(
      String storeName,
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    super(storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
  }

  @Override
  public DaVinciRecordTransformerResult<String> transform(
      Lazy<Integer> key,
      Lazy<String> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Override
  public void processPut(
      Lazy<Integer> key,
      Lazy<String> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    return;
  }

  @Override
  public void close() throws IOException {

  }
}
