package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class TestRecordTransformerUsingUniformInputValueSchema
    extends DaVinciRecordTransformer<GenericRecord, GenericRecord, GenericRecord> {
  public TestRecordTransformerUsingUniformInputValueSchema(
      String storeName,
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    super(storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
  }

  @Override
  public DaVinciRecordTransformerResult<GenericRecord> transform(
      Lazy<GenericRecord> key,
      Lazy<GenericRecord> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Override
  public void processPut(
      Lazy<GenericRecord> key,
      Lazy<GenericRecord> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    return;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean useUniformInputValueSchema() {
    return true;
  }
}
