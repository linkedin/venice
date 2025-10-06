package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


/**
 * Transforms int values to strings
 */
public class TestIntToStringRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, String> {
  private final Map<Integer, String> inMemoryDB = new HashMap<>();
  private int transformInvocationCount = 0;

  public TestIntToStringRecordTransformer(
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
      Lazy<Integer> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    String valueStr = value.get().toString();
    String transformedValue = valueStr + "Transformed";
    transformInvocationCount++;
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, transformedValue);
  }

  @Override
  public void processPut(
      Lazy<Integer> key,
      Lazy<String> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    String valueStr = convertUtf8ToString(value.get());
    put(key.get(), valueStr);
  }

  @Override
  public void processDelete(Lazy<Integer> key, int partitionId, DaVinciRecordTransformerRecordMetadata recordMetadata) {
    delete(key.get());
  };

  private String convertUtf8ToString(Object valueObj) {
    String valueStr;
    if (valueObj instanceof Utf8) {
      valueStr = valueObj.toString();
    } else {
      valueStr = (String) valueObj;
    }

    return valueStr;
  }

  public void clearInMemoryDB() {
    inMemoryDB.clear();
  }

  public boolean isInMemoryDBEmpty() {
    return inMemoryDB.isEmpty();
  }

  public String get(Integer key) {
    return inMemoryDB.get(key);
  }

  private void put(Integer key, String value) {
    inMemoryDB.put(key, value);
  }

  private void delete(Integer key) {
    if (!inMemoryDB.containsKey(key)) {
      throw new IllegalArgumentException("Key not found: " + key);
    }
    inMemoryDB.remove(key);
  }

  public int getTransformInvocationCount() {
    return transformInvocationCount;
  }

  @Override
  public void close() throws IOException {

  }
}
