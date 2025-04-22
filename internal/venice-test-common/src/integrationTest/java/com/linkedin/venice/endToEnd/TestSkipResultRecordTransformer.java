package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


public class TestSkipResultRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  private final Map<Integer, String> inMemoryDB = new HashMap<>();

  public TestSkipResultRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    super(storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
  }

  @Override
  public DaVinciRecordTransformerResult<String> transform(Lazy<Integer> key, Lazy<String> value, int partitionId) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.SKIP);
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<String> value, int partitionId) {
    String valueStr = convertUtf8ToString(value.get());
    put(key.get(), valueStr);
  }

  public String convertUtf8ToString(Object valueObj) {
    String valueStr;
    if (valueObj instanceof Utf8) {
      valueStr = valueObj.toString();
    } else {
      valueStr = (String) valueObj;
    }

    return valueStr;
  }

  public boolean isInMemoryDBEmpty() {
    return inMemoryDB.isEmpty();
  }

  public String get(Integer key) {
    return inMemoryDB.get(key);
  }

  public void put(Integer key, String value) {
    inMemoryDB.put(key, value);
  }

  @Override
  public void close() throws IOException {

  }
}
