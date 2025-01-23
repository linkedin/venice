package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


public class TestStringRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  private final Map<Integer, String> inMemoryDB = new HashMap<>();
  private int transformInvocationCount = 0;

  public TestStringRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      boolean storeRecordsInDaVinci) {
    super(storeVersion, keySchema, inputValueSchema, outputValueSchema, storeRecordsInDaVinci);
  }

  @Override
  public DaVinciRecordTransformerResult<String> transform(Lazy<Integer> key, Lazy<String> value) {
    String valueStr = convertUtf8ToString(value.get());
    String transformedValue = valueStr + "Transformed";
    transformInvocationCount++;
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, transformedValue);
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<String> value) {
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

  public void clearInMemoryDB() {
    inMemoryDB.clear();
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

  public int getTransformInvocationCount() {
    return transformInvocationCount;
  }

  @Override
  public void close() throws IOException {

  }
}
