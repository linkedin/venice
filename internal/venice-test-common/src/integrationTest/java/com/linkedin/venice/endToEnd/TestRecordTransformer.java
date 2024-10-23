package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;


public class TestRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, Integer> {
  private final Map<Integer, Integer> inMemoryDb = new HashMap<>();

  public TestRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
  }

  public Schema getKeySchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getOutputValueSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public DaVinciRecordTransformerResult<Integer> transform(Lazy<Integer> key, Lazy<Integer> value) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, value.get() * 100);
  }

  public void processPut(Lazy<Integer> key, Lazy<Integer> value) {
    put(key.get(), value.get());
  }

  public void clearInMemoryDb() {
    inMemoryDb.clear();
  }

  public boolean isInMemoryDbEmpty() {
    return inMemoryDb.isEmpty();
  }

  public Integer get(Integer key) {
    return inMemoryDb.get(key);
  }

  public Integer put(Integer key, Integer value) {
    return inMemoryDb.put(key, value);
  }

}
