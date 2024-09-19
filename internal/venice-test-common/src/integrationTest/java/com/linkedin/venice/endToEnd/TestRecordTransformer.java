package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, Integer> {
  public TestRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
  }

  public Schema getKeySchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getOutputValueSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Integer transform(Lazy<Integer> key, Lazy<Integer> value) {
    return value.get() * 100;
  }

  public void processPut(Lazy<Integer> key, Lazy<Integer> value) {
    return;
  }

}
