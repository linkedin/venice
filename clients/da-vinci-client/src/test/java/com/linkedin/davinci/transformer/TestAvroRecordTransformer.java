package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestAvroRecordTransformer extends DaVinciRecordTransformer<Integer, Object, Object> {
  public TestAvroRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
  }

  public Schema getKeyOutputSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getValueOutputSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  public Object transform(Lazy<Integer> key, Lazy<Object> value) {
    return value.get() + "Transformed1";
  }

  public void processPut(Lazy<Integer> key, Lazy<Object> value) {
    return;
  }
}
