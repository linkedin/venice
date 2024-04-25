package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestStringRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public TestStringRecordTransformer(int storeVersion) {
    super(storeVersion);
  }

  public Schema getKeyOutputSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getValueOutputSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  public String put(Lazy<Integer> key, Lazy<String> value) {
    return value.get() + "Transformed";
  }
}
