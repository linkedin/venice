package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, Integer> {
  public TestRecordTransformer(int storeVersion) {
    super(storeVersion);
  }

  Schema originalSchema;

  public Schema getKeyOutputSchema() {
    return originalSchema;
  }

  public Schema getValueOutputSchema() {
    return originalSchema;
  }

  public void setOriginalSchema(Schema schema) {
    this.originalSchema = schema;
  }

  public Integer put(Lazy<Integer> key, Lazy<Integer> value) {
    return value.get() * 100;
  }

}
