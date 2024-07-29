package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, Integer> {
  public TestRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
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

  public Integer transform(Lazy<Integer> key, Lazy<Integer> value) {
    return value.get() * 100;
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<Integer> value) {
    return;
  }

}
