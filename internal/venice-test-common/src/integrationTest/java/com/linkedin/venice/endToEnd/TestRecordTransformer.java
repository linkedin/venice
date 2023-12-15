package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.TransformedRecord;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestRecordTransformer implements DaVinciRecordTransformer<Integer, Integer, TransformedRecord> {
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

  public TransformedRecord put(Lazy<Integer> key, Lazy<Integer> value) {
    TransformedRecord transformedRecord = new TransformedRecord<Integer, Integer>();
    transformedRecord.setValue(value.get() * 100);

    return transformedRecord;
  }

}
