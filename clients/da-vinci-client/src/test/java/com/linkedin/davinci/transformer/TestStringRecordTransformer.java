package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.TransformedRecord;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestStringRecordTransformer
    implements DaVinciRecordTransformer<Integer, Object, TransformedRecord<Integer, Object>> {
  public Schema getKeyOutputSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getValueOutputSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  public TransformedRecord<Integer, Object> put(Lazy<Integer> key, Lazy<Object> value) {
    TransformedRecord<Integer, Object> transformedRecord = new TransformedRecord<>();
    transformedRecord.setKey(key.get());
    transformedRecord.setValue(value.get() + "Transformed");
    return transformedRecord;
  }
}
