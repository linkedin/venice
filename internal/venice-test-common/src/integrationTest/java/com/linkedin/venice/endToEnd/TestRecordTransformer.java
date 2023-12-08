package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.TransformedRecord;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;


public class TestRecordTransformer implements DaVinciRecordTransformer<Integer, Integer, TransformedRecord> {
  Schema dummySchema =
      SchemaBuilder.record("DummyRecord").fields().name("field1").type().intType().noDefault().endRecord();

  public Schema getKeyOutputSchema() {
    return dummySchema;
  }

  public Schema getValueOutputSchema() {
    return dummySchema;
  }

  public TransformedRecord put(Lazy<Integer> key, Lazy<Integer> value) {
    Integer valueInteger = value.get();
    Integer transformedValue = valueInteger * 5;
    TransformedRecord transformedRecord = new TransformedRecord<Integer, Integer>();
    transformedRecord.setValue(transformedValue);

    return transformedRecord;
  }

}
