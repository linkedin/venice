package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


public class TestStringRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public TestStringRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
  }

  public Schema getKeyOutputSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getValueOutputSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  public String transform(Lazy<Integer> key, Lazy<String> value) {
    Object valueObj = value.get();
    String valueStr;

    if (valueObj instanceof Utf8) {
      valueStr = valueObj.toString();
    } else {
      valueStr = (String) valueObj;
    }

    return valueStr + "Transformed";
  }

  public void processPut(Lazy<Integer> key, Lazy<String> value) {
    return;
  }
}
