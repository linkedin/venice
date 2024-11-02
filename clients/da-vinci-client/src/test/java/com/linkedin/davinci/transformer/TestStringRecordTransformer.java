package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


public class TestStringRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public TestStringRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
  }

  @Override
  public Schema getKeySchema() {
    return Schema.create(Schema.Type.INT);
  }

  @Override
  public Schema getOutputValueSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  @Override
  public DaVinciRecordTransformerResult<String> transform(Lazy<Integer> key, Lazy<String> value) {
    Object valueObj = value.get();
    String valueStr;

    if (valueObj instanceof Utf8) {
      valueStr = valueObj.toString();
    } else {
      valueStr = (String) valueObj;
    }

    String transformedValue = valueStr + "Transformed";

    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, transformedValue);
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<String> value) {
    return;
  }
}
