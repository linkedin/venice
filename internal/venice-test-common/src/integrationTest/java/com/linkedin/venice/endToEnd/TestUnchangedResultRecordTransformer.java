package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestUnchangedResultRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public TestUnchangedResultRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
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
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<String> value) {
    return;
  }

}
