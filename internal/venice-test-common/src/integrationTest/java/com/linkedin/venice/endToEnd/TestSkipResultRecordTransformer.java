package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestSkipResultRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, Integer> {
  public TestSkipResultRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
  }

  public Schema getKeySchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getOutputValueSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public DaVinciRecordTransformerResult<Integer> transform(Lazy<Integer> key, Lazy<Integer> value) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.SKIP);
  }

  public void processPut(Lazy<Integer> key, Lazy<Integer> value) {
    return;
  }

}
