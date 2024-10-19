package com.linkedin.venice.endToEnd;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestUnchangedResultRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, Integer> {
  public TestUnchangedResultRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
  }

  public Schema getKeySchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getOutputValueSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public DaVinciRecordTransformerResult<Integer> transform(Lazy<Integer> key, Lazy<Integer> value) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  public void processPut(Lazy<Integer> key, Lazy<Integer> value) {
    return;
  }

}
