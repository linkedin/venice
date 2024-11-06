package com.linkedin.davinci.config;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class DaVinciConfigTest {
  public class TestRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, Integer> {
    public TestRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
      super(storeVersion, storeRecordsInDaVinci);
    }

    @Override
    public Schema getKeySchema() {
      return Schema.create(Schema.Type.INT);
    }

    @Override
    public Schema getOutputValueSchema() {
      return Schema.create(Schema.Type.INT);
    }

    @Override
    public DaVinciRecordTransformerResult<Integer> transform(Lazy<Integer> key, Lazy<Integer> value) {
      return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, value.get() + 1);
    }

    @Override
    public void processPut(Lazy<Integer> key, Lazy<Integer> value) {
      return;
    }
  }

  @Test
  public void testRecordTransformerEnabled() {
    DaVinciConfig config = new DaVinciConfig();
    assertFalse(config.isRecordTransformerEnabled());
    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
        (storeVersion) -> new TestRecordTransformer(storeVersion, true),
        Integer.class,
        Schema.create(Schema.Type.INT));
    config.setRecordTransformerConfig(recordTransformerConfig);
    assertTrue(config.isRecordTransformerEnabled());
  }

  @Test
  public void testGetAndSetRecordTransformer() {
    Integer testStoreVersion = 1;
    DaVinciConfig config = new DaVinciConfig();
    assertNull(config.getRecordTransformer(testStoreVersion));
    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
        (storeVersion) -> new TestRecordTransformer(storeVersion, true),
        Integer.class,
        Schema.create(Schema.Type.INT));
    config.setRecordTransformerConfig(recordTransformerConfig);
    assertNotNull(config.getRecordTransformer(testStoreVersion));
  }

}
