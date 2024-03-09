package com.linkedin.davinci.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class DaVinciConfigTest {
  public class TestRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, Integer> {
    public TestRecordTransformer(int storeVersion) {
      super(storeVersion);
    }

    public Schema getKeyOutputSchema() {
      return Schema.create(Schema.Type.INT);
    }

    public Schema getValueOutputSchema() {
      return Schema.create(Schema.Type.INT);
    }

    public Integer put(Lazy<Integer> value) {
      return value.get() + 1;
    }
  }

  @Test
  public void testRecordTransformerEnabled() {
    DaVinciConfig config = new DaVinciConfig();
    assertFalse(config.isRecordTransformerEnabled());
    config.setRecordTransformerSupplier(() -> new TestRecordTransformer(0));
    assertTrue(config.isRecordTransformerEnabled());
  }

  @Test
  public void testGetAndSetRecordTransformer() {
    DaVinciRecordTransformer transformer = new TestRecordTransformer(0);
    DaVinciConfig config = new DaVinciConfig();
    assertNull(config.getRecordTransformer());
    config.setRecordTransformerSupplier(() -> transformer);
    assertEquals(transformer, config.getRecordTransformer());
  }

}
