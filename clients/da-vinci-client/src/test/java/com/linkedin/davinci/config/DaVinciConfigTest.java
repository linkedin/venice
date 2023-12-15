package com.linkedin.davinci.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.TransformedRecord;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class DaVinciConfigTest {
  public class TestRecordTransformer
      implements DaVinciRecordTransformer<Integer, Integer, TransformedRecord<Integer, Integer>> {
    public Schema getKeyOutputSchema() {
      return Schema.create(Schema.Type.INT);
    }

    public Schema getValueOutputSchema() {
      return Schema.create(Schema.Type.INT);
    }

    public TransformedRecord<Integer, Integer> put(Lazy<Integer> key, Lazy<Integer> value) {
      TransformedRecord<Integer, Integer> transformedRecord = new TransformedRecord<>();
      transformedRecord.setKey(key.get());
      transformedRecord.setValue(value.get() + 1);
      return transformedRecord;
    }
  }

  @Test
  public void testRecordTransformerEnabled() {
    DaVinciRecordTransformer transformer = new TestRecordTransformer();
    DaVinciConfig config = new DaVinciConfig();
    assertFalse(config.isRecordTransformerEnabled());
    config.setRecordTransformer(transformer);
    assertTrue(config.isRecordTransformerEnabled());
  }

  @Test
  public void testGetAndSetRecordTransformer() {
    DaVinciRecordTransformer transformer = new TestRecordTransformer();
    DaVinciConfig config = new DaVinciConfig();
    assertNull(config.getRecordTransformer());
    config.setRecordTransformer(transformer);
    assertEquals(transformer, config.getRecordTransformer());
  }

}
