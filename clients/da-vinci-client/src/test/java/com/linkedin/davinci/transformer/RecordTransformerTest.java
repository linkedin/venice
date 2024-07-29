package com.linkedin.davinci.transformer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class RecordTransformerTest {
  @Test
  public void testRecordTransformer() {
    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(0, true);

    assertEquals(recordTransformer.getStoreVersion(), 0);

    Schema keyOutputSchema = recordTransformer.getKeyOutputSchema();
    assertEquals(keyOutputSchema.getType(), Schema.Type.INT);

    Schema valueOutputSchema = recordTransformer.getValueOutputSchema();
    assertEquals(valueOutputSchema.getType(), Schema.Type.STRING);

    Lazy<Integer> lazyKey = Lazy.of(() -> 42);
    Lazy<String> lazyValue = Lazy.of(() -> "SampleValue");
    String transformedRecord = recordTransformer.transform(lazyKey, lazyValue);
    recordTransformer.processPut(lazyKey, lazyValue);
    assertEquals(transformedRecord, "SampleValueTransformed");

    recordTransformer.processDelete(lazyKey);
    String deletedRecord = recordTransformer.processDelete(lazyKey);
    assertNull(deletedRecord);

    assertTrue(recordTransformer.getStoreRecordsInDaVinci());

    assertEquals(recordTransformer.getOutputValueClass(), String.class);

    int classHash = recordTransformer.getClassHash();
    assertNotNull(classHash);
    assertFalse(recordTransformer.hasTransformationLogicChanged(classHash));
  }

}
