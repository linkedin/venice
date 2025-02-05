package com.linkedin.davinci.transformer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class DaVinciRecordTransformerConfigTest {
  @Test
  public void testRecordTransformerFunctionRequired() {
    assertThrows(VeniceException.class, () -> new DaVinciRecordTransformerConfig.Builder().build());
  }

  @Test
  public void testOutputValueClassAndSchemaBothRequired() {
    assertThrows(
        VeniceException.class,
        () -> new DaVinciRecordTransformerConfig.Builder()
            .setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setOutputValueClass(String.class)
            .build());

    assertThrows(
        VeniceException.class,
        () -> new DaVinciRecordTransformerConfig.Builder()
            .setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setOutputValueSchema(Schema.create(Schema.Type.STRING))
            .build());
  }

  @Test
  public void testDefaults() {
    Class outputValueClass = String.class;
    Schema outputValueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setOutputValueClass(outputValueClass)
            .setOutputValueSchema(outputValueSchema)
            .build();

    assertNotNull(recordTransformerConfig.getRecordTransformerFunction());
    assertEquals(recordTransformerConfig.getOutputValueClass(), outputValueClass);
    assertEquals(recordTransformerConfig.getOutputValueSchema(), outputValueSchema);
    assertTrue(recordTransformerConfig.getStoreRecordsInDaVinci());
    assertFalse(recordTransformerConfig.getAlwaysBootstrapFromVersionTopic());
  }

  @Test
  public void testNonDefaults() {
    Class outputValueClass = String.class;
    Schema outputValueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setOutputValueClass(outputValueClass)
            .setOutputValueSchema(outputValueSchema)
            .setStoreRecordsInDaVinci(false)
            .setAlwaysBootstrapFromVersionTopic(true)
            .build();

    assertNotNull(recordTransformerConfig.getRecordTransformerFunction());
    assertEquals(recordTransformerConfig.getOutputValueClass(), outputValueClass);
    assertEquals(recordTransformerConfig.getOutputValueSchema(), outputValueSchema);
    assertFalse(recordTransformerConfig.getStoreRecordsInDaVinci());
    assertTrue(recordTransformerConfig.getAlwaysBootstrapFromVersionTopic());
  }

  @Test
  public void testUndefinedOutputValueClassAndSchema() {
    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .build();

    assertNotNull(recordTransformerConfig.getRecordTransformerFunction());
    assertNull(recordTransformerConfig.getOutputValueClass());
    assertNull(recordTransformerConfig.getOutputValueSchema());
    assertTrue(recordTransformerConfig.getStoreRecordsInDaVinci());
    assertFalse(recordTransformerConfig.getAlwaysBootstrapFromVersionTopic());
  }
}
