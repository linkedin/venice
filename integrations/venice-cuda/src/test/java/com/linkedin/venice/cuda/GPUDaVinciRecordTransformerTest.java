package com.linkedin.venice.cuda;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GPUDaVinciRecordTransformerTest {
  @Test
  public void testNativeLibraryLoading() {
    // This test verifies that the native library loads successfully
    try {
      // Create a simple schema for testing
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.create(Schema.Type.STRING);

      DaVinciRecordTransformerConfig recordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder()
              .setRecordTransformerFunction(
                  (
                      storeVersion,
                      keySchema1,
                      inputValueSchema,
                      outputValueSchema,
                      recordTransformerConfig1) -> new GPUDaVinciRecordTransformer<>(
                          1,
                          keySchema,
                          valueSchema,
                          valueSchema,
                          recordTransformerConfig1,
                          0,
                          1024))
              .setOutputValueClass(String.class)
              .setOutputValueSchema(valueSchema)
              .build();

      // Create transformer instance - this will trigger native library loading
      GPUDaVinciRecordTransformer<String, String, String> transformer = new GPUDaVinciRecordTransformer<>(
          1, // version
          keySchema,
          valueSchema,
          valueSchema,
          recordTransformerConfig,
          0, // GPU device ID
          1024 // initial capacity
      );

      Assert.assertNotNull(transformer);
      System.out.println("Native library loaded successfully!");

    } catch (UnsatisfiedLinkError e) {
      Assert.fail("Failed to load native library: " + e.getMessage());
    } catch (Exception e) {
      Assert.fail("Unexpected error: " + e.getMessage());
    }
  }

  @Test
  public void testGPUInitialization() {
    try {
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.create(Schema.Type.STRING);

      DaVinciRecordTransformerConfig recordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder()
              .setRecordTransformerFunction(
                  (
                      storeVersion,
                      keySchema1,
                      inputValueSchema,
                      outputValueSchema,
                      recordTransformerConfig1) -> new GPUDaVinciRecordTransformer<>(
                          1,
                          keySchema,
                          valueSchema,
                          valueSchema,
                          recordTransformerConfig1,
                          0,
                          1024))
              .setOutputValueClass(String.class)
              .setOutputValueSchema(valueSchema)
              .build();

      GPUDaVinciRecordTransformer<String, String, String> transformer =
          new GPUDaVinciRecordTransformer<>(1, keySchema, valueSchema, valueSchema, recordTransformerConfig, 0, 1024);

      // Trigger GPU initialization
      transformer.onStartVersionIngestion(true);

      // Verify initialization
      Assert.assertTrue(transformer.isInitialized(), "GPU should be initialized");

      // Clean up
      transformer.close();
      Assert.assertFalse(transformer.isInitialized(), "GPU should be shut down after close");

      System.out.println("GPU initialization test passed!");

    } catch (Exception e) {
      Assert.fail("GPU initialization test failed: " + e.getMessage());
    }
  }

  @Test
  public void testBasicPutAndGet() {
    try {
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.create(Schema.Type.STRING);
      DaVinciRecordTransformerConfig recordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder()
              .setRecordTransformerFunction(
                  (
                      storeVersion,
                      keySchema1,
                      inputValueSchema,
                      outputValueSchema,
                      recordTransformerConfig1) -> new GPUDaVinciRecordTransformer<>(
                          1,
                          keySchema,
                          valueSchema,
                          valueSchema,
                          recordTransformerConfig1,
                          0,
                          1024))
              .setOutputValueClass(String.class)
              .setOutputValueSchema(valueSchema)
              .build();

      GPUDaVinciRecordTransformer<String, String, String> transformer =
          new GPUDaVinciRecordTransformer<>(1, keySchema, valueSchema, valueSchema, recordTransformerConfig, 0, 1024);

      // Initialize GPU
      transformer.onStartVersionIngestion(true);

      // Test data
      String testKey = "test-key-1";
      String testValue = "test-value-1";

      // Store data
      transformer.processPut(Lazy.of(() -> testKey), Lazy.of(() -> testValue), 0);

      // Retrieve data
      byte[] retrievedValue = transformer.get(testKey);

      Assert.assertNotNull(retrievedValue, "Value should be retrieved from GPU");
      Assert.assertEquals(new String(retrievedValue), testValue, "Retrieved value should match stored value");

      System.out.println("Basic put/get test passed!");

      // Clean up
      transformer.close();

    } catch (Exception e) {
      Assert.fail("Basic put/get test failed: " + e.getMessage());
    }
  }
}
