package com.linkedin.davinci.config;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerFunctionalInterface;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class DaVinciConfigTest {
  public class TestRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, Integer> {
    public TestRecordTransformer(
        String storeName,
        int storeVersion,
        Schema keySchema,
        Schema inputValueSchema,
        Schema outputValueSchema,
        DaVinciRecordTransformerConfig recordTransformerConfig) {
      super(storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
    }

    @Override
    public DaVinciRecordTransformerResult<Integer> transform(
        Lazy<Integer> key,
        Lazy<Integer> value,
        int partitionId,
        DaVinciRecordTransformerRecordMetadata recordMetadata) {
      return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, value.get() + 1);
    }

    @Override
    public void processPut(
        Lazy<Integer> key,
        Lazy<Integer> value,
        int partitionId,
        DaVinciRecordTransformerRecordMetadata recordMetadata) {
      return;
    }

    @Override
    public void close() throws IOException {

    }
  }

  @Test
  public void testRecordTransformerEnabled() {
    DaVinciConfig config = new DaVinciConfig();
    assertFalse(config.isRecordTransformerEnabled());

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestRecordTransformer::new).build();

    DaVinciRecordTransformerFunctionalInterface recordTransformerFunction =
        (storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config1) -> new TestRecordTransformer(
            storeName,
            storeVersion,
            keySchema,
            inputValueSchema,
            outputValueSchema,
            dummyRecordTransformerConfig);

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(recordTransformerFunction).build();
    config.setRecordTransformerConfig(recordTransformerConfig);

    assertTrue(config.isRecordTransformerEnabled());
  }

  @Test
  public void testGetAndSetRecordTransformer() {
    DaVinciConfig config = new DaVinciConfig();
    assertNull(config.getRecordTransformerConfig());

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestRecordTransformer::new).build();

    DaVinciRecordTransformerFunctionalInterface recordTransformerFunction =
        (storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config1) -> new TestRecordTransformer(
            storeName,
            storeVersion,
            keySchema,
            inputValueSchema,
            outputValueSchema,
            dummyRecordTransformerConfig);

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(recordTransformerFunction).build();
    config.setRecordTransformerConfig(recordTransformerConfig);

    assertNotNull(config.getRecordTransformerConfig());
  }

  @Test
  public void testDisableBlockCacheDefaultsToFalse() {
    DaVinciConfig config = new DaVinciConfig();
    assertFalse(config.isDisableBlockCache(), "disableBlockCache should default to false");
  }

  @Test
  public void testDisableBlockCacheSetterGetter() {
    DaVinciConfig config = new DaVinciConfig();
    assertFalse(config.isDisableBlockCache());

    config.setDisableBlockCache(true);
    assertTrue(config.isDisableBlockCache());

    config.setDisableBlockCache(false);
    assertFalse(config.isDisableBlockCache());
  }

}
