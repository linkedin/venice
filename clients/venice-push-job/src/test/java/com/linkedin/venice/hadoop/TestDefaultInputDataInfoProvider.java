package com.linkedin.venice.hadoop;

import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.exceptions.VeniceInvalidInputException;
import com.linkedin.venice.utils.KeyAndValueSchemas;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.file.Files;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class TestDefaultInputDataInfoProvider {
  @Test
  public void testValidInputAtTheBeginningAndModifiedAfter() throws Exception {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.isZstdDictCreationRequired = false;
    pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;
    VeniceProperties props = VeniceProperties.empty();

    try (DefaultInputDataInfoProvider provider = new DefaultInputDataInfoProvider(pushJobSetting, props)) {
      File inputDir = getTempDataDirectory();
      String inputUri = "file://" + inputDir.getAbsolutePath();
      TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);

      InputDataInfoProvider.InputDataInfo inputDataInfo = provider.validateInputAndGetInfo(inputUri);
      assertTrue(inputDataInfo.hasRecords());
      assertEquals(inputDataInfo.getNumInputFiles(), 1);

      try {
        for (File file: inputDir.listFiles()) {
          Files.delete(file.toPath());
        }
        Files.delete(inputDir.toPath());
        provider.getInputLastModificationTime(inputUri);
        fail("Expected VeniceInvalidInputException to be thrown when input directory is deleted.");
      } catch (VeniceInvalidInputException ignored) {
        // not using the default test expectedExceptions to just make sure the test doesn't run into false positive
        // in case the exception is encountered in the first attempt
      }
    }
  }

  @Test(expectedExceptions = VeniceInvalidInputException.class)
  public void testEmptyInputDirectory() throws Exception {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.isZstdDictCreationRequired = false;
    pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;
    VeniceProperties props = VeniceProperties.empty();

    try (DefaultInputDataInfoProvider provider = new DefaultInputDataInfoProvider(pushJobSetting, props)) {
      File inputDir = getTempDataDirectory();
      String inputUri = "file://" + inputDir.getAbsolutePath();
      provider.validateInputAndGetInfo(inputUri);
    }
  }

  @Test
  public void testValidateAvroInput() throws Exception {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.isZstdDictCreationRequired = false;
    pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;

    VeniceProperties props = VeniceProperties.empty();

    try (DefaultInputDataInfoProvider provider = new DefaultInputDataInfoProvider(pushJobSetting, props)) {

      File inputDir = getTempDataDirectory();
      Schema dataSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);

      InputDataInfoProvider.InputDataInfo inputDataInfo =
          provider.validateInputAndGetInfo("file://" + inputDir.getAbsolutePath());
      assertTrue(inputDataInfo.hasRecords());
      assertEquals(inputDataInfo.getNumInputFiles(), 1);
      assertTrue(pushJobSetting.isAvro);

      Schema stringSchema = Schema.create(Schema.Type.STRING);
      String schemaStr = stringSchema.toString();

      assertEquals(pushJobSetting.keySchema, stringSchema);
      assertEquals(pushJobSetting.keySchemaString, schemaStr);
      assertEquals(pushJobSetting.valueSchema, stringSchema);
      assertEquals(pushJobSetting.valueSchemaString, schemaStr);
      assertEquals(pushJobSetting.inputDataSchema, dataSchema);
      assertEquals(pushJobSetting.inputDataSchemaString, dataSchema.toString());
    }
  }

  @Test
  public void testValidateVsonInput() throws Exception {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.isZstdDictCreationRequired = false;
    pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;

    VeniceProperties props = VeniceProperties.empty();

    try (DefaultInputDataInfoProvider provider = new DefaultInputDataInfoProvider(pushJobSetting, props)) {

      File inputDir = getTempDataDirectory();
      KeyAndValueSchemas dataSchema = TestWriteUtils.writeSimpleVsonFile(inputDir);

      InputDataInfoProvider.InputDataInfo inputDataInfo =
          provider.validateInputAndGetInfo("file://" + inputDir.getAbsolutePath());
      assertTrue(inputDataInfo.hasRecords());
      assertEquals(inputDataInfo.getNumInputFiles(), 1);
      assertFalse(pushJobSetting.isAvro);

      assertEquals(pushJobSetting.keySchema, dataSchema.getKey());
      assertEquals(pushJobSetting.keySchemaString, dataSchema.getKey().toString());
      assertEquals(pushJobSetting.valueSchema, dataSchema.getValue());
      assertEquals(pushJobSetting.valueSchemaString, dataSchema.getValue().toString());
      assertNull(pushJobSetting.inputDataSchema);
      assertNull(pushJobSetting.inputDataSchemaString);
    }
  }

  @Test
  public void testTrainZstdDictionary() throws Exception {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.isZstdDictCreationRequired = true;
    pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;

    VeniceProperties props = VeniceProperties.empty();

    try (DefaultInputDataInfoProvider provider = new DefaultInputDataInfoProvider(pushJobSetting, props)) {
      File inputDir = getTempDataDirectory();
      TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
      InputDataInfoProvider.InputDataInfo inputDataInfo =
          provider.validateInputAndGetInfo("file://" + inputDir.getAbsolutePath());
      assertTrue(inputDataInfo.hasRecords());

      byte[] dictionary = provider.trainZstdDictionary();
      assertNotNull(dictionary);
      assertNotEquals(dictionary.length, 0);
    }
  }

  @Test(expectedExceptions = VeniceInvalidInputException.class)
  public void testInvalidInputSource() throws Exception {
    PushJobSetting pushJobSetting = new PushJobSetting();

    VeniceProperties props = VeniceProperties.empty();
    try (DefaultInputDataInfoProvider provider = new DefaultInputDataInfoProvider(pushJobSetting, props)) {
      File inputDir = getTempDataDirectory();
      provider.validateInputAndGetInfo("file://" + inputDir.getAbsolutePath());
    }
  }
}
