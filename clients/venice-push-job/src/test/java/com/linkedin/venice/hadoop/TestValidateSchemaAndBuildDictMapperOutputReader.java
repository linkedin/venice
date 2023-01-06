package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestValidateSchemaAndBuildDictMapperOutputReader {
  private static final int TEST_TIMEOUT = 10 * Time.MS_PER_SECOND;
  private static final Schema fileSchema = ValidateSchemaAndBuildDictMapperOutput.getClassSchema();

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = ".* output directory should not be empty")
  public void testGetWithDirAsNull() throws Exception {
    ValidateSchemaAndBuildDictMapperOutputReader reader = new ValidateSchemaAndBuildDictMapperOutputReader(null, null);
    reader.close();
  }

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".* output directory should not be empty")
  public void testGetWithDirAsEmpty() throws Exception {
    ValidateSchemaAndBuildDictMapperOutputReader reader = new ValidateSchemaAndBuildDictMapperOutputReader("", null);
    reader.close();
  }

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = ".* output fileName should not be empty")
  public void testGetWithFileAsNull() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    ValidateSchemaAndBuildDictMapperOutputReader reader =
        new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath(), null);
    reader.close();
  }

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".* output fileName should not be empty")
  public void testGetWithFileAsEmpty() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    ValidateSchemaAndBuildDictMapperOutputReader reader =
        new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath(), "");
    reader.close();
  }

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Encountered exception reading Avro data from.*")
  public void testGetWithNoFile() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    String avroOutputFile = "nofile.avro"; // This file is not present
    ValidateSchemaAndBuildDictMapperOutputReader reader =
        new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath(), avroOutputFile);
    reader.close();
  }

  /**
   * The file has only the schema and no data
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "File .* contains no records.*")
  public void testGetWithEmptyFile() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    String avroOutputFile = "empty_file.avro";
    TestWriteUtils.writeEmptyAvroFileWithUserSchema(inputDir, avroOutputFile, fileSchema.toString());
    ValidateSchemaAndBuildDictMapperOutputReader reader =
        new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath(), avroOutputFile);
    reader.close();
  }

  /**
   * The file can be empty or invalid avro file
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Encountered exception reading Avro data from.*")
  public void testGetWithInvalidAvroFile() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    String avroOutputFile = "invalid_file.avro";
    TestWriteUtils.writeInvalidAvroFile(inputDir, avroOutputFile);
    ValidateSchemaAndBuildDictMapperOutputReader reader =
        new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath(), avroOutputFile);
    reader.close();
  }

  /**
   * inputFileDataSize should be > 0. Even for empty pushes, the file should have schema details
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Retrieved inputFileDataSize .* is not valid")
  public void testGetWithInvalidInputFileDataSize() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    String avroOutputFile = "valid_file.avro";
    TestWriteUtils.writeSimpleAvroFileForValidateSchemaAndBuildDictMapperOutput(
        inputDir,
        avroOutputFile,
        0,
        ByteBuffer.wrap("TestDictionary".getBytes()),
        fileSchema.toString());
    ValidateSchemaAndBuildDictMapperOutputReader reader =
        new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath(), avroOutputFile);
    reader.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetWithValidInputFileDataSize() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    String avroOutputFile = "valid_file.avro";
    TestWriteUtils.writeSimpleAvroFileForValidateSchemaAndBuildDictMapperOutput(
        inputDir,
        avroOutputFile,
        1,
        ByteBuffer.wrap("TestDictionary".getBytes()),
        fileSchema.toString());
    ValidateSchemaAndBuildDictMapperOutputReader reader =
        new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath(), avroOutputFile);
    ValidateSchemaAndBuildDictMapperOutput output = reader.getOutput();
    reader.close();

    // validate the data
    Assert.assertEquals(output.getInputFileDataSize(), 1);
    Assert.assertEquals(output.getZstdDictionary(), ByteBuffer.wrap("TestDictionary".getBytes()));
  }

  /**
   * Should not Fail as the zstdDictionary is optional in the schema
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testGetWithNoZstdDictionary() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    String avroOutputFile = "valid_file.avro";
    TestWriteUtils.writeSimpleAvroFileForValidateSchemaAndBuildDictMapperOutput(
        inputDir,
        avroOutputFile,
        1,
        null,
        fileSchema.toString());
    ValidateSchemaAndBuildDictMapperOutputReader reader =
        new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath(), avroOutputFile);
    ValidateSchemaAndBuildDictMapperOutput output = reader.getOutput();
    reader.close();

    // validate the data
    Assert.assertEquals(output.getInputFileDataSize(), 1);
    Assert.assertNull(output.getZstdDictionary());
  }
}
