package com.linkedin.venice.hadoop;

import static com.linkedin.venice.utils.TestPushUtils.getTempDataDirectory;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.Time;
import java.io.File;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class TestValidateSchemaAndBuildDictMapperOutputReader {
  private static final int TEST_TIMEOUT = 10 * Time.MS_PER_SECOND;
  private static final Schema fileSchema = ValidateSchemaAndBuildDictMapperOutput.getClassSchema();
  private final File inputDir = getTempDataDirectory();

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "File should not be null")
  public void testGetWithNullAsFile() throws Exception {
    new ValidateSchemaAndBuildDictMapperOutputReader(null);
  }

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Encountered exception reading Avro data from.*")
  public void testGetWithNoFile() throws Exception {
    String avroOutputFile = "nofile.avro"; // This file is not present
    new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath() + '/' + avroOutputFile);
  }

  /**
   * The file has only the schema and no data
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "File .* contains no records.*")
  public void testGetWithEmptyFile() throws Exception {
    String avroOutputFile = "empty_file.avro";
    TestPushUtils.writeEmptyAvroFileWithUserSchema(inputDir, avroOutputFile, fileSchema.toString());
    new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath() + '/' + avroOutputFile);
  }

  /**
   * The file can be empty or invalid avro file
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Encountered exception reading Avro data from.*")
  public void testGetWithInvalidAvroFile() throws Exception {
    String avroOutputFile = "invalid_file.avro";
    TestPushUtils.writeInvalidAvroFile(inputDir, avroOutputFile);
    new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath() + '/' + avroOutputFile);
  }

  /**
   * inputFileDataSize should be > 0. Even for empty pushes, the file should have schema details
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Retrieved inputFileDataSize .* is not valid")
  public void testGetWithInvalidInputFileDataSize() throws Exception {
    String avroOutputFile = "valid_file.avro";
    TestPushUtils.writeSimpleAvroFileForValidateSchemaAndBuildDictMapperOutput(
        inputDir,
        avroOutputFile,
        0,
        ByteBuffer.wrap("TestDictionary".getBytes()),
        fileSchema.toString());
    new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath() + '/' + avroOutputFile);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetWithValidInputFileDataSize() throws Exception {
    String avroOutputFile = "valid_file.avro";
    TestPushUtils.writeSimpleAvroFileForValidateSchemaAndBuildDictMapperOutput(
        inputDir,
        avroOutputFile,
        1,
        ByteBuffer.wrap("TestDictionary".getBytes()),
        fileSchema.toString());
    new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath() + '/' + avroOutputFile);
  }

  /**
   * Should not Fail as the zstdDictionary is optional in the schema
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testGetWithNoZstdDictionary() throws Exception {
    String avroOutputFile = "valid_file.avro";
    TestPushUtils.writeSimpleAvroFileForValidateSchemaAndBuildDictMapperOutput(
        inputDir,
        avroOutputFile,
        1,
        null,
        fileSchema.toString());
    new ValidateSchemaAndBuildDictMapperOutputReader(inputDir.getAbsolutePath() + '/' + avroOutputFile);
  }
}
