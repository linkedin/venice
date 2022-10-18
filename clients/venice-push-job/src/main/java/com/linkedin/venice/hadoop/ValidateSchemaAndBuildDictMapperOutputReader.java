package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class reads the data(total input size in bytes and zstd dictionary) persisted in HDFS
 * by {@link ValidateSchemaAndBuildDictMapper} based on the schema {@link ValidateSchemaAndBuildDictMapperOutput}
 */
public class ValidateSchemaAndBuildDictMapperOutputReader {
  private static final Logger LOGGER = LogManager.getLogger(ValidateSchemaAndBuildDictMapperOutputReader.class);
  private ValidateSchemaAndBuildDictMapperOutput output;

  public ValidateSchemaAndBuildDictMapperOutputReader(String file) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    if (file != null) {
      try {
        InputStream inputStream = fs.open(new Path(file));
        DataFileStream avroDataFileStream =
            new DataFileStream(inputStream, new SpecificDatumReader(ValidateSchemaAndBuildDictMapperOutput.class));
        try {
          output = (ValidateSchemaAndBuildDictMapperOutput) avroDataFileStream.next();
        } catch (NoSuchElementException e) {
          throw new VeniceException("File " + file + " contains no records", e);
        }
      } catch (IOException e) {
        throw new VeniceException(
            "Encountered exception reading Avro data from " + file
                + ". Check if the file exists and the data is in Avro format.",
            e);
      }
      validateOutput();
    } else {
      throw new VeniceException("File should not be null");
    }
  }

  private void validateOutput() throws VeniceException {
    validateInputFileDataSizeFromOutput();
    validateCompressionDictionaryFromOutput();
  }

  /**
   * inputFileDataSize includes the file schema as well, so even for empty pushes it should not be 0
   * @throws Exception
   */
  private void validateInputFileDataSizeFromOutput() throws VeniceException {
    Long inputFileDataSize = output.getInputFileDataSize();
    if (inputFileDataSize <= 0) {
      LOGGER.error("Retrieved inputFileDataSize ({}) is not valid", inputFileDataSize);
      throw new VeniceException("Retrieved inputFileDataSize (" + inputFileDataSize + ") is not valid");
    }
    LOGGER.info("Retrieved inputFileDataSize is {}", inputFileDataSize);
  }

  /**
   * zstdDictionary can be null when
   * 1. both zstd compression and {@link VenicePushJob.PushJobSetting#compressionMetricCollectionEnabled}
   *    is not enabled
   * 2. When one or the both of above are enabled, but zstd trainer failed: Will be handled based on
   *    map reduce counters
   */
  private void validateCompressionDictionaryFromOutput() {
    ByteBuffer zstdDictionary = output.getZstdDictionary();
    LOGGER.info("Retrieved compressionDictionary is {} bytes", zstdDictionary == null ? 0 : zstdDictionary.limit());
  }

  public ByteBuffer getCompressionDictionary() {
    return output.getZstdDictionary();
  }

  public long getInputFileDataSize() {
    return output.getInputFileDataSize();
  }
}
