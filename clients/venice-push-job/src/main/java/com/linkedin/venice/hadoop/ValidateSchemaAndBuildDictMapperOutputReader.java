package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class reads the data(total input size in bytes and zstd dictionary) persisted in HDFS
 * by {@link ValidateSchemaAndBuildDictMapper} based on the schema {@link ValidateSchemaAndBuildDictMapperOutput}
 */
public class ValidateSchemaAndBuildDictMapperOutputReader implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(ValidateSchemaAndBuildDictMapperOutputReader.class);
  private final ValidateSchemaAndBuildDictMapperOutput output;
  private final InputStream inputStream;
  private final DataFileStream avroDataFileStream;
  private final FileSystem fs;
  private final String outputDir;

  public ValidateSchemaAndBuildDictMapperOutputReader(String outputDir, String fileName) throws Exception {
    Validate.notEmpty(
        outputDir,
        ValidateSchemaAndBuildDictMapper.class.getSimpleName() + " output directory should not be empty");
    Validate.notEmpty(
        fileName,
        ValidateSchemaAndBuildDictMapper.class.getSimpleName() + " output fileName should not be empty");

    this.outputDir = outputDir;
    String filePath = outputDir + "/" + fileName;

    LOGGER.info(
        "Reading file {} to retrieve info persisted by {}",
        filePath,
        ValidateSchemaAndBuildDictMapper.class.getSimpleName());
    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);

    try {
      inputStream = fs.open(new Path(filePath));
      avroDataFileStream =
          new DataFileStream(inputStream, new SpecificDatumReader(ValidateSchemaAndBuildDictMapperOutput.class));
      try {
        output = (ValidateSchemaAndBuildDictMapperOutput) avroDataFileStream.next();
      } catch (NoSuchElementException e) {
        throw new VeniceException("File " + filePath + " contains no records", e);
      }
    } catch (IOException e) {
      throw new VeniceException(
          "Encountered exception reading Avro data from " + filePath
              + ". Check if the fileName exists and the data is in Avro format.",
          e);
    }

    Long inputFileDataSize = output.getInputFileDataSize();
    if (inputFileDataSize <= 0) {
      // this includes the file schema as well, so even for empty pushes it should not be 0
      LOGGER.error("Retrieved inputFileDataSize ({}) is not valid", inputFileDataSize);
      throw new VeniceException("Retrieved inputFileDataSize (" + inputFileDataSize + ") is not valid");
    }
    LOGGER.info("Retrieved inputFileDataSize is {}", inputFileDataSize);

    /**
     * zstdDictionary can be null when
     * 1. both zstd compression and {@link VenicePushJob.PushJobSetting#compressionMetricCollectionEnabled}
     *    is not enabled
     * 2. When one or the both of above are enabled, but zstd trainer failed: but this would have been be
     *    already handled based on map reduce counters
     */
    ByteBuffer zstdDictionary = output.getZstdDictionary();
    LOGGER.info("Retrieved compressionDictionary is {} bytes", zstdDictionary == null ? 0 : zstdDictionary.remaining());
  }

  public ValidateSchemaAndBuildDictMapperOutput getOutput() {
    return output;
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(avroDataFileStream);
    Utils.closeQuietlyWithErrorLogged(inputStream);

    // delete the output directory: It should not affect other VPJs as this is unique
    try {
      fs.delete(new Path(outputDir), true);
    } catch (IOException e) {
      LOGGER.error("Failed to delete directory: {}", outputDir, e);
    }
  }
}
