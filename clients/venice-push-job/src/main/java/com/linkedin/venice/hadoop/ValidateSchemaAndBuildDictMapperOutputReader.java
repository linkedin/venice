package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.KEY_INPUT_FILE_DATA_SIZE;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_ZSTD_COMPRESSION_DICTIONARY;

import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import java.nio.ByteBuffer;
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
  private GenericAvroRecordReader genericAvroRecordReader;
  private final ValidateSchemaAndBuildDictMapperOutput output = new ValidateSchemaAndBuildDictMapperOutput();

  private Object getDataFromResponse(String keyToRetrieve) {
    return genericAvroRecordReader.getField(keyToRetrieve);
  }

  /**
   * inputFileDataSize includes the file schema as well, so even for empty pushes it should not be 0
   * @throws Exception
   */
  private void getInputFileDataSizeFromResponse() throws Exception {
    Long inputFileDataSize = (Long) getDataFromResponse(KEY_INPUT_FILE_DATA_SIZE);
    if (inputFileDataSize <= 0) {
      LOGGER.error("Retrieved inputFileDataSize ({}) is not valid", inputFileDataSize);
      throw new Exception("Retrieved inputFileDataSize (" + inputFileDataSize + ") is not valid");
    }
    LOGGER.info("Retrieved inputFileDataSize is {}", inputFileDataSize);
    output.setInputFileDataSize(inputFileDataSize);
  }

  /**
   * zstdDictionary can be null when
   * 1. both zstd compression and {@link VenicePushJob.PushJobSetting#compressionMetricCollectionEnabled}
   *    is not enabled
   * 2. When one or the both of above are enabled, but zstd trainer failed: Will be handled based on
   *    map reduce counters
   */
  private void getCompressionDictionaryFromResponse() {
    ByteBuffer zstdDictionary = (ByteBuffer) getDataFromResponse(KEY_ZSTD_COMPRESSION_DICTIONARY);
    LOGGER.info("Retrieved compressionDictionary is {} bytes", zstdDictionary == null ? 0 : zstdDictionary.limit());
    output.setZstdDictionary(zstdDictionary);
  }

  protected void getResponseFromHDFS(Path path) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    this.genericAvroRecordReader = new GenericAvroRecordReader(fs, path);
    getInputFileDataSizeFromResponse();
    getCompressionDictionaryFromResponse();
  }

  public ByteBuffer getCompressionDictionary() {
    return output.getZstdDictionary();
  }

  public long getInputFileDataSize() {
    return output.getInputFileDataSize();
  }
}
