package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.KEY_INPUT_FILE_DATA_SIZE;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_ZSTD_COMPRESSION_DICTIONARY;

import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictOutput;
import com.linkedin.venice.utils.Pair;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class reads the data(total input size in bytes and zstd dictionary) persisted in HDFS
 * by {@link ValidateSchemaAndBuildDictMapper} based on the schema {@link ValidateSchemaAndBuildDictOutput}
 * and deserializes it to be used by the VPJ Driver.
 */
public class ValidateSchemaAndBuildDictMapperResponse {
  private static final Logger LOGGER = LogManager.getLogger(ValidateSchemaAndBuildDictMapperResponse.class);
  private ByteBuffer compressionDictionary = null;
  private long inputFileDataSize = 0;

  private ByteBuffer getDataFromResponse(FileSystem fs, Path hdfsPath, String keyToRetrieve) {
    VeniceAvroRecordReader veniceAvroRecordReader =
        new VeniceAvroRecordReader(null, "key", "value", fs, hdfsPath, ETLValueSchemaTransformation.NONE);
    Iterator<Pair<byte[], byte[]>> it = veniceAvroRecordReader.iterator();
    while (it.hasNext()) {
      Pair<byte[], byte[]> record = it.next();
      if (record == null) {
        continue;
      }

      ByteBuffer key = (ByteBuffer) veniceAvroRecordReader.getKeySerializer().deserialize(null, record.getFirst());
      if (key.compareTo(ByteBuffer.wrap(keyToRetrieve.getBytes())) != 0) {
        continue;
      }

      byte[] value = record.getSecond();
      if (value == null || value.length == 0) {
        continue;
      }

      return (ByteBuffer) veniceAvroRecordReader.getKeySerializer().deserialize(null, value);
    }
    return null;
  }

  private void getInputFileDataSizeFromResponse(FileSystem fs, Path hdfsPath) throws Exception {
    ByteBuffer inputFileDataSizeBuffer = getDataFromResponse(fs, hdfsPath, KEY_INPUT_FILE_DATA_SIZE);
    inputFileDataSize = inputFileDataSizeBuffer.getLong();
    if (inputFileDataSize <= 0) {
      LOGGER.error("Retrieved inputFileDataSize ({}) is not valid", inputFileDataSize);
      throw new Exception("Retrieved inputFileDataSize (" + inputFileDataSize + ") is not valid");
    }
    LOGGER.info("Retrieved inputFileDataSize is {}", inputFileDataSize);
  }

  private void getCompressionDictionaryFromResponse(FileSystem fs, Path hdfsPath) throws Exception {
    compressionDictionary = getDataFromResponse(fs, hdfsPath, KEY_ZSTD_COMPRESSION_DICTIONARY);
    LOGGER.info(
        "Retrieved compressionDictionary is {} bytes",
        compressionDictionary == null ? 0 : compressionDictionary.limit());
  }

  protected void getResponseFromHDFS(Path path) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    getInputFileDataSizeFromResponse(fs, path);
    getCompressionDictionaryFromResponse(fs, path);
  }

  public ByteBuffer getCompressionDictionary() {
    return compressionDictionary;
  }

  public long getInputFileDataSize() {
    return inputFileDataSize;
  }
}
