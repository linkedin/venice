package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import java.io.IOException;
import org.apache.avro.Schema;


public class KafkaInputDataInfoProvider implements InputDataInfoProvider {
  @Override
  public InputDataInfo validateInputAndGetInfo(String inputUri) throws Exception {
    /**
     * Using a default number: 1GB here.
     * No need to specify the accurate input size since the re-push mustn't be the first version, so the partition
     * count calculation won't be affected by this random number.
     */
    long inputFileDataSize = 1024 * 1024 * 1024L;
    /**
     * This is used to ensure the {@link DataWriterComputeJob#validateTask()} function won't assume the reducer count
     * should be 0.
     */
    boolean inputFileHasRecords = true;

    /**
     * This is used to ensure the {@link DataWriterComputeJob#validateTask()} function won't assume the reducer count
     * should be 0.
     */
    int numInputFiles = 1;

    return new InputDataInfo(inputFileDataSize, numInputFiles, inputFileHasRecords, 0);
  }

  @Override
  public PushJobZstdConfig initZstdConfig(int numFiles) {
    throw new VeniceUnsupportedOperationException("Zstd for KafkaInputDataInfoProvider");
  }

  @Override
  public byte[] trainZstdDictionary() {
    throw new VeniceUnsupportedOperationException("Zstd for KafkaInputDataInfoProvider");
  }

  @Override
  public Schema extractAvroSubSchema(Schema origin, String fieldName) {
    throw new VeniceUnsupportedOperationException("extractAvroSubSchema for KafkaInputDataInfoProvider");
  }

  @Override
  public long getInputLastModificationTime(String inputUri) throws IOException {
    throw new VeniceUnsupportedOperationException("getInputLastModificationTime for KafkaInputDataInfoProvider");
  }

  @Override
  public void close() throws IOException {
  }
}
