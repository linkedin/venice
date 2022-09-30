package com.linkedin.venice.hadoop;

import java.io.Closeable;
import java.io.IOException;
import org.apache.avro.Schema;


/**
 * This interface lets users get input data information
 */
public interface InputDataInfoProvider extends Closeable {
  /**
   * A POJO that contains input data information (schema information and input data file size)
   */
  class InputDataInfo {
    private final PushJobSchemaInfo pushJobSchemaInfo;
    private final long inputFileDataSizeInBytes;
    private final int numInputFiles;
    private final boolean hasRecords;
    private final long inputModificationTime;

    InputDataInfo(
        PushJobSchemaInfo pushJobSchemaInfo,
        long inputFileDataSizeInBytes,
        int numInputFiles,
        boolean hasRecords,
        long inputModificationTime) {
      if (inputFileDataSizeInBytes <= 0) {
        throw new IllegalArgumentException(
            "The input data file size is expected to be positive. Got: " + inputFileDataSizeInBytes);
      }
      if (numInputFiles <= 0) {
        throw new IllegalArgumentException(
            "The Number of Input files is expected to be positive. Got: " + numInputFiles);
      }
      this.pushJobSchemaInfo = pushJobSchemaInfo;
      this.inputFileDataSizeInBytes = inputFileDataSizeInBytes;
      this.numInputFiles = numInputFiles;
      this.hasRecords = hasRecords;
      this.inputModificationTime = inputModificationTime;
    }

    public PushJobSchemaInfo getSchemaInfo() {
      return pushJobSchemaInfo;
    }

    public long getInputFileDataSizeInBytes() {
      return inputFileDataSizeInBytes;
    }

    public int getNumInputFiles() {
      return numInputFiles;
    }

    public boolean hasRecords() {
      return hasRecords;
    }

    public long getInputModificationTime() {
      return inputModificationTime;
    }
  }

  InputDataInfo validateInputAndGetInfo(String inputUri) throws Exception;

  void initZstdConfig(int numFiles);

  void loadZstdTrainingSamples(AbstractVeniceRecordReader recordReader);

  byte[] getZstdDictTrainSamples();

  Schema extractAvroSubSchema(Schema origin, String fieldName);

  long getInputLastModificationTime(String inputUri) throws IOException;
}
