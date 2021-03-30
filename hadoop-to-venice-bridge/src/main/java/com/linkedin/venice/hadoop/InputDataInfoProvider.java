package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;


/**
 * This interface lets users get input data information
 */
public interface InputDataInfoProvider extends AutoCloseable {

  /**
   * A POJO that contains input data information (schema information and input data file size)
   */
  class InputDataInfo {
    private final VenicePushJob.SchemaInfo schemaInfo;
    private final long inputFileDataSizeInBytes;
    private final boolean hasRecords;

    InputDataInfo(VenicePushJob.SchemaInfo schemaInfo, long inputFileDataSizeInBytes, boolean hasRecords) {
      if (inputFileDataSizeInBytes <= 0) {
        throw new IllegalArgumentException("The input data file size is expected to be positive. Got: " + inputFileDataSizeInBytes);
      }
      this.schemaInfo = schemaInfo;
      this.inputFileDataSizeInBytes = inputFileDataSizeInBytes;
      this.hasRecords = hasRecords;
    }

    public VenicePushJob.SchemaInfo getSchemaInfo() {
      return schemaInfo;
    }

    public long getInputFileDataSizeInBytes() {
      return inputFileDataSizeInBytes;
    }

    public boolean hasRecords() {
      return hasRecords;
    }
  }

  InputDataInfo validateInputAndGetInfo(String inputUri) throws Exception;

  void initZstdConfig(int numFiles);

  void loadZstdTrainingSamples(AbstractVeniceRecordReader recordReader);

  byte[] getZstdDictTrainSamples();

  Schema extractAvroSubSchema(Schema origin, String fieldName);
}
