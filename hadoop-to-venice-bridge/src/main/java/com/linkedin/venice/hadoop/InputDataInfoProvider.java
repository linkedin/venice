package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;


/**
 * This interface lets users get input data information
 */
public interface InputDataInfoProvider {

  /**
   * A POJO that contains input data information (schema information and input data file size)
   */
  class InputDataInfo {
    private final KafkaPushJob.SchemaInfo schemaInfo;
    private final long inputFileDataSizeInBytes;

    InputDataInfo(KafkaPushJob.SchemaInfo schemaInfo, long inputFileDataSizeInBytes) {
      this.schemaInfo = schemaInfo;
      this.inputFileDataSizeInBytes = inputFileDataSizeInBytes;
    }

    public KafkaPushJob.SchemaInfo getSchemaInfo() {
      return schemaInfo;
    }

    public long getInputFileDataSizeInBytes() {
      return inputFileDataSizeInBytes;
    }
  }

  InputDataInfo validateInputAndGetSchema(String inputUri, VeniceProperties props) throws Exception;

  byte[] getZstdDictTrainSamples();
}
