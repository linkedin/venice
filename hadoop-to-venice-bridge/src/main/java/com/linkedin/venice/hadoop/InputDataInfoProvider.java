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

    InputDataInfo(VenicePushJob.SchemaInfo schemaInfo, long inputFileDataSizeInBytes) {
      this.schemaInfo = schemaInfo;
      this.inputFileDataSizeInBytes = inputFileDataSizeInBytes;
    }

    public VenicePushJob.SchemaInfo getSchemaInfo() {
      return schemaInfo;
    }

    public long getInputFileDataSizeInBytes() {
      return inputFileDataSizeInBytes;
    }
  }

  InputDataInfo validateInputAndGetSchema(String inputUri, VeniceProperties props) throws Exception;

  void initZstdConfig(int numFiles);

  void loadZstdTrainingSamples(AbstractVeniceRecordReader recordReader);

  byte[] getZstdDictTrainSamples();

  Schema extractAvroSubSchema(Schema origin, String fieldName);
}
