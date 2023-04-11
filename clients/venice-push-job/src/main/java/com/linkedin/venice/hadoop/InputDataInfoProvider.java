package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Pair;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This interface lets users get input data information
 */
public interface InputDataInfoProvider extends Closeable {
  /**
   * A POJO that contains input data information (schema information and input data file size)
   */
  Logger LOGGER = LogManager.getLogger(InputDataInfoProvider.class.getName());

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
      this(pushJobSchemaInfo, inputFileDataSizeInBytes, numInputFiles, hasRecords, inputModificationTime, true);
    }

    InputDataInfo(
        PushJobSchemaInfo pushJobSchemaInfo,
        long inputFileDataSizeInBytes,
        int numInputFiles,
        boolean hasRecords,
        long inputModificationTime,
        boolean requireValidInputFileDataSizeInBytes) {
      if (requireValidInputFileDataSizeInBytes && inputFileDataSizeInBytes <= 0) {
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

  /**
   * This function loads training samples from recordReader abstraction for building the Zstd dictionary.
   * @param recordReader The data accessor of input records.
   */
  static void loadZstdTrainingSamples(AbstractVeniceRecordReader recordReader, PushJobZstdConfig pushJobZstdConfig) {
    int fileSampleSize = 0;
    Iterator<Pair<byte[], byte[]>> it = recordReader.iterator();
    while (it.hasNext()) {
      Pair<byte[], byte[]> record = it.next();
      if (record == null) {
        continue;
      }

      byte[] data = record.getSecond();

      if (data == null || data.length == 0) {
        continue;
      }

      if (fileSampleSize + data.length > pushJobZstdConfig.getMaxBytesPerFile()) {
        String perFileLimitErrorMsg = String.format(
            "Read %s to build dictionary. Reached limit per file of %s.",
            ByteUtils.generateHumanReadableByteCountString(fileSampleSize),
            ByteUtils.generateHumanReadableByteCountString(pushJobZstdConfig.getMaxBytesPerFile()));
        LOGGER.debug(perFileLimitErrorMsg);
        return;
      }

      // addSample returns false when the data read no longer fits in the 'sample' buffer limit
      if (!pushJobZstdConfig.getZstdDictTrainer().addSample(data)) {
        String maxSamplesReadErrorMsg = String.format(
            "Read %s to build dictionary. Reached sample limit of %s.",
            ByteUtils.generateHumanReadableByteCountString(fileSampleSize),
            ByteUtils.generateHumanReadableByteCountString(pushJobZstdConfig.getMaxSampleSize()));
        LOGGER.debug(maxSamplesReadErrorMsg);
        return;
      }
      fileSampleSize += data.length;
      pushJobZstdConfig.addFilledSize(data.length);
      pushJobZstdConfig.incrCollectedNumberOfSamples();
    }

    LOGGER.debug(
        "Read {} to build dictionary. Reached EOF.",
        ByteUtils.generateHumanReadableByteCountString(fileSampleSize));
  }

  byte[] getZstdDictTrainSamples();

  Schema extractAvroSubSchema(Schema origin, String fieldName);

  long getInputLastModificationTime(String inputUri) throws IOException;
}
