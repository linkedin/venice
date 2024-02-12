package com.linkedin.venice.hadoop;

import com.linkedin.venice.hadoop.input.recordreader.VeniceRecordIterator;
import com.linkedin.venice.utils.ByteUtils;
import java.io.Closeable;
import java.io.IOException;
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
  Logger LOGGER = LogManager.getLogger(InputDataInfoProvider.class);

  class InputDataInfo {
    private long inputFileDataSizeInBytes;
    private final int numInputFiles;
    private final boolean hasRecords;
    private final long inputModificationTime;

    InputDataInfo(long inputFileDataSizeInBytes, int numInputFiles, boolean hasRecords, long inputModificationTime) {
      this(inputFileDataSizeInBytes, numInputFiles, hasRecords, inputModificationTime, true);
    }

    InputDataInfo(
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
      this.inputFileDataSizeInBytes = inputFileDataSizeInBytes;
      this.numInputFiles = numInputFiles;
      this.hasRecords = hasRecords;
      this.inputModificationTime = inputModificationTime;
    }

    public long getInputFileDataSizeInBytes() {
      return inputFileDataSizeInBytes;
    }

    public void setInputFileDataSizeInBytes(long inputFileDataSizeInBytes) {
      this.inputFileDataSizeInBytes = inputFileDataSizeInBytes;
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
   * @param recordIterator The data accessor of input records.
   */
  static void loadZstdTrainingSamples(VeniceRecordIterator recordIterator, PushJobZstdConfig pushJobZstdConfig) {
    int fileSampleSize = 0;
    while (recordIterator.next()) {
      if (recordIterator.getCurrentKey() == null) {
        continue;
      }

      byte[] value = recordIterator.getCurrentValue();

      if (value == null || value.length == 0) {
        continue;
      }

      // At least 1 sample per file should be added until the max sample size is reached
      if (fileSampleSize > 0) {
        if (fileSampleSize + value.length > pushJobZstdConfig.getMaxBytesPerFile()) {
          LOGGER.debug(
              "Read {} to build dictionary. Reached limit per file of {}.",
              ByteUtils.generateHumanReadableByteCountString(fileSampleSize),
              ByteUtils.generateHumanReadableByteCountString(pushJobZstdConfig.getMaxBytesPerFile()));
          return;
        }
      }

      // addSample returns false when the data read no longer fits in the 'sample' buffer limit
      if (!pushJobZstdConfig.getZstdDictTrainer().addSample(value)) {
        LOGGER.debug(
            "Read {} to build dictionary. Reached sample limit of {}.",
            ByteUtils.generateHumanReadableByteCountString(fileSampleSize),
            ByteUtils.generateHumanReadableByteCountString(pushJobZstdConfig.getMaxSampleSize()));
        return;
      }
      fileSampleSize += value.length;
      pushJobZstdConfig.addFilledSize(value.length);
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
