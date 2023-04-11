package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SAMPLE_SIZE;
import static com.linkedin.venice.hadoop.DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SIZE_LIMIT;
import static com.linkedin.venice.hadoop.DefaultInputDataInfoProvider.DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;


public class PushJobZstdConfig {
  private ZstdDictTrainer zstdDictTrainer;
  private int maxBytesPerFile;
  private int maxDictSize;
  private int maxSampleSize;
  private int filledSize; // Duplicate of filledSize in ZstdDictTrainer as there is no getter for this
  /**
   * Known <a href="https://github.com/luben/zstd-jni/issues/253">zstd lib issue</a> which
   * crashes if the input sample is too small. So adding a preventive check to skip training
   * the dictionary in such cases using a minimum limit of 20. Keeping it simple and hard coding
   * it as if this check doesn't prevent some edge cases then we can disable the feature itself
   */
  protected static final int MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY = 20;
  private int collectedNumberOfSamples;

  public PushJobZstdConfig(VeniceProperties props, int numFiles) {
    maxDictSize = props
        .getInt(COMPRESSION_DICTIONARY_SIZE_LIMIT, VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES);
    maxSampleSize = props.getInt(COMPRESSION_DICTIONARY_SAMPLE_SIZE, DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE);
    maxBytesPerFile = maxSampleSize / numFiles;
    zstdDictTrainer = new ZstdDictTrainer(maxSampleSize, maxDictSize);
    filledSize = 0;
    collectedNumberOfSamples = 0;
  }

  public ZstdDictTrainer getZstdDictTrainer() {
    return zstdDictTrainer;
  }

  public int getMaxBytesPerFile() {
    return maxBytesPerFile;
  }

  public int getMaxSampleSize() {
    return maxSampleSize;
  }

  public int getFilledSize() {
    return filledSize;
  }

  public void addFilledSize(int filledSize) {
    this.filledSize += filledSize;
  }

  public int getCollectedNumberOfSamples() {
    return collectedNumberOfSamples;
  }

  public void incrCollectedNumberOfSamples() {
    this.collectedNumberOfSamples++;
  }
}
