package com.linkedin.venice.hadoop;

import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_DICTIONARY_SAMPLE_SIZE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_DICTIONARY_SIZE_LIMIT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;


public class PushJobZstdConfig {
  private final ZstdDictTrainer zstdDictTrainer;
  private final int maxBytesPerFile;
  private final int maxSampleSize;
  private int filledSize; // Duplicate of filledSize in ZstdDictTrainer as there is no getter for this
  private int collectedNumberOfSamples;

  public PushJobZstdConfig(VeniceProperties props, int numFiles) {
    int maxDictSize = props
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
