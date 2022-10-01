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
  private int dictSize;
  private int sampleSize;
  private int filledSize; // Duplicate of filledSize in ZstdDictTrainer as there is no getter for this

  public PushJobZstdConfig(VeniceProperties props, int numFiles) {
    dictSize = props
        .getInt(COMPRESSION_DICTIONARY_SIZE_LIMIT, VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES);
    sampleSize = props.getInt(COMPRESSION_DICTIONARY_SAMPLE_SIZE, DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE);
    maxBytesPerFile = sampleSize / numFiles;
    zstdDictTrainer = new ZstdDictTrainer(sampleSize, dictSize);
    filledSize = 0;
  }

  public ZstdDictTrainer getZstdDictTrainer() {
    return zstdDictTrainer;
  }

  public int getMaxBytesPerFile() {
    return maxBytesPerFile;
  }

  public int getSampleSize() {
    return sampleSize;
  }

  public int getFilledSize() {
    return filledSize;
  }

  public void setFilledSize(int filledSize) {
    this.filledSize = filledSize;
  }
}
