package com.linkedin.venice.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapred.InputSplit;


/**
 * Custom Input Split with the following specs to be used for the feature {@link VenicePushJob.PushJobSetting#useMapperToBuildDict} with
 * {@link ValidateSchemaAndBuildDictMapper}
 * 1. Holds an input directory
 * 2. {@link ValidateSchemaAndBuildDictMapper} is a map only MR supporting only 1 mapper, so only 1 split is created in total.
 */
class VeniceFileInputSplit implements InputSplit {
  protected static final int MAPPER_SENTINEL_KEY_TO_BUILD_DICTIONARY_AND_PERSIST_OUTPUT = -1;
  private String inputDirectory;

  public VeniceFileInputSplit() {
  }

  public VeniceFileInputSplit(String inputDirectory) {
    this.inputDirectory = inputDirectory;
  }

  @Override
  public long getLength() throws IOException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    inputDirectory = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(inputDirectory);
  }

  public String getInputDirectory() {
    return this.inputDirectory;
  }
}
