package com.linkedin.venice.hadoop;

import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Custom Input Format with the following specs to be used for the feature {@link PushJobSetting#useMapperToBuildDict} with
 * {@link ValidateSchemaAndBuildDictMapper}
 * 1. Only 1 split for the input directory => Only 1 Mapper
 * 2. Each file inside the split (i.e. input directory) is considered to be a separate record: n files => n records
 * 3. Add a sentinel record at the end to build dictionary if needed: n files => n+1 records
 */
public class VeniceFileInputFormat implements InputFormat<IntWritable, NullWritable> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceFileInputFormat.class);

  /**
   * Number of splits is set to be always 1: which will invoke only 1 mapper to handle all the files.
   * @param job MR Job configuration
   * @param numSplits not used in this function.
   * @return Detail of the 1 split
   * @throws IOException
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] splits = new InputSplit[1];
    String inputDirectory = job.get(INPUT_PATH_PROP);
    splits[0] = new VeniceFileInputSplit(inputDirectory);
    return splits;
  }

  @Override
  public RecordReader<IntWritable, NullWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    try {
      return new VeniceFileInputRecordReader(split, job);
    } catch (Exception e) {
      LOGGER.error("Unable to retrieve VeniceFileInputRecordReader : ", e);
      throw e;
    }
  }
}
