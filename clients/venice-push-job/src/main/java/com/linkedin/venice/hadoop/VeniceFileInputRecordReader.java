package com.linkedin.venice.hadoop;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PATH_FILTER;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;


/**
 * Custom Input Record reader with the following specs to be used for the feature {@link PushJobSetting#useMapperToBuildDict} with
 * {@link ValidateSchemaAndBuildDictMapper}
 * 1. Reads a split and creates one or more records off it.
 * 2. Each file inside the split (i.e. input directory) is considered to be a separate record: n files => n records
 * 3. Add a sentinel record at the end to build dictionary if needed: n files => n+1 records
 */
class VeniceFileInputRecordReader implements RecordReader<IntWritable, NullWritable> {
  private String inputDirectory;
  private int finishedNumberOfFiles;
  private int totalNumberOfFiles;

  public VeniceFileInputRecordReader() {
  }

  public VeniceFileInputRecordReader(InputSplit split, JobConf job) throws IOException {
    inputDirectory = ((VeniceFileInputSplit) split).getInputDirectory();
    finishedNumberOfFiles = 0;
    totalNumberOfFiles = getTotalNumberOfFiles(inputDirectory, job);
  }

  protected int getTotalNumberOfFiles(String inputDirectory, JobConf job) throws IOException {
    Configuration conf = new Configuration();
    Path srcPath = new Path(inputDirectory);
    FileSystem fs = srcPath.getFileSystem(conf);
    FileStatus[] fileStatuses = fs.listStatus(srcPath, PATH_FILTER);
    // Path validity and length validity are already checked for the flow to be here, so not checking again
    return fileStatuses.length;
  }

  /**
   * Checks whether there is a next file to process and pass the fileIdx via key if found
   * It also adds a new sentinel record in the end to build dictionary if needed and
   * persist data to HDFS to be used by VPJ Driver
   * @param key fileIdx is passed to map()
   * @param value Null
   * @return true or false based on whether a next file exists or not
   * @throws IOException
   */
  @Override
  public boolean next(IntWritable key, NullWritable value) throws IOException {
    if (finishedNumberOfFiles < totalNumberOfFiles) {
      key.set(finishedNumberOfFiles);
      finishedNumberOfFiles += 1;
      return true;
    } else if (finishedNumberOfFiles == totalNumberOfFiles) {
      // sentinel record to build the dictionary and persist the data to be read by VPJ Driver
      key.set(VeniceFileInputSplit.MAPPER_SENTINEL_KEY_TO_BUILD_DICTIONARY_AND_PERSIST_OUTPUT);
      finishedNumberOfFiles += 1;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public IntWritable createKey() {
    return new IntWritable();
  }

  @Override
  public NullWritable createValue() {
    return NullWritable.get();
  }

  @Override
  public long getPos() throws IOException {
    return finishedNumberOfFiles;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public float getProgress() throws IOException {
    return finishedNumberOfFiles / (float) (totalNumberOfFiles + 1); // +1 for dictionary building phase
  }
}
