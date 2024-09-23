package com.linkedin.venice.hadoop;

import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_DICTIONARY_SAMPLE_SIZE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_LAST_MODIFIED_TIME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.USE_MAPPER_TO_BUILD_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_REQUIRED;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.mapreduce.AbstractTestVeniceMR;
import com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper;
import com.linkedin.venice.hadoop.mapreduce.engine.MapReduceEngineTaskConfigProvider;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestValidateSchemaAndBuildDictMapper extends AbstractTestVeniceMR {
  protected ValidateSchemaAndBuildDictMapper newMapper() {
    return new ValidateSchemaAndBuildDictMapper();
  }

  protected ValidateSchemaAndBuildDictMapper getMapper() {
    Consumer<JobConf> noOpJobConfigurator = jobConf -> {};
    return getMapper(noOpJobConfigurator);
  }

  protected ValidateSchemaAndBuildDictMapper getMapper(Consumer<JobConf> jobConfigurator) {
    ValidateSchemaAndBuildDictMapper mapper = newMapper();
    JobConf jobConf = setupJobConf(100);
    jobConfigurator.accept(jobConf);
    mapper.configure(jobConf);
    return mapper;
  }

  @Override
  protected JobConf setupJobConf(int partitionCount) {
    JobConf jobConf = super.setupJobConf(partitionCount);
    jobConf.setNumReduceTasks(partitionCount);
    TaskAttemptID taskAttemptID = new TaskAttemptID("200707121733", 3, TaskType.MAP, 0, 0);
    jobConf.set(MapReduceEngineTaskConfigProvider.MAPRED_TASK_ID_PROP_NAME, taskAttemptID.toString());
    return jobConf;
  }

  @Override
  protected Configuration getDefaultJobConfiguration(int partitionCount) {
    Configuration config = super.getDefaultJobConfiguration(partitionCount);

    // Add extra configuration for this mapper
    File inputDir = Utils.getTempDataDirectory();
    try {
      TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    config.set(INPUT_PATH_PROP, inputDir.getAbsolutePath());
    config.set(VENICE_STORE_NAME_PROP, "test_store");
    config.setBoolean(INCREMENTAL_PUSH, false);
    config.set(ETL_VALUE_SCHEMA_TRANSFORMATION, ETLValueSchemaTransformation.NONE.name());
    config.setLong(INPUT_PATH_LAST_MODIFIED_TIME, 0);
    return config;
  }

  private Reporter createMockReporter() {
    Reporter mockReporter = mock(Reporter.class);
    Counters.Counter mockCounter = mock(Counters.Counter.class);
    when(mockReporter.getCounter(anyString(), anyString())).thenReturn(mockCounter);
    return mockReporter;
  }

  /**
   * Check whether the default jobConf is enough
   */
  @Test()
  public void testConfigure() {
    JobConf job = setupJobConf(100);
    try (ValidateSchemaAndBuildDictMapper mapper = newMapper()) {
      try {
        mapper.configure(job);
      } catch (Exception e) {
        Assert.fail(
            "ValidateSchemaAndBuildDictMapper#configure should not throw any exception when all the required props are there\n"
                + e);
      }
    }
  }

  /**
   * {@link #getDefaultJobConfiguration} sets up the input folder with 1 file (index 0).
   * Passing in file idx as 1 results in invalid file index
   */
  @Test()
  public void testMapWithInvalidFileIdx() throws IOException {
    Reporter mockReporter = createMockReporter();
    try (ValidateSchemaAndBuildDictMapper mapper = getMapper()) {
      OutputCollector<AvroWrapper<SpecificRecord>, NullWritable> output = mock(OutputCollector.class);
      // passing in 1 as invalid idx
      mapper.map(new IntWritable(1), NullWritable.get(), output, mockReporter);
      Assert.assertTrue(mapper.hasReportedFailure);
      verify(mockReporter, times(1)).incrCounter(
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getCounterName()),
          eq(1L));
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
    }
  }

  /**
   * {@link #getDefaultJobConfiguration} sets up the input folder with 1 file (index 0).
   * Passing in file idx as 0 works fine
   */
  @Test()
  public void testMapWithValidFileIdx() throws IOException {
    Reporter mockReporter = createMockReporter();
    try (ValidateSchemaAndBuildDictMapper mapper = getMapper()) {
      OutputCollector<AvroWrapper<SpecificRecord>, NullWritable> output = mock(OutputCollector.class);
      // passing in 0 as valid idx: Reading the only file
      mapper.map(new IntWritable(0), NullWritable.get(), output, mockReporter);

      Assert.assertFalse(mapper.hasReportedFailure);
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
      verify(mockReporter, times(1)).incrCounter(
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getCounterName()),
          eq(1L));
    }
  }

  /**
   * Test with valid input index.
   * No config to build dictionary
   */
  @Test()
  public void testMap() throws IOException {
    Reporter mockReporter = createMockReporter();
    try (ValidateSchemaAndBuildDictMapper mapper = getMapper()) {
      OutputCollector<AvroWrapper<SpecificRecord>, NullWritable> output = mock(OutputCollector.class);
      // passing in 0 as valid idx: Reading the only file
      mapper.map(new IntWritable(0), NullWritable.get(), output, mockReporter);
      // passing in sentinel key value to build dictionary
      mapper.map(
          new IntWritable(VeniceFileInputSplit.MAPPER_SENTINEL_KEY_TO_BUILD_DICTIONARY_AND_PERSIST_OUTPUT),
          NullWritable.get(),
          output,
          mockReporter);

      Assert.assertFalse(mapper.hasReportedFailure);
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
      verify(mockReporter, times(2)).incrCounter(
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getCounterName()),
          eq(1L));
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
    }
  }

  /**
   * Test with valid input index with config to build dictionary
   */
  @Test()
  public void testMapWithDictTrainSuccess() throws IOException {
    Reporter mockReporter = createMockReporter();
    try (ValidateSchemaAndBuildDictMapper mapper = getMapper(mapperJobConfig -> {
      mapperJobConfig.setBoolean(USE_MAPPER_TO_BUILD_DICTIONARY, true);
      mapperJobConfig.setBoolean(ZSTD_DICTIONARY_CREATION_REQUIRED, true);
      /** {@link VenicePushJobConstants#MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY} is 20
       *  for not skipping dictionary, so setting COMPRESSION_DICTIONARY_SAMPLE_SIZE >= 251 Bytes for
       *  this input file to not skip dictionary creation */
      mapperJobConfig.setInt(COMPRESSION_DICTIONARY_SAMPLE_SIZE, 251);
    })) {
      OutputCollector<AvroWrapper<SpecificRecord>, NullWritable> output = mock(OutputCollector.class);
      // passing in 0 as valid idx: Reading the only file
      mapper.map(new IntWritable(0), NullWritable.get(), output, mockReporter);
      // passing in sentinel key value to build dictionary
      mapper.map(
          new IntWritable(VeniceFileInputSplit.MAPPER_SENTINEL_KEY_TO_BUILD_DICTIONARY_AND_PERSIST_OUTPUT),
          NullWritable.get(),
          output,
          mockReporter);

      Assert.assertFalse(mapper.hasReportedFailure);
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
      verify(mockReporter, times(2)).incrCounter(
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getCounterName()),
          eq(1L));
      verify(mockReporter, times(1)).incrCounter(
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME.getCounterName()),
          eq(1L));
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
    }
  }

  /**
   * Test with valid input index with config to build dictionary, but the
   * COMPRESSION_DICTIONARY_SAMPLE_SIZE is small leading to number of samples
   * collected not meeting the minimum number of samples needed
   */
  @Test()
  public void testMapWithDictTrainSkipped() throws IOException {
    Reporter mockReporter = createMockReporter();
    try (ValidateSchemaAndBuildDictMapper mapper = getMapper(mapperJobConfig -> {
      mapperJobConfig.setBoolean(USE_MAPPER_TO_BUILD_DICTIONARY, true);
      mapperJobConfig.setBoolean(ZSTD_DICTIONARY_CREATION_REQUIRED, true);
      /** {@link VenicePushJobConstants#MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY} is 20
       *  for not skipping dictionary, so setting COMPRESSION_DICTIONARY_SAMPLE_SIZE <= 250 Bytes for
       *  this input file to skip dictionary creation */
      mapperJobConfig.setInt(COMPRESSION_DICTIONARY_SAMPLE_SIZE, 250);
    })) {
      OutputCollector<AvroWrapper<SpecificRecord>, NullWritable> output = mock(OutputCollector.class);
      // passing in 0 as valid idx: Reading the only file
      mapper.map(new IntWritable(0), NullWritable.get(), output, mockReporter);
      // passing in sentinel key value to build dictionary
      mapper.map(
          new IntWritable(VeniceFileInputSplit.MAPPER_SENTINEL_KEY_TO_BUILD_DICTIONARY_AND_PERSIST_OUTPUT),
          NullWritable.get(),
          output,
          mockReporter);

      Assert.assertTrue(mapper.hasReportedFailure);
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_INVALID_INPUT_IDX_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
      verify(mockReporter, times(1)).incrCounter(
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_NUM_RECORDS_SUCCESSFULLY_PROCESSED_GROUP_COUNTER_NAME.getCounterName()),
          eq(1L));
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SUCCESS_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
      verify(mockReporter, never()).incrCounter(
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_FAILURE_GROUP_COUNTER_NAME.getCounterName()),
          anyLong());
      verify(mockReporter, times(1)).incrCounter(
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME.getGroupName()),
          eq(MRJobCounterHelper.MAPPER_ZSTD_DICT_TRAIN_SKIPPED_GROUP_COUNTER_NAME.getCounterName()),
          eq(1L));
    }
  }
}
