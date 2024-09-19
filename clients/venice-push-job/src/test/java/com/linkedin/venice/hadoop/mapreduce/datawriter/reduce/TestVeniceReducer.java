package com.linkedin.venice.hadoop.mapreduce.datawriter.reduce;

import static com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper.TOTAL_KEY_SIZE_GROUP_COUNTER_NAME;
import static com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ALLOW_DUPLICATE_KEY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DERIVED_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.STORAGE_QUOTA_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.RecordTooLargeException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceResourceAccessException;
import com.linkedin.venice.hadoop.mapreduce.AbstractTestVeniceMR;
import com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.ReporterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.mapreduce.engine.HadoopJobClientProvider;
import com.linkedin.venice.hadoop.mapreduce.engine.MapReduceEngineTaskConfigProvider;
import com.linkedin.venice.hadoop.task.datawriter.AbstractPartitionWriter;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.adapter.SimplePubSubProduceResultImpl;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceReducer extends AbstractTestVeniceMR {
  private static final int TASK_ID = 2;

  @Override
  protected Configuration getDefaultJobConfiguration(int partitionCount) {
    Configuration conf = super.getDefaultJobConfiguration(partitionCount);
    TaskAttemptID taskAttemptID = new TaskAttemptID("200707121733", 3, TaskType.REDUCE, TASK_ID, 0);
    conf.set(MapReduceEngineTaskConfigProvider.MAPRED_TASK_ID_PROP_NAME, taskAttemptID.toString());
    conf.set(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());
    conf.setLong(TELEMETRY_MESSAGE_INTERVAL, 2);
    return conf;
  }

  @Test
  public void testReducerPutWithTooLargeValueAndChunkingDisabled() {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    when(mockWriter.put(any(), any(), anyInt(), any(), any()))
        .thenThrow(new RecordTooLargeException("expected exception"));
    testReduceWithTooLargeValueAndChunkingDisabled(mockWriter, setupJobConf(100));
  }

  @Test
  public void testReducerUpdateWithTooLargeValueAndChunkingDisabled() {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    when(mockWriter.update(any(), any(), anyInt(), anyInt(), any()))
        .thenThrow(new RecordTooLargeException("expected exception"));
    JobConf jobConf = setupJobConf(100);
    jobConf.setInt(DERIVED_SCHEMA_ID_PROP, 2);
    jobConf.setBoolean(ENABLE_WRITE_COMPUTE, true);
    testReduceWithTooLargeValueAndChunkingDisabled(mockWriter, jobConf);
  }

  private void testReduceWithTooLargeValueAndChunkingDisabled(AbstractVeniceWriter mockWriter, JobConf jobConf) {
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    reducer.configure(jobConf);
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    BytesWritable valueWritable = new BytesWritable(valueFieldValue.getBytes());
    List<BytesWritable> values = Collections.singletonList(valueWritable);
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = createZeroCountReporterMock();
    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);

    verify(mockReporter).incrCounter(
        MRJobCounterHelper.RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME.getGroupName(),
        MRJobCounterHelper.RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME.getCounterName(),
        1);
  }

  @Test
  public void testReduce() {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    reducer.configure(setupJobConf(100));
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    BytesWritable valueWritable = new BytesWritable(valueFieldValue.getBytes());
    List<BytesWritable> values = Collections.singletonList(valueWritable);
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = createZeroCountReporterMock();

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaIdCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<PutMetadata> metadataArgumentCaptor = ArgumentCaptor.forClass(PutMetadata.class);
    ArgumentCaptor<AbstractPartitionWriter.PartitionWriterProducerCallback> callbackCaptor =
        ArgumentCaptor.forClass(AbstractPartitionWriter.PartitionWriterProducerCallback.class);

    verify(mockWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaIdCaptor.capture(),
        callbackCaptor.capture(),
        metadataArgumentCaptor.capture());
    Assert.assertEquals(keyCaptor.getValue(), keyFieldValue.getBytes());
    Assert.assertEquals(valueCaptor.getValue(), valueFieldValue.getBytes());
    Assert.assertEquals((int) schemaIdCaptor.getValue(), VALUE_SCHEMA_ID);
    Assert.assertTrue(reducer.getDataWriterTaskTracker() instanceof ReporterBackedMapReduceDataWriterTaskTracker);
    Assert.assertEquals(
        ((ReporterBackedMapReduceDataWriterTaskTracker) reducer.getDataWriterTaskTracker()).getReporter(),
        mockReporter);

    verify(mockReporter).incrCounter(
        MRJobCounterHelper.OUTPUT_RECORD_COUNT_GROUP_COUNTER_NAME.getGroupName(),
        MRJobCounterHelper.OUTPUT_RECORD_COUNT_GROUP_COUNTER_NAME.getCounterName(),
        1);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testReduceWithNoValue() {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    reducer.configure(setupJobConf(100));
    final String keyFieldValue = "test_key";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    List<BytesWritable> values = new ArrayList<>();
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = createZeroCountReporterMock();

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);
  }

  @Test
  public void testReduceWithMultipleSameValues() {
    Reporter mockReporter = createZeroCountReporterMock();
    // Duplicate key with same values should not fail
    testDuplicateKey(true, mockReporter);
    verify(mockReporter, never()).incrCounter(
        eq(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME.getCounterName()),
        anyLong());
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.DUP_KEY_WITH_IDENTICAL_VALUE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.DUP_KEY_WITH_IDENTICAL_VALUE_GROUP_COUNTER_NAME.getCounterName()),
        eq(1L));
  }

  @Test
  public void testReduceWithMultipleDistinctValues() {
    Reporter mockReporter = createZeroCountReporterMock();
    // Duplicate key with distinct values should not fail
    testDuplicateKey(false, mockReporter);
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME.getCounterName()),
        eq(1L));
    verify(mockReporter, never()).incrCounter(
        eq(MRJobCounterHelper.DUP_KEY_WITH_IDENTICAL_VALUE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.DUP_KEY_WITH_IDENTICAL_VALUE_GROUP_COUNTER_NAME.getCounterName()),
        anyLong());
  }

  private void testDuplicateKey(boolean sameValue, Reporter reporter) {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    reducer.configure(setupJobConf(100));

    // key needs to be Avro-formatted bytes here cause
    // Reducer is gonna try deserialize it if it finds duplicates key
    byte[] keyBytes = new VeniceAvroKafkaSerializer("\"string\"").serialize("test_topic", "test_key");

    BytesWritable keyWritable = new BytesWritable(keyBytes);
    List<BytesWritable> values = new ArrayList<>();
    values.add(new BytesWritable("test_value".getBytes()));
    values.add(sameValue ? new BytesWritable("test_value".getBytes()) : new BytesWritable("test_value1".getBytes()));
    OutputCollector mockCollector = mock(OutputCollector.class);
    reducer.reduce(keyWritable, values.iterator(), mockCollector, reporter);
  }

  @Test
  public void testReducerConfigWithUnlimitedStorageQuota() {
    VeniceReducer reducer = new VeniceReducer();
    Configuration jobConfig = getDefaultJobConfiguration(100);
    jobConfig.setLong(STORAGE_QUOTA_PROP, Store.UNLIMITED_STORAGE_QUOTA);
    reducer.configure(new JobConf(jobConfig));
    Assert.assertFalse(reducer.getExceedQuotaFlag());
  }

  @Test
  public void testReducerDetectExceededQuotaInConfig() throws IOException {
    reducerDetectExceededQuotaInConfig(1024, 1024, 2048); // Not exceed
    reducerDetectExceededQuotaInConfig(1024, 512, 2048); // Not exceed
    reducerDetectExceededQuotaInConfig(1024, 1024, 1024); // Exceed
  }

  private void reducerDetectExceededQuotaInConfig(
      long totalKeySizeInBytes,
      long totalValueSizeInBytes,
      long storageQuotaInBytes) throws IOException {

    JobClient jobClient = mock(JobClient.class);
    Counters counters = mock(Counters.class);
    Counters.Group group = mock(Counters.Group.class);
    when(counters.getGroup(TOTAL_KEY_SIZE_GROUP_COUNTER_NAME.getGroupName())).thenReturn(group);
    when(group.getCounter(TOTAL_KEY_SIZE_GROUP_COUNTER_NAME.getCounterName())).thenReturn(totalKeySizeInBytes);
    when(group.getCounter(TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME.getCounterName())).thenReturn(totalValueSizeInBytes);
    RunningJob runningJob = mock(RunningJob.class);
    when(runningJob.getCounters()).thenReturn(counters);
    when(jobClient.getJob(any(JobID.class))).thenReturn(runningJob);

    HadoopJobClientProvider hadoopJobClientProvider = mock(HadoopJobClientProvider.class);
    when(hadoopJobClientProvider.getJobClientFromConfig(any())).thenReturn(jobClient);
    Configuration jobConfig = getDefaultJobConfiguration(100);
    jobConfig.setLong(STORAGE_QUOTA_PROP, storageQuotaInBytes);
    jobConfig.setBoolean(VeniceWriter.ENABLE_CHUNKING, false);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setHadoopJobClientProvider(hadoopJobClientProvider);
    reducer.configure(new JobConf(jobConfig));

    if (totalKeySizeInBytes + totalValueSizeInBytes > storageQuotaInBytes) {
      Assert.assertTrue(reducer.getExceedQuotaFlag());
    } else {
      Assert.assertFalse(reducer.getExceedQuotaFlag());
    }
  }

  @Test
  public void testReducerAllowDuplicatedKeys() {
    Reporter mockReporter = mock(Reporter.class);
    Counters.Counter zeroCounters = mock(Counters.Counter.class);
    when(zeroCounters.getCounter()).thenReturn(0L); // No counted
    Counters.Counter nonZeroCounters = mock(Counters.Counter.class);
    when(nonZeroCounters.getCounter()).thenReturn(1L);

    when(
        mockReporter.getCounter(
            MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getGroupName(),
            MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getCounterName())).thenReturn(zeroCounters);
    when(
        mockReporter.getCounter(
            MRJobCounterHelper.TOTAL_KEY_SIZE_GROUP_COUNTER_NAME.getGroupName(),
            MRJobCounterHelper.TOTAL_KEY_SIZE_GROUP_COUNTER_NAME.getCounterName())).thenReturn(zeroCounters);
    when(
        mockReporter.getCounter(
            TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME.getGroupName(),
            TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME.getCounterName())).thenReturn(zeroCounters);

    when(
        mockReporter.getCounter(
            MRJobCounterHelper.RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME.getGroupName(),
            MRJobCounterHelper.RECORD_TOO_LARGE_FAILURE_GROUP_COUNTER_NAME.getCounterName())).thenReturn(zeroCounters);

    when(
        mockReporter.getCounter(
            MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME.getGroupName(),
            MRJobCounterHelper.DUP_KEY_WITH_DISTINCT_VALUE_GROUP_COUNTER_NAME.getCounterName()))
                .thenReturn(nonZeroCounters);

    final boolean isDuplicateKeyAllowed = true;

    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    Configuration jobConfiguration = getDefaultJobConfiguration(100);
    jobConfiguration.set(ALLOW_DUPLICATE_KEY, String.valueOf(isDuplicateKeyAllowed)); // Allow dup key
    reducer.configure(new JobConf(jobConfiguration));

    byte[] keyBytes = new VeniceAvroKafkaSerializer("\"string\"").serialize("test_topic", "test_key");
    List<BytesWritable> values =
        Arrays.asList(new BytesWritable("test_value_0".getBytes()), new BytesWritable("test_value_1".getBytes()));
    OutputCollector mockCollector = mock(OutputCollector.class);
    reducer.reduce(new BytesWritable(keyBytes), values.iterator(), mockCollector, mockReporter);
    verify(mockWriter).put(any(), any(), anyInt(), any(), any()); // Expect the writer to be invoked
    DataWriterTaskTracker dataWriterTaskTracker = new ReporterBackedMapReduceDataWriterTaskTracker(mockReporter);
    Assert.assertFalse(reducer.hasReportedFailure(dataWriterTaskTracker, isDuplicateKeyAllowed));
  }

  @Test
  public void testReduceWithTopicAuthorizationException() throws IOException {
    // One key and one value
    byte[] keyBytes = new VeniceAvroKafkaSerializer("\"string\"").serialize("test_topic", "test_key");
    BytesWritable keyWritable = new BytesWritable(keyBytes);
    ArrayList<BytesWritable> values = new ArrayList<>();
    values.add(new BytesWritable("test_value".getBytes()));
    Reporter mockReporter = createZeroCountReporterMock();

    OutputCollector mockCollector = mock(OutputCollector.class);
    AbstractVeniceWriter mockVeniceWriter = mock(AbstractVeniceWriter.class);
    when(mockVeniceWriter.put(any(), any(), anyInt(), any(), any()))
        .thenThrow(new VeniceResourceAccessException("No ACL permission"));
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockVeniceWriter);
    reducer.configure(setupJobConf(100));

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);

    // Expect the counter to record this authorization error
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getCounterName()),
        eq(1L));
    verify(mockCollector, never()).collect(any(), any());
  }

  @Test
  public void testCloseReducerAfterReduce() throws IOException {
    // One key and one value
    byte[] key1Bytes = new VeniceAvroKafkaSerializer("\"string\"").serialize("test_topic", "test_key_1");
    BytesWritable keyWritable1 = new BytesWritable(key1Bytes);
    ArrayList<BytesWritable> values1 = new ArrayList<>();
    values1.add(new BytesWritable("test_value".getBytes()));

    byte[] key2Bytes = new VeniceAvroKafkaSerializer("\"string\"").serialize("test_topic", "test_key_2");
    BytesWritable keyWritable2 = new BytesWritable(key2Bytes);
    ArrayList<BytesWritable> values2 = new ArrayList<>();
    values2.add(new BytesWritable("test_value_2".getBytes()));

    Reporter mockReporter = createZeroCountReporterMock();

    OutputCollector mockCollector = mock(OutputCollector.class);
    AbstractVeniceWriter mockVeniceWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockVeniceWriter);
    reducer.configure(setupJobConf(100));

    reducer.reduce(keyWritable1, values1.iterator(), mockCollector, mockReporter);
    PubSubProduceResult produceResult1 = new SimplePubSubProduceResultImpl("topic-name", TASK_ID, 1, 1);
    reducer.getCallback().onCompletion(produceResult1, null);

    reducer.reduce(keyWritable2, values2.iterator(), mockCollector, mockReporter);
    PubSubProduceResult produceResult2 = new SimplePubSubProduceResultImpl("topic-name", TASK_ID, 2, 1);
    reducer.getCallback().onCompletion(produceResult2, null);

    reducer.close();

    // Expect the counter to record the reducer close count
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME.getCounterName()),
        eq(1L));
  }

  @Test
  public void testCloseReducerWithNoReduce() throws IOException {
    VeniceReducer reducer = new VeniceReducer();
    reducer.configure(setupJobConf(100));
    reducer.close(); // Expect no exception
  }

  @Test
  public void testReduceWithExceedQuotaStillIncreaseCloseCounter() throws IOException {
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = createZeroCountReporterMock();
    VeniceReducer reducer = new VeniceReducer();
    reducer.setExceedQuota(true);
    reducer.reduce(
        new BytesWritable("test_key".getBytes()),
        Collections.singleton(new BytesWritable("test_value".getBytes())).iterator(),
        mockCollector,
        mockReporter);
    reducer.close();
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.REDUCER_CLOSED_COUNT_GROUP_COUNTER_NAME.getCounterName()),
        anyLong());
  }

  @Test
  public void testReducerDetectErrorInReportCounter() throws IOException {
    // One key and one value
    byte[] keyBytes = new VeniceAvroKafkaSerializer("\"string\"").serialize("test_topic", "test_key");
    BytesWritable keyWritable = new BytesWritable(keyBytes);
    ArrayList<BytesWritable> values = new ArrayList<>();
    values.add(new BytesWritable("test_value".getBytes()));
    Reporter mockReporter = mock(Reporter.class);
    Counters.Counter mockCounters = mock(Counters.Counter.class);
    when(mockCounters.getCounter()).thenReturn(1L); // Error counted
    when(mockReporter.getCounter(anyString(), anyString())).thenReturn(mockCounters);

    OutputCollector mockCollector = mock(OutputCollector.class);
    AbstractVeniceWriter mockVeniceWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockVeniceWriter);
    reducer.configure(setupJobConf(100));

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);
    verify(mockVeniceWriter, never()).put(any(), any(), anyInt(), any()); // Not expected to be invoked due to early
                                                                          // termination
    verify(mockCollector, never()).collect(any(), any());
  }

  @Test
  public void testReduceWithDifferentReporters() {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    reducer.configure(setupJobConf(100));
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    BytesWritable valueWritable = new BytesWritable(valueFieldValue.getBytes());
    List<BytesWritable> values = new ArrayList<>();
    values.add(valueWritable);
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = createZeroCountReporterMock();

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);

    ArgumentCaptor<AbstractPartitionWriter.PartitionWriterProducerCallback> callbackCaptor =
        ArgumentCaptor.forClass(AbstractPartitionWriter.PartitionWriterProducerCallback.class);

    verify(mockWriter).put(any(), any(), anyInt(), callbackCaptor.capture(), any());
    Assert.assertTrue(reducer.getDataWriterTaskTracker() instanceof ReporterBackedMapReduceDataWriterTaskTracker);
    Assert.assertEquals(
        ((ReporterBackedMapReduceDataWriterTaskTracker) reducer.getDataWriterTaskTracker()).getReporter(),
        mockReporter);

    // test with different reporter
    Reporter newMockReporter = createZeroCountReporterMock();

    reducer.reduce(keyWritable, values.iterator(), mockCollector, newMockReporter);
    verify(mockWriter, times(2)).put(any(), any(), anyInt(), callbackCaptor.capture(), any());
    Assert.assertTrue(reducer.getDataWriterTaskTracker() instanceof ReporterBackedMapReduceDataWriterTaskTracker);
    Assert.assertEquals(
        ((ReporterBackedMapReduceDataWriterTaskTracker) reducer.getDataWriterTaskTracker()).getReporter(),
        newMockReporter);
  }

  @Test
  public void testReduceWithWriterException() {
    AbstractVeniceWriter exceptionWriter = new AbstractVeniceWriter(TOPIC_NAME) {
      @Override
      public void close(boolean gracefulClose) {
        // no-op
      }

      @Override
      public CompletableFuture<PubSubProduceResult> put(
          Object key,
          Object value,
          int valueSchemaId,
          PubSubProducerCallback callback) {
        callback.onCompletion(null, new VeniceException("Fake exception"));
        return null;
      }

      @Override
      public Future<PubSubProduceResult> put(
          Object key,
          Object value,
          int valueSchemaId,
          PubSubProducerCallback callback,
          PutMetadata putMetadata) {
        callback.onCompletion(null, new VeniceException("Fake exception"));
        return null;
      }

      @Override
      public Future<PubSubProduceResult> delete(
          Object key,
          PubSubProducerCallback callback,
          DeleteMetadata deleteMetadata) {
        return null;
      }

      @Override
      public Future<PubSubProduceResult> update(
          Object key,
          Object update,
          int valueSchemaId,
          int derivedSchemaId,
          PubSubProducerCallback callback) {
        // no-op
        return null;
      }

      @Override
      public void flush() {
        // no-op
      }

      @Override
      public void close() {
        // no-op
      }
    };

    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(exceptionWriter);
    reducer.configure(setupJobConf(100));
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    BytesWritable valueWritable = new BytesWritable(valueFieldValue.getBytes());
    List<BytesWritable> values = new ArrayList<>();
    values.add(valueWritable);
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = createZeroCountReporterMock();

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);
    // The following 'reduce' operation will throw exception
    Assert.assertThrows(
        VeniceException.class,
        () -> reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter));
  }

  @Test
  public void testClosingReducerWithWriterException() throws IOException {
    AbstractVeniceWriter exceptionWriter = new AbstractVeniceWriter(TOPIC_NAME) {
      @Override
      public Future<PubSubProduceResult> put(
          Object key,
          Object value,
          int valueSchemaId,
          PubSubProducerCallback callback,
          PutMetadata putMetadata) {
        callback.onCompletion(null, new VeniceException("Some writer exception"));
        return null;
      }

      @Override
      public Future<PubSubProduceResult> delete(
          Object key,
          PubSubProducerCallback callback,
          DeleteMetadata deleteMetadata) {
        return null;
      }

      @Override
      public CompletableFuture<PubSubProduceResult> put(
          Object key,
          Object value,
          int valueSchemaId,
          PubSubProducerCallback callback) {
        callback.onCompletion(null, new VeniceException("Some writer exception"));
        return null;
      }

      @Override
      public Future<PubSubProduceResult> update(
          Object key,
          Object update,
          int valueSchemaId,
          int derivedSchemaId,
          PubSubProducerCallback callback) {
        // no-op
        return null;
      }

      @Override
      public void flush() {
        // no-op
      }

      @Override
      public void close(boolean gracefulClose) {
        Assert.assertFalse(gracefulClose, "A writer exception is thrown, should not close all segments");
      }

      @Override
      public void close() throws IOException {
        // no-op
      }
    };
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(exceptionWriter);
    reducer.configure(setupJobConf(100));
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    BytesWritable valueWritable = new BytesWritable(valueFieldValue.getBytes());
    List<BytesWritable> values = new ArrayList<>();
    values.add(valueWritable);
    OutputCollector mockCollector = mock(OutputCollector.class);
    reducer.reduce(keyWritable, values.iterator(), mockCollector, createZeroCountReporterMock());
    Assert.assertThrows(VeniceException.class, () -> reducer.close());
  }

  private Reporter createZeroCountReporterMock() {
    Reporter mockReporter = mock(Reporter.class);
    Counters.Counter mockCounters = mock(Counters.Counter.class);
    when(mockCounters.getCounter()).thenReturn(0L); // No counted
    when(mockReporter.getCounter(anyString(), anyString())).thenReturn(mockCounters);
    return mockReporter;
  }
}
