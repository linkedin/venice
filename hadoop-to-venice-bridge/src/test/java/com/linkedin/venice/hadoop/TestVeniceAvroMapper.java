package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.TopicAuthorizationVeniceException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static org.mockito.Mockito.*;

public class TestVeniceAvroMapper extends AbstractTestVeniceMR {

  @Test
  public void testConfigure() {
    JobConf job = setupJobConf();
    VeniceAvroMapper mapper = new VeniceAvroMapper();
    try {
      mapper.configure(job);
    } catch (Exception e) {
      Assert.fail("VeniceAvroMapper#configure should not throw any exception when all the required props are there");
    }
  }

  @Test (expectedExceptions = UndefinedPropertyException.class)
  public void testConfigureWithMissingProps() {
    JobConf job = setupJobConf();
    job.unset(KafkaPushJob.TOPIC_PROP);
    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(job);
  }

  @Test
  public void testMap() throws IOException {
    final String keyFieldValue = "key_field_value";
    final String valueFieldValue = "value_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(keyFieldValue, valueFieldValue);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(setupJobConf());
    mapper.map(wrapper, NullWritable.get(), output, null);

    ArgumentCaptor<BytesWritable> keyCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    ArgumentCaptor<BytesWritable> valueCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    verify(output).collect(keyCaptor.capture(), valueCaptor.capture());
    Assert.assertTrue(getHexString(keyCaptor.getValue().copyBytes())
        .endsWith(getHexString(keyFieldValue.getBytes())));
    Assert.assertTrue(getHexString(valueCaptor.getValue().copyBytes())
        .endsWith(getHexString(valueFieldValue.getBytes())));
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testMapWithNullKey() throws IOException {
    final String valueFieldValue = "value_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(null, valueFieldValue);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(setupJobConf());
    mapper.map(wrapper, NullWritable.get(), output, null);
  }

  @Test
  public void testMapWithNullValue() throws IOException {
    final String keyFieldValue = "key_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(keyFieldValue, null);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(setupJobConf());
    mapper.map(wrapper, NullWritable.get(), output, mock(Reporter.class));

    verify(output, never()).collect(Mockito.any(), Mockito.any());
  }

  @Test
  public void testMapWithTopicAuthorizationException() throws IOException {
    final String keyFieldValue = "key_field_value";
    final String valueFieldValue = "value_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(keyFieldValue, valueFieldValue);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);
    Reporter mockReporter = createMockReporterWithCount(0L);
    VeniceReducer mockReducer = mock(VeniceReducer.class);
    doThrow(new TopicAuthorizationVeniceException("No ACL permission")).when(mockReducer).sendMessageToKafka(any(), any(), any());

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(setupJobConf());
    mapper.setVeniceReducer(mockReducer);
    mapper.setIsMapperOnly(true);
    mapper.map(wrapper, NullWritable.get(), output, mockReporter);
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getCounterName()),
        eq(1L)
    );
  }

  @Test
  public void testMapWithExceededQuota() throws IOException {
    final String keyFieldValue = "key_field_value";
    final String valueFieldValue = "value_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(keyFieldValue, valueFieldValue);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);
    Reporter mockReporter = createMockReporterWithCount(1L);

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    JobConf mapperJobConfig = setupJobConf();
    mapperJobConfig.set(STORAGE_QUOTA_PROP, "18"); // With this setup, storage quota should be exceeded
    mapperJobConfig.set(STORAGE_ENGINE_OVERHEAD_RATIO, "1.5");
    mapper.configure(mapperJobConfig);
    mapper.setIsMapperOnly(false);
    mapper.map(wrapper, NullWritable.get(), output, mockReporter);

    verify(mockReporter, times(1)).
        incrCounter(
            eq(MRJobCounterHelper.TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME.getGroupName()),
            eq(MRJobCounterHelper.TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME.getCounterName()),
            eq(18L)
        );

    // Expect no authorization error
    verify(mockReporter, never()).incrCounter(
        eq(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getCounterName()),
        anyLong()
    );
    // Even with exceeded quota, the mapper should still count key and value sizes in bytes and output
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.TOTAL_KEY_SIZE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.TOTAL_KEY_SIZE_GROUP_COUNTER_NAME.getCounterName()),
        anyLong()
    );
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME.getCounterName()),
        anyLong()
    );
    verify(output, times(1)).collect(any(), any());
  }

  @Test
  public void testMapWithQuotaNotExceeded() throws IOException {
    final String keyFieldValue = "key_field_value";
    final String valueFieldValue = "value_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(keyFieldValue, valueFieldValue);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);
    Reporter mockReporter = createMockReporterWithCount(1L);

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    JobConf mapperJobConfig = setupJobConf();
    mapperJobConfig.set(STORAGE_QUOTA_PROP, "100"); // With this setup, storage quota should not exceeded
    mapperJobConfig.set(STORAGE_ENGINE_OVERHEAD_RATIO, "1.5");
    mapper.configure(mapperJobConfig);
    mapper.setIsMapperOnly(false);
    mapper.map(wrapper, NullWritable.get(), output, mockReporter);

    verify(mockReporter, times(1)).
        incrCounter(
            eq(MRJobCounterHelper.TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME.getGroupName()),
            eq(MRJobCounterHelper.TOTAL_UNCOMPRESSED_VALUE_SIZE_GROUP_COUNTER_NAME.getCounterName()),
            eq(18L)
        );

    // Not write ACL failure
    verify(mockReporter, never()).incrCounter(
        eq(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.WRITE_ACL_FAILURE_GROUP_COUNTER_NAME.getCounterName()),
        anyLong()
    );
    // Expect reporter to record these counters due to no early termination
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.TOTAL_KEY_SIZE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.TOTAL_KEY_SIZE_GROUP_COUNTER_NAME.getCounterName()),
        anyLong()
    );
    verify(mockReporter, times(1)).incrCounter(
        eq(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME.getGroupName()),
        eq(MRJobCounterHelper.TOTAL_VALUE_SIZE_GROUP_COUNTER_NAME.getCounterName()),
        anyLong()
    );
    // Expect the output collect to collect output due to no early termination
    verify(output, times(1)).collect(any(), any());
  }

  private AvroWrapper<IndexedRecord> getAvroWrapper(String keyFieldValue, String valueFieldValue) {
    GenericData.Record record = new GenericData.Record(Schema.parse(SCHEMA_STR));
    record.put(KEY_FIELD, keyFieldValue);
    record.put(VALUE_FIELD, valueFieldValue);
    return new AvroWrapper<>(record);
  }

  private Reporter createMockReporterWithCount(long countToReturn) {
    Reporter mockReporter = mock(Reporter.class);
    Counters.Counter mockCounter = mock(Counters.Counter.class);
    when(mockCounter.getCounter()).thenReturn(countToReturn);
    when(mockReporter.getCounter(anyString(), anyString())).thenReturn(mockCounter);
    return mockReporter;
  }
}
