package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Assert;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class TestVeniceMapper extends AbstractTestVeniceMR {

  @Test
  public void testConfigure() {
    JobConf job = setupJobConf();
    VeniceMapper mapper = new VeniceMapper();
    try {
      mapper.configure(job);
    } catch (Exception e) {
      Assert.fail("VeniceMapper#configure should not throw any exception when all the required props are there");
    }
  }

  @Test (expectedExceptions = UndefinedPropertyException.class)
  public void testConfigureWithMissingProps() {
    JobConf job = setupJobConf();
    job.unset(KafkaPushJob.TOPIC_PROP);
    VeniceMapper mapper = new VeniceMapper();
    mapper.configure(job);
  }

  @Test
  public void testMap() throws IOException {
    final String keyFieldValue = "key_field_value";
    final String valueFieldValue = "value_field_value";
    GenericData.Record record = new GenericData.Record(Schema.parse(SCHEMA_STR));
    record.put(KEY_FIELD, keyFieldValue);
    record.put(VALUE_FIELD, valueFieldValue);

    AvroWrapper<IndexedRecord> wrapper = new AvroWrapper(record);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);

    VeniceMapper mapper = new VeniceMapper();
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
    GenericData.Record record = new GenericData.Record(Schema.parse(SCHEMA_STR));
    record.put(KEY_FIELD, null);
    record.put(VALUE_FIELD, valueFieldValue);

    AvroWrapper<IndexedRecord> wrapper = new AvroWrapper(record);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);

    VeniceMapper mapper = new VeniceMapper();
    mapper.configure(setupJobConf());
    mapper.map(wrapper, NullWritable.get(), output, null);
  }

  @Test
  public void testMapWithNullValue() throws IOException {
    final String keyFieldValue = "key_field_value";
    GenericData.Record record = new GenericData.Record(Schema.parse(SCHEMA_STR));
    record.put(KEY_FIELD, keyFieldValue);
    record.put(VALUE_FIELD, null);

    AvroWrapper<IndexedRecord> wrapper = new AvroWrapper(record);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);

    VeniceMapper mapper = new VeniceMapper();
    mapper.configure(setupJobConf());
    mapper.map(wrapper, NullWritable.get(), output, null);

    verify(output, never()).collect(any(), any());
  }
}
