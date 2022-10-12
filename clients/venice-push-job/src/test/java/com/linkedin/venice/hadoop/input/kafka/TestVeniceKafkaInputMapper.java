package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.VenicePushJob.REPUSH_TTL_IN_HOURS;
import static com.linkedin.venice.hadoop.VenicePushJob.REPUSH_TTL_POLICY;
import static com.linkedin.venice.hadoop.VenicePushJob.RMD_SCHEMA_DIR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.hadoop.AbstractTestVeniceMapper;
import com.linkedin.venice.hadoop.AbstractVeniceFilter;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceKafkaInputMapper extends AbstractTestVeniceMapper<VeniceKafkaInputMapper> {
  private static final BytesWritable BYTES_WRITABLE = new BytesWritable(new byte[0]);
  private static final String RMD = "rmd";

  @Override
  protected VeniceKafkaInputMapper newMapper() {
    return new VeniceKafkaInputMapper();
  }

  @Test(dataProvider = MAPPER_PARAMS_DATA_PROVIDER)
  public void testConfigure(int numReducers, int taskId) throws IOException {
    JobConf job = setupJobConf(numReducers, taskId);
    VeniceKafkaInputMapper mapper = getMapper(numReducers, taskId);
    try {
      mapper.configure(job);
    } catch (Exception e) {
      Assert.fail(
          "VeniceKafkaInputMapper#configure should not throw any exception when all the required props are there");
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUnsupportedGetRecordReader() {
    newMapper().getRecordReader(new VeniceProperties());
  }

  @Test
  public void testEmptyFilterWhenTTLNotSpecified() {
    try (VeniceKafkaInputMapper mapper = new VeniceKafkaInputMapper()) {
      Assert.assertNull(mapper.getFilter(new VeniceProperties()));
    }
  }

  @Test
  public void testValidFilterWhenTTLSpecified() {
    Properties props = new Properties();
    props.put(REPUSH_TTL_IN_HOURS, 10L);
    props.put(REPUSH_TTL_POLICY, 0);
    props.put(RMD_SCHEMA_DIR, "tmp");
    Assert.assertNotNull(newMapper().getFilter(new VeniceProperties(props)));
  }

  @Test(dataProvider = MAPPER_PARAMS_DATA_PROVIDER)
  public void testProcessWithoutFilter(int numReducers, int taskId) throws IOException {
    VeniceKafkaInputMapper mapper = getMapper(numReducers, taskId);

    ArgumentCaptor<BytesWritable> keyCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    ArgumentCaptor<BytesWritable> valueCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    OutputCollector<BytesWritable, BytesWritable> collector = mock(OutputCollector.class);

    Pair<BytesWritable, KafkaInputMapperValue> record = generateRecord();
    mapper.map(record.getFirst(), record.getSecond(), collector, null);

    // Given there's no filter and all records are valid, collector should collect all key and value
    verify(collector, times(getNumberOfCollectorInvocationForFirstMapInvocation(numReducers, taskId)))
        .collect(keyCaptor.capture(), valueCaptor.capture());
  }

  @Test
  public void testProcessWithFilterFilteringPartialRecords() {
    AbstractVeniceFilter<KafkaInputMapperValue> filter = mock(AbstractVeniceFilter.class);
    doReturn(true, false, true, false, false).when(filter).applyRecursively(any()); // filter out partial records

    VeniceKafkaInputMapper mapper = spy(newMapper());
    doReturn(filter).when(mapper).getFilter(any());
    mapper.configureTask(any(), any());
    int validCount = 0, filteredCount = 0;
    Pair<BytesWritable, KafkaInputMapperValue> record = generateRecord();
    for (int i = 0; i < 5; i++) {
      if (mapper.process(BYTES_WRITABLE, record.getSecond(), BYTES_WRITABLE, BYTES_WRITABLE, null)) {
        validCount++;
      } else {
        filteredCount++;
      }
    }
    Assert.assertEquals(validCount, 3);
    Assert.assertEquals(filteredCount, 2);
  }

  @Test(dataProvider = MAPPER_PARAMS_DATA_PROVIDER)
  public void testProcessWithFilterFilteringAllRecords(int numReducers, int taskId) throws IOException {
    AbstractVeniceFilter<KafkaInputMapperValue> filter = mock(AbstractVeniceFilter.class);
    doReturn(true).when(filter).applyRecursively(any()); // filter out all records

    VeniceKafkaInputMapper mapper = spy(getMapper(numReducers, taskId));
    doReturn(filter).when(mapper).getFilter(any());
    mapper.configureTask(any(), any());

    Assert.assertFalse(mapper.process(any(), any(), any(), any(), any()));
  }

  private Pair<BytesWritable, KafkaInputMapperValue> generateRecord() {
    return generateRecord(value -> {});
  }

  private Pair<BytesWritable, KafkaInputMapperValue> generateRecord(Consumer<KafkaInputMapperValue> consumer) {
    KafkaInputMapperValue value = new KafkaInputMapperValue();
    value.offset = 0;
    value.schemaId = -1;
    value.valueType = MapperValueType.PUT;
    value.replicationMetadataPayload = ByteBuffer.wrap(RMD.getBytes());
    value.value = ByteBuffer.wrap(new byte[0]);
    consumer.accept(value);
    return new Pair<>(BYTES_WRITABLE, value);
  }
}
