package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_POLICY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_DIR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.hadoop.AbstractVeniceFilter;
import com.linkedin.venice.hadoop.FilterChain;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.input.kafka.ttl.TTLResolutionPolicy;
import com.linkedin.venice.hadoop.mapreduce.datawriter.map.AbstractTestVeniceMapper;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceKafkaInputMapper extends AbstractTestVeniceMapper<VeniceKafkaInputMapper> {
  private static final AtomicReference<byte[]> EMPTY_BYTE_REF = new AtomicReference<>(new byte[0]);
  private static final AtomicReference<Long> EMPTY_LONG_REF = new AtomicReference<>(-1L);
  private static final KafkaInputMapperKey EMPTY_KEY = new KafkaInputMapperKey();
  static {
    PubSubPosition offset = ApacheKafkaOffsetPosition.of(1);
    EMPTY_KEY.key = ByteBuffer.wrap("test_key".getBytes());
    EMPTY_KEY.positionWireBytes = offset.toWireFormatBuffer();
    EMPTY_KEY.positionFactoryClass = offset.getFactoryClassName();
  }

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
    newMapper().getRecordReader(VeniceProperties.empty());
  }

  @Test
  public void testEmptyFilterWhenTTLNotSpecified() {
    try (VeniceKafkaInputMapper mapper = new VeniceKafkaInputMapper()) {
      Assert.assertNull(mapper.getFilterChain(VeniceProperties.empty()));
    }
  }

  @Test
  public void testValidFilterWhenTTLSpecified() {
    Properties props = new Properties();
    props.put(REPUSH_TTL_ENABLE, true);
    props.put(REPUSH_TTL_POLICY, TTLResolutionPolicy.RT_WRITE_ONLY.getValue());
    props.put(RMD_SCHEMA_DIR, "tmp");
    props.put(VALUE_SCHEMA_DIR, "tmp2");
    props.put(KAFKA_INPUT_TOPIC, "test_v1");
    props.put(KAFKA_INPUT_BROKER_URL, "dummy");
    props.put(KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.toString());
    props.put(REPUSH_TTL_START_TIMESTAMP, System.currentTimeMillis() - 10L * Time.MS_PER_SECOND);
    Assert.assertFalse(newMapper().getFilterChain(new VeniceProperties(props)).isEmpty());

    // filter is also present when chunking is enabled.
    props.put(VeniceWriter.ENABLE_CHUNKING, true);
    Assert.assertFalse(newMapper().getFilterChain(new VeniceProperties(props)).isEmpty());
  }

  @Test(dataProvider = MAPPER_PARAMS_DATA_PROVIDER)
  public void testProcessWithoutFilter(int numReducers, int taskId) throws IOException {
    VeniceKafkaInputMapper mapper = getMapper(numReducers, taskId);

    ArgumentCaptor<BytesWritable> keyCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    ArgumentCaptor<BytesWritable> valueCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    OutputCollector<BytesWritable, BytesWritable> collector = mock(OutputCollector.class);

    mapper.map(EMPTY_KEY, generateKIFRecord(), collector, null);

    // Given there's no filter and all records are valid, collector should collect all key and value
    verify(collector, times(getNumberOfCollectorInvocationForFirstMapInvocation(numReducers, taskId)))
        .collect(keyCaptor.capture(), valueCaptor.capture());
  }

  @Test
  public void testProcessWithFilterFilteringPartialRecords() {
    AbstractVeniceFilter<KafkaInputMapperValue> filter = mock(AbstractVeniceFilter.class);
    doReturn(true, false, true, false, false).when(filter).checkAndMaybeFilterValue(any());// filter out partial
                                                                                           // records

    VeniceKafkaInputMapper mapper = spy(newMapper());
    FilterChain<KafkaInputMapperValue> filterChain = new FilterChain<>(filter);
    doReturn(filterChain).when(mapper).getFilterChain(any());
    mapper.configureTask(any());
    int validCount = 0, filteredCount = 0;
    for (int i = 0; i < 5; i++) {
      if (mapper.process(
          EMPTY_KEY,
          generateKIFRecord(),
          -1L,
          EMPTY_BYTE_REF,
          EMPTY_BYTE_REF,
          EMPTY_LONG_REF,
          mock(DataWriterTaskTracker.class))) {
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
    doReturn(true).when(filter).checkAndMaybeFilterValue(any()); // filter out all records

    FilterChain<KafkaInputMapperValue> filterChain = new FilterChain<>(filter);
    VeniceKafkaInputMapper mapper = spy(getMapper(numReducers, taskId));
    doReturn(filterChain).when(mapper).getFilterChain(any());
    // Trigger manually to set the dummy filterChain
    mapper.configureTask(any());

    Assert.assertFalse(mapper.process(null, null, -1L, null, null, null, mock(DataWriterTaskTracker.class)));
  }

  private KafkaInputMapperValue generateKIFRecord() {
    KafkaInputMapperValue value = new KafkaInputMapperValue();
    PubSubPosition position = ApacheKafkaOffsetPosition.of(0);
    value.positionWireBytes = position.toWireFormatBuffer();
    value.positionFactoryClass = position.getFactoryClassName();
    value.schemaId = -1;
    value.valueType = MapperValueType.PUT;
    value.replicationMetadataPayload = ByteBuffer.wrap(RMD.getBytes());
    value.value = ByteBuffer.wrap(new byte[0]);
    return value;
  }
}
