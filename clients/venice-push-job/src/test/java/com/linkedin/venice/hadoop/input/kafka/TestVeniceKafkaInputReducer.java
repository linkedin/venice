package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.VenicePushJob.REPUSH_TTL_IN_SECONDS;
import static com.linkedin.venice.hadoop.VenicePushJob.REPUSH_TTL_POLICY;
import static com.linkedin.venice.hadoop.VenicePushJob.RMD_SCHEMA_DIR;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_SCHEMA_ID_PROP;
import static com.linkedin.venice.hadoop.VeniceReducer.MAP_REDUCE_JOB_ID_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.linkedin.venice.hadoop.AbstractVeniceFilter;
import com.linkedin.venice.hadoop.FilterChain;
import com.linkedin.venice.hadoop.VeniceReducer;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceKafkaInputReducer {
  private static final RecordSerializer KAFKA_INPUT_MAPPER_VALUE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperValue.SCHEMA$);
  private static final String VALUE_PREFIX = "value_";
  private static final String RMD_VALUE_PREFIX = "rmd_value_";

  @Test
  public void testTTLFilter() {
    // chunking is not specified, but ttl is on.
    VeniceKafkaInputReducer reducer = new VeniceKafkaInputReducer();
    Assert.assertNull(reducer.initFilterChain(getTestProps()));

    // both is specified so the filter is present
    reducer.setChunkingEnabled(true);
    Assert.assertNotNull(reducer.initFilterChain(getTestProps()));
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testExtract(boolean isChunkingEnabled) {
    byte[] keyBytes = "test_key".getBytes();
    BytesWritable keyWritable = new BytesWritable();
    keyWritable.set(keyBytes, 0, keyBytes.length);
    VeniceKafkaInputReducer reducer = new VeniceKafkaInputReducer();
    reducer.setChunkingEnabled(isChunkingEnabled);
    /**
     * Construct a list of values, which contain only 'PUT'.
     */
    List<BytesWritable> values =
        getValues(Arrays.asList(MapperValueType.PUT, MapperValueType.PUT, MapperValueType.PUT));

    VeniceReducer.VeniceWriterMessage message =
        reducer.extract(keyWritable, values.iterator(), Mockito.mock(Reporter.class));
    Assert.assertNotNull(message);
    Assert.assertEquals(message.getKeyBytes(), keyBytes);
    Assert.assertEquals(message.getValueBytes(), (VALUE_PREFIX + 2).getBytes());
    Assert.assertEquals(message.getValueSchemaId(), 1);
    // TODO: RMD chunking is not supported so RMD info doesn't get wrote back and remains -1.
    Assert.assertEquals(message.getRmdVersionId(), isChunkingEnabled ? -1 : 1);

    /**
     * Construct a list of values, which contains both 'PUT' and 'DELETE', but 'DELETE' is the last one.
     */
    values = getValues(Arrays.asList(MapperValueType.PUT, MapperValueType.PUT, MapperValueType.DELETE));

    message = reducer.extract(keyWritable, values.iterator(), Mockito.mock(Reporter.class));
    if (isChunkingEnabled) {
      Assert.assertNull(message);
    } else {
      Assert.assertNotNull(message);
    }

    /**
     * Construct a list of values, which contains both 'PUT' and 'DELETE', but 'DELETE' is in the middle.
     */
    values = getValues(Arrays.asList(MapperValueType.PUT, MapperValueType.DELETE, MapperValueType.PUT));

    message = reducer.extract(keyWritable, values.iterator(), Mockito.mock(Reporter.class));
    Assert.assertNotNull(message);
    Assert.assertEquals(message.getKeyBytes(), keyBytes);
    Assert.assertEquals(message.getValueBytes(), (VALUE_PREFIX + 2).getBytes());
    Assert.assertEquals(message.getValueSchemaId(), 1);
    // TODO: RMD chunking is not supported so RMD info doesn't get wrote back and remains -1.
    Assert.assertEquals(message.getRmdVersionId(), isChunkingEnabled ? -1 : 1);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testExtractWithTTL(boolean isChunkingEnabled) {
    // set up filter
    AbstractVeniceFilter<KafkaInputMapperValue> filter = mock(AbstractVeniceFilter.class);
    doReturn(true).when(filter).apply(any()); // filter out all records
    FilterChain<KafkaInputMapperValue> filterChain = new FilterChain<>(filter);

    byte[] keyBytes = "test_key".getBytes();
    BytesWritable keyWritable = new BytesWritable();
    keyWritable.set(keyBytes, 0, keyBytes.length);
    VeniceKafkaInputReducer reducer = spy(new VeniceKafkaInputReducer());
    doReturn(filterChain).when(reducer).initFilterChain(any());
    reducer.configureTask(getTestProps(), getTestJobConf());
    reducer.setChunkingEnabled(isChunkingEnabled);

    /**
     * Construct a list of values, which contain only 'PUT'.
     */
    List<BytesWritable> values =
        getValues(Arrays.asList(MapperValueType.PUT, MapperValueType.PUT, MapperValueType.PUT));

    VeniceReducer.VeniceWriterMessage message =
        reducer.extract(keyWritable, values.iterator(), Mockito.mock(Reporter.class));

    if (isChunkingEnabled) {
      // all records are filtered
      Assert.assertNull(message);
    } else {
      // when chunking isn't enabled, even with ttl config, filter should not be created so no effect at all.
      Assert.assertNotNull(message);
    }
  }

  public List<BytesWritable> getValues(List<MapperValueType> valueTypes) {
    List<BytesWritable> values = new ArrayList<>();
    long offset = 0;
    for (MapperValueType valueType: valueTypes) {
      KafkaInputMapperValue value = new KafkaInputMapperValue();
      value.offset = offset++;
      value.schemaId = 1;
      value.valueType = valueType;
      value.replicationMetadataVersionId = 1;
      value.replicationMetadataPayload = ByteBuffer.wrap(RMD_VALUE_PREFIX.getBytes());
      if (valueType.equals(MapperValueType.DELETE)) {
        value.value = ByteBuffer.wrap(new byte[0]);
      } else {
        value.value = ByteBuffer.wrap((VALUE_PREFIX + value.offset).getBytes());
      }
      BytesWritable valueWritable = new BytesWritable();
      byte[] serializedValue = KAFKA_INPUT_MAPPER_VALUE_SERIALIZER.serialize(value);
      valueWritable.set(serializedValue, 0, serializedValue.length);
      values.add(valueWritable);
    }
    return values;
  }

  private VeniceProperties getTestProps() {
    Properties props = new Properties();
    props.put(VALUE_SCHEMA_ID_PROP, 1);
    props.put(REPUSH_TTL_IN_SECONDS, 10L);
    props.put(REPUSH_TTL_POLICY, 0);
    props.put(RMD_SCHEMA_DIR, "tmp");
    return new VeniceProperties(props);
  }

  private JobConf getTestJobConf() {
    JobConf conf = new JobConf();
    conf.set(MAP_REDUCE_JOB_ID_PROP, "job_200707121733_0003");
    return conf;
  }
}
