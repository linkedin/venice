package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.hadoop.mapreduce.datawriter.reduce.VeniceReducer.MAP_REDUCE_JOB_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_POLICY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.GzipCompressor;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.hadoop.AbstractVeniceFilter;
import com.linkedin.venice.hadoop.FilterChain;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.input.kafka.ttl.TTLResolutionPolicy;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.ReporterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.task.datawriter.AbstractPartitionWriter;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
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

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testExtract(boolean isChunkingEnabled, boolean valueContainsRmdPayload) {
    byte[] keyBytes = "test_key".getBytes();
    KafkaInputMapperKey mapperKey = new KafkaInputMapperKey();
    mapperKey.key = ByteBuffer.wrap(keyBytes);
    mapperKey.offset = 1;
    RecordSerializer<KafkaInputMapperKey> keySerializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperKey.SCHEMA$);
    byte[] serializedMapperKey = keySerializer.serialize(mapperKey);
    VeniceKafkaInputReducer reducer = new VeniceKafkaInputReducer();
    reducer.setChunkingEnabled(isChunkingEnabled);
    reducer.setSourceVersionCompressor(new NoopCompressor());
    reducer.setDestVersionCompressor(new NoopCompressor());
    /**
     * Construct a list of values, which contain only 'PUT'.
     */
    List<byte[]> values = getValues(
        Arrays.asList(MapperValueType.PUT, MapperValueType.PUT, MapperValueType.PUT),
        valueContainsRmdPayload);

    AbstractPartitionWriter.VeniceWriterMessage message = reducer.extract(
        serializedMapperKey,
        values.iterator(),
        new ReporterBackedMapReduceDataWriterTaskTracker(Mockito.mock(Reporter.class)));
    Assert.assertNotNull(message);
    Assert.assertEquals(message.getKeyBytes(), keyBytes);
    Assert.assertEquals(message.getValueBytes(), (VALUE_PREFIX + 2).getBytes());
    Assert.assertEquals(message.getValueSchemaId(), 1);
    Assert.assertEquals(message.getRmdVersionId(), valueContainsRmdPayload ? 1 : -1);

    /**
     * Construct a list of values, which contains both 'PUT' and 'DELETE', but 'DELETE' is the last one.
     */
    values = getValues(
        Arrays.asList(MapperValueType.PUT, MapperValueType.PUT, MapperValueType.DELETE),
        valueContainsRmdPayload);

    message = reducer.extract(
        serializedMapperKey,
        values.iterator(),
        new ReporterBackedMapReduceDataWriterTaskTracker(Mockito.mock(Reporter.class)));
    // If DELETE contains RMD, it should be kept.
    if (valueContainsRmdPayload) {
      Assert.assertNotNull(message);
    } else {
      Assert.assertNull(message);
    }

    /**
     * Construct a list of values, which contains both 'PUT' and 'DELETE', but 'DELETE' is in the middle.
     */
    values = getValues(
        Arrays.asList(MapperValueType.PUT, MapperValueType.DELETE, MapperValueType.PUT),
        valueContainsRmdPayload);

    message = reducer.extract(
        serializedMapperKey,
        values.iterator(),
        new ReporterBackedMapReduceDataWriterTaskTracker(Mockito.mock(Reporter.class)));
    Assert.assertNotNull(message);
    Assert.assertEquals(message.getKeyBytes(), keyBytes);
    Assert.assertEquals(message.getValueBytes(), (VALUE_PREFIX + 2).getBytes());
    Assert.assertEquals(message.getValueSchemaId(), 1);
    Assert.assertEquals(message.getRmdVersionId(), valueContainsRmdPayload ? 1 : -1);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testExtractWithTTL(boolean isChunkingEnabled) {
    // set up filter
    AbstractVeniceFilter<KafkaInputMapperValue> filter = mock(AbstractVeniceFilter.class);
    doReturn(true).when(filter).checkAndMaybeFilterValue(any()); // filter out all records
    FilterChain<KafkaInputMapperValue> filterChain = new FilterChain<>(filter);

    byte[] keyBytes = "test_key".getBytes();
    KafkaInputMapperKey mapperKey = new KafkaInputMapperKey();
    mapperKey.key = ByteBuffer.wrap(keyBytes);
    mapperKey.offset = 1;
    RecordSerializer<KafkaInputMapperKey> keySerializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperKey.SCHEMA$);
    byte[] serializedMapperKey = keySerializer.serialize(mapperKey);
    VeniceKafkaInputReducer reducer = spy(new VeniceKafkaInputReducer());
    doReturn(filterChain).when(reducer).initFilterChain(any());
    reducer.configureTask(getTestProps());
    reducer.setChunkingEnabled(isChunkingEnabled);

    /**
     * Construct a list of values, which contain only 'PUT'.
     */
    List<byte[]> values = getValues(Arrays.asList(MapperValueType.PUT, MapperValueType.PUT, MapperValueType.PUT), true);

    AbstractPartitionWriter.VeniceWriterMessage message = reducer.extract(
        serializedMapperKey,
        values.iterator(),
        new ReporterBackedMapReduceDataWriterTaskTracker(Mockito.mock(Reporter.class)));

    if (isChunkingEnabled) {
      // all records are filtered
      Assert.assertNull(message);
    } else {
      // when chunking isn't enabled, even with ttl config, filter should not be created so no effect at all.
      Assert.assertNotNull(message);
    }
  }

  public List<byte[]> getValues(List<MapperValueType> valueTypes, boolean hasRmdPayload) {
    List<byte[]> values = new ArrayList<>();
    long offset = 0;
    for (MapperValueType valueType: valueTypes) {
      KafkaInputMapperValue value = new KafkaInputMapperValue();
      value.offset = offset++;
      value.schemaId = 1;
      value.valueType = valueType;
      value.replicationMetadataVersionId = hasRmdPayload ? 1 : -1;
      value.replicationMetadataPayload =
          hasRmdPayload ? ByteBuffer.wrap(RMD_VALUE_PREFIX.getBytes()) : ByteBuffer.allocate(0);
      if (valueType.equals(MapperValueType.DELETE)) {
        value.value = ByteBuffer.wrap(new byte[0]);
      } else {
        value.value = ByteBuffer.wrap((VALUE_PREFIX + value.offset).getBytes());
      }
      byte[] serializedValue = KAFKA_INPUT_MAPPER_VALUE_SERIALIZER.serialize(value);
      values.add(serializedValue);
    }
    Collections.reverse(values);
    return values;
  }

  private VeniceProperties getTestProps() {
    Properties props = new Properties();
    props.put(MAP_REDUCE_JOB_ID_PROP, "job_200707121733_0003");
    props.put(KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.name());
    props.put(COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.name());
    props.put(VALUE_SCHEMA_ID_PROP, 1);
    props.put(REPUSH_TTL_ENABLE, true);
    props.put(REPUSH_TTL_POLICY, TTLResolutionPolicy.RT_WRITE_ONLY.getValue());
    props.put(REPUSH_TTL_START_TIMESTAMP, 10000000L - 10L * Time.MS_PER_SECOND);
    props.put(RMD_SCHEMA_DIR, "tmp");
    props.put(VALUE_SCHEMA_DIR, "tmp2");
    props.put(KAFKA_BOOTSTRAP_SERVERS, "localhost:8090"); // Destination Kafka cluster
    props.put(TOPIC_PROP, "test_store_v2"); // Destination topic
    props.put(KAFKA_INPUT_BROKER_URL, "localhost:9092"); // Source Kafka cluster
    props.put(KAFKA_INPUT_TOPIC, "test_store_v1"); // Source topic
    return new VeniceProperties(props);
  }

  @Test
  public void testCompress() {
    final byte[] testValue = "abc".getBytes();
    VeniceKafkaInputReducer reducer = new VeniceKafkaInputReducer();
    reducer.setSourceVersionCompressor(new NoopCompressor());
    reducer.setDestVersionCompressor(new NoopCompressor());

    Assert.assertNull(reducer.compress(null));
    Assert.assertEquals(reducer.compress(testValue), testValue);

    // Setup different compressor
    reducer.setDestVersionCompressor(new GzipCompressor());
    Assert.assertNotEquals(reducer.compress(testValue), testValue);
  }
}
