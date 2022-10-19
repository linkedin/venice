package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.hadoop.VeniceReducer;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Reporter;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceKafkaInputReducer {
  private static final RecordSerializer KAFKA_INPUT_MAPPER_VALUE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperValue.SCHEMA$);
  private static final String VALUE_PREFIX = "value_";
  private static final String RMD_VALUE_PREFIX = "rmd_value_";

  public List<BytesWritable> getValues(List<MapperValueType> valueTypes) {
    List<BytesWritable> values = new ArrayList<>();
    long offset = 0;
    for (MapperValueType valueType: valueTypes) {
      KafkaInputMapperValue value = new KafkaInputMapperValue();
      value.offset = offset++;
      value.schemaId = -1;
      value.valueType = valueType;
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

  @Test
  public void testExtract() {
    byte[] keyBytes = "test_key".getBytes();
    BytesWritable keyWritable = new BytesWritable();
    keyWritable.set(keyBytes, 0, keyBytes.length);
    VeniceKafkaInputReducer reducer = new VeniceKafkaInputReducer();
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
    Assert.assertEquals(message.getValueSchemaId(), -1);

    /**
     * Construct a list of values, which contains both 'PUT' and 'DELETE', but 'DELETE' is the last one.
     */
    values = getValues(Arrays.asList(MapperValueType.PUT, MapperValueType.PUT, MapperValueType.DELETE));

    message = reducer.extract(keyWritable, values.iterator(), Mockito.mock(Reporter.class));
    Assert.assertNotNull(message);

    /**
     * Construct a list of values, which contains both 'PUT' and 'DELETE', but 'DELETE' is in the middle.
     */
    values = getValues(Arrays.asList(MapperValueType.PUT, MapperValueType.DELETE, MapperValueType.PUT));

    message = reducer.extract(keyWritable, values.iterator(), Mockito.mock(Reporter.class));
    Assert.assertNotNull(message);
    Assert.assertEquals(message.getKeyBytes(), keyBytes);
    Assert.assertEquals(message.getValueBytes(), (VALUE_PREFIX + 2).getBytes());
    Assert.assertEquals(message.getValueSchemaId(), -1);
  }
}
