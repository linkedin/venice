package com.linkedin.venice.hadoop.mapreduce.datawriter.map;

import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_KEY_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.Pair;
import java.io.IOException;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestVeniceVsonMapper extends AbstractTestVeniceMapper<VeniceVsonMapper> {
  private String fileKeySchemaStr = "\"int32\"";
  private String fileValueSchemaStr = "{\"userId\": \"int32\", \"userEmail\": \"string\"}";

  private VsonAvroSerializer keyDeserializer;
  private VsonAvroSerializer valueDeserializer;

  private VeniceAvroKafkaSerializer keySerializer;

  protected VeniceVsonMapper newMapper() {
    return new VeniceVsonMapper();
  }

  @BeforeTest
  public void setUp() {
    keyDeserializer = VsonAvroSerializer.fromSchemaStr(fileKeySchemaStr);
    valueDeserializer = VsonAvroSerializer.fromSchemaStr(fileValueSchemaStr);
    keySerializer = new VeniceAvroKafkaSerializer(VsonAvroSchemaAdapter.parse(fileKeySchemaStr).toString());
  }

  @Test(dataProvider = MAPPER_PARAMS_DATA_PROVIDER)
  public void testMapWithoutSelectedField(int numReducers, int taskId) throws IOException {
    VeniceVsonMapper mapper = getMapper(numReducers, taskId);

    OutputCollector<BytesWritable, BytesWritable> collector = mock(OutputCollector.class);
    ArgumentCaptor<BytesWritable> keyCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    ArgumentCaptor<BytesWritable> valueCaptor = ArgumentCaptor.forClass(BytesWritable.class);

    VeniceAvroKafkaSerializer valueSerializer =
        new VeniceAvroKafkaSerializer((VsonAvroSchemaAdapter.parse(fileValueSchemaStr).toString()));

    Pair<BytesWritable, BytesWritable> record = generateRecord();
    mapper.map(record.getFirst(), record.getSecond(), collector, null);

    verify(collector, times(getNumberOfCollectorInvocationForFirstMapInvocation(numReducers, taskId)))
        .collect(keyCaptor.capture(), valueCaptor.capture());
    Assert.assertEquals(
        keyCaptor.getValue().copyBytes(),
        keySerializer.serialize("fake_topic", keyDeserializer.bytesToAvro(record.getFirst().copyBytes())));
    Assert.assertEquals(
        valueCaptor.getValue().copyBytes(),
        valueSerializer.serialize("fake_topic", valueDeserializer.bytesToAvro(record.getSecond().copyBytes())));
  }

  @Test(dataProvider = MAPPER_PARAMS_DATA_PROVIDER)
  public void testMapWithSelectedField(int numReducers, int taskId) throws IOException {
    VeniceVsonMapper mapper = getMapper(numReducers, taskId, conf -> conf.set(VALUE_FIELD_PROP, "userId"));

    OutputCollector<BytesWritable, BytesWritable> collector = mock(OutputCollector.class);
    ArgumentCaptor<BytesWritable> keyCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    ArgumentCaptor<BytesWritable> valueCaptor = ArgumentCaptor.forClass(BytesWritable.class);

    Schema schema = VsonAvroSchemaAdapter.stripFromUnion(VsonAvroSchemaAdapter.parse(fileValueSchemaStr));
    VeniceAvroKafkaSerializer valueSerializer =
        new VeniceAvroKafkaSerializer(schema.getField("userId").schema().toString());

    Pair<BytesWritable, BytesWritable> record = generateRecord();
    mapper.map(record.getFirst(), record.getSecond(), collector, null);

    verify(collector, times(getNumberOfCollectorInvocationForFirstMapInvocation(numReducers, taskId)))
        .collect(keyCaptor.capture(), valueCaptor.capture());
    Assert.assertEquals(
        keyCaptor.getValue().copyBytes(),
        keySerializer.serialize("fake_topic", keyDeserializer.bytesToAvro(record.getFirst().copyBytes())));

    GenericData.Record valueRecord = (GenericData.Record) valueDeserializer.bytesToAvro(record.getSecond().copyBytes());
    Assert.assertEquals(
        valueCaptor.getValue().copyBytes(),
        valueSerializer.serialize("fake_topic", valueRecord.get("userId")));
  }

  @Override
  protected JobConf setupJobConf(int numReducers, int taskId) {
    JobConf jobConf = super.setupJobConf(numReducers, taskId);

    // remove key/value fields
    jobConf.set(KEY_FIELD_PROP, "");
    jobConf.set(VALUE_FIELD_PROP, "");

    jobConf.set(FILE_KEY_SCHEMA, fileKeySchemaStr);
    jobConf.set(FILE_VALUE_SCHEMA, fileValueSchemaStr);

    return jobConf;
  }

  private Pair<BytesWritable, BytesWritable> generateRecord() {
    BytesWritable keyBytes = new BytesWritable(keyDeserializer.toBytes(1));

    HashMap<String, Object> valueObject = new HashMap<>();
    valueObject.put("userId", 1);
    valueObject.put("userEmail", "a@b.com");
    BytesWritable valueBytes = new BytesWritable(valueDeserializer.toBytes(valueObject));

    return new Pair<>(keyBytes, valueBytes);
  }
}
