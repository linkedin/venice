package com.linkedin.venice.hadoop;

import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.Pair;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;

public class TestVeniceVsonMapper extends AbstractTestVeniceMR {
  private String fileKeySchemaStr = "\"int32\"";
  private String fileValueSchemaStr = "{\"userId\": \"int32\", \"userEmail\": \"string\"}";

  private VeniceVsonMapper mapper;

  private VsonAvroSerializer keyDeserializer;
  private VsonAvroSerializer valueDeserializer;

  private VeniceAvroKafkaSerializer keySerializer;

  @BeforeTest
  public void setup() {
    mapper = new VeniceVsonMapper();
    keyDeserializer = VsonAvroSerializer.fromSchemaStr(fileKeySchemaStr);
    valueDeserializer = VsonAvroSerializer.fromSchemaStr(fileValueSchemaStr);
    keySerializer =
        new VeniceAvroKafkaSerializer(VsonAvroSchemaAdapter.parse(fileKeySchemaStr).toString());
  }

  @Test
  public void testMapWithoutSelectedField() throws IOException {
    mapper.configure(setupJobConf());

    OutputCollector<BytesWritable, BytesWritable> collector = Mockito.mock(OutputCollector.class);
    ArgumentCaptor<BytesWritable> keyCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    ArgumentCaptor<BytesWritable> valueCaptor = ArgumentCaptor.forClass(BytesWritable.class);

    VeniceAvroKafkaSerializer valueSerializer =
        new VeniceAvroKafkaSerializer((VsonAvroSchemaAdapter.parse(fileValueSchemaStr).toString()));

    Pair<BytesWritable, BytesWritable> record = generateRecord();
    mapper.map(record.getFirst(), record.getSecond(), collector, null);

    Mockito.verify(collector).collect(keyCaptor.capture(), valueCaptor.capture());
    Assert.assertEquals(keyCaptor.getValue().getBytes(),
        keySerializer.serialize("fake_topic", keyDeserializer.bytesToAvro(record.getFirst().getBytes())));
    Assert.assertEquals(valueCaptor.getValue().getBytes(),
        valueSerializer.serialize("fake_topic", valueDeserializer.bytesToAvro(record.getSecond().getBytes())));
  }

  @Test
  public void testMapWithSelectedField() throws IOException {
    JobConf conf = setupJobConf();
    conf.set(KafkaPushJob.VALUE_FIELD_PROP, "userId");
    mapper.configure(conf);

    OutputCollector<BytesWritable, BytesWritable> collector = Mockito.mock(OutputCollector.class);
    ArgumentCaptor<BytesWritable> keyCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    ArgumentCaptor<BytesWritable> valueCaptor = ArgumentCaptor.forClass(BytesWritable.class);

    Schema schema = VsonAvroSchemaAdapter.stripFromUnion(VsonAvroSchemaAdapter.parse(fileValueSchemaStr));
    VeniceAvroKafkaSerializer valueSerializer =
        new VeniceAvroKafkaSerializer(schema.getField("userId").schema().toString());

    Pair<BytesWritable, BytesWritable> record = generateRecord();
    mapper.map(record.getFirst(), record.getSecond(), collector, null);

    Mockito.verify(collector).collect(keyCaptor.capture(), valueCaptor.capture());
    Assert.assertEquals(keyCaptor.getValue().getBytes(),
        keySerializer.serialize("fake_topic", keyDeserializer.bytesToAvro(record.getFirst().getBytes())));

    GenericData.Record valueRecord = (GenericData.Record) valueDeserializer.bytesToAvro(record.getSecond().getBytes());
    Assert.assertEquals(valueCaptor.getValue().getBytes(),
        valueSerializer.serialize("fake_topic", valueRecord.get("userId")));
  }

  @Override
  protected JobConf setupJobConf() {
    JobConf jobConf = super.setupJobConf();

    //remove key/value fields
    jobConf.set(KEY_FIELD_PROP, "");
    jobConf.set(VALUE_FIELD_PROP, "");

    jobConf.set(FILE_KEY_SCHEMA, fileKeySchemaStr);
    jobConf.set(FILE_VALUE_SCHEMA, fileValueSchemaStr);

    return jobConf;
  }

  private Pair<BytesWritable, BytesWritable> generateRecord() {
    BytesWritable keyBytes =
        new BytesWritable(keyDeserializer.toBytes(1));

    HashMap<String, Object> valueObject = new HashMap<>();
    valueObject.put("userId", 1);
    valueObject.put("userEmail", "a@b.com");
    BytesWritable valueBytes =
        new BytesWritable(valueDeserializer.toBytes(valueObject));

    return new Pair<>(keyBytes, valueBytes);
  }
}
