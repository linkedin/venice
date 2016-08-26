package com.linkedin.venice.hadoop;

import com.linkedin.venice.client.MockVeniceWriter;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.serialization.avro.AvroGenericSerializer;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestAvroKafkaRecordWriter {

  private KafkaBrokerWrapper kafkaBrokerWrapper;
  private int valueSchemaId = 1;

  @BeforeClass
  public void setUp() {
    kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
  }

  @AfterClass
  public void cleanUp() {
    if (kafkaBrokerWrapper != null) {
      kafkaBrokerWrapper.close();
    }
  }

  private Properties getKafkaProperties(String schemaString, String keyField, String valueField) {
    Configuration conf = new Configuration();
    conf.set(KafkaPushJob.KAFKA_URL_PROP, kafkaBrokerWrapper.getAddress());
    conf.set(KafkaPushJob.TOPIC_PROP, "test_topic");
    conf.set(KafkaPushJob.SCHEMA_STRING_PROP, schemaString);
    conf.set(KafkaPushJob.AVRO_KEY_FIELD_PROP, keyField);
    conf.set(KafkaPushJob.AVRO_VALUE_FIELD_PROP, valueField);
    conf.set(KafkaPushJob.BATCH_NUM_BYTES_PROP, Integer.toString(KafkaPushJob.DEFAULT_BATCH_BYTES_SIZE));
    conf.set(KafkaPushJob.VALUE_SCHEMA_ID_PROP, Integer.toString(valueSchemaId));

    return AvroKafkaOutputFormat.getKafkaProperties(new JobConf(conf));
  }

  @Test
  public void testWriteSimpleKeyValuePairWithKeyAsInteger() throws Exception {
    String schemaString = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"int\" },  " +
        "       { \"name\": \"name\", \"type\": \"string\" },  " +
        "       { \"name\": \"age\", \"type\": \"int\" }  " +
        "  ] " +
        " } ";
    String keyField = "id";
    String valueField = "name";
    Schema schema = Schema.parse(schemaString);
    Schema keySchema = schema.getField(keyField).schema();
    Schema valueSchema = schema.getField(valueField).schema();

    Properties kafkaProps = getKafkaProperties(schemaString, keyField, valueField);
    MockVeniceWriter veniceWriter = new MockVeniceWriter(kafkaProps);
    AvroKafkaRecordWriter recordWriter = new AvroKafkaRecordWriter(kafkaProps, veniceWriter);

    // Setup simple record
    GenericData.Record simpleRecord = new GenericData.Record(Schema.parse(schemaString));
    simpleRecord.put("id", new Integer(123));
    simpleRecord.put("name", "test_name");
    simpleRecord.put("age", new Integer(30));

    recordWriter.write(new AvroWrapper<IndexedRecord>(simpleRecord), NullWritable.get());

    assertKeyGetsValue(new Integer(123), "test_name", keySchema, valueSchema, veniceWriter);
    Assert.assertNull(veniceWriter.getValue("123"));
    Assert.assertNull(veniceWriter.getValue("124"));

    String keyStr = new String(new AvroGenericSerializer(keySchema.toString()).serialize("test_topic", new Integer(123)));
    Assert.assertEquals(veniceWriter.getValueSchemaId(keyStr), valueSchemaId);
  }

  @Test
  public void testWriteSimpleKeyValuePairWithKeyAsString() throws IOException {
    String schemaString = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\" },  " +
        "       { \"name\": \"name\", \"type\": \"string\" },  " +
        "       { \"name\": \"age\", \"type\": \"int\" }  " +
        "  ] " +
        " } ";
    String keyField = "id";
    String valueField = "name";
    Schema schema = Schema.parse(schemaString);
    Schema keySchema = schema.getField(keyField).schema();
    Schema valueSchema = schema.getField(valueField).schema();

    Properties kafkaProps = getKafkaProperties(schemaString, keyField, valueField);
    MockVeniceWriter veniceWriter = new MockVeniceWriter(kafkaProps);
    AvroKafkaRecordWriter recordWriter = new AvroKafkaRecordWriter(kafkaProps, veniceWriter);

    // Setup simple record
    GenericData.Record simpleRecord = new GenericData.Record(Schema.parse(schemaString));
    simpleRecord.put("id", "123");
    simpleRecord.put("name", "test_name");
    simpleRecord.put("age", new Integer(30));

    recordWriter.write(new AvroWrapper<IndexedRecord>(simpleRecord), NullWritable.get());

    assertKeyGetsValue("123", "test_name", keySchema, valueSchema, veniceWriter);
    Assert.assertNull(veniceWriter.getValue("124"));
  }

  @Test
  public void testWriteComplicatedKeyValuePair() throws IOException {
    String schemaString = "{\"namespace\": \"example.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "      {\n" +
        "       \"name\": \"key\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"}\n" +
        "           ]\n" +
        "        }\n" +
        "      },\n" +
        "      {\n" +
        "       \"name\": \"value\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"ValueRecord\",\n" +
        "           \"fields\" : [\n" +
        "              {\"name\": \"favorite_number\", \"type\": \"string\"},\n" +
        "              {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
        "           ]\n" +
        "        }\n" +
        "      }\n" +
        " ]\n" +
        "}";
    String keyField = "key";
    String valueField = "value";
    Schema schema = Schema.parse(schemaString);
    Schema keySchema = schema.getField(keyField).schema();
    Schema valueSchema = schema.getField(valueField).schema();

    Properties kafkaProps = getKafkaProperties(schemaString, keyField, valueField);
    MockVeniceWriter veniceWriter = new MockVeniceWriter(kafkaProps);
    AvroKafkaRecordWriter recordWriter = new AvroKafkaRecordWriter(kafkaProps, veniceWriter);

    // Setup complicate record
    // Key part
    GenericData.Record keyRecord = new GenericData.Record(keySchema);
    keyRecord.put("name", "test_name");
    keyRecord.put("company", "test_company");
    // Value part
    GenericData.Record valueRecord = new GenericData.Record(valueSchema);
    valueRecord.put("favorite_number", "886");
    valueRecord.put("favorite_color", "blue");

    GenericData.Record record = new GenericData.Record(schema);
    record.put("key", keyRecord);
    record.put("value", valueRecord);

    recordWriter.write(new AvroWrapper<IndexedRecord>(record), NullWritable.get());

    assertKeyGetsValue(keyRecord, valueRecord, keySchema, valueSchema, veniceWriter);
    Assert.assertNull(veniceWriter.getValue("unknown_key"));
  }

  @Test
  public void testWriteKeyValuePairWithKeyAsMapType() throws IOException {
    String schemaString = "{\"namespace\": \"example.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "      {\n" +
        "       \"name\": \"key\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"map\",\n" +
        "           \"values\": \"string\"\n" +
        "           }\n" +
        "      },\n" +
        "      {\n" +
        "       \"name\": \"value\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"ValueRecord\",\n" +
        "           \"fields\" : [\n" +
        "              {\"name\": \"favorite_number\", \"type\": \"string\"},\n" +
        "              {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
        "           ]\n" +
        "        }\n" +
        "      }\n" +
        " ]\n" +
        "}";
    String keyField = "key";
    String valueField = "value";
    Schema schema = Schema.parse(schemaString);
    Schema keySchema = schema.getField(keyField).schema();
    Schema valueSchema = schema.getField(valueField).schema();

    Properties kafkaProps = getKafkaProperties(schemaString, keyField, valueField);
    MockVeniceWriter veniceWriter = new MockVeniceWriter(kafkaProps);
    AvroKafkaRecordWriter recordWriter = new AvroKafkaRecordWriter(kafkaProps, veniceWriter);

    // Setup complicate record
    // Key part
    Map<String, String> keyMap = new HashMap<>();
    keyMap.put("name", "test_name");
    keyMap.put("company", "test_company");
    // Value part
    GenericData.Record valueRecord = new GenericData.Record(valueSchema);
    valueRecord.put("favorite_number", "886");
    valueRecord.put("favorite_color", "blue");

    GenericData.Record record = new GenericData.Record(schema);
    record.put("key", keyMap);
    record.put("value", valueRecord);

    recordWriter.write(new AvroWrapper<IndexedRecord>(record), NullWritable.get());

    assertKeyGetsValue(keyMap, valueRecord, keySchema, valueSchema, veniceWriter);
    Assert.assertNull(veniceWriter.getValue(keyMap.toString())); /* not serialized */
  }

  private void assertKeyGetsValue(Object key, Object value, Schema keySchema, Schema valueSchema, MockVeniceWriter writer){
    AvroGenericSerializer keySerializer = new AvroGenericSerializer(keySchema.toString());
    AvroGenericSerializer valueSerializer = new AvroGenericSerializer(valueSchema.toString());

    String keyString = new String(keySerializer.serialize("", key), StandardCharsets.UTF_8);
    String valueString = new String(valueSerializer.serialize("", value), StandardCharsets.UTF_8);

    Assert.assertEquals(
        writer.getValue(keyString), valueString,
        "Request for key: " + key + " should return value: " + value
    );
  }

}
