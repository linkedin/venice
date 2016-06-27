package com.linkedin.venice.hadoop;

import com.linkedin.venice.client.VeniceWriter;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.avro.AvroGenericSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class TestAvroKafkaRecordWriter {
  private static class MockVeniceWriter extends VeniceWriter<byte[], byte[]> {
    private Map<String, String> messages = new HashMap<>();
    private Map<String, Integer> keyValueSchemaIdMapping = new HashMap<>();

    public MockVeniceWriter(Properties properties) {
      super(new VeniceProperties(properties),
          properties.getProperty(KafkaPushJob.TOPIC_PROP),
          new DefaultSerializer(),
          new DefaultSerializer()
      );
    }

    @Override
    public Future<RecordMetadata> put(byte[] key, byte[] value, int schemaId) {
      messages.put(new String(key), new String(value));
      keyValueSchemaIdMapping.put(new String(key), schemaId);
      return null;
    }

    public String getValue(String key) {
      return messages.get(key);
    }

    public int getValueSchemaId(String key) {
      return keyValueSchemaIdMapping.get(key);
    }

    public void printMessages() {
      System.out.println("messages:");
      for (Map.Entry<String, String> entry : messages.entrySet()) {
        System.out.println(entry.getKey() + " => " + entry.getValue());
      }
    }
  }

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
  public void testWriteSimpleKeyValuePairWithKeyAsInteger() throws IOException {
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

    Properties kafkaProps = getKafkaProperties(schemaString, keyField, valueField);
    MockVeniceWriter veniceWriter = new MockVeniceWriter(kafkaProps);
    AvroKafkaRecordWriter recordWriter = new AvroKafkaRecordWriter(kafkaProps, veniceWriter);

    // Setup simple record
    GenericData.Record simpleRecord = new GenericData.Record(Schema.parse(schemaString));
    simpleRecord.put("id", new Integer(123));
    simpleRecord.put("name", "test_name");
    simpleRecord.put("age", new Integer(30));

    recordWriter.write(new AvroWrapper<IndexedRecord>(simpleRecord), NullWritable.get());

    //veniceWriter.printMessages();

    String keyStr = new String(new AvroGenericSerializer(keySchema.toString()).serialize("test_topic", new Integer(123)));
    Assert.assertEquals(veniceWriter.getValue(keyStr), "test_name");
    Assert.assertNull(veniceWriter.getValue("123"));
    Assert.assertNull(veniceWriter.getValue("124"));
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

    Properties kafkaProps = getKafkaProperties(schemaString, keyField, valueField);
    MockVeniceWriter veniceWriter = new MockVeniceWriter(kafkaProps);
    AvroKafkaRecordWriter recordWriter = new AvroKafkaRecordWriter(kafkaProps, veniceWriter);

    // Setup simple record
    GenericData.Record simpleRecord = new GenericData.Record(Schema.parse(schemaString));
    simpleRecord.put("id", "123");
    simpleRecord.put("name", "test_name");
    simpleRecord.put("age", new Integer(30));

    recordWriter.write(new AvroWrapper<IndexedRecord>(simpleRecord), NullWritable.get());

    //veniceWriter.printMessages();
    Assert.assertEquals(veniceWriter.getValue("123"), "test_name");
    Assert.assertNull(veniceWriter.getValue("124"));
  }

  @Test
  public void testWriteComplicateKeyValuePair() throws IOException {
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

    //veniceWriter.printMessages();

    String keyStr = new String(new AvroGenericSerializer(keySchema.toString()).serialize("test_topic", keyRecord));
    String valueStr = new String(new AvroGenericSerializer(valueSchema.toString()).serialize("test_topic", valueRecord));

    Assert.assertEquals(veniceWriter.getValue(keyStr), valueStr);
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

    veniceWriter.printMessages();

    String keyStr = new String(new AvroGenericSerializer(keySchema.toString()).serialize("test_topic", keyMap));
    String valueStr = new String(new AvroGenericSerializer(valueSchema.toString()).serialize("test_topic", valueRecord));
    Assert.assertEquals(veniceWriter.getValue(keyStr), valueStr);
    // Since recordWriter is using avro serialization logic instead of map.toString
    Assert.assertNull(veniceWriter.getValue(keyMap.toString()));
  }
}
