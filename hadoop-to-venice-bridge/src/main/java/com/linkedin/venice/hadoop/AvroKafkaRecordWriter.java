package com.linkedin.venice.hadoop;

import com.linkedin.venice.client.AbstractVeniceWriter;
import com.linkedin.venice.client.VeniceWriter;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.serialization.avro.AvroGenericSerializer;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.StringSerializer;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * A {@link org.apache.hadoop.mapred.RecordWriter} implementation which writes into
 * Kafka using a {@link com.linkedin.venice.client.VeniceWriter}.
 */
public class AvroKafkaRecordWriter implements RecordWriter<AvroWrapper<IndexedRecord>, NullWritable> {

  private static Logger logger = Logger.getLogger(AvroKafkaRecordWriter.class);
  private final String topicName;

  private final AbstractVeniceWriter<byte[], byte[]> veniceWriter;
  private final String keyField;
  private final String valueField;
  private final int keyFieldPos;
  private final int valueFieldPos;
  private final VeniceSerializer keySerializer;
  private final VeniceSerializer valueSerializer;
  private final int valueSchemaId;

  public AvroKafkaRecordWriter(Properties properties) {
    this(properties,
        new VeniceWriter<>(
            new VeniceProperties(properties),
            getPropString(properties, KafkaPushJob.TOPIC_PROP),
            new DefaultSerializer(),
            new DefaultSerializer()
        )
    );
  }

  public AvroKafkaRecordWriter(Properties properties, AbstractVeniceWriter<byte[], byte[]> veniceWriter) {
    this.veniceWriter = veniceWriter;

    // N.B.: These getters will throw an UndefinedPropertyException if anything is missing
    keyField = getPropString(properties, KafkaPushJob.AVRO_KEY_FIELD_PROP);
    valueField = getPropString(properties, KafkaPushJob.AVRO_VALUE_FIELD_PROP);
    // FIXME: Communicate with controller to get topic name
    topicName = veniceWriter.getTopicName();

    String schemaStr = getPropString(properties, KafkaPushJob.SCHEMA_STRING_PROP);
    keySerializer = getSerializer(schemaStr, keyField);
    keyFieldPos = getFieldPos(schemaStr, keyField);
    valueSerializer = getSerializer(schemaStr, valueField);
    valueFieldPos = getFieldPos(schemaStr, valueField);
    valueSchemaId = Integer.parseInt(getPropString(properties, KafkaPushJob.VALUE_SCHEMA_ID_PROP));
  }

  private static String getPropString(Properties properties, String field) {
    String value = properties.getProperty(field);
    if (null == value) {
      throw new UndefinedPropertyException(field);
    }
    return value;
  }

  private VeniceSerializer getSerializer(String schemaStr, String field) {
    // The upstream has already checked that the key/value fields must exist in the given schema,
    // so we won't check it again here
    Schema schema = Schema.parse(schemaStr).getField(field).schema();
    if (schema.getType() == Schema.Type.STRING) {
      // TODO: Theoretically, we should always use AvroGenricSerializer when schema registry is ready;
      // Right now, we still want to use StringSerializer for String type since venice-client is passing whatever it receives to the backend.
      return new StringSerializer();
    }
    return new AvroGenericSerializer(schema.toString());
  }

  private int getFieldPos(String schemaStr, String field) {
    return Schema.parse(schemaStr).getField(field).pos();
  }

  @Override
  public void close(Reporter arg0) throws IOException {
    veniceWriter.close();
  }

  @Override
  public void write(AvroWrapper<IndexedRecord> record, NullWritable nullArg) throws IOException {
    IndexedRecord datum = record.datum();

    Object keyDatum = datum.get(keyFieldPos);
    if (null == keyDatum) {
      // Invalid data
      // Theoretically it should not happen since all the avro records are sharing the same schema in the same file
      logger.warn("Skipping record with null key value.");
      return;
    }
    // TODO: remove the following hack when schema registry is fully supported
    if (keySerializer instanceof StringSerializer) {
      keyDatum = keyDatum.toString();
    }
    Object valueDatum = datum.get(valueFieldPos);
    if (null == valueDatum) {
      // TODO: maybe we want to write a null value anyways?
      logger.warn("Skipping record with null value.");
      return;
    }
    // TODO: remove the following hack when schema registry is fully supported
    if (valueSerializer instanceof StringSerializer) {
      valueDatum = valueDatum.toString();
    }

    veniceWriter.put(keySerializer.serialize(topicName, keyDatum), valueSerializer.serialize(topicName, valueDatum), valueSchemaId);
  }
}