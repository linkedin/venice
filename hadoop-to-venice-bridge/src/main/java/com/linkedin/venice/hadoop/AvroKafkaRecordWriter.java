package com.linkedin.venice.hadoop;

import com.linkedin.venice.client.VeniceWriter;
import com.linkedin.venice.serialization.avro.AvroGenericSerializer;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.StringSerializer;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.Props;
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

  private VeniceWriter<byte[], byte[]> veniceWriter;
  private String keyField;
  private String valueField;

  public AvroKafkaRecordWriter(Properties properties) {
    Props props = new Props(properties);

    // N.B.: These getters will throw an UndefinedPropertyException if anything is missing
    keyField = props.getString(KafkaPushJob.AVRO_KEY_FIELD_PROP);
    valueField = props.getString(KafkaPushJob.AVRO_VALUE_FIELD_PROP);
    // FIXME: Communicate with controller to get topic name
    this.topicName = props.getString(KafkaPushJob.TOPIC_PROP);

    veniceWriter = new VeniceWriter<>(props,
        topicName,
        new DefaultSerializer(),
        new DefaultSerializer());
  }

  @Override
  public void close(Reporter arg0) throws IOException {
    veniceWriter.close();
  }

  @Override
  public void write(AvroWrapper<IndexedRecord> record, NullWritable nullArg) throws IOException {

    IndexedRecord datum = record.datum();

    VeniceSerializer keySerializer;
    VeniceSerializer valueSerializer;
    Object keyDatum;
    Object valueDatum;

    // TODO : The Key Serializer and Value serializer are initialized for every record.
    // See if there is a way to reuse them. The code is redundant copy for both serializers
    // reuse the code by using common functions.
    if (keyField.equals("*")) {
      keyDatum = datum;
      keySerializer = new AvroGenericSerializer(datum.getSchema().toString());
    } else {

    /* Parse the Key */
      Schema.Field avroKeyField = datum.getSchema().getField(keyField);
      if (null == avroKeyField) {
        throw new RuntimeException("Key field name: " + keyField + " could not be found in Avro schema.");
      }

      keyDatum = datum.get(avroKeyField.pos());
      if (null == keyDatum) {
        logger.warn("Skipping record with null key value.");
        return;
      }

      if (avroKeyField.schema().getType() == Schema.Type.RECORD) {
        // Write as an avro RECORD type, can cast to IndexedRecord
        String keySchema = ((IndexedRecord) keyDatum).getSchema().toString();
        keySerializer = new AvroGenericSerializer(keySchema);
      } else {
        // serialize the object as a string
        keyDatum = keyDatum.toString();
        keySerializer = new StringSerializer();
      }
    }

    if (valueField.equals("*")) {
      valueDatum = datum;
      valueSerializer = new AvroGenericSerializer(datum.getSchema().toString());

    } else {

      /* Parse the Value */
      Schema.Field avroValueField = datum.getSchema().getField(valueField);

      if (null == avroValueField) {
        throw new RuntimeException("Value field name: " + valueField + " could not be found in Avro schema.");
      }

      valueDatum = datum.get(avroValueField.pos());

      if (null == valueDatum) {
        // TODO: maybe we want to write a null value anyways?
        logger.warn("Skipping record with null value.");
        return;
      }

      if (avroValueField.schema().getType() == Schema.Type.RECORD) {
        String valueSchema = ((IndexedRecord) valueDatum).getSchema().toString();
        valueSerializer = new AvroGenericSerializer(valueSchema);
      } else {
        valueDatum = valueDatum.toString();
        valueSerializer = new StringSerializer();
      }
    }

    veniceWriter.put(keySerializer.serialize(topicName, keyDatum), valueSerializer.serialize(topicName, valueDatum));
  }
}