package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.serialization.avro.AvroGenericSerializer;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link org.apache.hadoop.mapred.RecordWriter} implementation which writes into
 * Kafka using a {@link VeniceWriter}.
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

  private final Progressable progress;
  private final AtomicReference<Exception> sendException = new AtomicReference<>();
  /**
   * This doesn't need to be atomic since {@link #write(AvroWrapper, NullWritable)} will be called sequentially.
    */
  private long messageSent = 0;
  private final AtomicLong messageCompleted = new AtomicLong();
  private final AtomicLong messageErrored = new AtomicLong();

  private Callback kafkaMessageCallback = new KafkaMessageCallback();

  public AvroKafkaRecordWriter(Properties properties, Progressable progress) {
    this(properties,
        new VeniceWriter<>(
            new VeniceProperties(properties),
            getPropString(properties, KafkaPushJob.TOPIC_PROP),
            new DefaultSerializer(),
            new DefaultSerializer()
        ),
        progress
    );
  }

  public AvroKafkaRecordWriter(Properties properties, AbstractVeniceWriter<byte[], byte[]> veniceWriter, Progressable progress) {
    this.veniceWriter = veniceWriter;

    // N.B.: These getters will throw an UndefinedPropertyException if anything is missing
    this.keyField = getPropString(properties, KafkaPushJob.AVRO_KEY_FIELD_PROP);
    this.valueField = getPropString(properties, KafkaPushJob.AVRO_VALUE_FIELD_PROP);
    this.topicName = veniceWriter.getTopicName();

    String schemaStr = getPropString(properties, KafkaPushJob.SCHEMA_STRING_PROP);
    this.keySerializer = getSerializer(schemaStr, keyField);
    this.keyFieldPos = getFieldPos(schemaStr, keyField);
    this.valueSerializer = getSerializer(schemaStr, valueField);
    this.valueFieldPos = getFieldPos(schemaStr, valueField);
    this.valueSchemaId = Integer.parseInt(getPropString(properties, KafkaPushJob.VALUE_SCHEMA_ID_PROP));
    this.progress = progress;
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
    return new AvroGenericSerializer(schema.toString());
  }

  private int getFieldPos(String schemaStr, String field) {
    return Schema.parse(schemaStr).getField(field).pos();
  }

  @Override
  public void close(Reporter arg0) throws IOException {
    logger.info("Kafka message progress before flushing and closing producer:");
    logMessageProgress();

    veniceWriter.close();

    maybePropagateCallbackException();
    logger.info("Kafka message progress after flushing and closing producer:");
    logMessageProgress();
    if (messageSent != messageCompleted.get()) {
      throw new VeniceException("Message sent: " + messageSent + " doesn't match message completed: " + messageCompleted.get());
    }
  }

  @Override
  public void write(AvroWrapper<IndexedRecord> record, NullWritable nullArg) throws IOException {
    maybePropagateCallbackException();

    IndexedRecord datum = record.datum();
    Object keyDatum = datum.get(keyFieldPos);
    if (null == keyDatum) {
      // Invalid data
      // Theoretically it should not happen since all the avro records are sharing the same schema in the same file
      throw new VeniceException("Encountered record with null key");
    }

    Object valueDatum = datum.get(valueFieldPos);
    if (null == valueDatum) {
      // TODO: maybe we want to write a null value anyways?
      logger.warn("Skipping record with null value.");
      return;
    }

    veniceWriter.put(keySerializer.serialize(topicName, keyDatum), valueSerializer.serialize(topicName, valueDatum), valueSchemaId, kafkaMessageCallback);
    ++messageSent;
  }

  private void maybePropagateCallbackException() {
    if (null != sendException.get()) {
      throw new VeniceException("KafkaPushJob failed with exception", sendException.get());
    }
  }

  private void logMessageProgress() {
    logger.info("Message sent: " + messageSent);
    logger.info("Message completed: " + messageCompleted.get());
    logger.info("Message errored: " + messageErrored.get());
  }

  private class KafkaMessageCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (null != e) {
        messageErrored.incrementAndGet();
        sendException.set(e);
      } else {
        messageCompleted.incrementAndGet();
      }
      // Report progress so map-reduce framework won't kill current mapper when it finishes
      // sending all the messages to Kafka broker, but not yet flushed and closed.
      progress.progress();
    }
  }
}