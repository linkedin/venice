package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.serialization.avro.AvroGenericSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import javafx.util.Pair;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * A simple implementation of {@link Mapper}, which will serialize the avro records of the user's input data file.
 */
public class VeniceMapper implements Mapper<AvroWrapper<IndexedRecord>, NullWritable, BytesWritable, BytesWritable> {
  private static final Logger LOGGER = Logger.getLogger(VeniceMapper.class);

  private String topicName;
  private String keyField;
  private String valueField;
  private int keyFieldPos;
  private int valueFieldPos;
  private VeniceSerializer keySerializer;
  private VeniceSerializer valueSerializer;

  @Override
  public void map(AvroWrapper<IndexedRecord> record, NullWritable value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter) throws IOException {
    Pair<byte[], byte[]> keyValueBytesPair = parseAvroRecord(record);
    if (null != keyValueBytesPair) {
      /**
       * Skip record with null value, check {@link #parseAvroRecord(AvroWrapper)}.
       */
      output.collect(new BytesWritable(keyValueBytesPair.getKey()), new BytesWritable(keyValueBytesPair.getValue()));
    }
  }

  protected Pair<byte[], byte[]> parseAvroRecord(AvroWrapper<IndexedRecord> record) {
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
      LOGGER.warn("Skipping record with null value.");
      return null;
    }
    byte[] keyBytes = keySerializer.serialize(topicName, keyDatum);
    byte[] valueBytes = valueSerializer.serialize(topicName, valueDatum);

    return new Pair<>(keyBytes, valueBytes);
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public void configure(JobConf job) {
    VeniceProperties props = HadoopUtils.getVeniceProps(job);

    // N.B.: These getters will throw an UndefinedPropertyException if anything is missing
    this.topicName = props.getString(KafkaPushJob.TOPIC_PROP);
    this.keyField = props.getString(KafkaPushJob.AVRO_KEY_FIELD_PROP);
    this.valueField = props.getString(KafkaPushJob.AVRO_VALUE_FIELD_PROP);
    String schemaStr = props.getString(KafkaPushJob.SCHEMA_STRING_PROP);
    this.keySerializer = getSerializer(schemaStr, keyField);
    this.keyFieldPos = getFieldPos(schemaStr, keyField);
    this.valueSerializer = getSerializer(schemaStr, valueField);
    this.valueFieldPos = getFieldPos(schemaStr, valueField);
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
}
