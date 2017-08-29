package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;

public class VeniceAvroMapper extends AbstractVeniceMapper<AvroWrapper<IndexedRecord>, NullWritable> {
  private static final Logger LOGGER = Logger.getLogger(VeniceAvroMapper.class);

  private String keySchemaStr;
  private String valueSchemaStr;

  private int keyFieldPos;
  private int valueFieldPos;

  @Override
  protected Object getAvroKey(AvroWrapper<IndexedRecord> record, NullWritable nullValue) {
    Object keyDatum = record.datum().get(keyFieldPos);

    if (keyDatum == null) {
      // Invalid data
      // Theoretically it should not happen since all the avro records are sharing the same schema in the same file
      throw new VeniceException("Encountered record with null key");
    }

    return keyDatum;
  }

  @Override
  protected Object getAvroValue(AvroWrapper<IndexedRecord> record, NullWritable nullValue) {
    Object valueDatum = record.datum().get(valueFieldPos);

    if (null == valueDatum) {
      // TODO: maybe we want to write a null value anyways?
      LOGGER.warn("Skipping record with null value.");
      return null;
    }

    return valueDatum;
  }

  @Override
  protected String getKeySchemaStr() {
    return keySchemaStr;
  }

  @Override
  protected String getValueSchemaStr() {
    return valueSchemaStr;
  }

  @Override
  public void configure(VeniceProperties props) {
    Schema fileSchema = Schema.parse(props.getString(SCHEMA_STRING_PROP));
    Schema.Field keyField = fileSchema.getField(props.getString(KEY_FIELD_PROP));
    Schema.Field valueField = fileSchema.getField(props.getString(VALUE_FIELD_PROP));

    keySchemaStr = keyField.schema().toString();
    keyFieldPos = keyField.pos();

    valueSchemaStr = valueField.schema().toString();
    valueFieldPos = valueField.pos();
  }
}