package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;

public class VeniceAvroMapper extends AbstractVeniceMapper<AvroWrapper<IndexedRecord>, NullWritable> {

  @Override
  public void configure(VeniceProperties props) {
    Schema fileSchema = Schema.parse(props.getString(SCHEMA_STRING_PROP));
    String keyField = props.getString(KEY_FIELD_PROP);
    String valueField = props.getString(VALUE_FIELD_PROP);
    String topicName = props.getString(TOPIC_PROP);
    boolean isSourceETL = props.getBoolean(SOURCE_ETL, false);

    this.veniceRecordReader = new VeniceAvroRecordReader(topicName, fileSchema, keyField, valueField, isSourceETL);
  }
}