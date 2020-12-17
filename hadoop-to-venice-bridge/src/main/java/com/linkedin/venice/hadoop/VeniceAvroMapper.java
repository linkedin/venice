package com.linkedin.venice.hadoop;

import com.linkedin.venice.etl.ETLValueSchemaTransformation;
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
    String etlValueTransformationStr = props.getString(ETL_VALUE_SCHEMA_TRANSFORMATION, ETLValueSchemaTransformation.NONE.name());
    ETLValueSchemaTransformation etlValueSchemaTransformation = ETLValueSchemaTransformation.valueOf(etlValueTransformationStr);

    this.veniceRecordReader = new VeniceAvroRecordReader(topicName, fileSchema, keyField, valueField, etlValueSchemaTransformation);
  }
}