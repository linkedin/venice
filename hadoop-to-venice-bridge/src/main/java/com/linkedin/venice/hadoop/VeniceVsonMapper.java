package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.hadoop.io.BytesWritable;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;

/**
 * Mapper that reads Vson input and deserializes it as Avro object and then Avro binary
 */
public class VeniceVsonMapper extends AbstractVeniceMapper<BytesWritable, BytesWritable> {

  @Override
  public void configure(VeniceProperties props) {
    String fileVsonKeySchemaStr = props.getString(FILE_KEY_SCHEMA);
    String fileVsonValueSchemaStr = props.getString(FILE_VALUE_SCHEMA);
    String keyField = props.getString(KEY_FIELD_PROP, "");
    String valueField = props.getString(VALUE_FIELD_PROP, "");
    String topicName = props.getString(TOPIC_PROP);

    this.veniceRecordReader = new VeniceVsonRecordReader(topicName, fileVsonKeySchemaStr, fileVsonValueSchemaStr, keyField, valueField);
  }
}
