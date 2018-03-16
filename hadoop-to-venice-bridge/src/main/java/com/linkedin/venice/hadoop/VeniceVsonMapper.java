package com.linkedin.venice.hadoop;

import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.BytesWritable;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;

/**
 * Mapper that reads Vson input and deserializes it as Avro object and then Avro binary
 */
public class VeniceVsonMapper extends AbstractVeniceMapper<BytesWritable, BytesWritable> {
  private VsonAvroSerializer keyDeserializer;
  private VsonAvroSerializer valueDeserializer;

  private String keyAvroSchemaStr;
  private String valueAvroSchemaStr;


  private String keyField;
  private String valueField;

  @Override
  protected Object getAvroKey(BytesWritable inputKey, BytesWritable inputValue) {
    Object avroKeyObject = keyDeserializer.bytesToAvro(inputKey.getBytes());
    if (!keyField.isEmpty()) {
      return ((GenericData.Record) avroKeyObject).get(keyField);
    }
    return avroKeyObject;
  }

  @Override
  protected Object getAvroValue(BytesWritable inputKey, BytesWritable inputValue) {
    Object avroValueObject = valueDeserializer.bytesToAvro(inputValue.getBytes());
    if (!valueField.isEmpty()) {
      return ((GenericData.Record) avroValueObject).get(valueField);
    }
    return avroValueObject;
  }

  @Override
  protected String getKeySchemaStr() {
    return keyAvroSchemaStr;
  }

  @Override
  protected String getValueSchemaStr() {
    return valueAvroSchemaStr;
  }

  @Override
  public void configure(VeniceProperties props) {
    String fileVsonKeySchemaStr = props.getString(FILE_KEY_SCHEMA);
    String fileVsonValueSchemaStr = props.getString(FILE_VALUE_SCHEMA);

    keyDeserializer = VsonAvroSerializer.fromSchemaStr(fileVsonKeySchemaStr);
    valueDeserializer = VsonAvroSerializer.fromSchemaStr(fileVsonValueSchemaStr);

    keyField = props.getString(KEY_FIELD_PROP, "");
    if (keyField.isEmpty()) {
      keyAvroSchemaStr = VsonAvroSchemaAdapter.parse(fileVsonKeySchemaStr).toString();
    } else {
      keyAvroSchemaStr = VsonAvroSchemaAdapter.stripFromUnion(VsonAvroSchemaAdapter.parse(fileVsonKeySchemaStr))
          .getField(keyField).schema().toString();
    }

    valueField = props.getString(VALUE_FIELD_PROP, "");
    if (valueField.isEmpty()) {
      valueAvroSchemaStr = VsonAvroSchemaAdapter.parse(fileVsonValueSchemaStr).toString();
    } else {
      valueAvroSchemaStr = VsonAvroSchemaAdapter.stripFromUnion(VsonAvroSchemaAdapter.parse(fileVsonValueSchemaStr))
          .getField(valueField).schema().toString();
    }
  }
}
