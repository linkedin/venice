package com.linkedin.venice.hadoop.input.recordreader;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import org.apache.avro.Schema;


/**
 * An abstraction for a record reader that reads records from the configured input into Avro-serialized keys and values.
 * @param <INPUT_KEY> The format of the key as controlled by the input format
 * @param <INPUT_VALUE> The format of the value as controlled by the input format
 */
public abstract class AbstractVeniceRecordReader<INPUT_KEY, INPUT_VALUE> {
  private Schema keySchema;
  private Schema valueSchema;

  private Schema rmdSchema;

  private RecordSerializer<Object> keySerializer;
  private RecordSerializer<Object> valueSerializer;

  private RecordSerializer<Object> rmdSerializer;

  public Schema getKeySchema() {
    return keySchema;
  }

  public Schema getValueSchema() {
    return valueSchema;
  }

  public Schema getRmdSchema() {
    return rmdSchema;
  }

  /**
   * Configure the record serializers
   */
  protected void configure(Schema keySchema, Schema valueSchema) {
    configure(keySchema, valueSchema, null);
  }

  protected void configure(Schema keySchema, Schema valueSchema, Schema rmdSchema) {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.rmdSchema = rmdSchema;
    keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    valueSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
    if (rmdSchema != null) {
      rmdSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(rmdSchema);
    } else {
      rmdSerializer = null;
    }
  }

  /**
   * Return an Avro output key
   */
  public abstract Object getAvroKey(INPUT_KEY inputKey, INPUT_VALUE inputValue);

  /**
   * return an Avro output value
   */
  public abstract Object getAvroValue(INPUT_KEY inputKey, INPUT_VALUE inputValue);

  public abstract Object getRmdValue(INPUT_KEY inputKey, INPUT_VALUE inputValue);

  /**
   * Return a serialized output key
   */
  public byte[] getKeyBytes(INPUT_KEY inputKey, INPUT_VALUE inputValue) {
    if (keySerializer == null) {
      throw new VeniceException("Record reader must be configured before calling getKeyBytes");
    }

    Object avroKey = getAvroKey(inputKey, inputValue);

    if (avroKey == null) {
      return null;
    }

    return keySerializer.serialize(avroKey);
  }

  /**
   * Return a serialized output value
   */
  public byte[] getValueBytes(INPUT_KEY inputKey, INPUT_VALUE inputValue) {
    if (valueSerializer == null) {
      throw new VeniceException("Record reader must be configured before calling getValueBytes");
    }

    Object avroValue = getAvroValue(inputKey, inputValue);

    if (avroValue == null) {
      return null;
    }

    return valueSerializer.serialize(avroValue);
  }

  public byte[] getRmdBytes(INPUT_KEY inputKey, INPUT_VALUE inputValue) {
    if (rmdSerializer == null) {
      return null;
    }

    Object rmdValue = getRmdValue(inputKey, inputValue);

    if (rmdValue == null) {
      return null;
    }

    return rmdSerializer.serialize(rmdValue);
  }
}
