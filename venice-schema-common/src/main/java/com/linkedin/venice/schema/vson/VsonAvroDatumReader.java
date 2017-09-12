package com.linkedin.venice.schema.vson;

import com.linkedin.venice.serializer.VsonSerializationException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;

public class VsonAvroDatumReader<D> extends GenericDatumReader<D> {
  public VsonAvroDatumReader(Schema schema) {
    super(schema);
  }

  public VsonAvroDatumReader(Schema writer, Schema reader) {
    super(writer, reader);
  }

  @Override
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    HashMap<String, Object> record = new HashMap<>();
    for (Schema.Field field : in.readFieldOrder()) {
      record.put(field.name(), read(null, field.schema(), in));
    }

    return record;
  }

  @Override
  protected Object readString(Object old, Schema expected, Decoder in) throws IOException {
    return super.readString(old, expected, in).toString();
  }

  @Override
  protected Object readBytes(Object old, Decoder in) throws IOException {
    ByteBuffer byteBuffer = (ByteBuffer) super.readBytes(old, in);
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
    return bytes;
  }

  @Override
  protected Object readEnum(Schema expected, Decoder in) throws IOException {
    throw notSupportType(expected.getType());
  }

  @Override
  protected Object readMap(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    throw notSupportType(expected.getType());
  }

  @Override
  protected Object readFixed(Object old, Schema expected, Decoder in) throws IOException {
    throw notSupportType(expected.getType());
  }

  static VsonSerializationException notSupportType(Schema.Type type) {
    return new VsonSerializationException(String.format("Does not support casting type: %s between Vson and Avro", type.toString()));
  }
}
