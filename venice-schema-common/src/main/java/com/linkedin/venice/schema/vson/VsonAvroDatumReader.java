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
  protected Object readFixed(Object old, Schema expected, Decoder in) throws IOException {
    if (expected.getFixedSize() == 1) {
      return readByte(new byte[1], in);
    } else if (expected.getFixedSize() == 2) {
      return readShort(new byte[2], in);
    } else {
      throw illegalFixedLength(expected.getFixedSize());
    }
  }

  private Byte readByte(byte[] bytes, Decoder in) throws IOException {
    in.readFixed(bytes, 0, 1);
    return bytes[0];
  }

  private Short readShort(byte[] bytes, Decoder in) throws IOException {
    in.readFixed(bytes, 0, 2);
    return (short)(((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF));
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

  static VsonSerializationException notSupportType(Schema.Type type) {
    return new VsonSerializationException(String.format("Does not support casting type: %s between Vson and Avro", type.toString()));
  }

  //this should not happen. If the program goes to here, bad thing happened
  static VsonSerializationException illegalFixedLength(int len) {
    return new VsonSerializationException("illegal Fixed type length: " + len +
        "Fixed type is only for single byte or short and should not have size greater than 2");
  }
}
