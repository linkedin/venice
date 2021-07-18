package com.linkedin.venice.serializer;

import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.avro.io.BinaryDecoder;


/**
 * This class is used as an identity function to return the same bytes that were passed in the input to the output. It
 * will reuse the inputs as much as possible.
 */
public class IdentityRecordDeserializer implements RecordDeserializer<ByteBuffer> {
  private static final IdentityRecordDeserializer INSTANCE = new IdentityRecordDeserializer();

  private IdentityRecordDeserializer() {}

  public static IdentityRecordDeserializer getInstance() {
    return INSTANCE;
  }

  @Override
  public ByteBuffer deserialize(byte[] bytes) throws VeniceSerializationException {
    return ByteBuffer.wrap(bytes);
  }

  @Override
  public ByteBuffer deserialize(ByteBuffer byteBuffer) throws VeniceSerializationException {
    return byteBuffer;
  }

  @Override
  public ByteBuffer deserialize(ByteBuffer reuse, byte[] bytes) throws VeniceSerializationException {
    return deserialize(bytes);
  }

  @Override
  public ByteBuffer deserialize(BinaryDecoder binaryDecoder) throws VeniceSerializationException {
    throw new UnsupportedOperationException("Cannot extract raw value when using a BinaryDecoder.");
  }

  @Override
  public ByteBuffer deserialize(ByteBuffer reuse, BinaryDecoder binaryDecoder) throws VeniceSerializationException {
    return deserialize(binaryDecoder);
  }

  @Override
  public Iterable<ByteBuffer> deserializeObjects(byte[] bytes) throws VeniceSerializationException {
    return Collections.singleton(deserialize(bytes));
  }

  @Override
  public Iterable<ByteBuffer> deserializeObjects(BinaryDecoder binaryDecoder) throws VeniceSerializationException {
    return Collections.singleton(deserialize(binaryDecoder));
  }
}
