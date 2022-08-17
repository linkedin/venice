package com.linkedin.venice.serialization;

import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.VeniceSerializationException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoder;
import org.apache.commons.io.IOUtils;


/**
 * This class is used as an identity function to return the same bytes that were passed in the input to the output. It
 * will reuse the inputs as much as possible.
 */
public class IdentityRecordDeserializer implements RecordDeserializer<ByteBuffer> {
  private static final IdentityRecordDeserializer INSTANCE = new IdentityRecordDeserializer();

  private IdentityRecordDeserializer() {
  }

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
  public ByteBuffer deserialize(ByteBuffer reuse, ByteBuffer byteBuffer, BinaryDecoder reusedDecoder)
      throws VeniceSerializationException {
    return deserialize(byteBuffer);
  }

  @Override
  public ByteBuffer deserialize(ByteBuffer reuse, byte[] bytes) throws VeniceSerializationException {
    return deserialize(bytes);
  }

  @Override
  public ByteBuffer deserialize(BinaryDecoder binaryDecoder) throws VeniceSerializationException {
    if (!(binaryDecoder instanceof OptimizedBinaryDecoder)) {
      throw new UnsupportedOperationException(
          "Cannot extract raw value when using a BinaryDecoder unless it is an OptimizedBinaryDecoder.");
    }

    return deserialize(((OptimizedBinaryDecoder) binaryDecoder).getRawBytes());
  }

  @Override
  public ByteBuffer deserialize(ByteBuffer reuse, BinaryDecoder binaryDecoder) throws VeniceSerializationException {
    return deserialize(binaryDecoder);
  }

  @Override
  public ByteBuffer deserialize(ByteBuffer reuse, InputStream in, BinaryDecoder reusedDecoder)
      throws VeniceSerializationException {
    final byte[] bytes;
    try {
      bytes = IOUtils.toByteArray(in);
    } catch (IOException e) {
      throw new VeniceSerializationException("Could not extract bytes from InputStream", e);
    }
    return deserialize(reuse, bytes);
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
