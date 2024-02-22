package com.linkedin.venice.hadoop.input.recordreader.avro;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.hadoop.input.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;


/**
 * A record reader that returns the input key and value as is.
 */
public class IdentityVeniceRecordReader extends AbstractVeniceRecordReader<ByteBuffer, ByteBuffer> {
  private static final IdentityVeniceRecordReader INSTANCE = new IdentityVeniceRecordReader();

  private IdentityVeniceRecordReader() {
    final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
    configure(BYTES_SCHEMA, BYTES_SCHEMA);
  }

  public static IdentityVeniceRecordReader getInstance() {
    return INSTANCE;
  }

  @Override
  public Object getAvroKey(ByteBuffer keyBytes, ByteBuffer valueBytes) {
    throw new VeniceUnsupportedOperationException("getAvroKey in IdentityVeniceRecordReader");
  }

  @Override
  public byte[] getKeyBytes(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
    return ByteUtils.extractByteArray(keyBuffer);
  }

  @Override
  public Object getAvroValue(ByteBuffer keyBytes, ByteBuffer valueBytes) {
    throw new VeniceUnsupportedOperationException("getAvroValue in IdentityVeniceRecordReader");
  }

  @Override
  public byte[] getValueBytes(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
    return ByteUtils.extractByteArray(valueBuffer);
  }
}
