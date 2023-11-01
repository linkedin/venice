package com.linkedin.venice.hadoop.recordreader.avro;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.hadoop.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Pair;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.avro.Schema;


/**
 * A record reader that returns the input key and value as is.
 */
public class IdentityAvroRecordReader extends AbstractVeniceRecordReader<ByteBuffer, ByteBuffer> {
  private static final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
  private static final String BYTES_SCHEMA_STR = BYTES_SCHEMA.toString();

  public IdentityAvroRecordReader(String topicName) {
    super(topicName);
    configure(BYTES_SCHEMA_STR, BYTES_SCHEMA_STR);
  }

  @Override
  public Object getAvroKey(ByteBuffer keyBytes, ByteBuffer valueBytes) {
    throw new VeniceUnsupportedOperationException("getAvroKey in IdentityAvroRecordReader");
  }

  @Override
  public byte[] getKeyBytes(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
    return ByteUtils.extractByteArray(keyBuffer);
  }

  @Override
  public Object getAvroValue(ByteBuffer keyBytes, ByteBuffer valueBytes) {
    throw new VeniceUnsupportedOperationException("getAvroValue in IdentityAvroRecordReader");
  }

  @Override
  public byte[] getValueBytes(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
    return ByteUtils.extractByteArray(valueBuffer);
  }

  @Override
  public Iterator<Pair<byte[], byte[]>> iterator() {
    throw new VeniceUnsupportedOperationException("Iteration in IdentityAvroRecordReader");
  }

  @Override
  public void close() throws IOException {
    // Nothing to do
  }
}
