package com.linkedin.venice.client.serializer;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;

public class AvroGenericDeserializer<V> implements RecordDeserializer<V> {
  private final GenericDatumReader<V> datumReader;

  public AvroGenericDeserializer(Schema writer, Schema reader) {
    datumReader = new GenericDatumReader<>(writer, reader);
  }

  @Override
  public V deserialize(byte[] bytes) throws VeniceClientException {
    Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    try {
      // TODO: consider to reuse the object to improve performance (GC tuning)
      return datumReader.read(null, decoder);
    } catch(Exception e) {
      throw new VeniceClientException("Could not deserialize bytes back into Avro Abject" + e);
    }
  }
}
