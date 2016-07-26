package com.linkedin.venice.client.serializer;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;

public class AvroSpecificDeserializer<T extends SpecificRecord> implements RecordDeserializer<T> {
  private final SpecificDatumReader<T> datumReader;

  public AvroSpecificDeserializer(Schema writer, Class<T> c) {
    Schema reader = SpecificData.get().getSchema(c);
    this.datumReader = new SpecificDatumReader<>(writer, reader);
  }

  @Override
  public T deserialize(byte[] bytes) throws VeniceClientException {
    Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    try {
      // TODO: consider to reuse the object to improve performance (GC tuning)
      return datumReader.read(null, decoder);
    } catch(IOException e) {
      throw new VeniceClientException("Could not deserialize bytes back into Avro Abject" + e);
    }
  }
}
