package com.linkedin.venice.serializer;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.util.ArrayList;
import java.util.List;

public class AvroGenericDeserializer<V> implements RecordDeserializer<V> {
  private final DatumReader<V> datumReader;

  public AvroGenericDeserializer(Schema writer, Schema reader) {
    datumReader = new GenericDatumReader<>(writer, reader);
  }

  protected AvroGenericDeserializer(DatumReader<V> datumReader) {
    this.datumReader = datumReader;
  }

  @Override
  public V deserialize(byte[] bytes) throws VeniceException {
    // This param is to re-use a decoder instance. TODO: explore GC tuning later.
    BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    try {
      return datumReader.read(null, decoder);
    } catch (Exception e) {
      throw new VeniceException("Could not deserialize bytes back into Avro object", e);
    }
  }

  @Override
  public Iterable<V> deserializeObjects(byte[] bytes) throws VeniceException {
    // This param is to re-use a decoder instance. TODO: explore GC tuning later.
    BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    List<V> objects = new ArrayList();
    try {
      while (!decoder.isEnd()) {
        objects.add(datumReader.read(null, decoder));
      }
    } catch (Exception e) {
      throw new VeniceException("Could not deserialize bytes back into Avro objects", e);
    }

    return objects;
  }
}
