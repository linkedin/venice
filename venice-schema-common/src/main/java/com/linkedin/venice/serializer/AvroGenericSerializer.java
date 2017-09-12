package com.linkedin.venice.serializer;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


public class AvroGenericSerializer<K> implements RecordSerializer<K> {
  private final Logger logger = Logger.getLogger(AvroGenericSerializer.class);
  private final GenericDatumWriter<K> datumWriter;

  public AvroGenericSerializer(Schema schema) {
    this(new GenericDatumWriter<>(schema));
  }

  protected AvroGenericSerializer(GenericDatumWriter datumWriter) {
    this.datumWriter = datumWriter;
  }

  @Override
  public byte[] serialize(K object) throws VeniceException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(output);
    try {
      datumWriter.write(object, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new VeniceException("Could not serialize the Avro object", e);
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (IOException e) {
          logger.error("Failed to close stream", e);
        }
      }
    }
    return output.toByteArray();
  }

  @Override
  public byte[] serializeObjects(Iterable<K> objects) throws VeniceException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(output);
    try {
      objects.forEach(object -> {
        try {
          datumWriter.write(object, encoder);
        } catch (IOException e) {
          throw new VeniceException("Could not serialize the Avro object", e);
        }
      });
      encoder.flush();
    } catch (IOException e) {
      throw new VeniceException("Could not flush BinaryEncoder", e);
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (IOException e) {
          logger.error("Failed to close stream", e);
        }
      }
    }
    return output.toByteArray();
  }
}
