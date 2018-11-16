package com.linkedin.venice.serializer;

import com.linkedin.venice.exceptions.VeniceException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.AvroVersion;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.LinkedinAvroMigrationHelper;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


public class AvroGenericSerializer<K> implements RecordSerializer<K> {
  private static final Logger logger = Logger.getLogger(AvroGenericSerializer.class);
  private final GenericDatumWriter<K> datumWriter;

  static {
    AvroVersion version = LinkedinAvroMigrationHelper.getRuntimeAvroVersion();
    logger.info("Detected: " + version.toString() + " on the classpath.");
  }

  public AvroGenericSerializer(Schema schema) {
    this(new GenericDatumWriter<>(schema));
  }

  protected AvroGenericSerializer(GenericDatumWriter datumWriter) {
    this.datumWriter = datumWriter;
  }

  @Override
  public byte[] serialize(K object) throws VeniceException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = LinkedinAvroMigrationHelper.newBinaryEncoder(output);
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
    return serializeObjects(objects, new ByteArrayOutputStream());
  }

  private byte[] serializeObjects(Iterable<K> objects, ByteArrayOutputStream output) throws VeniceException {
    Encoder encoder = LinkedinAvroMigrationHelper.newBinaryEncoder(output);
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

  /**
   * Serialize a list of objects and put the prefix before the serialized objects.
   * This function could avoid unnecessary byte array copy when you want to serialize
   * two different kinds of objects together.
   * Essentially, the {@param prefix} will be the serialized byte array of the first
   * kind of objects.
   *
   * @param objects
   * @param prefix
   * @return
   * @throws VeniceException
   */
  @Override
  public byte[] serializeObjects(Iterable<K> objects, ByteBuffer prefix) throws VeniceException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.write(prefix.array(), prefix.position(), prefix.remaining());
    return serializeObjects(objects, output);
  }
}
