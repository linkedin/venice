package com.linkedin.venice.serializer;

import com.linkedin.venice.exceptions.VeniceException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.AvroVersion;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.LinkedinAvroMigrationHelper;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


public class AvroSerializer<K> implements RecordSerializer<K> {
  private static final Logger logger = Logger.getLogger(AvroSerializer.class);
  private final DatumWriter<K> genericDatumWriter;
  private final DatumWriter<K> specificDatumWriter;

  static {
    AvroVersion version = LinkedinAvroMigrationHelper.getRuntimeAvroVersion();
    logger.info("Detected: " + version.toString() + " on the classpath.");
  }

  public AvroSerializer(Schema schema) {
    this(new GenericDatumWriter<>(schema), new SpecificDatumWriter(schema));
  }

  protected AvroSerializer(DatumWriter genericDatumWriter, DatumWriter specificDatumWriter) {
    this.genericDatumWriter = genericDatumWriter;
    this.specificDatumWriter = specificDatumWriter;
  }

  private void write(K object, Encoder encoder) throws IOException {
    try {
      if (object instanceof SpecificRecord) {
        specificDatumWriter.write(object, encoder);
      } else {
        genericDatumWriter.write(object, encoder);
      }
    } catch (NullPointerException e) {
      if (object instanceof SpecificRecord && null == specificDatumWriter) {
        /**
         * Defensive code...
         *
         * At the time of writing this commit, only the {@link VsonAvroGenericSerializer}
         * uses the protected constructor to pass in a null {@link specificDatumWriter},
         * and the Vson serializer should never be used with a SpecificRecord, so the NPE
         * should never happen. If this assumption is broken in the future, and this code
         * regresses, then hopefully this exception can help future maintainers to find
         * the issue more easily.
         */
        throw new IllegalStateException("This instance of " + this.getClass().getSimpleName()
            + " was instantiated with a null specificDatumWriter, and was used to serialize a SpecificRecord.", e);
      }
      throw e;
    }
  }

  @Override
  public byte[] serialize(K object) throws VeniceException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = LinkedinAvroMigrationHelper.newBinaryEncoder(output);
    try {
      write(object, encoder);
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
          write(object, encoder);
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
