package com.linkedin.venice.serializer;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.DeterministicMapOrderGenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.DeterministicMapOrderSpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@code AvroSerializer} provides the functionality to serialize and deserialize objects by using Avro.
 */
public class AvroSerializer<K> implements RecordSerializer<K> {
  private static final Logger LOGGER = LogManager.getLogger(AvroSerializer.class);
  private static final ThreadLocal<ReusableObjects> REUSABLE_OBJECTS = ThreadLocal.withInitial(ReusableObjects::new);

  private final DatumWriter<K> genericDatumWriter;
  private final DatumWriter<K> specificDatumWriter;
  private final boolean buffered;

  private static class ReusableObjects {
    public final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    public final BinaryEncoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(outputStream, true, null);
  }

  static {
    LOGGER.info("Detected: {} on the classpath.", AvroCompatibilityHelperCommon.getRuntimeAvroVersion());
  }

  public AvroSerializer(Schema schema) {
    this(new DeterministicMapOrderGenericDatumWriter<>(schema), new DeterministicMapOrderSpecificDatumWriter<>(schema));
  }

  public AvroSerializer(Schema schema, boolean buffered) {
    this(
        new DeterministicMapOrderGenericDatumWriter<>(schema),
        new DeterministicMapOrderSpecificDatumWriter<>(schema),
        buffered);
  }

  protected AvroSerializer(DatumWriter<K> genericDatumWriter, DatumWriter<K> specificDatumWriter) {
    this(genericDatumWriter, specificDatumWriter, true);
  }

  protected AvroSerializer(DatumWriter<K> genericDatumWriter, DatumWriter<K> specificDatumWriter, boolean buffered) {
    this.genericDatumWriter = genericDatumWriter;
    this.specificDatumWriter = specificDatumWriter;
    this.buffered = buffered;
  }

  @Override
  public byte[] serialize(K object) throws VeniceException {
    ReusableObjects reusableObjects = REUSABLE_OBJECTS.get();
    reusableObjects.outputStream.reset();
    Encoder encoder =
        AvroCompatibilityHelper.newBinaryEncoder(reusableObjects.outputStream, buffered, reusableObjects.binaryEncoder);
    try {
      write(object, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new VeniceException("Unable to serialize object", e);
    }
    return reusableObjects.outputStream.toByteArray();
  }

  @Override
  public byte[] serializeObjects(Iterable<K> objects) throws VeniceException {
    ReusableObjects reusableObjects = REUSABLE_OBJECTS.get();
    reusableObjects.outputStream.reset();
    return serializeObjects(objects, reusableObjects.binaryEncoder, reusableObjects.outputStream);
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
    ReusableObjects reusableObjects = REUSABLE_OBJECTS.get();
    reusableObjects.outputStream.reset();
    reusableObjects.outputStream.write(prefix.array(), prefix.position(), prefix.remaining());
    return serializeObjects(objects, reusableObjects.binaryEncoder, reusableObjects.outputStream);
  }

  protected byte[] serializeObjects(
      Iterable<K> objects,
      BinaryEncoder reusedEncoder,
      ByteArrayOutputStream outputStream) throws VeniceException {
    Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(outputStream, buffered, reusedEncoder);
    try {
      for (K object: objects) {
        try {
          write(object, encoder);
        } catch (IOException e) {
          throw new VeniceException("Unable to serialize object", e);
        }
      }
      encoder.flush();
    } catch (IOException e) {
      throw new VeniceException("Unable to flush BinaryEncoder", e);
    }
    return outputStream.toByteArray();
  }

  protected void write(K object, Encoder encoder) throws IOException {
    try {
      if (object instanceof SpecificRecord) {
        specificDatumWriter.write(object, encoder);
      } else {
        genericDatumWriter.write(object, encoder);
      }
    } catch (NullPointerException e) {
      if (object instanceof SpecificRecord && specificDatumWriter == null) {
        /*
         * Defensive code...
         *
         * At the time of writing this commit, only the {@link VsonAvroGenericSerializer}
         * uses the protected constructor to pass in a null {@link specificDatumWriter},
         * and the Vson serializer should never be used with a SpecificRecord, so the NPE
         * should never happen. If this assumption is broken in the future, and this code
         * regresses, then hopefully this exception can help future maintainers to find
         * the issue more easily.
         */
        throw new IllegalStateException(
            "This instance of " + this.getClass().getSimpleName()
                + " was instantiated with a null specificDatumWriter, and was used to serialize a SpecificRecord.",
            e);
      }
      throw e;
    }
  }
}
