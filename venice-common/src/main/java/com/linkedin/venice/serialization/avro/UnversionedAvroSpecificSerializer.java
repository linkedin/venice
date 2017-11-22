package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;


/**
 * Serializer for translating an unversioned protocol of Avro records.
 */
public class UnversionedAvroSpecificSerializer<SPECIFIC_RECORD extends SpecificRecord>
    implements VeniceKafkaSerializer<SPECIFIC_RECORD> {

  private static final Logger logger = Logger.getLogger(UnversionedAvroSpecificSerializer.class);

  // Re-usable Avro facilities. Can be shared across multiple threads, so we only need one per process.

  /** Used to generate decoders. */
  private static final DecoderFactory DECODER_FACTORY = new DecoderFactory();

  /** Used to serialize objects into binary-encoded Avro according to the latest protocol version. */
  private final SpecificDatumWriter writer;

  /** Maintains the mapping between protocol version and the corresponding {@link SpecificDatumReader<SPECIFIC_RECORD>} */
  private final SpecificDatumReader<SPECIFIC_RECORD> reader;

  protected UnversionedAvroSpecificSerializer(Class<? extends SpecificRecord> specificRecordClass) {
    this.writer = new SpecificDatumWriter(specificRecordClass);
    Schema compiledProtocol = SpecificData.get().getSchema(specificRecordClass);
    this.reader = new SpecificDatumReader<>();
    reader.setSchema(compiledProtocol); // Writer's schema
    try {
      reader.setExpected(compiledProtocol); // Reader's schema
    } catch (IOException e) {
      throw new VeniceException("Failed to setup reader schema", e);
    }
  }

  /**
   * Close this serializer.
   * This method has to be idempotent if the serializer is used in KafkaProducer because it might be called
   * multiple times.
   */
  @Override
  public void close() {

  }

  /**
   * Configure this class.
   *
   * @param configMap configs in key/value pairs
   * @param isKey     whether is for key or value
   */
  @Override
  public void configure(Map<String, ?> configMap, boolean isKey) {
    if (isKey) {
      throw new VeniceException("Cannot use " + getClass().getSimpleName() + " for key data.");
    }
  }

  /**
   * Construct an array of bytes from the given object
   *
   * @param topic  Topic to which the object belongs.
   * @param object A {@link SPECIFIC_RECORD} instance to be serialized.
   * @return The Avro binary format bytes which represent the {@param object}
   */
  @Override
  public byte[] serialize(String topic, SPECIFIC_RECORD object) {
    try {
      // If single-threaded, both the ByteArrayOutputStream and Encoder can be re-used. TODO: explore GC tuning later.
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(byteArrayOutputStream);

      writer.write(object, encoder);

      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new VeniceMessageException("Failed to encode message from topic '" + topic + "': " + object.toString(), e);
    }
  }

  /**
   * Create an object from an array of bytes
   *
   * @param topic Topic to which the array of bytes belongs.
   * @param bytes An array of bytes representing the object's data serialized in Avro binary format.
   * @return A {@link SPECIFIC_RECORD} serialized from the bytes
   */
  @Override
  public SPECIFIC_RECORD deserialize(String topic, byte[] bytes) {
    try {
      if (bytes == null) {
        throw new IllegalArgumentException("Invalid byte array for serialization - no bytes to read");
      }

      /**
       * Reuse SpecificDatumReader since it is thread-safe, and generating a brand new SpecificDatumReader is very slow
       * sometimes.
       *
       * When generating a new {@link SpecificDatumReader}, both reader schema and writer schema need to be setup, and
       * internally {@link SpecificDatumReader} needs to calculate {@link org.apache.avro.io.ResolvingDecoder}, but
       * the slowest part is to persist those info to thread-local variables, which could take tens of seconds sometimes.
       *
       * TODO: investigate why {@link ThreadLocal} operations (internally {@link ThreadLocal.ThreadLocalMap})
       * are so slow sometimes.
        */

      Decoder decoder = DECODER_FACTORY.createBinaryDecoder(
          bytes,                         // The bytes array we wish to decode
          null                           // This param is to re-use a Decoder instance. TODO: explore GC tuning later.
      );

      SPECIFIC_RECORD record = reader.read(
          null, // This param is to re-use a SPECIFIC_RECORD instance. TODO: explore GC tuning later.
          decoder
      );

      return record;
    } catch (IOException e) {
      throw new VeniceMessageException("Failed to decode message from '" + topic + "': " + ByteUtils.toHexString(bytes), e);
    }
  }
}
