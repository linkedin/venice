package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.OptimizedKafkaValueBinaryDecoder;
import org.apache.avro.io.OptimizedKafkaValueBinaryDecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.SpecificDatumReader;


/**
 * This class is to reuse the original array for put payload of each message
 * to reduce the unnecessary byte array allocation.
 */
public class OptimizedKafkaValueSerializer extends KafkaValueSerializer {
  private static final String PUT_RECORD_NAME = "Put";
  private static final String PUT_VALUE_FIELD_NAME = "putValue";
  private static final String BUFFER_REUSE_FLAG = "buffer_reuse";
  private static final Schema BYTES_SCHEMA_WITH_BUFFER_REUSE_FLAG = Schema.create(Schema.Type.BYTES);
  static {
    /**
     * Flag to indicate {@link OptimizedKafkaValueSerializer} to enable buffer reuse.
     */
    BYTES_SCHEMA_WITH_BUFFER_REUSE_FLAG.addProp(BUFFER_REUSE_FLAG, "true");
  }
  private static final OptimizedKafkaValueBinaryDecoderFactory DECODER_FACTORY = new OptimizedKafkaValueBinaryDecoderFactory();

  @Override
  protected BinaryDecoder createBinaryDecoder(byte[] bytes, int offset,
      int length, BinaryDecoder reuse) {
    return DECODER_FACTORY.createBinaryDecoder(bytes, offset, length);
  }

  @Override
  protected SpecificDatumReader<KafkaMessageEnvelope> createSpecificDatumReader(Schema writer, Schema reader) {
    return new KafkaValueDatumReader(writer, reader);
  }

  /**
   * This class is used to enable buffer reuse for field: {@link #PUT_VALUE_FIELD_NAME}.
   */
  class KafkaValueDatumReader extends SpecificDatumReader<KafkaMessageEnvelope> {
    private ThreadLocal<OptimizedKafkaValueBinaryDecoder> binaryDecoder = new ThreadLocal<>();

    public KafkaValueDatumReader(Schema writer, Schema reader) {
      super(writer, reader);
    }

    @Override
    public KafkaMessageEnvelope read(KafkaMessageEnvelope reuse, Decoder in) throws IOException {
      if (in instanceof OptimizedKafkaValueBinaryDecoder) {
        binaryDecoder.set((OptimizedKafkaValueBinaryDecoder)in);
      } else {
        throw new VeniceException("Decoder should be an instance of 'OptimizedKafkaValueBinaryDecoder'");
      }
      return super.read(reuse, in);
    }

    @Override
    protected Object read(Object old, Schema expected, ResolvingDecoder in) throws IOException {
      if (expected.getType() == Schema.Type.BYTES && expected.getProp(BUFFER_REUSE_FLAG) != null) {
        OptimizedKafkaValueBinaryDecoder decoder = binaryDecoder.get();
        if (null == decoder) {
          throw new VeniceException("binaryDecoder shouldn't be null");
        }
        /**
         * With the following flag, the underlying {@link OptimizedKafkaValueBinaryDecoder} will wrap the original byte array
         * as a byte buffer instead of creating a new byte array.
         */
        decoder.enableBufferReuse();
        Object obj = super.read(old, expected, in);
        /**
         * Disable the buffer reuse flag for the following fields.
         */
        decoder.disableBufferReuse();
        return obj;
      }

      return super.read(old, expected, in);
    }

    /**
     * This piece of code is copied from Avro 1.4 implementation, and we should audit it when
     * upgrading to some newer Avro version.
     * @param old
     * @param expected
     * @param in
     * @return
     * @throws IOException
     */
    @Override
    protected Object readRecord(Object old, Schema expected,
        ResolvingDecoder in) throws IOException {
      Object record = newRecord(old, expected);

      for (Schema.Field f : in.readFieldOrder()) {
        int pos = f.pos();
        String name = f.name();
        Object oldDatum = (old != null) ? getField(record, name, pos) : null;
        Schema fieldSchema = f.schema();
        if (fieldSchema.getType().equals(Schema.Type.BYTES) &&
            expected.getName().equals(PUT_RECORD_NAME) &&
            f.name().equals(PUT_VALUE_FIELD_NAME)) {
          /**
           * This is used to indicate the current field should reuse the original byte array.
           * We could not modify {@link f.schema()} since it will actually change the underlying schema.
           */
          fieldSchema = BYTES_SCHEMA_WITH_BUFFER_REUSE_FLAG;
        }

        setField(record, name, pos, read(oldDatum, fieldSchema, in));
      }

      return record;
    }
  }
}
