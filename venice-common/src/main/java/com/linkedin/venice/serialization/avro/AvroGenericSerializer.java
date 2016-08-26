package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.serialization.VeniceSerializer;
import java.util.Map;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroGenericSerializer implements VeniceSerializer<Object> {
    private final Schema typeDef;
    private GenericDatumWriter<Object> datumWriter;
    private GenericDatumReader<Object> reader;

    private static final Logger logger = Logger.getLogger(AvroGenericSerializer.class);

    // general constructor
    public AvroGenericSerializer(String schema) {
        typeDef = Schema.parse(schema);
        datumWriter = new GenericDatumWriter<>(typeDef);
        reader = new GenericDatumReader<>(typeDef);
    }

    /**
     * Close this serializer.
     * This method has to be idempotent if the serializer is used in KafkaProducer because it might be called
     * multiple times.
     */
    @Override
    public void close() {
      /* This function is not used, but is required for the interfaces. */
    }

    /**
     * Configure this class.

     * @param configMap configs in key/value pairs
     * @param isKey whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configMap, boolean isKey) {
      /* This function is not used, but is required for the interfaces. */
    }

    public byte[] serialize(String topic, Object object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = new BinaryEncoder(output);
        try {
            datumWriter.write(object, encoder);
            encoder.flush();
        } catch(IOException e) {
            throw new VeniceMessageException("Could not serialize the Avro object" + e);
        } finally {
            if(output != null) {
                try {
                    output.close();
                } catch(IOException e) {
                    logger.error("Failed to close stream", e);
                }
            }
        }
        return output.toByteArray();
    }

    public Object deserialize(String topic, byte[] bytes) {
        Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
        try {
            return reader.read(null, decoder);
        } catch(IOException e) {
            throw new VeniceMessageException("Could not deserialize bytes back into Avro object" + e);
        }
    }
}
