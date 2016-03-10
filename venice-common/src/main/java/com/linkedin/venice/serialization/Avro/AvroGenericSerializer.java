package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.serialization.VeniceSerializer;
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

    private static final Logger logger = Logger.getLogger(AvroGenericSerializer.class.getName());

    // general constructor
    public AvroGenericSerializer(String schema) {
        typeDef = Schema.parse(schema);
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

    public byte[] serialize(Object object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = new BinaryEncoder(output);
        GenericDatumWriter<Object> datumWriter = null;
        try {
            datumWriter = new GenericDatumWriter<Object>(typeDef);
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

    public Object deserialize(byte[] bytes) {
        Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
        GenericDatumReader<Object> reader = null;
        try {
            reader = new GenericDatumReader<Object>(typeDef);
            return reader.read(null, decoder);
        } catch(IOException e) {
            throw new VeniceMessageException("Could not deserialze bytes back into Avro Abject" + e);
        }
    }
}
