package com.linkedin.venice.client.serializer;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroGenericSerializer implements RecordSerializer<Object> {
  private final Logger logger = Logger.getLogger(AvroGenericSerializer.class);
  private final GenericDatumWriter<Object> datumWriter;

  public AvroGenericSerializer(Schema schema) {
    this.datumWriter = new GenericDatumWriter<>(schema);
  }

  @Override
  public byte[] serialize(Object object) throws VeniceClientException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(output);
    try {
      datumWriter.write(object, encoder);
      encoder.flush();
    } catch(IOException e) {
      throw new VeniceClientException("Could not serialize the Avro object" + e);
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
}
