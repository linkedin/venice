package org.apache.avro.generic;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;


/**
 * The intention of having this interface is to convert {
 * @link GenericDatumWriter#write(Schema, Object, Encoder)} to be public
 * access.
 */
public interface MapAwareDatumWriter {
  void internalWrite(Schema schema, Object datum, Encoder out)  throws IOException;
}
