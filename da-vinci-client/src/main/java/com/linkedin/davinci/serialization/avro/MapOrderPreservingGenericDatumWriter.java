package com.linkedin.davinci.serialization.avro;

import com.linkedin.venice.utils.IndexedHashMap;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.SortedMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;


public class MapOrderPreservingGenericDatumWriter<T> extends GenericDatumWriter<T> {

  public MapOrderPreservingGenericDatumWriter(Schema schema) {
    super(schema);
  }

  @Override
  protected void writeMap(Schema schema, Object datum, Encoder out) throws IOException {
    if (!(datum instanceof LinkedHashMap || datum instanceof IndexedHashMap || datum instanceof SortedMap)) {
      throw new IllegalStateException("Expect map to be either a LinkedHashMap or a IndexedHashMap or a SortedMap because"
          + " the notion of ordering is required. Otherwise, it does not make sense to preserve \"order\". "
          + "Got datum type: " + datum.getClass());
    }
    super.writeMap(schema, datum, out);
  }
}
