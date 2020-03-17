package org.apache.avro.specific;

import java.io.IOException;
import java.util.SortedMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.MapAwareDatumWriter;
import org.apache.avro.generic.MapAwareGenericDatumWriter;
import org.apache.avro.io.Encoder;


/**
 * An Avro specific datum writer that sorts the map entries by keys before
 * serializing the map.
 *
 * See {@link MapAwareGenericDatumWriter} for more details.
 */
public class MapAwareSpecificDatumWriter<T> extends SpecificDatumWriter<T>
    implements MapAwareDatumWriter {
  public MapAwareSpecificDatumWriter() {
    super();
  }

  public MapAwareSpecificDatumWriter(Class<T> c) {
    super(c);
  }

  public MapAwareSpecificDatumWriter(Schema schema) {
    super(schema);
  }

  public MapAwareSpecificDatumWriter(Schema root, SpecificData specificData) {
    super(root, specificData);
  }

  protected MapAwareSpecificDatumWriter(SpecificData specificData) {
    super(specificData);
  }

  /** Returns the {@link SpecificData} implementation used by this writer. */
  public SpecificData getSpecificData() {
    return (SpecificData) getData();
  }

  @Override
  public void internalWrite(Schema schema, Object datum, Encoder out) throws IOException {
    super.write(schema, datum, out);
  }

  @Override
  protected void writeMap(Schema schema, Object datum, Encoder out) throws IOException {
    if (datum instanceof SortedMap && ((SortedMap) datum).comparator() == null) {
      super.writeMap(schema, datum, out);
    } else {
      MapAwareGenericDatumWriter.writeMapInOrder(this, schema, datum, out);
    }
  }
}
