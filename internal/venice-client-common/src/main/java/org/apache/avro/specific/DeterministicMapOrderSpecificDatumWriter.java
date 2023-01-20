package org.apache.avro.specific;

import java.io.IOException;
import java.util.SortedMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.DeterministicMapOrderDatumWriter;
import org.apache.avro.generic.DeterministicMapOrderGenericDatumWriter;
import org.apache.avro.io.Encoder;


/**
 * An Avro specific datum writer that sorts the map entries by keys before
 * serializing the map.
 *
 * See {@link DeterministicMapOrderGenericDatumWriter} for more details.
 */
public class DeterministicMapOrderSpecificDatumWriter<T> extends SpecificDatumWriter<T>
    implements DeterministicMapOrderDatumWriter {
  public DeterministicMapOrderSpecificDatumWriter() {
    super();
  }

  public DeterministicMapOrderSpecificDatumWriter(Class<T> c) {
    super(c);
  }

  public DeterministicMapOrderSpecificDatumWriter(Schema schema) {
    super(schema);
  }

  public DeterministicMapOrderSpecificDatumWriter(Schema root, SpecificData specificData) {
    super(root, specificData);
  }

  protected DeterministicMapOrderSpecificDatumWriter(SpecificData specificData) {
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
      writeMapWithDeterministicOrder(schema, datum, out);
    }
  }
}
