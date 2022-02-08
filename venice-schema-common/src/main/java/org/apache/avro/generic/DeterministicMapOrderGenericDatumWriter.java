package org.apache.avro.generic;

import java.io.IOException;
import java.util.SortedMap;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.DeterministicMapOrderSpecificDatumWriter;


/**
 * An Avro generic datum writer that sorts the map entries by keys before
 * serializing the map.
 *
 * In original Avro implementation {@link GenericDatumWriter} Avro gets
 * all entries from the Map and serializes the entries right away. This
 * may generate inconsistent results if 2 maps have the same entry set but
 * are constructed in the different orders.
 *
 * A specific datum writer is also provided. See
 * {@link DeterministicMapOrderSpecificDatumWriter} for more
 * details.
 *
 * TODO: consider putting the class to Fast Avro library
 */
public class DeterministicMapOrderGenericDatumWriter<T> extends GenericDatumWriter<T>
    implements DeterministicMapOrderDatumWriter {
  public DeterministicMapOrderGenericDatumWriter(Schema root) {
    super(root);
  }

  public DeterministicMapOrderGenericDatumWriter(GenericData data) {
    super(data);
  }

  public DeterministicMapOrderGenericDatumWriter(Schema root, GenericData data) {
    super(data);
    setSchema(root);
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
