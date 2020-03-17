package org.apache.avro.generic;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;


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
 * {@link org.apache.avro.specific.MapAwareSpecificDatumWriter} for more
 * details.
 *
 * TODO: consider putting the class to Fast Avro library
 */
public class MapAwareGenericDatumWriter<T> extends GenericDatumWriter<T>
    implements MapAwareDatumWriter {
  public MapAwareGenericDatumWriter(Schema root) {
    super(root);
  }

  public MapAwareGenericDatumWriter(GenericData data) {
    super(data);
  }

  public MapAwareGenericDatumWriter(Schema root, GenericData data) {
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
      MapAwareGenericDatumWriter.writeMapInOrder(this, schema, datum, out);
    }
  }

  public static void writeMapInOrder(MapAwareDatumWriter datumWriter, Schema schema, Object datum, Encoder out)
      throws IOException {
    Schema valueSchemaType = schema.getValueType();
    Map map = (Map) datum;
    int size = map.size();
    int actualSize = 0;
    out.writeMapStart();
    out.setItemCount(size);

    List<Map.Entry> sortedEntryList = (List<Map.Entry>) map.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .collect(Collectors.toList());

    for (Map.Entry entry : sortedEntryList) {
      out.startItem();
      out.writeString((CharSequence) entry.getKey().toString());
      datumWriter.internalWrite(valueSchemaType, entry.getValue(), out);
      actualSize ++;
    }

    out.writeMapEnd();
    if (actualSize != size) {
      throw new ConcurrentModificationException("Size of map written was " +
          size + ", but number of entries written was " + actualSize + ". ");
    }
  }
}
