package com.linkedin.davinci.serializer.avro.fast;

import com.linkedin.avro.fastserde.FastGenericDatumWriter;
import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.avro.fastserde.FastSpecificDatumWriter;
import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.venice.serializer.AvroSerializer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import org.apache.avro.Schema;


public class MapOrderPreservingFastSerializer<K> extends AvroSerializer<K> {
  private static final DatumWriterCustomization MAP_ORDER_PRESERVING_CHECK_CUSTOMIZATION =
      new DatumWriterCustomization.Builder().setCheckMapTypeFunction(datum -> {
        Map map = (Map) datum;
        if (map.isEmpty()) {
          return;
        }
        if (!(map instanceof LinkedHashMap || map instanceof IndexedHashMap || map instanceof SortedMap)) {
          throw new IllegalStateException(
              "Expect map to be either a LinkedHashMap or a IndexedHashMap or a SortedMap because"
                  + " the notion of ordering is required. Otherwise, it does not make sense to preserve \"order\". "
                  + "Got datum type: " + map.getClass());
        }
      }).build();

  public MapOrderPreservingFastSerializer(Schema schema) {
    super(
        new FastGenericDatumWriter<>(
            schema,
            null,
            FastSerdeCache.getDefaultInstance(),
            MAP_ORDER_PRESERVING_CHECK_CUSTOMIZATION),
        new FastSpecificDatumWriter<>(
            schema,
            null,
            FastSerdeCache.getDefaultInstance(),
            MAP_ORDER_PRESERVING_CHECK_CUSTOMIZATION));
  }
}
