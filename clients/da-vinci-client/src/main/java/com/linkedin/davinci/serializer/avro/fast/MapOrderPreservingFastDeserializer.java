package com.linkedin.davinci.serializer.avro.fast;

import com.linkedin.avro.fastserde.FastGenericDatumReader;
import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.davinci.utils.IndexedMap;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class MapOrderPreservingFastDeserializer extends AvroGenericDeserializer<GenericRecord> {
  public MapOrderPreservingFastDeserializer(Schema writer, Schema reader) {
    super(
        new FastGenericDatumReader<>(
            writer,
            reader,
            FastSerdeCache.getDefaultInstance(),
            null,
            new DatumReaderCustomization.Builder().setNewMapOverrideFunc((old, size) -> {
              if (old instanceof IndexedMap) {
                ((Map<?, ?>) old).clear();
                return old;
              } else {
                return new IndexedHashMap<>(size);
              }
            }).build()));
  }
}
