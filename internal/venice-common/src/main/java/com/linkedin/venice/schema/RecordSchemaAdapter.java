package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.VeniceSerializationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;


class RecordSchemaAdapter implements SchemaAdapter {
  private static RecordSchemaAdapter INSTANCE = new RecordSchemaAdapter();

  private RecordSchemaAdapter() {
  }

  static RecordSchemaAdapter getInstance() {
    return INSTANCE;
  }

  @Override
  public IndexedRecord adapt(Schema expectedSchema, Object datum) {
    if (!(datum instanceof IndexedRecord)) {
      throw new VeniceException("Expected datum to be an IndexedRecord");
    }

    IndexedRecord datumRecord = ((IndexedRecord) datum);
    Schema datumSchema = datumRecord.getSchema();

    if (datumSchema.equals(expectedSchema)) {
      return datumRecord;
    }

    RecordDeserializer<IndexedRecord> deserializer =
        FastSerializerDeserializerFactory.getAvroGenericDeserializer(datumSchema, expectedSchema);
    RecordSerializer<IndexedRecord> serializer =
        FastSerializerDeserializerFactory.getAvroGenericSerializer(datumSchema);

    try {
      return deserializer.deserialize(serializer.serialize(datumRecord));
    } catch (VeniceSerializationException e) {
      throw new VeniceException(e);
    }
  }
}
