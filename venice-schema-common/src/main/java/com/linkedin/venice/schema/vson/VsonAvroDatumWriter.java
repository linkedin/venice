package com.linkedin.venice.schema.vson;

import com.linkedin.venice.serializer.VsonSerializationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

import static com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter.stripFromUnion;

public class VsonAvroDatumWriter<K> extends GenericDatumWriter<K> {
  private Map<Schema, Schema> cachedStrippedSchema;
  public VsonAvroDatumWriter(Schema root) {
    super(root);
    cachedStrippedSchema = new HashMap<>();
  }

  @Override
  protected void write(Schema schema, Object datum, Encoder out)
      throws IOException {
      switch (schema.getType()) {
        case RECORD:  writeRecord(schema, datum, out);  break;
        case ARRAY:   writeArray(schema, datum, out);   break;
        case UNION:   writeUnion(schema, datum, out);   break;
        case STRING:  writeString(schema, datum, out);  break;
        case BYTES:   writeBytes(datum, out);           break;
        case INT:     out.writeInt((Integer)datum);     break;
        case LONG:    out.writeLong((Long)datum);       break;
        case FLOAT:   out.writeFloat((Float)datum);     break;
        case DOUBLE:  out.writeDouble((Double)datum);   break;
        case BOOLEAN: out.writeBoolean((Boolean)datum); break;
        case NULL:    out.writeNull();                  break;
        case MAP:
        case ENUM:
        case FIXED:
        default:
          throw VsonAvroDatumReader.notSupportType(schema.getType());
      }
  }

  @Override
  protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException{
    for (Schema.Field field : schema.getFields()) {
      Object value = ((Map) datum).get(field.name());
      write(field.schema(), value, out);
    }
  }

  /**
   * This method is kinda hacking. Since we know that 'Union' is only able to have two fields and
   * the index is fixed, we explicitly set them. The reason we don't use
   * {@link org.apache.avro.generic.GenericData#resolveUnion} is that Vson Record is presented as
   * a map. This is inconsistent with {@link org.apache.avro.generic.GenericData#isRecord(Object)}
   * and I am hesitated to involve another override as long as it work fine here.
   *
   * P.S. this also means we need to override {@link #write(Schema, Object, Encoder)} since 'UNION'
   * is not processed in a method call in super class
   */
  protected void writeUnion(Schema schema, Object datum, Encoder out) throws IOException {
    if (datum != null) {
      out.writeIndex(0);
      write(cachedStrippedSchema.computeIfAbsent(schema, s -> stripFromUnion(s)), datum, out);
    } else {
      out.writeIndex(1);
      write(Schema.create(Schema.Type.NULL), datum, out);
    }
  }
}
