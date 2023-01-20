package com.linkedin.venice.schema.vson;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.serializer.VsonSerializationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;


/**
 * VsonAvroSerializer is in charge of de/serializing between Vson binary and Vson object.
 * It's also able to deserialize Vson binary to Avro object.
 *
 * This class was inspired and referred from Voldemort.
 * https://github.com/voldemort/voldemort/blob/master/src/java/voldemort/serialization/json/JsonTypeSerializer.java
 */
@Deprecated
public class VsonAvroSerializer {
  private static final int MAX_SEQ_LENGTH = 0x3FFFFFFF;

  private final VsonSchema schema;

  private final Schema avroSchema;

  public static VsonAvroSerializer fromSchemaStr(String vsonSchemaStr) {
    return new VsonAvroSerializer(VsonSchema.parse(vsonSchemaStr));
  }

  private VsonAvroSerializer(VsonSchema schema) {
    this.schema = schema;
    this.avroSchema = VsonAvroSchemaAdapter.parse(schema.toString());
  }

  public String toString(Object object) {
    return toByteStream(object).toString();
  }

  public byte[] toBytes(Object object) {
    return toByteStream(object).toByteArray();
  }

  private ByteArrayOutputStream toByteStream(Object object) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    try {
      toBytes(object, output);
      output.flush();
      return bytes;
    } catch (IOException e) {
      throw new VsonSerializationException(e);
    }
  }

  public void toBytes(Object object, DataOutputStream output) throws IOException {
    write(output, object, schema.getType());
  }

  public Object toObject(byte[] bytes) {
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      return toObject(input);
    } catch (IOException e) {
      throw new VsonSerializationException(e);
    }
  }

  private Object toObject(DataInputStream input) throws IOException {
    return read(input, schema.getType());
  }

  public Object bytesToAvro(byte[] bytes) {
    return bytesToAvro(bytes, 0, bytes.length);
  }

  public Object bytesToAvro(byte[] bytes, int offset, int length) {
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes, offset, length))) {
      return bytesToAvro(input);
    } catch (IOException e) {
      throw new VsonSerializationException(e);
    }
  }

  private Object bytesToAvro(DataInputStream input) throws IOException {
    return readToAvro(input, schema, avroSchema);
  }

  @SuppressWarnings("unchecked")
  private void write(DataOutputStream output, Object object, Object type) throws IOException {
    try {
      if (type instanceof Map) {
        if (object != null && !(object instanceof Map))
          throw new VsonSerializationException("Expected Map, but got " + object.getClass() + ": " + object);
        writeMap(output, (Map<String, Object>) object, (Map<String, Object>) type);
      } else if (type instanceof List) {
        if (object != null && !(object instanceof List))
          throw new VsonSerializationException("Expected List but got " + object.getClass() + ": " + object);
        writeList(output, (List<Object>) object, (List<Object>) type);
      } else if (type instanceof VsonTypes) {
        VsonTypes jsonType = (VsonTypes) type;
        switch (jsonType) {
          case STRING:
            writeString(output, (String) object);
            break;
          case INT8:
            writeInt8(output, (Byte) object);
            break;
          case INT16:
            writeInt16(output, coerceToShort(object));
            break;
          case INT32:
            writeInt32(output, coerceToInteger(object));
            break;
          case INT64:
            writeInt64(output, coerceToLong(object));
            break;
          case FLOAT32:
            writeFloat32(output, coerceToFloat(object));
            break;
          case FLOAT64:
            writeFloat64(output, coerceToDouble(object));
            break;
          case DATE:
            writeDate(output, coerceToDate(object));
            break;
          case BYTES:
            writeBytes(output, (byte[]) object);
            break;
          case BOOLEAN:
            writeBoolean(output, (Boolean) object);
            break;
          default:
            throw new VsonSerializationException("Unknown type: " + type);
        }
      }
    } catch (ClassCastException e) {
      // simpler than doing every test
      throw new VsonSerializationException(
          "Expected type " + type + " but got object of incompatible type " + object.getClass().getName() + ".",
          e);
    }
  }

  private Object readToAvro(DataInputStream stream, VsonSchema vsonSchema, Schema avroSchema) throws IOException {
    Object vsonSchemaType = vsonSchema.getType();
    if (vsonSchemaType instanceof Map) {
      return readMapToAvro(stream, vsonSchema, avroSchema);
    } else if (vsonSchemaType instanceof List) {
      return readListToAvro(stream, vsonSchema, avroSchema);
    } else {
      // vson primitive type
      switch ((VsonTypes) vsonSchemaType) {
        case BOOLEAN:
          return readBoolean(stream);
        case INT8:
          /**
           * In Avro 1.7, both the bytes array and a schema are needed to construct a Fixed type of data;
           * however, 'avroSchema' has already been transformed by the VsonAvroSchemaAdapter, so 'avroSchema'
           * is an UNION schema (e.g., [{"type":"fixed","name":"byteWrapper","size":1},"null"]); therefore,
           * the actual schema (e.g., {"type":"fixed","name":"byteWrapper","size":1}) should be extracted
           * before calling readByteToAvro() function which will construct a GenericData.Fixed object.
           * While in Avro 1.4, the schema is not needed to construct a GenericData.Fixed object.
           */
          return readByteToAvro(stream, VsonAvroSchemaAdapter.stripFromUnion(avroSchema));
        case INT16:
          /**
           * Same reason as above. This time we are extracting {"type":"fixed","name":"shortWrapper","size":2}
           * from the UNION schema [{"type":"fixed","name":"shortWrapper","size":2},"null"].
           */
          return readShortToAvro(stream, VsonAvroSchemaAdapter.stripFromUnion(avroSchema));
        case INT32:
          return readInt32(stream);
        case INT64:
          return readInt64(stream);
        case FLOAT32:
          return readFloat32(stream);
        case FLOAT64:
          return readFloat64(stream);
        case BYTES:
          byte[] bytes = readBytes(stream);
          return bytes == null ? null : ByteBuffer.wrap(bytes);
        case STRING:
          String string = readString(stream);
          return string == null ? null : new Utf8(string);
        case DATE:
          throw new VsonSerializationException("Converting Date to Avro is not supported");
        default:
          throw new VsonSerializationException("Unknown type of class " + vsonSchemaType.getClass());

      }
    }
  }

  private GenericData.Record readMapToAvro(DataInputStream stream, VsonSchema vsonSchema, Schema avroSchema)
      throws IOException {
    if (stream.readByte() == -1) {
      return null;
    }

    Schema curAvroSchema = VsonAvroSchemaAdapter.stripFromUnion(avroSchema);

    GenericData.Record record = new GenericData.Record(curAvroSchema);

    Map<String, Object> mapSchemaType = (Map<String, Object>) vsonSchema.getType();

    for (Map.Entry<String, Object> field: mapSchemaType.entrySet()) {
      String fieldName = field.getKey();
      VsonSchema fieldVsonSchema = vsonSchema.recordSubtype(fieldName);
      Schema fieldAvroSchema = curAvroSchema.getField(fieldName).schema();
      record.put(fieldName, readToAvro(stream, fieldVsonSchema, fieldAvroSchema));
    }

    return record;
  }

  private GenericData.Array readListToAvro(DataInputStream stream, VsonSchema vsonSchema, Schema avroSchema)
      throws IOException {
    int size = readLength(stream);
    if (size < 0) {
      return null;
    }

    Schema curAvroSchema = VsonAvroSchemaAdapter.stripFromUnion(avroSchema);

    GenericData.Array array = new GenericData.Array(size, curAvroSchema);
    for (int i = 0; i < size; i++) {
      array.add(readToAvro(stream, vsonSchema.listSubtype(), curAvroSchema.getElementType()));
    }

    return array;
  }

  private GenericData.Fixed readByteToAvro(DataInputStream stream, Schema avroSchema) throws IOException {
    byte b = stream.readByte();
    if (b == Byte.MIN_VALUE) {
      return null;
    }

    byte[] byteArray = { b };
    return AvroCompatibilityHelper.newFixedField(avroSchema, byteArray);
  }

  private GenericData.Fixed readShortToAvro(DataInputStream stream, Schema avroSchema) throws IOException {
    short s = stream.readShort();
    if (s == Short.MIN_VALUE)
      return null;

    // big-endian encoding
    byte[] byteArray = { (byte) ((s >>> 8) & 0xff), (byte) (s & 0xff) };
    return AvroCompatibilityHelper.newFixedField(avroSchema, byteArray);
  }

  @SuppressWarnings("unchecked")
  private Object read(DataInputStream stream, Object type) throws IOException {
    if (type instanceof Map) {
      return readMap(stream, (Map<String, Object>) type);
    } else if (type instanceof List) {
      return readList(stream, (List<?>) type);
    } else if (type instanceof VsonTypes) {
      VsonTypes vsonType = (VsonTypes) type;
      switch (vsonType) {
        case BOOLEAN:
          return readBoolean(stream);
        case INT8:
          return readInt8(stream);
        case INT16:
          return readInt16(stream);
        case INT32:
          return readInt32(stream);
        case INT64:
          return readInt64(stream);
        case FLOAT32:
          return readFloat32(stream);
        case FLOAT64:
          return readFloat64(stream);
        case DATE:
          throw new VsonSerializationException("Date is not supported since it is not compatible type in Avro.");
        case BYTES:
          return readBytes(stream);
        case STRING:
          return readString(stream);
        default:
          throw new VsonSerializationException("Unknown type: " + type);
      }
    } else {
      throw new VsonSerializationException("Unknown type of class " + type.getClass());
    }
  }

  private void writeBoolean(DataOutputStream output, Boolean b) throws IOException {
    if (b == null)
      output.writeByte(-1);
    else if (b.booleanValue())
      output.writeByte(1);
    else
      output.write(0);
  }

  private Boolean readBoolean(DataInputStream stream) throws IOException {
    byte b = stream.readByte();
    if (b < 0)
      return null;
    else if (b == 0)
      return false;
    else
      return true;
  }

  private Short coerceToShort(Object o) {
    if (o == null)
      return null;
    Class<?> c = o.getClass();
    if (c == Short.class)
      return (Short) o;
    else if (c == Byte.class)
      return ((Byte) o).shortValue();
    else
      throw new VsonSerializationException(
          "Object of type " + c.getName() + " cannot be coerced to type " + VsonTypes.INT16
              + " as the schema specifies.");
  }

  private Integer coerceToInteger(Object o) {
    if (o == null)
      return null;
    Class<?> c = o.getClass();
    if (c == Integer.class)
      return (Integer) o;
    else if (c == Byte.class)
      return ((Byte) o).intValue();
    else if (c == Short.class)
      return ((Short) o).intValue();
    else
      throw new VsonSerializationException(
          "Object of type " + c.getName() + " cannot be coerced to type " + VsonTypes.INT32
              + " as the schema specifies.");
  }

  private Long coerceToLong(Object o) {
    if (o == null)
      return null;
    Class<?> c = o.getClass();
    if (c == Long.class)
      return (Long) o;
    else if (c == Byte.class)
      return ((Byte) o).longValue();
    else if (c == Short.class)
      return ((Short) o).longValue();
    else if (c == Integer.class)
      return ((Integer) o).longValue();
    else
      throw new VsonSerializationException(
          "Object of type " + c.getName() + " cannot be coerced to type " + VsonTypes.INT64
              + " as the schema specifies.");
  }

  private Float coerceToFloat(Object o) {
    if (o == null)
      return null;
    Class<?> c = o.getClass();
    if (c == Float.class)
      return (Float) o;
    else if (c == Byte.class)
      return ((Byte) o).floatValue();
    else if (c == Short.class)
      return ((Short) o).floatValue();
    else if (c == Integer.class)
      return ((Integer) o).floatValue();
    else
      throw new VsonSerializationException(
          "Object of type " + c.getName() + " cannot be coerced to type " + VsonTypes.FLOAT32
              + " as the schema specifies.");
  }

  private Double coerceToDouble(Object o) {
    if (o == null)
      return null;
    Class<?> c = o.getClass();
    if (c == Double.class)
      return (Double) o;
    else if (c == Byte.class)
      return ((Byte) o).doubleValue();
    else if (c == Short.class)
      return ((Short) o).doubleValue();
    else if (c == Integer.class)
      return ((Integer) o).doubleValue();
    else if (c == Float.class)
      return ((Float) o).doubleValue();
    else
      throw new VsonSerializationException(
          "Object of type " + c.getName() + " cannot be coerced to type " + VsonTypes.FLOAT32
              + " as the schema specifies.");
  }

  private void writeString(DataOutputStream stream, String s) throws IOException {
    writeBytes(stream, s == null ? null : s.getBytes("UTF-8"));
  }

  private String readString(DataInputStream stream) throws IOException {
    byte[] bytes = readBytes(stream);
    if (bytes == null)
      return null;
    else
      return new String(bytes, "UTF-8");
  }

  private Byte readInt8(DataInputStream stream) throws IOException {
    byte b = stream.readByte();
    if (b == Byte.MIN_VALUE)
      return null;
    else
      return b;
  }

  private void writeInt8(DataOutputStream output, Byte b) throws IOException {
    if (b == null)
      output.writeByte(Byte.MIN_VALUE);
    else if (b.byteValue() == Byte.MIN_VALUE)
      throw new VsonSerializationException(
          "Underflow: attempt to store " + Byte.MIN_VALUE + " in int8, but minimum value is " + (Byte.MIN_VALUE - 1)
              + ".");
    else
      output.writeByte(b.byteValue());
  }

  private Short readInt16(DataInputStream stream) throws IOException {
    short s = stream.readShort();
    if (s == Short.MIN_VALUE)
      return null;
    else
      return s;
  }

  private void writeInt16(DataOutputStream output, Short s) throws IOException {
    if (s == null)
      output.writeShort(Short.MIN_VALUE);
    else if (s.shortValue() == Short.MIN_VALUE)
      throw new VsonSerializationException(
          "Underflow: attempt to store " + Short.MIN_VALUE + " in int16, but minimum value is " + (Short.MIN_VALUE - 1)
              + ".");
    else
      output.writeShort(s.shortValue());
  }

  private Integer readInt32(DataInputStream stream) throws IOException {
    int i = stream.readInt();
    if (i == Integer.MIN_VALUE)
      return null;
    else
      return i;
  }

  private void writeInt32(DataOutputStream output, Integer i) throws IOException {
    if (i == null)
      output.writeInt(Integer.MIN_VALUE);
    else if (i.intValue() == Integer.MIN_VALUE)
      throw new VsonSerializationException(
          "Underflow: attempt to store " + Integer.MIN_VALUE + " in int32, but minimum value is "
              + (Integer.MIN_VALUE - 1) + ".");
    else
      output.writeInt(i.intValue());
  }

  private Long readInt64(DataInputStream stream) throws IOException {
    long l = stream.readLong();
    if (l == Long.MIN_VALUE)
      return null;
    else
      return l;
  }

  private void writeInt64(DataOutputStream output, Long l) throws IOException {
    if (l == null)
      output.writeLong(Long.MIN_VALUE);
    else if (l.longValue() == Long.MIN_VALUE)
      throw new VsonSerializationException(
          "Underflow: attempt to store " + Long.MIN_VALUE + " in int64, but minimum value is " + (Long.MIN_VALUE - 1)
              + ".");
    else
      output.writeLong(l.longValue());
  }

  private Float readFloat32(DataInputStream stream) throws IOException {
    float f = stream.readFloat();
    if (f == Float.MIN_VALUE)
      return null;
    else
      return f;
  }

  private void writeFloat32(DataOutputStream output, Float f) throws IOException {
    if (f == null)
      output.writeFloat(Float.MIN_VALUE);
    else if (f.floatValue() == Float.MIN_VALUE)
      throw new VsonSerializationException(
          "Underflow: attempt to store " + Float.MIN_VALUE + " in float32, but that value is reserved for null.");
    else
      output.writeFloat(f.floatValue());
  }

  private Double readFloat64(DataInputStream stream) throws IOException {
    double d = stream.readDouble();
    if (d == Double.MIN_VALUE)
      return null;
    else
      return d;
  }

  private void writeFloat64(DataOutputStream output, Double d) throws IOException {
    if (d == null)
      output.writeDouble(Double.MIN_VALUE);
    else if (d.doubleValue() == Double.MIN_VALUE)
      throw new VsonSerializationException(
          "Underflow: attempt to store " + Double.MIN_VALUE + " in float64, but that value is reserved for null.");
    else
      output.writeDouble(d.doubleValue());
  }

  private Date coerceToDate(Object o) {
    if (o == null)
      return null;
    else if (o instanceof Date)
      return (Date) o;
    else if (o instanceof Number)
      return new Date(((Number) o).longValue());
    else
      throw new VsonSerializationException(
          "Object of type " + o.getClass() + " can not be coerced to type " + VsonTypes.DATE);
  }

  private void writeDate(DataOutputStream output, Date d) throws IOException {
    if (d == null)
      output.writeLong(Long.MIN_VALUE);
    else if (d.getTime() == Long.MIN_VALUE)
      throw new VsonSerializationException(
          "Underflow: attempt to store " + new Date(Long.MIN_VALUE) + " in date, but that value is reserved for null.");
    else
      output.writeLong(d.getTime());
  }

  private byte[] readBytes(DataInputStream stream) throws IOException {
    int size = readLength(stream);
    if (size < 0)
      return null;
    byte[] bytes = new byte[size];
    readToByteArray(stream, bytes);
    return bytes;
  }

  private void writeBytes(DataOutputStream output, byte[] b) throws IOException {
    if (b == null) {
      writeLength(output, -1);
    } else {
      writeLength(output, b.length);
      output.write(b);
    }
  }

  private void writeMap(DataOutputStream output, Map<String, Object> object, Map<String, Object> type)
      throws IOException {
    if (object == null) {
      output.writeByte(-1);
      return;
    } else {
      output.writeByte(1);
      if (object.size() != type.size())
        throw new VsonSerializationException("Invalid map for serialization, expected: " + type + " but got " + object);
      for (Map.Entry<String, Object> entry: type.entrySet()) {
        if (!object.containsKey(entry.getKey()))
          throw new VsonSerializationException(
              "Missing property: " + entry.getKey() + " that is required by the type (" + type + ")");
        try {
          write(output, object.get(entry.getKey()), entry.getValue());
        } catch (VsonSerializationException e) {
          throw new VsonSerializationException("Fail to write property: " + entry.getKey(), e);
        }
      }
    }
  }

  private Map<?, ?> readMap(DataInputStream stream, Map<String, Object> type) throws IOException {
    if (stream.readByte() == -1)
      return null;
    Map<String, Object> m = new HashMap<String, Object>(type.size());
    for (Map.Entry<String, Object> typeMapEntry: type.entrySet())
      m.put(typeMapEntry.getKey(), read(stream, typeMapEntry.getValue()));
    return m;
  }

  private void writeList(DataOutputStream output, List<Object> objects, List<Object> type) throws IOException {
    if (type.size() != 1)
      throw new VsonSerializationException("Invalid type: expected single value type in list: " + type);
    Object entryType = type.get(0);
    if (objects == null) {
      writeLength(output, -1);
    } else {
      writeLength(output, objects.size());
      for (Object o: objects)
        write(output, o, entryType);
    }
  }

  private List<?> readList(DataInputStream stream, List<?> type) throws IOException {
    int size = readLength(stream);
    if (size < 0)
      return null;
    List<Object> items = new ArrayList<Object>(size);
    Object entryType = type.get(0);
    for (int i = 0; i < size; i++)
      items.add(read(stream, entryType));
    return items;
  }

  private void writeLength(DataOutputStream stream, int size) throws IOException {
    if (size < Short.MAX_VALUE) {
      stream.writeShort(size);
    } else if (size <= MAX_SEQ_LENGTH) {
      stream.writeInt(size | 0xC0000000);
    } else {
      throw new VsonSerializationException("Invalid length: maximum is " + MAX_SEQ_LENGTH);
    }
  }

  int readLength(DataInputStream stream) throws IOException {
    short size = stream.readShort();
    // this is a hack for backwards compatibility
    if (size == -1) {
      return -1;
    } else if (size < -1) {
      // mask off first two bits, remainder is the size
      int fixedSize = size & 0x3FFF;
      fixedSize <<= 16;
      fixedSize += stream.readShort() & 0xFFFF;
      return fixedSize;
    } else {
      return size;
    }
  }

  /**
   * Read exactly buffer.length bytes from the stream into the buffer
   *
   * @param stream The stream to read from
   * @param buffer The buffer to read into
   */
  private void readToByteArray(InputStream stream, byte[] buffer) throws IOException {
    int read = 0;
    while (read < buffer.length) {
      int newlyRead = stream.read(buffer, read, buffer.length - read);
      if (newlyRead == -1)
        throw new EOFException("Attempt to read " + buffer.length + " bytes failed due to EOF.");
      read += newlyRead;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    VsonAvroSerializer other = (VsonAvroSerializer) obj;

    if (schema == null) {
      if (other.schema != null)
        return false;
    } else if (!schema.equals(other.schema))
      return false;
    return true;
  }
}
