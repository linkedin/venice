package com.linkedin.venice.schema.vson;

import com.linkedin.venice.serializer.VsonSerializationException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;


@Deprecated
public class VsonAvroDatumReader<D> extends GenericDatumReader<D> {
  public VsonAvroDatumReader(Schema schema) {
    super(schema, schema);
  }

  public VsonAvroDatumReader(Schema writer, Schema reader) {
    super(writer, reader);
  }

  @Override
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    HashMap<String, Object> record = new DeepEqualsHashMap();
    for (Schema.Field field: in.readFieldOrder()) {
      record.put(field.name(), read(null, field.schema(), in));
    }

    return record;
  }

  @Override
  protected Object readFixed(Object old, Schema expected, Decoder in) throws IOException {
    if (expected.getFixedSize() == 1) {
      return readByte(new byte[1], in);
    } else if (expected.getFixedSize() == 2) {
      return readShort(new byte[2], in);
    } else {
      throw illegalFixedLength(expected.getFixedSize());
    }
  }

  private Byte readByte(byte[] bytes, Decoder in) throws IOException {
    in.readFixed(bytes, 0, 1);
    return bytes[0];
  }

  private Short readShort(byte[] bytes, Decoder in) throws IOException {
    in.readFixed(bytes, 0, 2);
    return (short) (((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF));
  }

  @Override
  protected Object readString(Object old, Schema expected, Decoder in) throws IOException {
    return super.readString(old, expected, in).toString();
  }

  @Override
  protected Object readBytes(Object old, Decoder in) throws IOException {
    ByteBuffer byteBuffer = (ByteBuffer) super.readBytes(old, in);
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
    return bytes;
  }

  @Override
  protected Object readEnum(Schema expected, Decoder in) throws IOException {
    throw notSupportType(expected.getType());
  }

  @Override
  protected Object readMap(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    throw notSupportType(expected.getType());
  }

  /**
   * The reason to override this function is that VSON would like to use {@link java.util.ArrayList} instead of
   * {@link GenericData.Array} to support comparison against another {@link java.util.AbstractList}.
   */
  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    if (old instanceof Collection) {
      ((Collection) old).clear();
      return old;
    } else
      return new DeepEqualsArrayList(size);
  }

  static VsonSerializationException notSupportType(Schema.Type type) {
    return new VsonSerializationException(
        String.format("Does not support casting type: %s between Vson and Avro", type.toString()));
  }

  // this should not happen. If the program goes to here, bad thing happened
  static VsonSerializationException illegalFixedLength(int len) {
    return new VsonSerializationException(
        "illegal Fixed type length: " + len
            + "Fixed type is only for single byte or short and should not have size greater than 2");
  }

  /**
   * This class supports the special byte[] check.
   * With this class, you can compare the map result retrieved from VSON store.
   *
   * Most of the logic is copied from {@link java.util.AbstractMap#equals(Object)}
   */
  public static class DeepEqualsHashMap extends HashMap<String, Object> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;

      if (!(o instanceof Map))
        return false;
      Map<?, ?> m = (Map<?, ?>) o;
      if (m.size() != size())
        return false;

      try {
        Iterator<Entry<String, Object>> i = entrySet().iterator();
        while (i.hasNext()) {
          Entry<String, Object> e = i.next();
          String key = e.getKey();
          Object value = e.getValue();
          if (value == null) {
            if (!(m.get(key) == null && m.containsKey(key)))
              return false;
          } else {
            /**
             * Special check for byte[].
             */
            Object rightValue = m.get(key);
            if (value.getClass().equals(byte[].class) && rightValue.getClass().equals(byte[].class)) {
              return Arrays.equals((byte[]) value, (byte[]) rightValue);
            }
            /**
             * We should not use 'rightValue.equals(value)' here since the right value may not support deep equals check.
             */
            if (!value.equals(rightValue))
              return false;
          }
        }
      } catch (ClassCastException | NullPointerException unused) {
        return false;
      }

      return true;
    }
  }

  /**
   * This class supports the special byte[] check.
   * With this class, you can compare the array result retrieved from VSON store.
   *
   * Most of the logic is copied from {@link java.util.AbstractList#equals(Object)}
   */
  public static class DeepEqualsArrayList extends ArrayList<Object> {
    private static final long serialVersionUID = 1L;

    public DeepEqualsArrayList() {
      super();
    }

    public DeepEqualsArrayList(int size) {
      super(size);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof List))
        return false;

      ListIterator<Object> e1 = listIterator();
      ListIterator<?> e2 = ((List<?>) o).listIterator();
      while (e1.hasNext() && e2.hasNext()) {
        Object o1 = e1.next();
        Object o2 = e2.next();
        // Special check for byte[]
        if (o1 != null && o2 != null && o1.getClass().equals(byte[].class) && o2.getClass().equals(byte[].class)) {
          if (!Arrays.equals((byte[]) o1, (byte[]) o2)) {
            return false;
          }
        }
        /**
         * We should not use 'o2.equals(o1)' here since the right value may not support deep equals check.
         */
        else if (!(o1 == null ? o2 == null : o1.equals(o2))) {
          return false;
        }
      }
      return !(e1.hasNext() || e2.hasNext());
    }

    @Override
    public int hashCode() {
      return listIterator().hashCode();
    }
  }
}
