package com.linkedin.venice.schema.avro;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.ResolvingDecoder;

/**
 * This is a mash up of Avro's {@link GenericData.Array} class, as well as Quasar's cacheable computation.
 *
 * It is used by the Venice server to provide efficient deserialization and handling of float lists in
 * read computation use cases.
 *
 * Compared to the Avro implementation, it offers the following GC-related optimizations:
 *
 * - It does not, by default, box primitive floats into Object Floats, though it will still do so if the
 *   regular functions are called (e.g.: {@link #get(int)}, for compatibility purposes. In order to avoid
 *   boxing, the {@link #getPrimitive(int)} function can be used instead.
 *
 * - It does not maintain a reference to a {@link Schema} instance, since that schema would always be the
 *   same. Instead, it defines a static {@link #SCHEMA} which is used by all instances.
 *
 * - It re-implements {@link #compareTo(GenericArray)}, {@link #equals(Object)} and {@link #hashCode()}
 *   in order to leverage the primitive types, rather than causing unintended boxing.
 *
 * It also offers a {@link #squaredL2Norm()} function, which internally caches its results so that it
 * avoids recomputing the same thing repeatedly. This functionality comes at the expense of an extra
 * float and boolean properties, which adds up to less than the overhead saved by the object minimization
 * described above.
 */
public class ComputablePrimitiveFloatList extends AbstractList<Float>
    implements GenericArray<Float>, Comparable<GenericArray<Float>> {
  private static final float[] EMPTY = new float[0];
  private static final Schema SCHEMA = Schema.createArray(Schema.create(Schema.Type.FLOAT));
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private int size;
  private float[] elements = EMPTY;
  private float l2norm = 0;
  private volatile boolean l2normComputed = false;

  /**
   * Instantiate (or re-use) and populate a {@link ComputablePrimitiveFloatList} from a {@link ResolvingDecoder}.
   *
   * N.B.: the caller must ensure the data is of the appropriate type by calling {@link #isFloatArray(Schema)}.
   */
  public static Object readPrimitiveFloatArray(Object old, ResolvingDecoder in) throws IOException {
    long l = in.readArrayStart();
    if (l > 0) {
      ComputablePrimitiveFloatList array = (ComputablePrimitiveFloatList) newPrimitiveFloatArray(old, (int) l);
      do {
        for (long i = 0; i < l; i++) {
          array.addPrimitive(in.readFloat());
        }
      } while ((l = in.arrayNext()) > 0);
      return array;
    } else {
      return newPrimitiveFloatArray(old, 0);
    }
  }

  /**
   * @param expected {@link Schema} to inspect
   * @return true is the {@param expected} SCHEMA is of the right type to decode as a {@link ComputablePrimitiveFloatList}
   *         false otherwise
   */
  public static boolean isFloatArray(Schema expected) {
    return expected != null && Schema.Type.ARRAY.equals(expected.getType()) && FLOAT_SCHEMA.equals(expected.getElementType());
  }

  private static Object newPrimitiveFloatArray(Object old, int size) {
    if (old instanceof ComputablePrimitiveFloatList) {
      ((ComputablePrimitiveFloatList) old).clear();
      return old;
    } else {
      return new ComputablePrimitiveFloatList(size);
    }
  }

  public ComputablePrimitiveFloatList(int capacity) {
    if (capacity != 0)
      elements = new float[capacity];
  }

  public ComputablePrimitiveFloatList(Collection<Float> c) {
    if (c != null) {
      elements = new float[c.size()];
      addAll(c);
    }
  }

  public float squaredL2Norm() {
    if (l2normComputed) {
      return l2norm;
    }
    float l2norm = 0.0f;
    for (int i = 0; i < size(); i++) {
      l2norm += getPrimitive(i) * getPrimitive(i);
    }
    this.l2norm = l2norm;
    l2normComputed = true;
    return this.l2norm;
  }

  @Override
  public Schema getSchema() { return SCHEMA; }

  @Override
  public int size() { return size; }

  @Override
  public void clear() {
    l2normComputed = false;
    size = 0;
  }

  @Override
  public Iterator<Float> iterator() {
    return new Iterator<Float>() {
      private int position = 0;
      @Override
      public boolean hasNext() { return position < size; }
      @Override
      public Float next() { return elements[position++]; }
      @Override
      public void remove() { throw new UnsupportedOperationException(); }
    };
  }

  public float getPrimitive(int i) {
    if (i >= size)
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
    return elements[i];
  }

  @Override
  public Float get(int i) {
    return getPrimitive(i);
  }

  /**
   * Add a primitive float inside the list, without boxing.
   *
   * Note that this function does not clear the {@link #l2normComputed} flag. This state management
   * is expected to be handled (or skipped) in a correct manner by callers of this function. For this
   * reason, this function's visibility is private.
   */
  private boolean addPrimitive(float o) {
    if (size == elements.length) {
      float[] newElements = new float[(size * 3)/2 + 1];
      System.arraycopy(elements, 0, newElements, 0, size);
      elements = newElements;
    }
    elements[size++] = o;
    return true;
  }

  @Override
  public boolean add(Float o) {
    l2normComputed = false;
    return addPrimitive(o);
  }

  @Override
  public void add(int location, Float o) {
    l2normComputed = false;
    if (location > size || location < 0) {
      throw new IndexOutOfBoundsException("Index " + location + " out of bounds.");
    }
    if (size == elements.length) {
      float[] newElements = new float[(size * 3)/2 + 1];
      System.arraycopy(elements, 0, newElements, 0, size);
      elements = newElements;
    }
    System.arraycopy(elements, location, elements, location + 1, size - location);
    elements[location] = o;
    size++;
  }

  @Override
  public Float set(int i, Float o) {
    l2normComputed = false;
    if (i >= size)
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
    Float response = elements[i];
    elements[i] = o;

    return response;
  }

  @Override
  public Float remove(int i) {
    l2normComputed = false;
    if (i >= size)
      throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
    Float result = elements[i];
    --size;
    System.arraycopy(elements, i+1, elements, i, (size-i));
    elements[size] = 0;
    return result;
  }

  public float peekPrimitive() {
    return (size < elements.length) ? elements[size] : null;
  }

  @Override
  public Float peek() {
    return peekPrimitive();
  }

  @Override
  public int compareTo(GenericArray<Float> that) {
    if (that instanceof ComputablePrimitiveFloatList) {
      ComputablePrimitiveFloatList thatPrimitiveList = (ComputablePrimitiveFloatList) that;
      if (this.size == thatPrimitiveList.size) {
        for (int i = 0; i < this.size; i++) {
          int compare = Float.compare(this.elements[i], thatPrimitiveList.elements[i]);
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      } if (this.size > thatPrimitiveList.size) {
        return 1;
      } else {
        return -1;
      }
    } else {
      // Not our own type of primitive list, so we will delegate to the regular implementation, which will do boxing
      return GenericData.get().compare(this, that, this.getSchema());
    }
  }

  @Override
  public void reverse() {
    l2normComputed = false;
    int left = 0;
    int right = elements.length - 1;

    while (left < right) {
      float tmp = elements[left];
      elements[left] = elements[right];
      elements[right] = tmp;

      left++;
      right--;
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("[");
    int count = 0;
    for (Float e : this) {
      buffer.append(e==null ? "null" : e.toString());
      if (++count < size())
        buffer.append(", ");
    }
    buffer.append("]");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof GenericArray) {
      return compareTo((GenericArray) o) == 0;
    } else {
      return super.equals(o);
    }
  }

  @Override
  public int hashCode() {
    int hashCode = 1;
    for (int i = 0; i < this.size; i++) {
      hashCode = 31 * hashCode + Float.hashCode(elements[i]);
    }
    return hashCode;
  }
}