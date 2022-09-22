package com.linkedin.alpini.base.misc;

import java.lang.reflect.Field;


/**
 * Field accessor using Reflection. This is slow but does not use the faster Unsafe mechanism which is no longer
 * permitted in Java.
 *
 * @param <T> Containing class
 * @param <V> Class of contained field
 */
public abstract class SlowFieldAccessor<T, V> implements TypedFieldAccessor<T, V> {
  public static <T, V> SlowFieldAccessor<T, V> forField(Field field) {
    field.setAccessible(true);
    if (field.getType().isPrimitive()) {
      switch (field.getType().getName()) {
        case "int":
          return new SlowFieldAccessor<T, V>() {
            @Override
            public void accept(T t, V v) {
              try {
                field.setInt(t, ((Integer) v));
              } catch (IllegalAccessException e) {
                throw new Error(e);
              }
            }

            @Override
            public V apply(T t) {
              try {
                // noinspection unchecked
                return (V) (Object) field.getInt(t);
              } catch (IllegalAccessException e) {
                throw new Error(e);
              }
            }
          };
        default:
          throw new UnsupportedOperationException();
      }
    }
    return new SlowFieldAccessor<T, V>() {
      @Override
      public void accept(T t, V v) {
        try {
          field.set(t, v);
        } catch (IllegalAccessException e) {
          throw new Error(e);
        }
      }

      @Override
      public V apply(T t) {
        try {
          // noinspection unchecked
          return (V) field.get(t);
        } catch (IllegalAccessException e) {
          throw new Error(e);
        }
      }
    };
  }
}
