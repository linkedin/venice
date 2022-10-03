package com.linkedin.alpini.base.misc;

import java.lang.reflect.Field;
import java.util.function.BiConsumer;
import java.util.function.Function;


/**
 * Interface for a generic typed field accessor.
 *
 * @param <T> Containing class
 * @param <V> Class of contained field
 */
public interface TypedFieldAccessor<T, V> extends BiConsumer<T, V>, Function<T, V> {
  static <T, V> TypedFieldAccessor<T, V> forField(Class<? super T> clazz, String fieldName) {
    try {
      return forField(clazz.getDeclaredField(fieldName));
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new Error(e);
    }
  }

  static <T, V> TypedFieldAccessor<T, V> forField(Field field) throws NoSuchFieldException, IllegalAccessException {
    return TypedFieldAccessorHelper.instance().forField(field);
  }

}
