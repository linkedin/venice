package com.linkedin.alpini.base.misc;

import java.lang.reflect.Field;


/**
 * Helper class for the {@link SlowFieldAccessor} implementation.
 */
public final class SlowFieldAccessorHelper extends TypedFieldAccessorHelper {
  @Override
  public <V, T> TypedFieldAccessor<T, V> forField(Field field) throws NoSuchFieldException, IllegalAccessException {
    return SlowFieldAccessor.forField(field);
  }
}
