package com.linkedin.alpini.base.misc;

import java.lang.reflect.Field;


/**
 * Utilities for Classes / reflection.
 */
public final class ClassUtil {
  private ClassUtil() {
  }

  public static Field getAccessibleField(Class<?> clazz, String fieldName) {
    try {
      return getAccessibleField(clazz.getDeclaredField(fieldName));
    } catch (NoSuchFieldException ex) {
      throw new Error(ex);
    }
  }

  public static Field getAccessibleField(Field field) {
    field.setAccessible(true);
    return field;
  }
}
