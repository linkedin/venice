package com.linkedin.alpini.base.misc;

import java.lang.reflect.Field;


/**
 * Helper class to determine at run time which implementation to use.
 */
public abstract class TypedFieldAccessorHelper {
  private static TypedFieldAccessorHelper _instance;

  static TypedFieldAccessorHelper instance() {
    if (_instance == null) {
      try {
        _instance = Class.forName("com.linkedin.alpini.lnkd.misc.GenericFieldAccessorHelper")
            .asSubclass(TypedFieldAccessorHelper.class)
            .newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
        try {
          _instance = Class.forName("com.linkedin.alpini.base.misc.SlowFieldAccessorHelper")
              .asSubclass(TypedFieldAccessorHelper.class)
              .newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
          throw new Error(e);
        }
      }
    }
    return _instance;
  }

  public abstract <V, T> TypedFieldAccessor<T, V> forField(Field field)
      throws NoSuchFieldException, IllegalAccessException;
}
