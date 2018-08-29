package com.linkedin.venice.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


/**
 * Utilities for reflection
 *
 * TODO This class may not be needed if we decide to proceed with Guice library
 * for reflections and need to be discarded then
 */
public class ReflectUtils {

  /**
   * Load the given class using the default constructor
   *
   * @param className The name of the class
   * @return The class object
   */
  public static <T> Class<T> loadClass(String className) {
    try {
      return (Class<T>) Class.forName(className);
    } catch (ClassNotFoundException | ClassCastException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Call the class constructor with the given arguments
   *
   * @param c The class
   * @param argTypes The type of each argument
   * @param args The arguments
   * @param <T>  Type of the class
   * @return The constructed object
   */
  public static <T> T callConstructor(Class<T> c, Class<?>[] argTypes, Object[] args) {
    try {
      Constructor<T> cons = c.getConstructor(argTypes);
      return cons.newInstance(args);
    } catch (InvocationTargetException e) {
      throw getCause(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Get the root cause of the Exception
   *
   * @param e  The Exception
   * @return The root cause of the Exception
   */
  private static RuntimeException getCause(InvocationTargetException e) {
    Throwable cause = e.getCause();
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else {
      throw new IllegalArgumentException(e.getCause());
    }
  }
}
