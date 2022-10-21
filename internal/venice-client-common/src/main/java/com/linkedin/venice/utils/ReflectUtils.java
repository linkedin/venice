package com.linkedin.venice.utils;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utilities for reflection
 *
 * TODO This class may not be needed if we decide to proceed with Guice library
 * for reflections and need to be discarded then
 */
public class ReflectUtils {
  private static final Logger LOGGER = LogManager.getLogger(ReflectUtils.class);

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

  /**
   * Print to the logs the entire classpath (one line per jar)
   */
  public static void printClasspath() {
    try (ScanResult scanResult = new ClassGraph().scan()) {
      for (File file: scanResult.getClasspathFiles()) {
        LOGGER.info(file.getAbsolutePath());
      }
    }
  }

  /**
   * Given an exception about a class that doesn't have the expected API,
   * print to the logs which jar is that class coming from.
   */
  public static void printJarContainingBadClass(NoSuchMethodError e) {
    String rawMessage = e.getMessage();
    int methodBoundary = rawMessage.lastIndexOf('(');
    if (methodBoundary == -1) {
      throw new IllegalArgumentException(
          "Unexpected exception message format. Could not find any opening parenthesis '('.",
          e);
    }
    String classAndMethodName = rawMessage.substring(0, methodBoundary);
    int classStartBoundary = classAndMethodName.indexOf(' ');
    if (classStartBoundary == -1) {
      throw new IllegalArgumentException(
          "Unexpected exception message format. Could not find any ' ' between return type and function signature.",
          e);
    }
    int classEndBoundary = classAndMethodName.lastIndexOf('.');
    if (classEndBoundary == -1) {
      throw new IllegalArgumentException(
          "Unexpected exception message format. Could not split class name from method name.",
          e);
    }
    String className = classAndMethodName.substring(classStartBoundary + 1, classEndBoundary);
    Class klass = loadClass(className);
    LOGGER.info(
        "Class '{}' is loaded from: {}",
        klass.getSimpleName(),
        klass.getProtectionDomain().getCodeSource().getLocation());
  }
}
