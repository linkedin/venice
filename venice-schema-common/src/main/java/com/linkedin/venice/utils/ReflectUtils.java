package com.linkedin.venice.utils;

import com.google.common.reflect.ClassPath;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * Utilities for reflection
 *
 * TODO This class may not be needed if we decide to proceed with Guice library
 * for reflections and need to be discarded then
 */
public class ReflectUtils {
  private static final Logger LOGGER = Logger.getLogger(ReflectUtils.class);

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
    ClassLoader cl = ClassLoader.getSystemClassLoader();

    URL[] urls = ((URLClassLoader)cl).getURLs();

    LOGGER.info("Classpath:");
    for(URL url: urls){
      LOGGER.info(url.getFile());
    }
  }

  /**
   * Given an exception about a class that doesn't have the expected API,
   * print to the logs which jar is that class coming from.
   */
  public static void printJarContainingBadClass(NoSuchMethodError e) {
    String rawMessage =  e.getMessage();
    int methodBoundary = rawMessage.lastIndexOf('.');
    if (methodBoundary == -1) {
      throw new IllegalArgumentException("Unexpected exception message format. Could not find any dot.", e);
    }
    String className = rawMessage.substring(0, methodBoundary);
    Class klass = loadClass(className);
    LOGGER.info("Class '" + klass.getSimpleName() + "' is loaded from: " +
        klass.getProtectionDomain().getCodeSource().getLocation().toString());
  }

  /**
   * Get all subclasses of given class. Subclasses are assumed to be in the same package as base class.
   */
  public static List<String> getSubclassNames(Class<?> klass) {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    final ClassPath classPath;
    try {
      classPath = ClassPath.from(cl);
    } catch (IOException e) {
      throw new VeniceException("Failed to instantiate ClassPath");
    }
    List<String> classNames = classPath
        .getTopLevelClasses(klass.getPackage().getName())
        .stream()
        .filter(classInfo -> {
          // skip base class itself
          if (classInfo.getName().equals(klass.getName())) {
            return false;
          }
          Class<?> subclass = loadClass(classInfo.getName());
          return klass.isAssignableFrom(subclass);
        })
        .map(ClassPath.ClassInfo::getName)
        .collect(Collectors.toList());
    return classNames;
  }
}
