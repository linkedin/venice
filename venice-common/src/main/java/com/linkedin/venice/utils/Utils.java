package com.linkedin.venice.utils;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


/**
 * Helper functions
 */
public class Utils {
  /**
   * Print an error and exit with error code 1
   *
   * @param message The error to print
   */
  public static void croak(String message) {
    System.err.println(message);
    System.exit(1);
  }

  /**
   * Print an error and exit with the given error code
   *
   * @param message The error to print
   * @param errorCode The error code to exit with
   */
  public static void croak(String message, int errorCode) {
    System.err.println(message);
    System.exit(errorCode);
  }

  /**
   * A reversed copy of the given list
   *
   * @param <T> The type of the items in the list
   * @param l The list to reverse
   * @return The list, reversed
   */
  public static <T> List<T> reversed(List<T> l) {
    List<T> copy = new ArrayList<T>(l);
    Collections.reverse(copy);
    return copy;
  }

  /**
   * Throw an IllegalArgumentException if the argument is null, otherwise just
   * return the argument.
   *
   * @param t The thing to check for nullness.
   * @param message The message to put in the exception if it is null
   * @param <T> The type of the thing
   * @return t
   */
  public static <T> T notNull(T t, String message) {
    if (t == null) {
      throw new IllegalArgumentException(message);
    }
    return t;
  }

  /**
   * Throw an IllegalArgumentException if the argument is null, otherwise just
   * return the argument.
   *
   * Useful for assignment as in this.thing = Utils.notNull(thing);
   *
   * @param t  The thing to check for nullness.
   * @param <T>  The type of the thing
   * @return t
   */
  public static <T> T notNull(T t) {
    if (t == null) {
      throw new IllegalArgumentException("This object MUST be non-null.");
    }
    return t;
  }

  /**
   *  Given a filePath, reads into a Venice Props object
   *  @param configFileName - String path to a properties file
   *  @return A @Props object with the given configurations
   * */
  public static Props parseProperties(String configFileName)
      throws Exception {
    Properties props = new Properties();
    FileInputStream inputStream = null;
    try {
      inputStream = new FileInputStream(configFileName);
      props.load(inputStream);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    return new Props(props);
  }

  /**
   * Given a .property file, reads into a Venice Props object
   * @param propertyFile The .property file
   * @return A @Props object with the given properties
   * @throws Exception  if File not found or not accessible
   */
  public static Props parseProperties(File propertyFile)
      throws Exception {
    Properties props = new Properties();
    FileInputStream inputStream = null;
    try {
      inputStream = new FileInputStream(propertyFile);
      props.load(inputStream);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    return new Props(props);
  }

  /**
   * Check if a directory exists and is readable
   *
   * @param d  The directory
   * @return true iff the argument is a readable directory
   */
  public static boolean isReadableDir(File d) {
    return d.exists() && d.isDirectory() && d.canRead();
  }

  /**
   * Check if a directory exists and is readable
   *
   * @param dirName The directory name
   * @return true iff the argument is the name of a readable directory
   */
  public static boolean isReadableDir(String dirName) {
    return isReadableDir(new File(dirName));
  }

  /**
   * Check if a file exists and is readable
   *
   * @param fileName
   * @return true iff the argument is the name of a readable file
   */
  public static boolean isReadableFile(String fileName) {
    return isReadableFile(new File(fileName));
  }

  /**
   * Check if a file exists and is readable
   * @param f The file
   * @return true iff the argument is a readable file
   */
  public static boolean isReadableFile(File f) {
    return f.exists() && f.isFile() && f.canRead();
  }
}
