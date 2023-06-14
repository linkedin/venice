package com.linkedin.venice;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;


public class LogConfigurator {
  private static final String TMP_DIR = FileUtils.getTempDirectoryPath();
  private static final String LOG4J_PATH =
      Paths.get(TMP_DIR, "venice-shell-tool-log4j.properties").toAbsolutePath().toString();
  private static final String LOG4J2_PATH =
      Paths.get(TMP_DIR, "venice-shell-tool-log4j2.properties").toAbsolutePath().toString();

  public static void disableLog() throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG4J_PATH))) {
      writer.write("log4j.rootLogger=off");
    } catch (IOException e) {
      System.err.println("Could not write to file " + LOG4J_PATH + " Exception: " + e);
      throw e;
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG4J2_PATH))) {
      writer.write("rootLogger.level=off");
    } catch (IOException e) {
      System.err.println("Could not write to file " + LOG4J2_PATH + " Exception: " + e);
      throw e;
    }
    System.setProperty("log4j.configuration", LOG4J_PATH); // D2 is still using log4j. Configure log4j to disable d2
                                                           // logs
    System.setProperty("log4j2.configurationFile", LOG4J2_PATH);
  }
}
