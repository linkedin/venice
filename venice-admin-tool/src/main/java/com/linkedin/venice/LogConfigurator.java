package com.linkedin.venice;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;


public class LogConfigurator {
  private static final String tmpDir = FileUtils.getTempDirectoryPath();
  private static final String log4jPath =
      Paths.get(tmpDir, "venice-admin-tool-log4j.properties").toAbsolutePath().toString();
  private static final String log4j2Path =
      Paths.get(tmpDir, "venice-admin-tool-log4j2.properties").toAbsolutePath().toString();

  static void disableLog() throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(log4jPath))) {
      writer.write("log4j.rootLogger=off");
    } catch (IOException e) {
      System.err.println("Could not write to file " + log4jPath + " Exception: " + e);
      throw e;
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(log4j2Path))) {
      writer.write("rootLogger.level=off");
    } catch (IOException e) {
      System.err.println("Could not write to file " + log4j2Path + " Exception: " + e);
      throw e;
    }
    System.setProperty("log4j.configuration", log4jPath); // D2 is still using log4j. Configure log4j to disable d2 logs
    System.setProperty("log4j2.configurationFile", log4j2Path);
  }
}
