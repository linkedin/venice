package com.linkedin.venice.config;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;


/**
 * Class which reads configuration parameters for Venice.
 * Inputs may come from file or another service.
 */
public class GlobalConfiguration {

  private static final Logger logger = Logger.getLogger(GlobalConfiguration.class.getName()); // log4j logger

  // kafka related configs
  private static String kafkaBrokerUrl;

  /* Cannot instantiate object */
  private GlobalConfiguration() {
  }

  /**
   *  Given a filePath, reads into a Java Properties object
   *  @param configFileName - String path to a properties file
   *  @return A Java properties object with the given configurations
   * */
  public static Properties parseProperties(String configFileName)
      throws Exception {
    Properties prop = new Properties();
    FileInputStream inputStream = null;

    try {
      inputStream = new FileInputStream(configFileName);
      prop.load(inputStream);
    } finally {
      // safely close input stream
      if (inputStream != null) {
        inputStream.close();
      }
    }
    return prop;
  }

  /**
   *  Initializes the Venice configuration from a given file input
   *  @param configFileName - The path to the input configuration file
   *  @throws Exception if inputs are of an illegal format
   * */
  public static void initializeFromFile(String configFileName)
      throws Exception {

    logger.info("Loading config: " + configFileName);

    Properties prop = parseProperties(configFileName);

    kafkaBrokerUrl = prop.getProperty("kafka.broker.url", "localhost:9092");

    logger.info("Finished initialization from file");
  }

  public static String getKafkaBrokerUrl() {
    return kafkaBrokerUrl;
  }
}
