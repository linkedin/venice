package com.linkedin.venice.config;

import com.linkedin.venice.storage.StorageType;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Class which reads configuration parameters for Venice.
 * Inputs may come from file or another service.
 */
public class GlobalConfiguration {

  static final Logger logger = Logger.getLogger(GlobalConfiguration.class.getName());

  // server related configs
  private static StorageType storageType;
  private static int numStorageNodes;
  private static int numStorageCopies;

  // kafka related configs
  private static String kafKaZookeeperUrl;
  private static String kafkaBrokerUrl;
  private static int kafkaBrokerPort;
  private static int numThreadsPerPartition;
  private static int numKafkaPartitions;
  private static List<String> brokerList;

  // kafka consumer tuning
  private static int kafkaConsumerNumRetries = 3;
  private static int kafkaConsumerTimeout = 100000;
  private static int kafkaConsumerMaxFetchSize = 100000;
  private static int kafkaConsumerBufferSize = 64 * 1024;

  /**
   *  Initializes the Venice configuration from a given file input
   *  @param configFileName - The path to the input configuration file
   *  @throws Exception if inputs are of an illegal format
   * */
  public static void initializeFromFile(String configFileName) throws Exception {

    logger.info("Loading config: " + configFileName);

    Properties prop = parseProperties(configFileName);

    // Get kafka replication configs
    numKafkaPartitions = Integer.parseInt(prop.getProperty("kafka.number.partitions", "4"));
    numThreadsPerPartition = Integer.parseInt(prop.getProperty("kafka.threads.per.partition", "1"));

    kafKaZookeeperUrl = prop.getProperty("kafka.zookeeper.url", "localhost:2181");
    kafkaBrokerUrl = prop.getProperty("kafka.broker.url", "localhost:9092");

    // split the kafka URL
    String[] kafkaUrlSplits = kafkaBrokerUrl.split(":");

    // Retrieve the host and post of the URL
    if (kafkaUrlSplits.length == 2) {
      brokerList = Arrays.asList(kafkaBrokerUrl.split(":")[0]);
      kafkaBrokerPort = Integer.parseInt(kafkaBrokerUrl.split(":")[1]);
    } else {
      throw new Exception("Specified kafka URL is of an illegal format: " + kafkaBrokerUrl);
    }

    storageType = convertToStorageType(prop.getProperty("storage.type", "memory"));

    numStorageNodes = Integer.parseInt(prop.getProperty("storage.node.count", "3"));
    numStorageCopies = Integer.parseInt(prop.getProperty("storage.node.replicas", "2"));

    logger.info("Finished initialization from file");

  }

  /**
   *  Given a filePath, reads into a Java Properties object
   *  @param configFileName - String path to a properties file
   *  @return A Java properties object with the given configurations
   * */
  public static Properties parseProperties(String configFileName) throws Exception {

    Properties prop = new Properties();
    FileInputStream inputStream = null;

    try {

      inputStream = new FileInputStream(configFileName);
      prop.load(inputStream);

      if (null == inputStream) {
        throw new Exception("Config File " + configFileName + " cannot be found.");
      }

    } finally {

      // safely close input stream
      if (inputStream != null)
        inputStream.close();

    }

    return prop;

  }

  /**
   *  Given a String type, returns the enum type
   *  @param type - String name of a storage type
   *  @return The enum equivalent of the given type
   * */
  private static StorageType convertToStorageType(String type) throws Exception {

    StorageType returnType;

    switch (type) {

      case "memory":
        returnType = StorageType.MEMORY;
        break;

      case "bdb":
        returnType = StorageType.BDB;
        break;

      default:
        throw new Exception("Bad configuration file given!");

    }

    return returnType;

  }

  public static String getKafkaBrokerUrl() {
    return kafkaBrokerUrl;
  }

  public static String getZookeeperURL() {
    return kafKaZookeeperUrl;
  }

  public static int getNumThreadsPerPartition() {
    return numThreadsPerPartition;
  }

  public static int getNumKafkaPartitions() {
    return numKafkaPartitions;
  }

  public static StorageType getStorageType() {
    return storageType;
  }

  public static int getNumStorageCopies() {
    return numStorageCopies;
  }

  public static List<String> getBrokerList() {
    return brokerList;
  }

  public static int getKafkaBrokerPort() {
    return kafkaBrokerPort;
  }

  public static int getNumStorageNodes() {
    return numStorageNodes;
  }

  public static int getKafkaConsumerBufferSize() {
    return kafkaConsumerBufferSize;
  }

  public static int getKafkaConsumerMaxFetchSize() {
    return kafkaConsumerMaxFetchSize;
  }

  public static int getKafkaConsumerTimeout() {
    return kafkaConsumerTimeout;
  }

  public static int getKafkaConsumerNumRetries() {
    return kafkaConsumerNumRetries;
  }

}
