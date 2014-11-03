package com.linkedin.venice.server;

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
public class VeniceConfig {

  private static final Logger logger = Logger.getLogger(VeniceConfig.class.getName());

  // Kafka related properties
  private int numKafkaPartitions;
  private String kafKaZookeeperUrl;
  private String kafkaBrokerUrl;
  private int kafkaBrokerPort;
  private List<String> brokerList;

  // Storage related properties
  private StorageType storageType;
  private int numStorageNodes;
  private int numStorageCopies;

  public VeniceConfig(Properties props) {
    numKafkaPartitions = Integer.parseInt(props.getProperty("kafka.number.partitions", "4"));
    kafKaZookeeperUrl = props.getProperty("kafka.zookeeper.url", "localhost:2181");
    kafkaBrokerUrl = props.getProperty("kafka.broker.url", "localhost:9092");
    kafkaBrokerPort = Integer.parseInt(props.getProperty("kafka.broker.port", "9092"));

    numStorageNodes = Integer.parseInt(props.getProperty("kafka.number.partitions", "4"));
    try {
      storageType = convertToStorageType(props.getProperty("storage.type", "memory"));
    } catch (Exception e) {
      e.printStackTrace();
    }
    numStorageNodes = Integer.parseInt(props.getProperty("storage.node.count", "3"));
    numStorageCopies = Integer.parseInt(props.getProperty("storage.node.replicas", "2"));

    validateParams();
  }

  private void validateParams() {
    if (numKafkaPartitions < 0) {
      throw new IllegalArgumentException("core.threads cannot be less than 1");
    } else if (kafKaZookeeperUrl.isEmpty()) {
      throw new IllegalArgumentException("kafkaZookeeperUrl can't be empty");
    } else if (kafkaBrokerUrl.isEmpty()) {
      throw new IllegalArgumentException("kafkaZookeeperUrl can't be empty");
    }
  }

  /**
   *  Initializes the Venice configuration from a given file input
   *  @param configFileName - The path to the input configuration file
   *  @throws Exception if inputs are of an illegal format
   * */
  public static VeniceConfig initializeFromFile(String configFileName)
      throws Exception {
    logger.info("Loading config: " + configFileName);
    Properties props = parseProperties(configFileName);
    return new VeniceConfig(props);
  }

  /**
   *  Given a filePath, reads into a Java Properties object
   *  @param configFileName - String path to a properties file
   *  @return A Java properties object with the given configurations
   * */
  public static Properties parseProperties(String configFileName)
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
    return props;
  }

  /**
   *  Given a String type, returns the enum type
   *  @param type - String name of a storage type
   *  @return The enum equivalent of the given type
   * */
  private static StorageType convertToStorageType(String type)
      throws Exception {
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

  public int getNumKafkaPartitions() {
    return numKafkaPartitions;
  }

  public void setNumKafkaPartitions(int numKafkaPartitions) {
    this.numKafkaPartitions = numKafkaPartitions;
  }

  public String getKafKaZookeeperUrl() {
    return kafKaZookeeperUrl;
  }

  public void setKafKaZookeeperUrl(String kafKaZookeeperUrl) {
    this.kafKaZookeeperUrl = kafKaZookeeperUrl;
  }

  public String getKafkaBrokerUrl() {
    return kafkaBrokerUrl;
  }

  public void setKafkaBrokerUrl(String kafkaBrokerUrl) {
    this.kafkaBrokerUrl = kafkaBrokerUrl;
  }

  public int getKafkaBrokerPort() {
    return kafkaBrokerPort;
  }

  public void setKafkaBrokerPort(int kafkaBrokerPort) {
    this.kafkaBrokerPort = kafkaBrokerPort;
  }

  public List<String> getBrokerList() {
    return brokerList;
  }

  public void setBrokerList(List<String> brokerList) {
    this.brokerList = brokerList;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public void setStorageType(StorageType storageType) {
    this.storageType = storageType;
  }

  public int getNumStorageNodes() {
    return numStorageNodes;
  }

  public void setNumStorageNodes(int numStorageNodes) {
    this.numStorageNodes = numStorageNodes;
  }

  public int getNumStorageCopies() {
    return numStorageCopies;
  }

  public void setNumStorageCopies(int numStorageCopies) {
    this.numStorageCopies = numStorageCopies;
  }
}
