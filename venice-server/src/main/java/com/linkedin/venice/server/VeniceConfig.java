package com.linkedin.venice.server;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.storage.StorageType;

import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Map;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;


/**
 * Class which reads configuration parameters for Venice.
 * Inputs may come from file or another service.
 */
public class VeniceConfig {

  private static final Logger logger = Logger.getLogger(VeniceConfig.class.getName());

  public static final String VENICE_HOME_VAR_NAME = "VENICE_HOME";
  public static final String VENICE_CONFIG_DIR = "VENICE_CONFIG_DIR";
  public final static String CONFIG_FILE_NAME = "config.properties";
  public final static String STORE_DEFINITIONS_DIR_NAME = "STORES";

  private Map<String, String> storageEngineFactoryClassNameMap;

  private String CONFIG_DIR_ABSOLUTE_PATH;

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

    //initialize StorageEngineFactory types
    storageEngineFactoryClassNameMap = ImmutableMap
        .of("inMemory", InMemoryStorageEngineFactory.class.getName(), "bdb", BdbStorageEngineFactory.class.getName());

    validateParams();
  }

  public void setConfigDirAbsolutePath(String configDirPath) {
    this.CONFIG_DIR_ABSOLUTE_PATH = configDirPath;
  }

  public String getConfigDirAbsolutePath() {
    return CONFIG_DIR_ABSOLUTE_PATH;
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
   * Initializes the Venice configuration from known environment variables
   *
   * @return VeniceConfig object
   * @throws Exception
   */
  public static VeniceConfig loadFromEnvironmentVariable()
      throws Exception {
    String veniceHome = System.getenv(VeniceConfig.VENICE_HOME_VAR_NAME);
    if (veniceHome == null) {
      // TODO throw appropriate exception
      throw new Exception(
          "No environment variable " + VeniceConfig.VENICE_HOME_VAR_NAME + " has been defined, set it!");
    }
    String veniceConfigDir = System.getenv(VeniceConfig.VENICE_CONFIG_DIR);
    if (veniceConfigDir == null) {
      // TODO throw appropriate exception
      throw new Exception("No environment variable " + VeniceConfig.VENICE_CONFIG_DIR + "  has been defined, set it!");
    } else {
      if (!Utils.isReadableDir(veniceConfigDir)) {
        throw new Exception("Attempt to load configuration from VENICE_CONFIG_DIR, " + veniceConfigDir
            + " failed. That is not a readable directory.");
      }
    }
    return loadFromVeniceHome(veniceHome, veniceConfigDir);
  }

  /**
   * Initializes the Venice configuration given venice home dir path
   *
   * @param veniceHome absolute path to Venice Home directory
   * @return VeniceConfig object
   * @throws Exception
   */
  public static VeniceConfig loadFromVeniceHome(String veniceHome)
      throws Exception {
    String veniceConfigDir = veniceHome + File.separator + "config";
    return loadFromVeniceHome(veniceHome, veniceConfigDir);
  }

  /**
   * Initializes the Venice configuration given venice home dir path and config dir path
   *
   * @param veniceHome  absolute path to Venice Home directory
   * @param veniceConfigDir absolute path to venice config directory
   * @return VeniceConfig object
   * @throws Exception
   */
  public static VeniceConfig loadFromVeniceHome(String veniceHome, String veniceConfigDir)
      throws Exception {
    if (!Utils.isReadableDir(veniceHome)) {
      // TODO throw appropriate exception
      throw new Exception("Attempt to load configuration from VENICE_HOME, " + veniceHome
          + " failed. That is not a readable directory.");
    }
    if (veniceConfigDir == null) {
      veniceConfigDir = veniceHome + File.separator + "config";
    }
    String propertiesFile = veniceConfigDir + File.separator + CONFIG_FILE_NAME;
    if (!Utils.isReadableFile(propertiesFile)) {
      // TODO throw appropriate exception
      throw new Exception(propertiesFile + " is not a readable configuration file.");
    }
    Properties props = Utils.parseProperties(propertiesFile);
    VeniceConfig veniceConfig = new VeniceConfig(props);
    //set the absolute config directory path
    veniceConfig.setConfigDirAbsolutePath(new File(veniceConfigDir).getAbsolutePath());
    return veniceConfig;
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

  public Map<String, String> getAllStorageEngineFactoryClassNameMap() {
    return this.storageEngineFactoryClassNameMap;
  }

  public String getStorageEngineFactoryClassName(String persistenceType) {
    return this.storageEngineFactoryClassNameMap.get(persistenceType);
  }

  public void setStorageEngineFactoryClassNameMap(Map storageEngineFactoryClassNameMap) {
    this.storageEngineFactoryClassNameMap = storageEngineFactoryClassNameMap;
  }
}
