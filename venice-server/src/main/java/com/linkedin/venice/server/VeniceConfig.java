package com.linkedin.venice.server;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.partition.ModuloPartitionNodeAssignmentScheme;

import com.linkedin.venice.store.memory.InMemoryStorageEngineFactory;
import com.linkedin.venice.utils.ConfigurationException;
import com.linkedin.venice.utils.UndefinedPropertyException;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Map;

import java.util.List;
import java.util.Properties;


/**
 * Class which reads configuration parameters for Venice.
 * Inputs may come from file or another service.
 */
public class VeniceConfig {

  public static final String VENICE_HOME_VAR_NAME = "VENICE_HOME";
  public static final String VENICE_CONFIG_DIR = "VENICE_CONFIG_DIR";
  public static final String CONFIG_FILE_NAME = "config.properties";
  public static final String STORE_CONFIGS_DIR_NAME = "STORES";
  private static final String VENICE_NODE_ID_VAR_NAME = "VENICE_NODE_ID";
  private Map<String, String> storageEngineFactoryClassNameMap;
  private Map<String, String> partitionNodeAssignmentSchemeClassMap;
  private String CONFIG_DIR_ABSOLUTE_PATH;
  private int nodeId;
  private String partitionNodeAssignmentSchemeName;
  // Kafka related properties
  private int numKafkaPartitions;
  private String kafKaZookeeperUrl;
  private String kafkaBrokerUrl;
  private int kafkaBrokerPort;
  private List<String> brokerList;
  private int numKafkaConsumerThreads;
  // Storage related properties
  private String storageType;
  private int numStorageNodes;
  private int numStorageCopies;

  public VeniceConfig(Properties props) {
    try {
      this.nodeId = Integer.parseInt(props.getProperty("node.id"));
    } catch (UndefinedPropertyException e) {
      this.nodeId = getIntEnvVariable(VENICE_NODE_ID_VAR_NAME);
    }
    numKafkaPartitions = Integer.parseInt(props.getProperty("kafka.number.partitions", "4"));
    kafKaZookeeperUrl = props.getProperty("kafka.zookeeper.url", "localhost:2181");
    kafkaBrokerUrl = props.getProperty("kafka.broker.url", "localhost:9092");
    kafkaBrokerPort = Integer.parseInt(props.getProperty("kafka.broker.port", "9092"));
    numKafkaConsumerThreads = Integer.parseInt(props.getProperty("kafka.number.consumer.threads",
        "50"));   //TODO This variable and default value needs to be set to an appropriate value later
    try {
      storageType = props.getProperty("storage.type", "memory");
    } catch (Exception e) {
      e.printStackTrace();
    }
    numStorageNodes = Integer.parseInt(props.getProperty("storage.node.count", "3"));
    numStorageCopies = Integer.parseInt(props.getProperty("storage.node.replicas", "2"));

    //initialize StorageEngineFactory types
    /* TODO once we have BDB implementation, add BDB type to  storageEngineFactoryClassNameMap like shown below
        storageEngineFactoryClassNameMap = ImmutableMap
        .of("inMemory", InMemoryStorageEngineFactory.class.getName(), "bdb", BdbStorageEngineFactory.class.getName());
    */
    storageEngineFactoryClassNameMap = ImmutableMap.of("inMemory", InMemoryStorageEngineFactory.class.getName());
    partitionNodeAssignmentSchemeClassMap =
        ImmutableMap.of("modulo", ModuloPartitionNodeAssignmentScheme.class.getName());
    partitionNodeAssignmentSchemeName = props.getProperty("partition.node.assignment.scheme", "modulo");
    validateParams();
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

  private int getIntEnvVariable(String name) {
    String var = System.getenv(name);
    if (var == null) {
      throw new ConfigurationException("The environment variable " + name + " is not defined.");
    }
    try {
      return Integer.parseInt(var);
    } catch (NumberFormatException e) {
      throw new ConfigurationException("Invalid format for environment variable " + name + ", expecting an integer.",
          e);
    }
  }

  public String getConfigDirAbsolutePath() {
    return CONFIG_DIR_ABSOLUTE_PATH;
  }

  public void setConfigDirAbsolutePath(String configDirPath) {
    this.CONFIG_DIR_ABSOLUTE_PATH = configDirPath;
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

  public int getKafkaConsumerThreads() {
    return this.numKafkaConsumerThreads;
  }

  public void setKafkaConsumerThreads(int numKafkaConsumerThreads) {
    this.numKafkaConsumerThreads = numKafkaConsumerThreads;
  }

  public String getStorageType() {
    return storageType;
  }

  public void setStorageType(String storageType) {
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

  public Map<String, String> getAllPartitionNodeAssignmentSchemeClassMap() {
    return this.partitionNodeAssignmentSchemeClassMap;
  }

  public String getPartitionNodeAssignmentSchemeClassMap(String assignmentSchemeName) {
    return this.partitionNodeAssignmentSchemeClassMap.get(assignmentSchemeName);
  }

  public void setPartitionNodeAssignmentSchemeClassMap(Map partitionNodeAssignmentSchemeClassMap) {
    this.partitionNodeAssignmentSchemeClassMap = partitionNodeAssignmentSchemeClassMap;
  }

  public int getNodeId() {
    return nodeId;
  }

  /**
   * Id of the server(a.k.a. node) within the cluster
   * @param nodeId
   */
  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public String getPartitionNodeAssignmentSchemeName() {
    return this.partitionNodeAssignmentSchemeName;
  }
}
