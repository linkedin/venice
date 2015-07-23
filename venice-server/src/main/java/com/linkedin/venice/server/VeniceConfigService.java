package com.linkedin.venice.server;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.utils.Props;
import com.linkedin.venice.utils.Utils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Class that manages configs for Venice server side.
 * <p/>
 * Is responsible for parsing and initializing configs for cluster, server and individual stores
 * <p/>
 * <p/>
 * Config Management Design:
 * This class and the rest of the config classes does not have setters for now.
 * <p/>
 * Physical layout of the properties:
 * config
 * |
 * | _ _ _ cluster.properties
 * | _ _ _ server.properties
 * | _ _ _ STORES
 *         | _ _ _ <store1>.properties
 *         | _ _ _ <stores>.properties
 *         | _ _ _  ...
 * <p/>
 * Classes responsible for different configs
 * VeniceClusterConfig - will contain configs specific to Venice cluster
 * VeniceServerConfig - will contain configs specific to Venice server
 * VeniceStoreConfig - will contain all other properties including store configs.
 * <p/>
 * Hierarchy of configs:
 * First properties from cluster.properties are taken. Then properties from server.properties are applied. Finally
 * properties from individual stores are applied.The idea behind is to be able to override a config at any level, unless
 * it is a non-overridable config specific to server/cluster.
 * <p/>
 * Example of what cannot be done!
 * 1. one cannot override cluster name or node id accidentally or intentionally from any of the property files
 * <p/>
 * Examples of what can be done!
 * 1. Override a database configuration at store level by defining that property in <store-name>.property. So a specific
 *    store - <store-name> can have different database configurations than all other stores in the server.
 * 2. Define kafka broker urls in cluster.properties to have common kafka broker urls for all stores in the cluster.
 * 3. Define specific broker urls in <store-name>.properties to be able to consume from different kafka brokers for
 *    that particular store - <store-name>
 * <p/>
 * VeniceConfigService Responsibility:
 * 1. parses all property files and validates that non-overridable properties are not actually overriden.
 * 2. Provide APIs for accessing cluster, server and store level configs
 * <p/>
 * <p/>
 * <p/>
 * FIXME Current design has some gap. Ideally we would like to have a config management such that:
 * 1. clusters have specific mandatory properties that cannot be overriden at any level.
 * 2. Servers have specific mandatory properties that cannot be overriden at any level
 * 3. Stores have mandatory properties like store name, etc.
 * 4. There are certain general properties that can be defined common to a cluster but <b>can be overriden at store
 *    level</b> but <b> cannot be overriden at server level</b>. For example:
 * 1. kafka broker urls can be defined at cluster level. If none of the stores override this, it means a single
 *    Venice cluster can consume from a single Kafka cluster. However if we wish to consume from different kafka
 *    clusters, then the individual store configs can override this property. TODO This also mandates the need to
 *    validate that all nodes in the cluster have same configs for a single store. We do not want to end up in a
 *    situation where one node consumes from one kafka cluster and another from another kafka cluster for the same
 *    store. Ideally this validation should be taken care by the admin service which adds/updates/deletes store
 *    configs. Manual file editing should never be done.
 * 5. There are certain properties that can be defined at store level and can be overriden at store level. For example:
 *    1. ???
 */
public class VeniceConfigService {
  private static final Logger logger = Logger.getLogger(VeniceConfigService.class.getName());

  public static final String VENICE_CONFIG_DIR = "VENICE_CONFIG_DIR";
  public static final String VENICE_CLUSTER_PROPERTIES_FILE = "cluster.properties";
  public static final String VENICE_SERVER_PROPERTIES_FILE = "server.properties";
  public static final String STORE_CONFIGS_DIR_NAME = "STORES";

  // cluster specific properties
  public static final String CLUSTER_NAME = "cluster.name";
  public static final String STORAGE_NODE_COUNT = "storage.node.count";
  public static final String DATA_BASE_PATH = "data.base.path";
  public static final String PARTITION_NODE_ASSIGNMENT_SCHEME = "partition.node.assignment.scheme";
  public static final String ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT = "enable.kafka.consumers.offset.management";
  public static final String OFFSET_MANAGER_TYPE = "offset.manager.type";
  public static final String OFFSET_DATA_BASE_PATH = "offsets.data.base.path";
  public static final String OFFSET_MANAGER_FLUSH_INTERVAL_MS = "offset.manager.flush.interval.ms";
  public static final String ENABLE_CONSUMPTION_ACKS_FOR_AZKABAN_JOBS = "enable.consumption.acks.for.azkaban.jobs";
  public static final String KAFKA_CONSUMPTION_ACKS_BROKER_URL = "kafka.consumptions.acks.broker.url";
  public static final String HELIX_ENABLED = "helix.enabled";
  public static final String ZOOKEEPER_ADDRESS = "zookeeper.address";
  public static final Set<String> clusterSpecificProperties = new HashSet<String>(Arrays
      .asList(CLUSTER_NAME, STORAGE_NODE_COUNT, PARTITION_NODE_ASSIGNMENT_SCHEME,
          ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT, OFFSET_MANAGER_TYPE, OFFSET_DATA_BASE_PATH,
          OFFSET_MANAGER_FLUSH_INTERVAL_MS, ENABLE_CONSUMPTION_ACKS_FOR_AZKABAN_JOBS, KAFKA_CONSUMPTION_ACKS_BROKER_URL,
          HELIX_ENABLED, ZOOKEEPER_ADDRESS));

  // server specific properties
  public static final String NODE_ID = "node.id";
  public static final Set<String> serverSpecificProperties = new HashSet<String>(Arrays.asList(NODE_ID));

  // store specific properties
  public static final String STORE_NAME = "store.name";
  public static final String PERSISTENCE_TYPE = "persistence.type";
  public static final String STORAGE_REPLICATION_FACTOR = "storage.node.replicas";
  public static final String NUMBER_OF_KAFKA_PARTITIONS = "kafka.number.partitions";
  public static final String KAFKA_ZOOKEEPER_URL = "kafka.zookeeper.url";
  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_BROKER_PORT = "kafka.broker.port";
  public static final String KAFKA_CONSUMER_FETCH_BUFFER_SIZE = "kafka.consumer.fetch.buffer.size";
  public static final String KAFKA_CONSUMER_SOCKET_TIMEOUT_MS = "kafka.consumer.socket.timeout.ms";
  public static final String KAFKA_CONSUMER_NUM_METADATA_REFRESH_RETRIES =
      "kafka.consumer.num.metadata.refresh.retries";
  public static final String KAFKA_CONSUMER_METADATA_REFRESH_BACKOFF_MS = "kafka.consumer.metadata.refresh.backoff.ms";
  public static final String KAFKA_CONSUMER_ENABLE_AUTO_OFFSET_COMMIT = "kafka.enable.auto.commit";

  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  public static final String KAFKA_AUTO_COMMIT_INTERVAL_MS = "kafka.auto.commit.interval.ms";

  // all other properties go here
  private String veniceConfigDir;
  private String clusterPropertiesFile;
  private String serverPropertiesFile;
  private String storesConfigDir;

  private Props clusterAndServerProperties;
  private VeniceServerConfig veniceServerConfig;
  private Map<String, VeniceStoreConfig> storeToConfigsMap;

  private Map<String, String> partitionNodeAssignmentSchemeClassMap;

  public VeniceConfigService(String configDirPath)
      throws Exception {
    /**
     * 1. validate the path to see if all needed files are present
     * 2. load properties
     *    2.a. parse cluster.properties
     *    2.b. parse server.properties and validate if server properties file overrides any cluster specific property.
     *        2.b.1  If violated  return exception and exit
     *        2.b.2  If it looks good, merge server and cluster properties and create VeniceServerConfigInstance
     *    2.c Loop over the store configs and do the following
     *        2.c.1 parse a store config and check if it override any server or cluster specific property
     *              * If it does return exception and exit
     *              * If it looks good merge store, server and cluster properties and create VeniceStoreConfig
     *    2.d end of above step you will have a map of VeniceStoreConfig instances.
     *
     */
    storeToConfigsMap = new ConcurrentHashMap();
    validateConfigPath(configDirPath);
    loadClusterAndServerConfigs();
    loadStoreConfigs();
  }

  /**
   * validate if all necessary config files exist and are readable
   *
   * @param configDirPath
   */
  private void validateConfigPath(String configDirPath) {
    logger.info("validating config dir path...");
    if (!Utils.isReadableDir(configDirPath)) {
      throw new ConfigurationException(
          "Attempt to load configuration from , " + configDirPath + " failed. That is not a readable directory.");
    }
    veniceConfigDir = configDirPath;
    clusterPropertiesFile = veniceConfigDir + File.separator + VENICE_CLUSTER_PROPERTIES_FILE;
    if (!Utils.isReadableFile(clusterPropertiesFile)) {
      throw new ConfigurationException(clusterPropertiesFile + " is not a readable configuration file.");
    }

    serverPropertiesFile = veniceConfigDir + File.separator + VENICE_SERVER_PROPERTIES_FILE;
    if (!Utils.isReadableFile(serverPropertiesFile)) {
      throw new ConfigurationException(serverPropertiesFile + " is not a readable configuration file.");
    }

    storesConfigDir = veniceConfigDir + File.separator + VeniceConfigService.STORE_CONFIGS_DIR_NAME;
    if (!Utils.isReadableDir(storesConfigDir)) {
      String errorMessage =
          "Either the " + VeniceConfigService.STORE_CONFIGS_DIR_NAME + " directory does not exist or is not readable.";
      throw new ConfigurationException(errorMessage);
    }
  }

  /**
   * load cluster and server specific configs
   *
   * @throws Exception
   */
  private void loadClusterAndServerConfigs()
      throws Exception {
    logger.info("loading cluster and server configs...");
    Props clusterProperties, serverProperties;
    clusterProperties = Utils.parseProperties(clusterPropertiesFile);

    serverProperties = Utils.parseProperties(serverPropertiesFile);

    // validate scope of server Properties. They should not override any cluster related property.
    List<String> invalidProperties = new ArrayList<String>();
    for (String propertyKey : serverProperties.keySet()) {
      if (clusterSpecificProperties.contains(propertyKey)) {
        invalidProperties.add(propertyKey);
      }
    }

    if (invalidProperties.size() > 0) {
      StringBuilder sb = new StringBuilder();
      sb.append(invalidProperties.get(0));
      for (int i = 1; i < invalidProperties.size(); i++) {
        sb.append(", " + invalidProperties.get(i));
      }

      throw new ConfigurationException(
          "Cluster specific properties - " + sb.toString() + " , cannot be overwritten by server properties");
    }

    // safe to merge both the properties
    clusterAndServerProperties = clusterProperties.mergeWithProperties(serverProperties);

    veniceServerConfig = new VeniceServerConfig(clusterAndServerProperties);
  }

  /**
   * load the store configs for all stores defined in STORES directory
   *
   * @throws Exception
   */
  private void loadStoreConfigs()
      throws Exception {
    logger.info("loading store configs...");
    storeToConfigsMap = new HashMap<String, VeniceStoreConfig>();

    // Get all .properties file in config/STORES directory
    List<File> storeConfigurationFiles = Arrays.asList(new File(storesConfigDir).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".properties");
      }
    }));

    //parse the properties for each store
    for (File storeConfig : storeConfigurationFiles) {
      loadStoreConfig(storeConfig);
    }
  }

  /**
   * load the store configs for a particular store
   *
   * @param storeFileName
   * @throws Exception
   */
  private void loadStoreConfig(File storeFileName)
      throws Exception {
    logger.info("initializing configs for store: " + storeFileName.getName());
    Props storeProps = Utils.parseProperties(storeFileName);
    checkConfigScope(storeProps);

    //merge all store. server and cluster properties
    Props storeProperties = clusterAndServerProperties.mergeWithProperties(storeProps);

    //safe to create the VeniceStoreConfig instance
    if (!storeToConfigsMap.containsKey(storeProps.getString(STORE_NAME))) {
      storeToConfigsMap.put(storeProps.getString(STORE_NAME), new VeniceStoreConfig(storeProperties));
    }
  }

  /**
   * Verify that the store configs do not override server and cluster specific configs
   *
   * @param storeProps individual store configs
   * @throws ConfigurationException
   */
  private void checkConfigScope(Props storeProps)
      throws ConfigurationException {
    List<String> invalidClusterSpecificProperties = new ArrayList<String>();
    List<String> invalidServerSpecificProperties = new ArrayList<String>();
    for (String propertyKey : storeProps.keySet()) {
      if (clusterSpecificProperties.contains(propertyKey)) {
        invalidClusterSpecificProperties.add(propertyKey);
      } else if (serverSpecificProperties.contains(propertyKey)) {
        invalidServerSpecificProperties.add(propertyKey);
      }
    }
    String errorMessage = "";
    StringBuilder sbClusterProps = new StringBuilder();
    if (invalidClusterSpecificProperties.size() > 0) {
      sbClusterProps.append(
          "\nCluster specific properties attempted to be overridden- " + invalidClusterSpecificProperties.get(0));
      for (int i = 1; i < invalidClusterSpecificProperties.size(); i++) {
        sbClusterProps.append(", " + invalidClusterSpecificProperties.get(i));
      }
    }
    StringBuilder sbServerProps = new StringBuilder();
    if (invalidServerSpecificProperties.size() > 0) {
      sbServerProps.append(
          "\nServer specific properties  attempted to be overridden - " + invalidServerSpecificProperties.get(0));
      for (int i = 1; i < invalidServerSpecificProperties.size(); i++) {
        sbServerProps.append(", " + invalidServerSpecificProperties.get(i));
      }
    }
    if (invalidClusterSpecificProperties.size() > 0 || invalidServerSpecificProperties.size() > 0) {
      errorMessage =
          "Attempt to override non-overridable properties." + sbClusterProps.toString() + sbServerProps.toString();
      throw new ConfigurationException(errorMessage);
    }
  }

  public String getVeniceConfigDir() {
    return veniceConfigDir;
  }

  public String getClusterPropertiesFile() {
    return clusterPropertiesFile;
  }

  public String getServerPropertiesFile() {
    return serverPropertiesFile;
  }

  public String getStoresConfigDir() {
    return storesConfigDir;
  }

  public VeniceClusterConfig getVeniceClusterConfig() {
    return veniceServerConfig;
  }

  public VeniceServerConfig getVeniceServerConfig() {
    return veniceServerConfig;
  }

  public Map<String, VeniceStoreConfig> getAllStoreConfigs() {
    return storeToConfigsMap;
  }

  public VeniceStoreConfig getStoreConfig(String storeName) {
    return Utils.notNull(storeToConfigsMap.get(storeName));
  }

  /**
   * Initializes the Venice configuration service from known environment variables
   *
   * @return VeniceConfigService object
   * @throws Exception
   */
  public static VeniceConfigService loadFromEnvironmentVariable()
      throws Exception {
    String veniceConfigDir = System.getenv(VeniceConfigService.VENICE_CONFIG_DIR);
    if (veniceConfigDir == null) {
      throw new ConfigurationException(
          "No environment variable " + VeniceConfigService.VENICE_CONFIG_DIR + " has been defined, set it!");
    } else {
      if (!Utils.isReadableDir(veniceConfigDir)) {
        throw new ConfigurationException("Attempt to load configuration from VENICE_CONFIG_DIR, " + veniceConfigDir
            + " failed. That is not a readable directory.");
      }
    }
    return new VeniceConfigService(veniceConfigDir);
  }
}
