package com.linkedin.davinci.config;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;

import org.apache.log4j.Logger;


/**
 * VeniceConfigService loads the static configuration that does not change
 * once the Server is started. Modifying these configs once the server
 * is up and running, does not get reflected until the server is restarted.
 *
 * There are 3 configurations provided.
 *  1) Cluster Configuration
 *      The configuration that needs to be shared between the Controller and
 *      Server goes here.
 *  2) Server Configuration
 *    Multiple nodes that should share the same value should go in here.
 *    Cluster Name, ZooKeeper Address or any other configuration that is common
 *    between many storage nodes go here.
 *  3) Override Configuration
 *    These value override the values in Base Configuration ( if present). Node Id
 *    for each server goes here.
 *
 *  There are two types of overrides supported.
 *  1) Precedence among the files
 *  Any Key in Server Configuration overrides the same key in Cluster Configuration.
 *  Any Key in Override Configuration overrides the same key in Server Configuration.
 *
 *  2) Overriding keys for specific store only.
 *  The following config sets default to all stores and override for foobar store.
 *  bdb.max.logfile.size=1000
 *  store-foobar.bdb.max.logfile.size=5000
 *
 * There are two ways of providing the Configs.
 *  1) Config directory. The Config uses 3 files cluster.properties, server.properties and
 *  server-override.properties (optional).
 *  2) java.util.properties. It takes in 3 props for the above 3 configuration.
 *
 *  The static config initializes the config and starts the default services.
 *  The configuration does not specify what stores Storage Node hosts/serves.
 *
 *  The Storage Node waits for Commands from Helix to understand what stores
 *  and partitions it was assigned to.
 *
 *  This design, assumes that without Helix, the
 *  storage node/server will not be able to read/write data as it has no
 *  knowledge of the stores.
 *
 *  Helix Controller may also provide dynamic properties that can override
 *  the properties at the store level. This is not implemented currently.
 *
 *  There is no validation in the code, to differentiate between Valid and
 *  Invalid Overrides for now. Once the code is evolved and our usage
 *  patterns are narrowed down, checks can be enforced if necessary.
 */
public class VeniceConfigLoader {
  private static final Logger logger = Logger.getLogger(VeniceConfigLoader.class);

  public static final String VENICE_CONFIG_DIR = "VENICE_CONFIG_DIR";
  public static final String CLUSTER_PROPERTIES_FILE = "cluster.properties";
  public static final String SERVER_PROPERTIES_FILE = "server.properties";

  private final VeniceServerConfig veniceServerConfig;
  private final VeniceProperties combinedProperties;

  public VeniceConfigLoader(VeniceProperties properties) {
    this(properties, new VeniceProperties());
  }

  public VeniceConfigLoader(VeniceProperties clusterProperties, VeniceProperties serverProperties) {
    this.combinedProperties =
        new PropertyBuilder()
            .put(clusterProperties.toProperties())
            .put(serverProperties.toProperties())
            .build();
    this.veniceServerConfig = new VeniceServerConfig(combinedProperties);
  }

  public VeniceClusterConfig getVeniceClusterConfig() {
    return veniceServerConfig;
  }

  public VeniceServerConfig getVeniceServerConfig() {
    return veniceServerConfig;
  }

  public VeniceProperties getCombinedProperties() {
    return combinedProperties;
  }

  public VeniceStoreVersionConfig getStoreConfig(String storeName) {
    VeniceProperties storeProperties = combinedProperties.getStoreProperties(storeName);
    return new VeniceStoreVersionConfig(storeName, storeProperties);
  }

  public VeniceStoreVersionConfig getStoreConfig(String storeName, PersistenceType storePersistenceType) {
    VeniceProperties storeProperties = combinedProperties.getStoreProperties(storeName);
    return new VeniceStoreVersionConfig(storeName, storeProperties, storePersistenceType);
  }

  /**
   * Initializes the Venice configuration service from known environment variables
   *
   * @return VeniceConfigService object
   */
  public static VeniceConfigLoader loadFromEnvironmentVariable() {
    String veniceConfigDir = System.getenv(VENICE_CONFIG_DIR);
    if (veniceConfigDir == null) {
      throw new ConfigurationException(
          "The Environment variable " + VENICE_CONFIG_DIR + " is undefined, set it!");
    }

    return loadFromConfigDirectory(veniceConfigDir);
  }

  public static VeniceConfigLoader loadFromConfigDirectory(String configDirPath) {
    logger.info("loading cluster and server configs...");

    if (!Utils.isReadableDir(configDirPath)) {
      String fullFilePath = Utils.getCanonicalPath(configDirPath);
      throw new ConfigurationException(
              "Attempt to load configuration from , " + fullFilePath + " failed. That is not a readable directory.");
    }

    VeniceProperties clusterProperties, serverProperties;
    try {
      clusterProperties = Utils.parseProperties(configDirPath, CLUSTER_PROPERTIES_FILE, false);
      serverProperties = Utils.parseProperties(configDirPath, SERVER_PROPERTIES_FILE, false);
    } catch (Exception e) {
      throw new ConfigurationException("Loading configuration files failed", e);
    }
    return new VeniceConfigLoader(clusterProperties, serverProperties);
  }
}
