package com.linkedin.davinci.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
  private static final Logger LOGGER = LogManager.getLogger(VeniceConfigLoader.class);

  public static final String VENICE_CONFIG_DIR = "VENICE_CONFIG_DIR";
  public static final String CLUSTER_PROPERTIES_FILE = "cluster.properties";
  public static final String SERVER_PROPERTIES_FILE = "server.properties";

  public static final String KAFKA_CLUSTER_MAP_FILE = "kafka.cluster.map";

  private final VeniceServerConfig veniceServerConfig;
  private final VeniceProperties combinedProperties;

  public VeniceConfigLoader(VeniceProperties properties) {
    this(properties, VeniceProperties.empty());
  }

  public VeniceConfigLoader(VeniceProperties clusterProperties, VeniceProperties serverProperties) {
    this(clusterProperties, serverProperties, Collections.emptyMap());
  }

  public VeniceConfigLoader(
      VeniceProperties clusterProperties,
      VeniceProperties serverProperties,
      Map<String, Map<String, String>> kafkaClusterMap) {
    this.combinedProperties =
        new PropertyBuilder().put(clusterProperties.toProperties()).put(serverProperties.toProperties()).build();
    this.veniceServerConfig = new VeniceServerConfig(combinedProperties, kafkaClusterMap);
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
    return new VeniceStoreVersionConfig(storeName, storeProperties, veniceServerConfig.getKafkaClusterMap());
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
      throw new ConfigurationException("The Environment variable " + VENICE_CONFIG_DIR + " is undefined, set it!");
    }

    return loadFromConfigDirectory(veniceConfigDir);
  }

  public static VeniceConfigLoader loadFromConfigDirectory(String configDirPath) {
    LOGGER.info("loading cluster and server configs...");

    if (!Utils.isReadableDir(configDirPath)) {
      String fullFilePath = Utils.getCanonicalPath(configDirPath);
      throw new ConfigurationException(
          "Attempt to load configuration from , " + fullFilePath + " failed. That is not a readable directory.");
    }

    try {
      VeniceProperties clusterProperties = Utils.parseProperties(configDirPath, CLUSTER_PROPERTIES_FILE, false);
      VeniceProperties serverProperties = Utils.parseProperties(configDirPath, SERVER_PROPERTIES_FILE, false);
      Map<String, Map<String, String>> kafkaClusterMap = parseKafkaClusterMap(configDirPath);
      return new VeniceConfigLoader(clusterProperties, serverProperties, kafkaClusterMap);
    } catch (Exception e) {
      throw new ConfigurationException("Loading configuration files failed", e);
    }
  }

  public static Map<String, Map<String, String>> parseKafkaClusterMap(String configDirectory) throws Exception {
    return parseKafkaClusterMap(configDirectory, VeniceConfigLoader.KAFKA_CLUSTER_MAP_FILE);
  }

  /**
   * The file contains the following info:
   *
   * <map>
   *   <entry key="0">
   *     <map>
   *       <entry key="name" value="dc0"/>
   *       <entry key="url" value="kafka.in.dc0.my.company.com:1234"/>
   *       <entry key="otherUrls" value="alt1.kafka.in.dc0.my.company.com:1234,alt2.kafka.in.dc0.my.company.com:1234"/>
   *       <entry key="securityProtocol" value="PLAINTEXT"/>
   *     </map>
   *   </entry>
   *   <entry key="1">
   *     <map>
   *       <entry key="name" value="dc1"/>
   *       <entry key="url" value="kafka.in.dc1.my.company.com:1234"/>
   *       <entry key="otherUrls" value="alt1.kafka.in.dc1.my.company.com:1234,alt2.kafka.in.dc1.my.company.com:1234"/>
   *       <entry key="securityProtocol" value="SSL"/>
   *     </map>
   *   </entry>
   * </map>
   *
   * The otherUrls field is used to translate URLs coming from other processes (e.g. coming from the
   * {@link com.linkedin.venice.kafka.protocol.TopicSwitch} emitted by the controller, or by a previous run of the
   * server which was configured differently). The various URLs included in otherUrls fields must be globally unique.
   *
   * The securityProtocol entry must be compatible with the url entry (i.e. have the right port).
   *
   * N.B.: This is actually JSON, not XML, and it has some weird escaping in it. TODO: Clean this up.
  =  */
  public static Map<String, Map<String, String>> parseKafkaClusterMap(String configDirectory, String fileName)
      throws Exception {
    String mapFilePath = configDirectory + File.separator + fileName;
    File mapFile = new File(mapFilePath);
    if (!mapFile.exists()) {
      return Collections.emptyMap();
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(mapFile))) {
      String flatMapString = reader.readLine();
      ObjectMapper mapper = ObjectMapperFactory.getInstance();

      Map<String, String> flatMap = mapper.readValue(flatMapString, Map.class);
      Map<String, Map<String, String>> kafkaClusterMap = new HashMap<>();

      for (Map.Entry<String, String> entry: flatMap.entrySet()) {
        kafkaClusterMap.put(entry.getKey(), mapper.readValue(entry.getValue(), Map.class));
      }
      return kafkaClusterMap;
    }
  }

  public static void storeKafkaClusterMap(File configDirectory, Map<String, Map<String, String>> kafkaClusterMap)
      throws Exception {
    storeKafkaClusterMap(configDirectory, VeniceConfigLoader.KAFKA_CLUSTER_MAP_FILE, kafkaClusterMap);
  }

  public static void storeKafkaClusterMap(
      File configDirectory,
      String fileName,
      Map<String, Map<String, String>> kafkaClusterMap) throws Exception {
    if (kafkaClusterMap.isEmpty()) {
      return;
    }

    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    Map<String, String> flatMap = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry: kafkaClusterMap.entrySet()) {
      flatMap.put(entry.getKey(), mapper.writeValueAsString(entry.getValue()));
    }

    String flatMapString = mapper.writeValueAsString(flatMap);

    File mapFile = new File(configDirectory, fileName);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(mapFile))) {
      writer.write(flatMapString);
    }
  }
}
