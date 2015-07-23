package com.linkedin.venice.config;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.utils.Props;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.apache.log4j.Logger;


/**
 * Class to load the store partition information from the store configuration files.
 */
public class StoreConfigsLoader {

  private static final Logger logger = Logger.getLogger(StoreConfigsLoader.class.getName());

  private static final String STORE_NAME = "store.name";
  private static final String STORE_NUMBER_PARTITIONS = "store.number.partitions";
  private static final String STORE_REPLICATION_FACTOR = "store.replication.factor";

  public static final String VENICE_STORE_INFO_DIR = "VENICE_STORE_INFO_DIR";

  /**
   * load the store configs for all stores defined in STORES directory
   */
  public static Map<String, VeniceStorePartitionInformation> loadStoreConfigs(String storesInfoDir)
      throws Exception {
    logger.info("loading store configs...");
    Map<String, VeniceStorePartitionInformation> storeToPartitionInfoMap = new HashMap<>();

    // Get all .properties file in the directory
    List<File> storeInformationFiles = Arrays.asList(new File(storesInfoDir).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".properties");
      }
    }));

    //parse the properties for each store
    for (File storeFile : storeInformationFiles) {
      logger.info("initializing configs for store: " + storeFile.getName());
      Props storeProps = Utils.parseProperties(storeFile);

      if (!storeToPartitionInfoMap.containsKey(storeProps.getString(STORE_NAME))) {
        storeToPartitionInfoMap.put(storeProps.getString(STORE_NAME)
            , new VeniceStorePartitionInformation(storeProps.getString(STORE_NAME)
            , storeProps.getInt(STORE_NUMBER_PARTITIONS), storeProps.getInt(STORE_REPLICATION_FACTOR)));
      }
    }
    return storeToPartitionInfoMap;
  }

  /**
   * Initializes the Venice store information from known environment variables
   *
   * @return Map, mapping store name to VeniceStorePartitionInformation
   */
  public static Map<String, VeniceStorePartitionInformation> loadFromEnvironmentVariable()
      throws Exception {
    String veniceStoreInfoDir = System.getenv(VENICE_STORE_INFO_DIR);
    if (veniceStoreInfoDir == null) {
      throw new ConfigurationException(
          "No environment variable " + VENICE_STORE_INFO_DIR + " has been defined, set it!");
    } else {
      if (!Utils.isReadableDir(veniceStoreInfoDir)) {
        throw new ConfigurationException("Attempt to load configuration from VENICE_STORE_INFO_DIR, "
            + veniceStoreInfoDir + " failed. That is not a readable directory.");
      }
    }
    return StoreConfigsLoader.loadStoreConfigs(veniceStoreInfoDir);
  }
}

