package config;

import java.io.File;

/**
 * Created by clfung on 9/11/14.
 */
public class GlobalConfiguration {

  private enum StoreType {VOLDEMORT, BDB, MEMORY};

  private static int kafkaReplicationFactor;
  private static int storageReplicationFactor;
  private static StoreType storageType;
  private static String kafKaZookeeperUrl;

  public static File configFile;

  public static void initialize(String configPath) {

    // normally here we would either read from file, or ZK.
    kafkaReplicationFactor = 1;
    storageReplicationFactor = 1;
    storageType = StoreType.MEMORY;

    kafKaZookeeperUrl = "localhost:2181";

    File configFile = new File(configPath);

  }

  public String getZookeeperURL() {
    return kafKaZookeeperUrl;
  }

}
