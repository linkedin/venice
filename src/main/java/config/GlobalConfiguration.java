package config;

import java.io.File;

/**
 * Created by clfung on 9/11/14.
 */
public class GlobalConfiguration {

  public enum StoreType {VOLDEMORT, MEMORY};

  public static int kafkaReplicationFactor;
  public static int storageReplicationFactor;
  public static StoreType storageType;

  public static File configFile;

  public static void initialize(String configPath) {

    // normally here we would either read from file, or ZK.

    kafkaReplicationFactor = 1;
    storageReplicationFactor = 1;
    storageType = StoreType.MEMORY;

    File configFile = new File(configPath);

  }

}
