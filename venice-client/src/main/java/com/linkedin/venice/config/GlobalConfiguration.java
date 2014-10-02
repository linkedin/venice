package com.linkedin.venice.config;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Class which reads configuration parameters for Venice.
 * Inputs may come from file or another service.
 */
public class GlobalConfiguration {

  // server related configs
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

  private static File configFile;


  /* Cannot instantiate object */
  private GlobalConfiguration() {
  }

  public static void initialize(String configPath) {

    File configFile = new File(configPath);

    // normally here we would either read from file, or ZK.
    numKafkaPartitions = 5;
    numThreadsPerPartition = 1;

    kafKaZookeeperUrl = "localhost:2181";
    kafkaBrokerUrl = "localhost:9092";
    kafkaBrokerPort = 9092;
    brokerList = Arrays.asList("localhost");

    numStorageNodes = 3;
    numStorageCopies = 2;

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
