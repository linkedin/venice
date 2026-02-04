package com.linkedin.davinci.config;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.INGESTION_USE_DA_VINCI_CLIENT;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_THROTTLER_FACTORS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_CROSS_TP_PARALLEL_PROCESSING_CURRENT_VERSION_AA_WC_LEADER_ONLY;
import static com.linkedin.venice.ConfigKeys.SERVER_CROSS_TP_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_CROSS_TP_PARALLEL_PROCESSING_THREAD_POOL_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_THROTTLER_FACTORS_FOR_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_THROTTLER_FACTORS_FOR_CURRENT_VERSION_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_THROTTLER_FACTORS_FOR_CURRENT_VERSION_NON_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_THROTTLER_FACTORS_FOR_NON_CURRENT_VERSION_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_THROTTLER_FACTORS_FOR_NON_CURRENT_VERSION_NON_AA_WC_LEADER;
import static com.linkedin.venice.ConfigKeys.SERVER_THROTTLER_FACTORS_FOR_SEP_RT_LEADER;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.testng.annotations.Test;


public class VeniceServerConfigTest {
  private Properties populatedBasicProperties() {
    Properties props = new Properties();
    props.setProperty(CLUSTER_NAME, "test_cluster");
    props.setProperty(ZOOKEEPER_ADDRESS, "fake_zk_addr");
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "fake_kafka_addr");
    props.setProperty(INGESTION_USE_DA_VINCI_CLIENT, "true");

    return props;
  }

  @Test
  public void testForkedJVMParams() {
    Properties props = populatedBasicProperties();
    props.put(SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST, "-Xms256M;  -Xmx256G");

    VeniceServerConfig config = new VeniceServerConfig(new VeniceProperties(props));

    List<String> jvmArgs = config.getForkedProcessJvmArgList();
    assertEquals(jvmArgs.size(), 2);
    assertEquals(jvmArgs.get(0), "-Xms256M");
    assertEquals(jvmArgs.get(1), "-Xmx256G");
  }

  @Test
  public void testConfig() {
    Properties props = populatedBasicProperties();

    Map<String, Function<VeniceServerConfig, List<Double>>> configMap = new HashMap<>();

    configMap.put(KAFKA_FETCH_THROTTLER_FACTORS_PER_SECOND, VeniceServerConfig::getKafkaFetchThrottlerFactorsPerSecond);

    configMap.put(SERVER_THROTTLER_FACTORS_FOR_AA_WC_LEADER, VeniceServerConfig::getThrottlerFactorsForAAWCLeader);
    configMap.put(SERVER_THROTTLER_FACTORS_FOR_SEP_RT_LEADER, VeniceServerConfig::getThrottlerFactorsForSepRTLeader);

    configMap.put(
        SERVER_THROTTLER_FACTORS_FOR_CURRENT_VERSION_AA_WC_LEADER,
        VeniceServerConfig::getThrottlerFactorsForCurrentVersionAAWCLeader);
    configMap.put(
        SERVER_THROTTLER_FACTORS_FOR_CURRENT_VERSION_NON_AA_WC_LEADER,
        VeniceServerConfig::getThrottlerFactorsForCurrentVersionNonAAWCLeader);
    configMap.put(
        SERVER_THROTTLER_FACTORS_FOR_NON_CURRENT_VERSION_AA_WC_LEADER,
        VeniceServerConfig::getThrottlerFactorsForNonCurrentVersionAAWCLeader);
    configMap.put(
        SERVER_THROTTLER_FACTORS_FOR_NON_CURRENT_VERSION_NON_AA_WC_LEADER,
        VeniceServerConfig::getThrottlerFactorsForNonCurrentVersionNonAAWCLeader);

    // Looping through all the factors config keys and checking if the values are same as default values
    for (Map.Entry<String, Function<VeniceServerConfig, List<Double>>> entry: configMap.entrySet()) {
      VeniceServerConfig config = new VeniceServerConfig(new VeniceProperties(props));
      List<Double> consumerPoolRecordsLimitFactors = entry.getValue().apply(config);
      assertEquals(consumerPoolRecordsLimitFactors.size(), config.getDefaultConsumerPoolLimitFactorsList().size());
      assertEquals(
          consumerPoolRecordsLimitFactors.toArray(),
          config.getDefaultConsumerPoolLimitFactorsList().toArray());
      Double[] factors = new Double[] { 0.6D, 0.8D, 1.0D, 1.2D };
      List<Double> factorsList = Arrays.asList(factors);
      // Convert list of double to string with comma separated
      String factorsListStr = factorsList.stream().map(String::valueOf).collect(Collectors.joining(", "));
      props.put(entry.getKey(), factorsListStr);
      config = new VeniceServerConfig(new VeniceProperties(props));
      consumerPoolRecordsLimitFactors = entry.getValue().apply(config);
      assertEquals(consumerPoolRecordsLimitFactors.size(), 4);
      assertEquals(consumerPoolRecordsLimitFactors.toArray(), factors);
    }
  }

  @Test
  public void testRocksDBPath() {
    Properties props = populatedBasicProperties();
    props.put(DATA_BASE_PATH, "db/path");

    VeniceServerConfig config = new VeniceServerConfig(new VeniceProperties(props));

    String path = config.getRocksDBPath();
    assertEquals(path, "db/path/rocksdb");
  }

  // TODO: Delete this test once we fully delete the HelixMessagingChannel.
  @Test
  public void testParticipantStoreConfigs() {
    Properties props = populatedBasicProperties();
    VeniceServerConfig config = new VeniceServerConfig(new VeniceProperties(props));
    assertTrue(config.isParticipantMessageStoreEnabled());

    props.put(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    config = new VeniceServerConfig(new VeniceProperties(props));
    assertTrue(config.isParticipantMessageStoreEnabled());

    props.put(PARTICIPANT_MESSAGE_STORE_ENABLED, "false");
    config = new VeniceServerConfig(new VeniceProperties(props));
    assertFalse(config.isParticipantMessageStoreEnabled());
  }

  @Test
  public void testCrossTpParallelProcessingConfigs() {
    // Test default values
    Properties props = populatedBasicProperties();
    VeniceServerConfig config = new VeniceServerConfig(new VeniceProperties(props));

    assertFalse(config.isCrossTpParallelProcessingEnabled());
    assertEquals(config.getCrossTpParallelProcessingThreadPoolSize(), 4);
    assertFalse(config.isCrossTpParallelProcessingCurrentVersionAAWCLeaderOnly());

    // Test enabling cross-TP parallel processing
    props.put(SERVER_CROSS_TP_PARALLEL_PROCESSING_ENABLED, "true");
    config = new VeniceServerConfig(new VeniceProperties(props));
    assertTrue(config.isCrossTpParallelProcessingEnabled());

    // Test custom thread pool size
    props.put(SERVER_CROSS_TP_PARALLEL_PROCESSING_THREAD_POOL_SIZE, "8");
    config = new VeniceServerConfig(new VeniceProperties(props));
    assertEquals(config.getCrossTpParallelProcessingThreadPoolSize(), 8);

    // Test enabling CURRENT_VERSION_AA_WC_LEADER_ONLY mode
    props.put(SERVER_CROSS_TP_PARALLEL_PROCESSING_CURRENT_VERSION_AA_WC_LEADER_ONLY, "true");
    config = new VeniceServerConfig(new VeniceProperties(props));
    assertTrue(config.isCrossTpParallelProcessingCurrentVersionAAWCLeaderOnly());

    // Test disabling CURRENT_VERSION_AA_WC_LEADER_ONLY mode explicitly
    props.put(SERVER_CROSS_TP_PARALLEL_PROCESSING_CURRENT_VERSION_AA_WC_LEADER_ONLY, "false");
    config = new VeniceServerConfig(new VeniceProperties(props));
    assertFalse(config.isCrossTpParallelProcessingCurrentVersionAAWCLeaderOnly());
  }
}
