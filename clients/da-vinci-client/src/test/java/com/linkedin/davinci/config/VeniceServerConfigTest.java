package com.linkedin.davinci.config;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.INGESTION_ISOLATION_CONFIG_PREFIX;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.INGESTION_MEMORY_LIMIT;
import static com.linkedin.venice.ConfigKeys.INGESTION_USE_DA_VINCI_CLIENT;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_THROTTLER_FACTORS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_MODE;
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
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.IngestionMode;
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

  @Test
  public void testMemoryLimitConfigWithoutIngestionIsolation() {
    Properties propsForNonDaVinci = populatedBasicProperties();
    propsForNonDaVinci.setProperty(INGESTION_USE_DA_VINCI_CLIENT, "false");
    propsForNonDaVinci.setProperty(INGESTION_MEMORY_LIMIT, "100MB");

    VeniceException e =
        expectThrows(VeniceException.class, () -> new VeniceServerConfig(new VeniceProperties(propsForNonDaVinci)));
    assertTrue(e.getMessage().contains("only meaningful for DaVinci"));

    Properties props1 = populatedBasicProperties();
    props1.setProperty(INGESTION_MEMORY_LIMIT, "100MB");
    e = expectThrows(VeniceException.class, () -> new VeniceServerConfig(new VeniceProperties(props1)));
    assertTrue(e.getMessage().contains("meaningful when using RocksDB plaintable format"));

    Properties props2 = populatedBasicProperties();
    props2.setProperty(INGESTION_MEMORY_LIMIT, "100MB");
    props2.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    e = expectThrows(VeniceException.class, () -> new VeniceServerConfig(new VeniceProperties(props2)));
    assertTrue(e.getMessage().contains("should be bigger than total memtable usage cap"));

    Properties props3 = populatedBasicProperties();
    props3.setProperty(INGESTION_MEMORY_LIMIT, "100MB");
    props3.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    props3.setProperty(ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES, "32MB");
    VeniceServerConfig config1 = new VeniceServerConfig(new VeniceProperties(props3));
    assertEquals(config1.getIngestionMemoryLimit(), 68 * 1024 * 1024l);
  }

  @Test
  public void testMemoryLimitConfigWithIngestionIsolation() {
    Properties props1 = populatedBasicProperties();
    props1.setProperty(INGESTION_MEMORY_LIMIT, "100MB");
    props1.setProperty(SERVER_INGESTION_MODE, IngestionMode.ISOLATED.toString());
    props1.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");

    VeniceException e = expectThrows(VeniceException.class, () -> new VeniceServerConfig(new VeniceProperties(props1)));
    assertTrue(
        e.getMessage()
            .contains(
                "The max heap size of isolated process needs to be configured explicitly when enabling memory limiter"));

    Properties props2 = populatedBasicProperties();
    props2.setProperty(INGESTION_MEMORY_LIMIT, "100MB");
    props2.setProperty(SERVER_INGESTION_MODE, IngestionMode.ISOLATED.toString());
    props2.put(SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST, "-Xms32MB;-Xmx32MB");
    props2.setProperty(ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES, "50MB");
    props2.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    e = expectThrows(VeniceException.class, () -> new VeniceServerConfig(new VeniceProperties(props2)));
    assertTrue(e.getMessage().contains("should be positive after subtracting the usage from other components"));

    Properties props3 = populatedBasicProperties();
    props3.setProperty(INGESTION_MEMORY_LIMIT, "100MB");
    props3.setProperty(SERVER_INGESTION_MODE, IngestionMode.ISOLATED.toString());
    props3.put(SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST, "-Xms32MB;-Xmx32MB");
    props3.setProperty(ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES, "32MB");
    props3.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    props3.setProperty(INGESTION_ISOLATION_CONFIG_PREFIX + "." + ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES, "16MB");
    VeniceServerConfig config1 = new VeniceServerConfig(new VeniceProperties(props3));
    assertEquals(config1.getIngestionMemoryLimit(), 20 * 1024 * 1024l);
  }

  @Test
  public void testParticipantStoreConfigs() {
    Properties props = populatedBasicProperties();
    VeniceServerConfig config = new VeniceServerConfig(new VeniceProperties(props));
    assertFalse(config.isParticipantMessageStoreEnabled());

    props.put(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    config = new VeniceServerConfig(new VeniceProperties(props));
    assertTrue(config.isParticipantMessageStoreEnabled());
  }
}
