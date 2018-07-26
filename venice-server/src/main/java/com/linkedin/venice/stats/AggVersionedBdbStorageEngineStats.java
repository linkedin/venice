package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.sleepycat.je.Environment;
import io.tehuti.metrics.MetricsRepository;
import javafx.util.Pair;
import org.apache.log4j.Logger;


public class AggVersionedBdbStorageEngineStats
    extends AbstractVeniceAggVersionedStats<BdbStorageEngineStats, BdbStorageEngineStatsReporter> {
  private static final Logger logger = Logger.getLogger(AggVersionedBdbStorageEngineStats.class);

  public AggVersionedBdbStorageEngineStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository metadataRepository) {
    super(metricsRepository, metadataRepository, () -> new BdbStorageEngineStats(),
        (mr, name) -> new BdbStorageEngineStatsReporter(mr, name));
  }

  public void setBdbEnvironment(String topicName, Environment environment) {
    Pair<String, Integer> storeInfo = getStoreNameAndVersionFromTopic(topicName);
    if (storeInfo != null) {
      try {
        getStats(storeInfo.getKey(), storeInfo.getValue()).setBdbEnvironment(environment);
      } catch (VeniceException e) {
        logger.warn("Could not create BDB storage engine stats for store: " + topicName, e);
      }
    }
  }

  public void removeBdbEnvironment(String topicName) {
    Pair<String, Integer> storeInfo = getStoreNameAndVersionFromTopic(topicName);
    if (storeInfo != null) {
      try {
        getStats(storeInfo.getKey(), storeInfo.getValue()).removeBdbEnvironment();
      } catch (VeniceException e) {
        logger.warn("Could not remove BDB storage engine stats for store: " + topicName, e);
      }
    }
  }

  /**
   * This method works as a safe guard. It will return null if the topic name is not legit.
   */
  private Pair<String, Integer> getStoreNameAndVersionFromTopic(String topicName) {
    try {
      String storeName = Version.parseStoreFromKafkaTopicName(topicName);
      int version = Version.parseVersionFromKafkaTopicName(topicName);
      return new Pair<>(storeName, version);
    } catch (Exception e) {
      return null;
    }
  }
}
