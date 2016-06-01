package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;


/**
 * Kicks off a new thread to poll kafka for new topics
 * When it discovers new topics, it creates a corresponding helix resource so storage nodes start consuming
 */
public class TopicMonitor extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(TopicMonitor.class);

  private Admin admin;
  private String clusterName;  /* to support multiple clusters we'll need a topic to cluster mapping instead */
  private long pollIntervalMs;
  private TopicMonitorRunnable monitor;
  private Thread runner;
  private int replicationFactor;


  public TopicMonitor(Admin admin, String clusterName, int replicationFactor, long pollInterval, TimeUnit pollIntervalUnits) {
    super("TOPIC-MONITOR-SERVICE");
    this.admin = admin;
    this.clusterName = clusterName;
    this.replicationFactor = replicationFactor;
    this.pollIntervalMs = pollIntervalUnits.toMillis(pollInterval);
  }

  @Override
  public void startInner() throws Exception {
    String kafkaString = admin.getKafkaBootstrapServers();
    monitor = new TopicMonitorRunnable(admin);
    runner = new Thread(monitor);
    runner.setName("TopicMonitor - " + kafkaString);
    runner.setDaemon(true);
    runner.start();
  }

  @Override
  public void stopInner() throws Exception {
    monitor.setStop();
    runner.interrupt();
  }

  private class TopicMonitorRunnable implements Runnable {

    private volatile boolean stop = false;
    private Admin admin;
    private KafkaConsumer<String, String> kafkaClient;

    TopicMonitorRunnable(Admin admin){
      this.admin = admin;
      Properties kafkaProps = new Properties();
      kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, admin.getKafkaBootstrapServers());
      kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "controller-topic-monitor;" + Utils.getHostName());
      kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
      kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
      kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
      kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
      /* Only using consumer to list topics, key and value type are bogus */
      kafkaClient = new KafkaConsumer<String, String>(kafkaProps);
    }

    protected void setStop(){
      stop = true;
    }

    @Override
    public void run() {
      try {
        while (!stop) {
          try {
            Thread.sleep(pollIntervalMs);
            logger.info("Polling kafka: " + admin.getKafkaBootstrapServers() + " for new topics");
            Map<String, List<PartitionInfo>> topics = kafkaClient.listTopics();
            for (Map.Entry<String, List<PartitionInfo>> entry : topics.entrySet()) {
              String topic = entry.getKey();
              if (Version.topicIsValidStoreVersion(topic)) {
                String storeName = Version.parseStoreFromKafkaTopicName(topic);
                int version = Version.parseVersionFromKafkaTopicName(topic);
                try {
                  List<Version> currentVersions = admin.versionsForStore(clusterName, storeName); /* throws VeniceNoStore */
                  if (isValidNewVersion(version, currentVersions)) {
                    int partitions = entry.getValue().size();
                    admin.addVersion(clusterName, storeName, version, partitions, replicationFactor);
                  }
                } catch (VeniceNoStoreException e) {
                  logger.warn("There is a topic " + topic + " for store " + storeName + " but that store is not initialized in Venice");
                  continue; /* skip to the next topic */
                }
              }
            }
          } catch (Exception e) {
            if (stop) {
              logger.info("Topic monitor caught " + e.getMessage() + " and stop is signaled");
              break;
            }
            logger.error("Something bad happened, and will probably continue to happen", e);
          }
        }
      } finally {
        kafkaClient.close();
        logger.info("Topic monitor stopped");
      }
    }


  }

  /**
   * A valid new version is a version that doesn't currently exist,
   * and is a larger version number than at least one of the existing versions
   * If there are no existing versions, a new version is also valid
   *
   * This should cover a couple of different cases.
   * 1. New store gets it's first version. If there are no existing versions, any version number should be valid.
   * 2. Existing store gets a next version, existing versions 4 and 5, version 6 (or 7) should be valid.
   * 3. Versions come in out-of-order for some (possibly unexpected) reason.  existing versions 4 and 6, 5 should
   *     be valid to support roll-back.
   * 4. Legacy versions still have kafka topics.  Existing versions are 4, 5, and 6.  New version 3 is not valid,
   *     the version was probably already deleted and the topic just hasn't been cleaned up yet.
   *
   * @param newVersion
   * @param existingVersions
   * @return
   */
  protected static boolean isValidNewVersion(int newVersion, List<Version> existingVersions){
    if (newVersion < 1){
      return false;
    }
    if (existingVersions.isEmpty()){
      return true;
    }
    boolean isGreater = false;
    for (Version version : existingVersions){
      if (newVersion == version.getNumber()){
        return false; /* version already exists */
      }
      if (newVersion > version.getNumber()){
        isGreater = true;
      }
    }
    return isGreater;
  }
}
