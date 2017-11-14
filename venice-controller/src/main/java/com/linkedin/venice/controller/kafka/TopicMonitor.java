package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;


/**
 * Kicks off a new thread to poll kafka for new topics
 * When it discovers new topics, it creates a corresponding helix resource so storage nodes start consuming
 * <p>
 * TopicMonitor is shared by multiple cluster's controllers running in one physical Venice controller instance.
 */
public class TopicMonitor extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(TopicMonitor.class);

  private Admin admin;
  private long pollIntervalMs;
  private TopicMonitorRunnable monitor;
  private Thread runner;
  private VeniceConsumerFactory veniceConsumerFactory;


  public TopicMonitor(Admin admin, long pollIntervalMs, VeniceConsumerFactory veniceConsumerFactory) {
    this.admin = admin;
    this.pollIntervalMs = pollIntervalMs;
    this.veniceConsumerFactory = veniceConsumerFactory;
  }

  @Override
  public boolean startInner() throws Exception {
    String kafkaString = admin.getKafkaBootstrapServers();
    monitor = new TopicMonitorRunnable(admin);
    runner = new Thread(monitor);
    runner.setName("TopicMonitor - " + kafkaString);
    runner.setDaemon(true);
    runner.start();

    // Although the TopicMonitorRunnable is now running in its own thread, there is no async
    // process that needs to finish before the TopicMonitor can be considered started, so we
    // are done with the start up process.
    return true;
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
      kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "controller-topic-monitor;" + Utils.getHostName());
      kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
      kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
      kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
      kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
      /* Only using consumer to list topics, key and value type are bogus */
      this.kafkaClient = veniceConsumerFactory.getKafkaConsumer(kafkaProps);
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
            if (logger.isDebugEnabled()) {
              logger.debug("Polling kafka: " + admin.getKafkaBootstrapServers() + " for new topics");
            }
            Map<String, List<PartitionInfo>> topics = kafkaClient.listTopics();
            for (Map.Entry<String, List<PartitionInfo>> entry : topics.entrySet()) {
              String topic = entry.getKey();
              if (AdminTopicUtils.isAdminTopic(topic)) {
                logger.debug("Skip admin topic: " + topic + " in Topic Monitor thread.");
                continue;
              }
              if (AdminTopicUtils.isKafkaInternalTopic(topic)) {
                logger.debug("Skip kafka internal topic: " + topic + " in Topic Monitor thread");
                continue;
              }
              if (Version.isRealTimeTopic(topic)) {
                logger.debug("Skip real-time kafka topic: " + topic + " in Topic Monitor thread");
                continue;
              }
              if (Version.topicIsValidStoreVersion(topic)) {
                String storeName = Version.parseStoreFromKafkaTopicName(topic);
                int version = Version.parseVersionFromKafkaTopicName(topic);
                  Optional<String> clusterName = admin.getClusterOfStoreInMasterController(storeName);
                if (!clusterName.isPresent()) {
                  logger.debug(
                      "Could not handle topic: " + topic + " in this controller. Store does not exist in any cluster."
                          + " Or current controller is not the lead controller of the cluster owned that store.");
                  continue;
                } else if (!admin.isMasterController(clusterName.get())) {
                  // double check the master controller.
                  logger.debug("Could not handle topic: " + topic
                      + " in this controller. Current controller is not the lead controller of cluster: "
                      + clusterName.get());
                  continue;
                }
                // Found the cluster for store, and this controller is the master controller for that cluster.
                // So we could continue to create version.
                try {
                  Store store = admin.getStore(clusterName.get(), storeName);
                  if (null == store) {
                    throw new VeniceNoStoreException(storeName);
                  }
                  if (version > store.getLargestUsedVersionNumber()) {
                    int partitions = entry.getValue().size();

                    admin.addVersion(clusterName.get(), storeName, version, partitions,
                        admin.getReplicationFactor(clusterName.get(), storeName));
                  }
                } catch (VeniceNoStoreException e) {
                  logger.warn("There is a topic " + topic + " for store " + storeName + " but that store is not initialized in Venice");
                  continue; /* skip to the next topic */
                } catch (StoreDisabledException se) {
                  logger.info("There is a topic " + topic + " for store " + storeName + ". But store has been paused.", se);
                  continue;
                }
              } else if (Version.isRealTimeTopic(topic)) {
                logger.debug("Skip real-time buffer topic: " + topic + " in Topic Monitor thread");
                continue;
              } else {
                logger.warn("The topic name: " + topic + " is not valid fomrat for $storeName_V$vesion");
                continue;
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
}
