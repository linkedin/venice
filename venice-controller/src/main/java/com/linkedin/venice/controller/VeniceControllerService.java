package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask;
import com.linkedin.venice.controller.kafka.offsets.AdminOffsetManager;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * A service venice controller. Wraps Helix Controller.
 */
public class VeniceControllerService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(VeniceControllerService.class);

  private final Admin admin;
  private final VeniceControllerClusterConfig config;
  private final AdminConsumerService consumerService;

  public VeniceControllerService(VeniceControllerConfig config) {
    this.config = config;
    VeniceConsumerFactory consumerFactory = new VeniceConsumerFactory();
    VeniceHelixAdmin internalAdmin = new VeniceHelixAdmin(config);
    if (config.isParent()) {
      this.admin = new VeniceParentHelixAdmin(internalAdmin, config, consumerFactory);
      logger.info("Controller works as a parent controller.");
    } else {
      this.admin = internalAdmin;
      logger.info("Controller works as a normal controller.");
    }
    // The admin consumer needs to use VeniceHelixAdmin to update Zookeeper directly
    this.consumerService = new AdminConsumerService(internalAdmin, config, consumerFactory);
    this.admin.setAdminConsumerService(config.getClusterName(), consumerService);
  }

  @Override
  public boolean startInner() {
    admin.start(config.getClusterName());
    consumerService.start();
    logger.info("start cluster:" + config.getClusterName());

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() {
    admin.stopVeniceController();
    try {
      consumerService.stop();
    } catch (Exception e) {
      logger.error("Got exception when stop AdminConsumerService", e);
    }

    logger.info("Stop cluster:" + config.getClusterName());
  }

  public Admin getVeniceHelixAdmin(){
    return admin;
  }

  public static Properties getKafkaConsumerProperties(String kafkaBootstrapServers, String clusterName) {
    Properties kafkaConsumerProperties = new Properties();
    kafkaConsumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    /**
     * We should use the same consumer group for all the consumers for the given cluster since
     * the offset is being persisted in Kafka, which is per consumer group.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, clusterName);
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    /**
     * Two reasons to disable auto_commit
     * 1. {@link AdminConsumptionTask} is persisting customized {@link org.apache.kafka.clients.consumer.OffsetAndMetadata} to Kafka;
     * 2. We would like to commit offset only after successfully consuming the message.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        KafkaKeySerializer.class.getName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaValueSerializer.class.getName());
    return kafkaConsumerProperties;
  }
}
