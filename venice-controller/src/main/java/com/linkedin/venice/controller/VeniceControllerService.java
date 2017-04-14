package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import io.tehuti.metrics.MetricsRepository;
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
  private final MetricsRepository metricsRepository;

  public VeniceControllerService(VeniceControllerConfig config, MetricsRepository metricsRepository) {
    this.config = config;
    this.metricsRepository = metricsRepository;
    VeniceHelixAdmin internalAdmin = new VeniceHelixAdmin(config, metricsRepository);
    if (config.isParent()) {
      this.admin = new VeniceParentHelixAdmin(internalAdmin, config);
      logger.info("Controller works as a parent controller.");
    } else {
      this.admin = internalAdmin;
      logger.info("Controller works as a normal controller.");
    }
    // The admin consumer needs to use VeniceHelixAdmin to update Zookeeper directly
    this.consumerService = new AdminConsumerService(internalAdmin, config, metricsRepository);
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
    admin.stop(config.getClusterName());
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
     * {@link ConsumerConfig.CLIENT_ID_CONFIG} can be used to identify different consumers while checking Kafka related metrics.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clusterName);
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    /**
     * Reason to disable auto_commit
     * 1. {@link AdminConsumptionTask} is persisting {@link com.linkedin.venice.offsets.OffsetRecord} in Zookeeper.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        KafkaKeySerializer.class.getName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaValueSerializer.class.getName());
    return kafkaConsumerProperties;
  }
}
