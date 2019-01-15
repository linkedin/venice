package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.offsets.AdminOffsetManager;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

import java.util.concurrent.ThreadFactory;

public class AdminConsumerService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(AdminConsumerService.class);
  private static final long WAITING_TIME_FOR_STOP_IN_MS = 5000;

  private final VeniceControllerConfig config;
  private final VeniceHelixAdmin admin;
  private final AdminOffsetManager offsetManager;
  private final VeniceConsumerFactory consumerFactory;
  private final MetricsRepository metricsRepository;
  // Only support single cluster right now
  private AdminConsumptionTask consumerTask;
  private ThreadFactory threadFactory = new DaemonThreadFactory("AdminTopicConsumer");
  private Thread consumerThread;


  public AdminConsumerService(VeniceHelixAdmin admin, VeniceControllerConfig config, MetricsRepository metricsRepository) {
    this.config = config;
    this.admin = admin;
    this.offsetManager = new AdminOffsetManager(admin.getZkClient(), admin.getAdapterSerializer());
    this.metricsRepository = metricsRepository;
    this.consumerFactory = admin.getVeniceConsumerFactory();
  }

  @Override
  public boolean startInner() throws Exception {
    String clusterName = config.getClusterName();
    consumerTask = getAdminConsumptionTaskForCluster(clusterName);
    consumerThread = threadFactory.newThread(consumerTask);
    consumerThread.start();

    return true;
  }

  @Override
  public void stopInner() throws Exception {
    consumerTask.close();
    consumerThread.join(WAITING_TIME_FOR_STOP_IN_MS);
    if (consumerThread.isAlive()) {
      consumerThread.interrupt();
    }
  }

  private AdminConsumptionTask getAdminConsumptionTaskForCluster(String clusterName) {
    return new AdminConsumptionTask(clusterName,
        createKafkaConsumer(clusterName),
        admin,
        offsetManager,
        admin.getExecutionIdAccessor(),
        config.isParent(),
        new AdminConsumptionStats(metricsRepository, clusterName + "-admin_consumption_task"),
        config.getAdminTopicReplicationFactor(),
        config.getAdminConsumptionCycleTimeoutMs(),
        config.getAdminConsumptionMaxWorkerThreadPoolSize());
  }

  public void setOffsetToSkip(String clusterName, long offset){
    if (clusterName.equals(config.getClusterName())){
      consumerTask.skipMessageWithOffset(offset);
    } else {
      throw new VeniceException("This AdminConsumptionService is for cluster " + config.getClusterName()
          + ".  Cannot skip admin message with offset " + offset + " for cluster " + clusterName);
    }
  }

  public long getLastSucceedExecutionId(String clusterName) {
    if (clusterName.equals(config.getClusterName())) {
      return consumerTask.getLastSucceededExecutionId();
    } else {
      throw new VeniceException("This AdminConsumptionService is for cluster " + config.getClusterName()
          + ".  Cannot get the last succeed execution Id for cluster " + clusterName);
    }
  }

  public long getFailingOffset() {
    return consumerTask.getFailingOffset();
  }

  private KafkaConsumerWrapper createKafkaConsumer(String clusterName) {
    Properties kafkaConsumerProperties = new Properties();
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

    return consumerFactory.getConsumer(kafkaConsumerProperties);
  }

  // for test purpose
  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }
}
