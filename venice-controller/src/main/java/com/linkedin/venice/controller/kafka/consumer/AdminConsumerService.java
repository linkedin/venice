package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import java.util.concurrent.ThreadFactory;

public class AdminConsumerService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(AdminConsumerService.class);
  private static final long WAITING_TIME_FOR_STOP_IN_MS = 5000;

  private final VeniceControllerConfig config;
  private final VeniceHelixAdmin admin;
  private final OffsetManager offsetManager;
  private final KafkaConsumerWrapper consumer;
  // Only support single cluster right now
  private AdminConsumptionTask consumerTask;
  private ThreadFactory threadFactory = new DaemonThreadFactory("AdminTopicConsumer");
  private Thread consumerThread;

  public AdminConsumerService(VeniceControllerConfig config, VeniceHelixAdmin admin, OffsetManager offsetManager, KafkaConsumerWrapper consumer) {
    this.config = config;
    this.admin = admin;
    this.offsetManager = offsetManager;
    this.consumer = consumer;
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
    return new AdminConsumptionTask(clusterName, consumer, offsetManager, admin, TimeUnit.MINUTES.toMillis(config.getAdminConsumptionTimeoutMinutes()), config.isParent());
  }
}
