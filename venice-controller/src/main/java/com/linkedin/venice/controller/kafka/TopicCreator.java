package com.linkedin.venice.controller.kafka;

import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;

public class TopicCreator {

  private final String zkConnection;
  private final int sessionTimeoutMs;
  private final int connectionTimeoutMs;

  private static final Logger logger = Logger.getLogger(TopicCreator.class.getName());

  public TopicCreator(String zkConnection, int sessionTimeoutMs, int connectionTimeoutMs){
    this.zkConnection = zkConnection;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
  }

  public TopicCreator(String zkConnection){
    this.zkConnection = zkConnection;
    this.sessionTimeoutMs = 10*1000;
    this.connectionTimeoutMs = 8*1000;
  }

  //Store creation is relatively rare, so don't hold onto the zkConnection Object
  public void createTopic(String topicName, int numPartitions, int replication){
    logger.info("Creating topic: " + topicName + " partitions: " + numPartitions + " replication: " + replication);
    ZkClient zkClient = null;
    try {
      zkClient = new ZkClient(zkConnection, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
      ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnection), false);
      AdminUtils.createTopic(zkUtils, topicName, numPartitions, replication, new Properties());
    } catch (TopicExistsException e) {
      logger.warn("Met error when creating kakfa topic.", e);
    } finally {
      zkClient.close();
    }
  }
}
