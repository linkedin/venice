package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.config.VeniceStorePartitionInformation;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;


/**
 * Created by mwise on 2/9/16.
 */
public class TopicCreator {

  private String zkConnection;
  private int sessionTimeoutMs;
  private int connectionTimeoutMs;

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
      logger.warn(e.getMessage());
      e.printStackTrace();
    } finally {
      zkClient.close();
    }
  }

  public void createTopic(VeniceStorePartitionInformation storeInfo){
    createTopic(storeInfo.getStoreName(),storeInfo.getPartitionsCount(), storeInfo.getReplicationFactor());
  }

}
