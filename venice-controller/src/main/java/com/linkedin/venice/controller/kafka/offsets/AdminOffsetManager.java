package com.linkedin.venice.controller.kafka.offsets;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TrieBasedPathResourceRegistry;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is used to maintain the offset for admin topic.
 */
public class AdminOffsetManager implements OffsetManager {
  private static final Logger LOGGER = Logger.getLogger(AdminOffsetManager.class);
  /**
   * Only persist states for top {@link #STATE_PERSIST_NUM} producer guids, because of
   * offset+meta size limitation defined in Kafka Broker.
    */
  private static final int STATE_PERSIST_NUM = 5;
  /**
   * ZK node name used to store {@link OffsetRecord} for admin topic
   */
  private static final String ADMIN_TOPIC_OFFSET_NODE = "AdminTopicOffsetRecord";
  /**
   * Pattern for absolute path of ZK node for admin topic of all the clusters
   */
  private static final String ADMIN_TOPIC_OFFSET_NODE_PATH_PATTERN = getAdminTopicOffsetNodePathPattern();

  /**
   * Zookeeper update retry times
   */
  private static final int ZK_UPDATE_RETRY = 3;

  private ZkBaseDataAccessor<OffsetRecord> dataAccessor;

  public AdminOffsetManager(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    // Register serializer
    adapterSerializer.registerSerializer(ADMIN_TOPIC_OFFSET_NODE_PATH_PATTERN, new OffsetRecordSerializer());
    zkClient.setZkSerializer(adapterSerializer);
    dataAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  public static String getAdminTopicOffsetNodePathPattern() {
    return getAdminTopicOffsetNodePathForCluster(TrieBasedPathResourceRegistry.WILDCARD_MATCH_ANY);
  }

  public static String getAdminTopicOffsetNodePathForCluster(String clusterName) {
    StringBuilder sb = new StringBuilder("/")
        .append(clusterName)
        .append("/")
        .append(ADMIN_TOPIC_OFFSET_NODE);
    return sb.toString();
  }

  /**
   * Only keep the latest 'numToKeep' producer Guid states based on {@link com.linkedin.venice.kafka.protocol.state.ProducerPartitionState#messageTimestamp}.
   * @param record
   * @param numToKeep
   * @return
   */
  protected static void filterOldStates(OffsetRecord record, int numToKeep) {
    if (numToKeep <= 0) {
      throw new IllegalArgumentException("'numToKeep' should be positive");
    }
    Map<CharSequence, ProducerPartitionState> producerStates = record.getProducerPartitionStateMap();
    if (producerStates.size() <= numToKeep) {
      return;
    }
    Map<CharSequence, ProducerPartitionState> filteredProducerStates = producerStates.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue(
            (o1, o2) -> {
              if (o1.messageTimestamp > o2.messageTimestamp) {
                return -1;
              } else if (o1.messageTimestamp == o2.messageTimestamp) {
                return 0;
              } else {
                return 1;
              }
            }
        ))
        .limit(numToKeep)
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (e1, e2) -> e1,
            HashMap::new
        ));
    record.setProducerPartitionStateMap(filteredProducerStates);
  }

  @Override
  public void put(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    String clusterName = AdminTopicUtils.getClusterNameFromTopicName(topicName);
    String nodePath = getAdminTopicOffsetNodePathForCluster(clusterName);
    filterOldStates(record, STATE_PERSIST_NUM);
    // Persist offset to Zookeeper
    HelixUtils.update(dataAccessor, nodePath, record, ZK_UPDATE_RETRY);
    String logMessagePrefix = "Persisted offset record to ZK for topic: " + topicName + ", partition id: " + partitionId + ", ";
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(logMessagePrefix + "record: " + record.toDetailedString());
    } else {
      LOGGER.info(logMessagePrefix + "offset: " + record.getOffset() + " (full OffsetRecord logged at debug level).");
    }
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    /**
     * TODO: we can consider to set the offset to be {@link OffsetRecord.NON_EXISTENT_OFFSET} if necessary
      */
    throw new VeniceException("clearOffset is not supported yet!");
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId) throws VeniceException {
    String clusterName = AdminTopicUtils.getClusterNameFromTopicName(topicName);
    String nodePath = getAdminTopicOffsetNodePathForCluster(clusterName);
    OffsetRecord offsetRecord = dataAccessor.get(nodePath, null, AccessOption.PERSISTENT);
    if (null == offsetRecord) {
      offsetRecord = new OffsetRecord();
    }
    LOGGER.info("Looked up last offset record from ZK for topic:" + topicName + ", partition id: " + partitionId +
        ", record: " + offsetRecord.toDetailedString());
    return offsetRecord;
  }
}
