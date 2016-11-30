package com.linkedin.venice.controller.kafka.offsets;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.ByteUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * This class is used to maintain the offset for admin topic.
 * The thread-safety depends on the safety of the internal {@link KafkaConsumerWrapper}
 */
public class AdminOffsetManager implements OffsetManager, Closeable {
  private static final Logger LOGGER = Logger.getLogger(AdminOffsetManager.class);
  /**
   * Only persist states for top {@link #STATE_PERSIST_NUM} producer guids, because of
   * offset+meta size limitation defined in Kafka Broker.
    */
  private static final int STATE_PERSIST_NUM = 5;
  // Kafka Consumer could not be accessed by multiple threads at the same time
  private final KafkaConsumerWrapper consumer;

  public AdminOffsetManager(VeniceConsumerFactory consumerFactory, Properties kafkaConsumerProperties) {
    this.consumer = consumerFactory.getConsumer(kafkaConsumerProperties);
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
  public void recordOffset(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    filterOldStates(record, STATE_PERSIST_NUM);
    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.getOffset(), ByteUtils.toHexString(record.toBytes()));
    consumer.commitSync(topicName, partitionId, offsetAndMetadata);
    LOGGER.debug("Set offset: " + record + " for topic: " + topicName);
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
    OffsetAndMetadata offsetAndMetadata = consumer.committed(topicName, partitionId);
    if (null == offsetAndMetadata) {
      return new OffsetRecord();
    }
    return new OffsetRecord(ByteUtils.fromHexString(offsetAndMetadata.metadata()));
  }

  @Override
  public void close() {
    this.consumer.close();
  }
}
