package com.linkedin.venice.controller.kafka.offsets;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.ByteUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * This class is used to maintain the offset for admin topic.
 * The thread-safety depends on the safety of the internal {@link KafkaConsumerWrapper}
 */
public class AdminOffsetManager implements OffsetManager, Closeable {
  private static final Logger LOGGER = Logger.getLogger(AdminOffsetManager.class);
  // Kafka Consumer could not be accessed by multiple threads at the same time
  private final KafkaConsumerWrapper consumer;

  public AdminOffsetManager(VeniceConsumerFactory consumerFactory, Properties kafkaConsumerProperties) {
    this.consumer = consumerFactory.getConsumer(kafkaConsumerProperties);
  }

  @Override
  public void recordOffset(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
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
