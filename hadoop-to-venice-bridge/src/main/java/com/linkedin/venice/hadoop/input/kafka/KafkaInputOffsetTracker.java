package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.hadoop.input.kafka.KafkaInputUtils.*;


/**
 * This class is used in the KIF repush job to track the offsets of the source kafka topic.
 */
public class KafkaInputOffsetTracker {
  private static final Logger logger = Logger.getLogger(KafkaInputOffsetTracker.class);
  private Map<Integer, Long> latestOffsetsAtBeginning;
  private Map<Integer, Long> latestOffsetsAtEnd;
  private String kafkaTopic;
  private KafkaClientFactory consumerFactory;
  private TopicManager topicManager;

  public KafkaInputOffsetTracker(JobConf config) {
    consumerFactory = getConsumerFactory(config);
    topicManager = new TopicManager(consumerFactory);
    this.kafkaTopic = config.get(KAFKA_INPUT_TOPIC);
  }

  /**
   * Call this function at the beginning of push job to fetch and cache the latest offset at that time.
   */
  public void markLatestOffsetAtBeginning() {
    latestOffsetsAtBeginning = topicManager.getTopicLatestOffsets(kafkaTopic);
  }

  /**
   * Call this function at the end (of MR phase) of push job to fetch and cache the latest offset at that time.
   */
  public void markLatestOffsetAtEnd() {
    latestOffsetsAtEnd = topicManager.getTopicLatestOffsets(kafkaTopic);
  }

  /**
   * This compares the 2 latestOffsetsAtBeginning and latestOffsetsAtEnd maps and returns a boolean to indicate
   * if the map contents are same or not.
   * @return
   */
  public boolean compareOffsetsAtBeginningAndEnd() {
    if (latestOffsetsAtBeginning == null || latestOffsetsAtEnd == null) {
      return false;
    }
    return latestOffsetsAtEnd.equals(latestOffsetsAtBeginning);
  }

  public void close() {
    try {
      topicManager.close();
    } catch (Exception e) {
      logger.error("Exception in closing TopicManager: ", e);
    }
  }
}
