package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.hadoop.input.kafka.KafkaInputUtils.*;

import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used in the KIF repush job to track the offsets of the source kafka topic.
 */
public class KafkaInputOffsetTracker {
  private static final Logger logger = LogManager.getLogger(KafkaInputOffsetTracker.class);
  private Map<Integer, Long> latestOffsetsAtBeginning;
  private Map<Integer, Long> latestOffsetsAtEnd;
  private final String kafkaTopic;
  private final KafkaClientFactory consumerFactory;
  private final TopicManager topicManager;

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
