package com.linkedin.venice;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The design consideration to consume in admin tool directly instead of letting controller to
 * consume the required admin message and return them back to admin tool:
 * 1. Controller may not have all the latest schemas for {@link AdminOperation}, then controller
 * couldn't print human-readable string for unknown admin operations;
 * 2. Don't introduce any overhead to controller since sometimes controller itself is already
 * very slow;
 */
public class DumpAdminMessages {
  private static final Logger LOGGER = LogManager.getLogger(DumpAdminMessages.class);

  /**
   * Dumps admin messages from the admin topic for the given cluster.
   * Messages are logged as they are received instead of being buffered.
   *
   * @param consumer the PubSub consumer to use
   * @param clusterName the name of the cluster
   * @param startingPosition the starting position to consume from
   * @param messageCnt the maximum number of messages to consume
   * @return the number of messages processed
   */
  public static int dumpAdminMessages(
      PubSubConsumerAdapter consumer,
      String clusterName,
      PubSubPosition startingPosition,
      int messageCnt) {
    String adminTopic = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition adminTopicPartition = new PubSubTopicPartitionImpl(
        pubSubTopicRepository.getTopic(adminTopic),
        AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    consumer.subscribe(adminTopicPartition, startingPosition, true);
    AdminOperationSerializer deserializer = new AdminOperationSerializer();
    int curMsgCnt = 0;
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    KafkaMessageEnvelope messageEnvelope = null;
    while (curMsgCnt < messageCnt) {
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> records = consumer.poll(1000); // 1 second
      if (records.isEmpty()) {
        break;
      }
      Iterator<DefaultPubSubMessage> recordsIterator = Utils.iterateOnMapOfLists(records);
      while (recordsIterator.hasNext()) {
        DefaultPubSubMessage record = recordsIterator.next();
        messageEnvelope = record.getValue();
        // check message type
        MessageType messageType = MessageType.valueOf(messageEnvelope);
        if (messageType.equals(MessageType.PUT)) {
          if (curMsgCnt >= messageCnt) {
            break;
          }
          Put put = (Put) messageEnvelope.payloadUnion;
          AdminOperation adminMessage = deserializer.deserialize(put.putValue, put.schemaId);
          String operationType = AdminMessageType.valueOf(adminMessage).name();
          String publishTimeStamp = dateFormat.format(new Date(messageEnvelope.producerMetadata.messageTimestamp));
          // Log message as it is received
          LOGGER.info(
              "Position:{}; Type:{}; SchemaId:{}; Timestamp:{}; ProducerMd:{}; Operation:{}",
              record.getPosition(),
              operationType,
              put.schemaId,
              publishTimeStamp,
              messageEnvelope.producerMetadata,
              adminMessage);
          curMsgCnt++;
        }
      }
      if (curMsgCnt >= messageCnt) {
        break;
      }
    }
    LOGGER.info("Total admin messages processed: {}", curMsgCnt);
    return curMsgCnt;
  }
}
