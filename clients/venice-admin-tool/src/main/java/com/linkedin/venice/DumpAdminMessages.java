package com.linkedin.venice;

import static com.linkedin.venice.ConfigKeys.KAFKA_AUTO_OFFSET_RESET_CONFIG;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_ENABLE_AUTO_COMMIT_CONFIG;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;


/**
 * The design consideration to consume in admin tool directly instead of letting controller to
 * consume the required admin message and return them back to admin tool:
 * 1. Controller may not have all the latest schemas for {@link AdminOperation}, then controller
 * couldn't print human-readable string for unknown admin operations;
 * 2. Don't introduce any overhead to controller since sometimes controller itself is already
 * very slow;
 */
public class DumpAdminMessages {
  public static class AdminOperationInfo {
    public long offset;
    public int schemaId;
    public String operationType;
    public String adminOperation;
    public String publishTimeStamp;
    public String producerMetadata;
  }

  public static List<AdminOperationInfo> dumpAdminMessages(
      PubSubConsumerAdapter consumer,
      String clusterName,
      long startingOffset,
      int messageCnt) {
    String adminTopic = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    // include the message with startingOffset
    PubSubTopicPartition adminTopicPartition = new PubSubTopicPartitionImpl(
        pubSubTopicRepository.getTopic(adminTopic),
        AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    consumer.subscribe(adminTopicPartition, startingOffset - 1);
    AdminOperationSerializer deserializer = new AdminOperationSerializer();
    List<AdminOperationInfo> adminOperations = new ArrayList<>();
    int curMsgCnt = 0;
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    KafkaMessageEnvelope messageEnvelope = null;
    while (curMsgCnt < messageCnt) {
      Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> records =
          consumer.poll(1000); // 1 second
      if (records.isEmpty()) {
        break;
      }
      Iterator<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> recordsIterator =
          Utils.iterateOnMapOfLists(records);
      while (recordsIterator.hasNext()) {
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = recordsIterator.next();
        messageEnvelope = record.getValue();
        // check message type
        MessageType messageType = MessageType.valueOf(messageEnvelope);
        if (messageType.equals(MessageType.PUT)) {
          if (++curMsgCnt > messageCnt) {
            break;
          }
          Put put = (Put) messageEnvelope.payloadUnion;
          AdminOperation adminMessage = deserializer.deserialize(put.putValue, put.schemaId);
          AdminOperationInfo adminOperationInfo = new AdminOperationInfo();
          adminOperationInfo.offset = record.getOffset();
          adminOperationInfo.schemaId = put.schemaId;
          adminOperationInfo.adminOperation = adminMessage.toString();
          adminOperationInfo.operationType = AdminMessageType.valueOf(adminMessage).name();
          adminOperationInfo.publishTimeStamp =
              dateFormat.format(new Date(messageEnvelope.producerMetadata.messageTimestamp));
          adminOperationInfo.producerMetadata = messageEnvelope.producerMetadata.toString();
          adminOperations.add(adminOperationInfo);
        }
      }
      if (curMsgCnt > messageCnt) {
        break;
      }
    }
    return adminOperations;
  }

  public static Properties getPubSubConsumerProperties(String kafkaUrl, Properties pubSubConsumerProperties) {
    // ssl related config will be provided by param: kafkaConsumerProperties
    final String securityProtocolConfig = "security.protocol";
    final String sslProtocol = "SSL";
    String securityProtocol = pubSubConsumerProperties.getProperty(securityProtocolConfig);
    if (securityProtocol != null && securityProtocol.equals(sslProtocol)) {
      List<String> requiredSSLConfigList = new ArrayList<>();
      requiredSSLConfigList.add("ssl.key.password");
      requiredSSLConfigList.add("ssl.keymanager.algorithm");
      requiredSSLConfigList.add("ssl.keystore.location");
      requiredSSLConfigList.add("ssl.keystore.password");
      requiredSSLConfigList.add("ssl.keystore.type");
      requiredSSLConfigList.add("ssl.protocol");
      requiredSSLConfigList.add("ssl.secure.random.implementation");
      requiredSSLConfigList.add("ssl.trustmanager.algorithm");
      requiredSSLConfigList.add("ssl.truststore.location");
      requiredSSLConfigList.add("ssl.truststore.password");
      requiredSSLConfigList.add("ssl.truststore.type");
      requiredSSLConfigList.forEach(configProperty -> {
        if (pubSubConsumerProperties.getProperty(configProperty) == null) {
          throw new VeniceException("Consumer config property: " + configProperty + " is required");
        }
      });
    }

    pubSubConsumerProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
    pubSubConsumerProperties.setProperty(KAFKA_AUTO_OFFSET_RESET_CONFIG, "earliest");
    pubSubConsumerProperties.setProperty(KAFKA_ENABLE_AUTO_COMMIT_CONFIG, "false");
    return pubSubConsumerProperties;
  }
}
