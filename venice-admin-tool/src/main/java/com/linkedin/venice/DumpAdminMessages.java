package com.linkedin.venice;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


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
  }

  public static List<AdminOperationInfo> dumpAdminMessages(String kafkaUrl, String clusterName,
      Properties consumerProperties, long startingOffset, int messageCnt) {
    consumerProperties = getKafkaConsumerProperties(kafkaUrl, consumerProperties);
    String adminTopic = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    try (KafkaConsumerWrapper consumer = new ApacheKafkaConsumer(consumerProperties)) {
      OffsetRecord startOffset = new OffsetRecord();
      // include the message with startingOffset
      startOffset.setOffset(startingOffset - 1);
      consumer.subscribe(adminTopic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, startOffset);
      AdminOperationSerializer deserializer = new AdminOperationSerializer();
      List<AdminOperationInfo> adminOperations = new ArrayList<>();
      int curMsgCnt = 0;
      while (curMsgCnt < messageCnt) {
        ConsumerRecords records = consumer.poll(1000); // 1 second
        if (records.isEmpty()) {
          break;
        }
        Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> recordsIterator = records.iterator();
        while (recordsIterator.hasNext()) {
          ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = recordsIterator.next();
          KafkaMessageEnvelope messageEnvelope = record.value();
          // check message type
          MessageType messageType = MessageType.valueOf(messageEnvelope);
          if (MessageType.PUT != messageType) {
            continue;
          }
          if (++curMsgCnt > messageCnt) {
            break;
          }
          Put put = (Put) messageEnvelope.payloadUnion;
          AdminOperation adminMessage = deserializer.deserialize(put.putValue.array(), put.schemaId);
          AdminOperationInfo adminOperationInfo = new AdminOperationInfo();
          adminOperationInfo.offset = record.offset();
          adminOperationInfo.schemaId = put.schemaId;
          adminOperationInfo.adminOperation = adminMessage.toString();
          adminOperationInfo.operationType = AdminMessageType.valueOf(adminMessage).name();
          adminOperations.add(adminOperationInfo);
        }
        if (curMsgCnt > messageCnt) {
          break;
        }
      }
      return adminOperations;
    }
  }

  public static Properties getKafkaConsumerProperties(String kafkaUrl, Properties kafkaConsumerProperties) {
    // ssl related config will be provided by param: kafkaConsumerProperties
    final String securityProtocolConfig = "security.protocol";
    final String sslProtocol = "SSL";
    String securityProtocol = kafkaConsumerProperties.getProperty(securityProtocolConfig);
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
      requiredSSLConfigList.forEach( configProperty -> {
        if (null == kafkaConsumerProperties.getProperty(configProperty)) {
          throw new VeniceException("Consumer config property: " + configProperty + " is required");
        }
      });
    }

    kafkaConsumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        KafkaKeySerializer.class.getName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaValueSerializer.class.getName());

    return kafkaConsumerProperties;
  }
}
