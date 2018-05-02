package com.linkedin.venice.etl.client;

import com.linkedin.kafka.liclients.consumer.LiKafkaConsumer;
import com.linkedin.kafka.liclients.consumer.LiKafkaConsumerImpl;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.etl.source.VeniceKafkaSource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.gobblin.kafka.client.AbstractBaseKafkaConsumerClient;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.ToString;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.etl.source.VeniceKafkaSource.*;
import static com.linkedin.venice.utils.Utils.*;


public class VeniceKafkaConsumerClient extends AbstractBaseKafkaConsumerClient {
  private static final Logger logger = Logger.getLogger(VeniceKafkaConsumerClient.class);

  private static final String KAFKA_CONSUMER_POLLING_TIMEOUT_MS = "kafka.consumer.polling.timeout.ms";
  private static final int DEFAULT_KAFKA_CONSUMER_POLLING_TIMEOUT_MS = 300000;
  private final int kafkaConsumerPollingTimeoutMs;

  private final String kafkaBoostrapServers;

  /**
   * Configurations for secured kafka
   */
  private static final String DEFAULT_KAFKA_KEY_STORE_TYPE = "pkcs12";
  private static final String DEFAULT_KAFKA_TRUST_STORE_TYPE = "JKS";
  private static final String DEFAULT_KAFKA_TRUST_STORE_PASSWORD = "changeit";
  private static final String DEFAULT_KAFKA_SECURE_RANDOM_IMPLEMENTATION = "SHA1PRNG";
  private static final String DEFAULT_KAFKA_TRUST_MANAGER_ALGORITHM = "SunX509";
  private static final String DEFAULT_KAFKA_KEY_MANAGER_ALGORITHM = "SunX509";
  private static final String DEFAULT_SECURITY_PROTOCOL = "ssl";
  private static final String SSL_PROTOCOL = "ssl.protocol";
  private static final String DEFAULT_SSL_PROTOCOL = "TLS";
  private static final String DEFAULT_KAFKA_CONSUMER_GROUP_ID_PREFIX = "VeniceETL";

  private static final String SSL_KEY_STORE_PROPERTY_NAME = "ssl.key.store.property.name";
  private static final String SSL_TRUST_STORE_PROPERTY_NAME = "ssl.trust.store.property.name";
  private static final String SSL_KEY_STORE_PASSWORD_PROPERTY_NAME = "ssl.key.store.password.property.name";
  private static final String SSL_KEY_PASSWORD_PROPERTY_NAME= "ssl.key.password.property.name";

  private static final String GROUP_ID_FORMAT = "%s_%s";

  private String fabricName;
  private String[] veniceStoreNames;

  private final ControllerClient controllerClient;
  private final LiKafkaConsumer<KafkaKey, KafkaMessageEnvelope> veniceKafkaConsumer;

  public VeniceKafkaConsumerClient(Config baseConfig) {
    super(baseConfig);

    String veniceCluster = baseConfig.getString(VENICE_CLUSTER_NAME);
    String veniceControllerUrls = baseConfig.getString(VENICE_CONTROLLER_URLS);
    String veniceStoreNamesList = baseConfig.getString(VENICE_STORE_NAME);
    fabricName = baseConfig.getString(FABRIC_NAME);
    veniceStoreNames = veniceStoreNamesList.split(VENICE_STORE_NAME_SEPARATOR);
    controllerClient = new ControllerClient(veniceCluster, veniceControllerUrls);

    if (null == veniceStoreNames || 0 == veniceStoreNames.length) {
      throw new VeniceException("No store name specified when creating VeniceKafkaConsumerClient");
    }

    kafkaBoostrapServers = ConfigUtils.getString(baseConfig, VeniceKafkaSource.KAFKA_BOOSTRAP_SERVERS, null);
    if (null == kafkaBoostrapServers) {
      throw new VeniceException("No kafka bootstrap servers specified when creating VeniceKafkaConsumerClient");
    }

    kafkaConsumerPollingTimeoutMs = ConfigUtils.getInt(baseConfig, KAFKA_CONSUMER_POLLING_TIMEOUT_MS,
        DEFAULT_KAFKA_CONSUMER_POLLING_TIMEOUT_MS);
    Properties veniceKafkaProp = getKafkaConsumerProperties(kafkaBoostrapServers);

    try {
      String tokenFilePath = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
      File tokenFile = new File(tokenFilePath);
      Credentials credentials = Credentials.readTokenStorageFile(tokenFile, new Configuration());
      if (credentials.numberOfSecretKeys() < 4) {
        logger.info("Number of tokens found: " + credentials.numberOfTokens());           // Currently is 4
        logger.warn("Number of secret keys found: " + credentials.numberOfSecretKeys());  // Currently should be 4
        throw new VeniceException("Token file does not contain required secret keys!");
      }

      Charset UTF_8 = Charset.forName("UTF-8");
      String trustStorePath = writeToTempFile(credentials.getSecretKey(new Text(baseConfig.getString(SSL_TRUST_STORE_PROPERTY_NAME)))).getCanonicalPath();
      String keyStorePath = writeToTempFile(credentials.getSecretKey(new Text(baseConfig.getString(SSL_KEY_STORE_PROPERTY_NAME)))).getCanonicalPath();
      String keyStorePassword = new String(credentials.getSecretKey(new Text(baseConfig.getString(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME))), UTF_8);
      String keyPassword = new String(credentials.getSecretKey(new Text(baseConfig.getString(SSL_KEY_PASSWORD_PROPERTY_NAME))), UTF_8);

      veniceKafkaProp.setProperty(SSL_KEYSTORE_LOCATION, keyStorePath);
      veniceKafkaProp.setProperty(SSL_KEYSTORE_PASSWORD, keyStorePassword);
      veniceKafkaProp.setProperty(SSL_TRUSTSTORE_LOCATION, trustStorePath);
      veniceKafkaProp.setProperty(SSL_TRUSTSTORE_PASSWORD, DEFAULT_KAFKA_TRUST_STORE_PASSWORD);
      veniceKafkaProp.setProperty(SSL_KEYSTORE_TYPE, DEFAULT_KAFKA_KEY_STORE_TYPE);
      veniceKafkaProp.setProperty(SSL_TRUSTSTORE_TYPE, DEFAULT_KAFKA_TRUST_STORE_TYPE);
      veniceKafkaProp.setProperty(SSL_KEY_PASSWORD, keyPassword);
      veniceKafkaProp.setProperty(SSL_SECURE_RANDOM_IMPLEMENTATION, DEFAULT_KAFKA_SECURE_RANDOM_IMPLEMENTATION);
      veniceKafkaProp.setProperty(SSL_TRUSTMANAGER_ALGORITHM, DEFAULT_KAFKA_TRUST_MANAGER_ALGORITHM);
      veniceKafkaProp.setProperty(SSL_KEYMANAGER_ALGORITHM, DEFAULT_KAFKA_KEY_MANAGER_ALGORITHM);
      veniceKafkaProp.setProperty(KAFKA_SECURITY_PROTOCOL, DEFAULT_SECURITY_PROTOCOL);
      veniceKafkaProp.setProperty(SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);
    } catch (IOException e) {
      logger.error("error reading or writing to temp file on Azkaban: ", e);
    }

    veniceKafkaConsumer = new LiKafkaConsumerImpl<KafkaKey, KafkaMessageEnvelope>(veniceKafkaProp);
  }

  private static Properties getKafkaConsumerProperties(String kafkaBoostrapServers) {
    Properties kafkaConsumerProperties = new Properties();
    kafkaConsumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBoostrapServers);
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    String groupId = String.format(GROUP_ID_FORMAT, DEFAULT_KAFKA_CONSUMER_GROUP_ID_PREFIX, getHostName());;
    kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        KafkaKeySerializer.class.getName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaValueSerializer.class.getName());

    return kafkaConsumerProperties;
  }

  @Override
  public List<KafkaTopic> getTopics() {
    // get the topic names from Venice controllers
    Set<String> topicNames = new HashSet<String>();
    for (int i = 0; i < veniceStoreNames.length; i++) {
      StoreResponse storeResponse = controllerClient.getStore(veniceStoreNames[i]);
      StoreInfo storeInfo = storeResponse.getStore();
      Map<String, Integer> coloToCurrentVersions = storeInfo.getColoToCurrentVersions();
      String topicName = Version.composeKafkaTopic(veniceStoreNames[i], coloToCurrentVersions.get(fabricName));
      topicNames.add(topicName);
      logger.info("Topic name in this ETL pipeline: " + topicName);
    }

    Map<String, List<PartitionInfo>> topicList = this.veniceKafkaConsumer.listTopics();
    List<KafkaTopic> filteredTopics = new ArrayList<>();
    for (Map.Entry<String, List<PartitionInfo>> topicEntry : topicList.entrySet()) {
      // filter out all topics that don't match the topic name
      if (topicNames.contains(topicEntry.getKey())) {
        filteredTopics.add(new KafkaTopic(topicEntry.getKey(),
            Lists.transform(topicEntry.getValue(), PARTITION_INFO_TO_KAFKA_PARTITION)));

        topicNames.remove(topicEntry.getKey());
      }
    }

    if (topicNames.size() > 0) {
      logger.warn("Some topics (" + topicNames + ") that are expected to be ETL "
          + "are not found in the specified Kafka brokers:" + kafkaBoostrapServers);
    }

    return filteredTopics;
  }

  @Override
  public long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    TopicPartition topicPartition = new TopicPartition(partition.getTopicName(), partition.getId());
    Map<TopicPartition, Long> offsets = this.veniceKafkaConsumer.beginningOffsets(Lists.newArrayList(topicPartition));

    if (!offsets.containsKey(topicPartition)) {
      throw new KafkaOffsetRetrievalFailureException(String.format("Failed to get earliest offset for %s", partition));
    }
    return offsets.get(topicPartition);
  }

  @Override
  public long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    TopicPartition topicPartition = new TopicPartition(partition.getTopicName(), partition.getId());
    Map<TopicPartition, Long> offsets = this.veniceKafkaConsumer.endOffsets(Lists.newArrayList(topicPartition));
    if (!offsets.containsKey(topicPartition)) {
      throw new KafkaOffsetRetrievalFailureException(String.format("Failed to get latest offset for %s", partition));
    }
    return offsets.get(topicPartition);
  }


  @Override
  public Iterator<KafkaConsumerRecord> consume(KafkaPartition partition, long nextOffset, long maxOffset) {
    if (nextOffset >= maxOffset) {
      return null;
    }

    this.veniceKafkaConsumer.assign(Lists.newArrayList(new TopicPartition(partition.getTopicName(), partition.getId())));
    this.veniceKafkaConsumer.seek(new TopicPartition(partition.getTopicName(), partition.getId()), nextOffset);

    try {
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords = veniceKafkaConsumer.poll(kafkaConsumerPollingTimeoutMs);

      // remove all control message; get key/value byte array from records
      List<KafkaConsumerRecord> newRecords = new LinkedList<KafkaConsumerRecord>();
      Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> iterator = consumerRecords.iterator();
      while (iterator.hasNext()) {
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = iterator.next();
        KafkaKey key = record.key();
        if (!key.isControlMessage()) {
          // control message should be skip
          KafkaMessageEnvelope value = record.value();
          switch (MessageType.valueOf(value)) {
            case PUT:
              Put put = (Put) value.payloadUnion;
              // extract the raw value data
              byte[] valueBytes = put.putValue.array();
              newRecords.add(
                  new VeniceKafkaRecord(
                      new ConsumerRecord<byte[], byte[]>(
                          record.topic(),
                          record.partition(),
                          record.offset(),
                          key.getKey(),
                          valueBytes
                      ),
                      put.schemaId
                  )
              );
              break;
            case DELETE:
              newRecords.add(
                  new VeniceKafkaRecord(
                      new ConsumerRecord<byte[], byte[]>(
                          record.topic(),
                          record.partition(),
                          record.offset(),
                          key.getKey(),
                          null
                      ),
                      0
                  )
              );
              break;
            default:
              throw new VeniceException("no such type of message!");
          }


        }
      }

      return newRecords.iterator();

    } catch (Throwable t) {
      logger.error("Exception on polling records", t);

      // return a special record with a null value if an undecodable record is encountered so that upper layer can
      // handle the error
      return Collections.singleton((KafkaConsumerRecord) new VeniceKafkaRecord(
          new ConsumerRecord<byte[], byte[]>(partition.getTopicName(), partition.getId(), nextOffset,
              null, null),0)).iterator();
    }
  }

  @Override
  public void close() throws IOException {
    this.veniceKafkaConsumer.close();
  }

  /**
   * Write data to a temporary file
   * @param contents the content to write
   * @return the file object pointing to the temporary file
   * @throws IOException
   */
  private static File writeToTempFile(byte[] contents) throws IOException {
    java.nio.file.Path path = Files.createTempFile(null, null);
    IOUtils.copyBytes(new ByteArrayInputStream(contents), Files.newOutputStream(path), contents.length, true);

    File file = path.toFile();
    file.deleteOnExit();

    if (contents.length != file.length() || !file.setReadable(true, true)) {
      throw new VeniceException("Unable to create or chmod file " + path);
    }

    logger.debug("Created file as " + path.toAbsolutePath() + " of size " + contents.length + " bytes");

    return file;
  }

  private static final Function<PartitionInfo, KafkaPartition> PARTITION_INFO_TO_KAFKA_PARTITION =
      new Function<PartitionInfo, KafkaPartition>() {
        @Override
        public KafkaPartition apply(@Nonnull PartitionInfo partitionInfo) {
          return new KafkaPartition.Builder().withId(partitionInfo.partition())
              .withTopicName(partitionInfo.topic())
              .withLeaderId(partitionInfo.leader().id())
              .withLeaderHostAndPort(partitionInfo.leader().host(), partitionInfo.leader().port())
              .build();
        }
      };

  public static class VeniceKafkaConsumerClientFactory implements GobblinKafkaConsumerClientFactory {
    @Override
    public GobblinKafkaConsumerClient create(Config config) {
      return new VeniceKafkaConsumerClient(config);
    }
  }

  @ToString
  public static class VeniceKafkaRecord implements ByteArrayBasedKafkaRecord {
    private final ConsumerRecord<byte[], byte[]> consumerRecord;

    int schemaId;

    private VeniceKafkaRecord(ConsumerRecord<byte[], byte[]> consumerRecord, int schemaId) {
      this.consumerRecord = consumerRecord;
      this.schemaId = schemaId;
    }

    @Override
    public byte[] getKeyBytes() {
      return consumerRecord.key();
    }

    @Override
    public byte[] getMessageBytes() {
      return consumerRecord.value();
    }

    @Override
    public long getValueSizeInBytes() {
      if (null == consumerRecord.value()) return 0;
      return consumerRecord.value().length;
    }

    @Override
    public long getNextOffset() {
      return consumerRecord.offset() + 1l;
    }

    @Override
    public long getOffset() {
      return consumerRecord.offset();
    }

    public int getSchemaId() { return schemaId; }
  }

}
