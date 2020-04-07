package com.linkedin.venice.etl.client;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.etl.source.VeniceKafkaSource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.gobblin.configuration.State;
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

import com.typesafe.config.Config;

import lombok.ToString;

import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.etl.source.VeniceKafkaSource.*;
import static com.linkedin.venice.utils.Utils.*;
import static org.apache.gobblin.configuration.ConfigurationKeys.*;


public class VeniceKafkaConsumerClient extends AbstractBaseKafkaConsumerClient {
  private static final Logger logger = Logger.getLogger(VeniceKafkaConsumerClient.class);

  private static final int MAX_ATTEMPTS_FOR_EMPTY_POLLING = 3;
  private static final String KAFKA_CONSUMER_POLLING_TIMEOUT_MS = "kafka.consumer.polling.timeout.ms";
  private static final int DEFAULT_KAFKA_CONSUMER_POLLING_TIMEOUT_MS = 5000; // default 5s
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
  private static final String GROUP_ID_FORMAT = "%s_%s";

  private Optional<SSLFactory> sslFactory;
  private String fabricName;
  private String veniceControllerUrls;
  private Set<String> veniceStoreNames;
  private Set<String> futureETLEnabledStores;

  private Map<String, ControllerClient> storeToControllerClient;
  private final KafkaConsumerWrapper veniceKafkaConsumer;

  public VeniceKafkaConsumerClient(Config baseConfig) {
    super(baseConfig);
    this.fabricName = baseConfig.getString(FABRIC_NAME);
    this.veniceControllerUrls = baseConfig.getString(VENICE_CONTROLLER_URLS);
    this.storeToControllerClient = new HashMap<>();
    this.veniceStoreNames = new HashSet<>();
    String veniceStoreNamesList = baseConfig.getString(VENICE_STORE_NAME);
    String[] storeNames = veniceStoreNamesList.split(VENICE_STORE_NAME_SEPARATOR);
    for (String token : storeNames) {
      veniceStoreNames.add(token.trim());
    }
    if (null == veniceStoreNames || 0 == veniceStoreNames.size()) {
      throw new VeniceException("No store name specified when creating VeniceKafkaConsumerClient");
    }

    kafkaBoostrapServers = ConfigUtils.getString(baseConfig, VeniceKafkaSource.KAFKA_BOOSTRAP_SERVERS, null);
    if (null == kafkaBoostrapServers) {
      throw new VeniceException("No kafka bootstrap servers specified when creating VeniceKafkaConsumerClient");
    }

    kafkaConsumerPollingTimeoutMs = ConfigUtils.getInt(baseConfig, KAFKA_CONSUMER_POLLING_TIMEOUT_MS,
        DEFAULT_KAFKA_CONSUMER_POLLING_TIMEOUT_MS);
    Properties veniceKafkaProp = getKafkaConsumerProperties(kafkaBoostrapServers);

    this.futureETLEnabledStores = new HashSet<>();
    try {
      String futureETLStores = baseConfig.getString(FUTURE_ETL_ENABLED_STORES);
      String[] tokens = futureETLStores.split(VENICE_STORE_NAME_SEPARATOR);
      for (String token : tokens) {
        futureETLEnabledStores.add(token.trim());
      }
    } catch (Exception e) {
      logger.warn("The config for future-etl-enabled-stores doesn't exist.");
    }
    Properties props = new Properties();
    props.setProperty(SSL_KEY_STORE_PROPERTY_NAME, baseConfig.getString(SSL_KEY_STORE_PROPERTY_NAME));
    props.setProperty(SSL_TRUST_STORE_PROPERTY_NAME, baseConfig.getString(SSL_TRUST_STORE_PROPERTY_NAME));
    props.setProperty(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, baseConfig.getString(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME));
    props.setProperty(SSL_KEY_PASSWORD_PROPERTY_NAME, baseConfig.getString(SSL_KEY_PASSWORD_PROPERTY_NAME));
    veniceKafkaProp.putAll(setUpSSLProperties(new VeniceProperties(props)));
    this.veniceKafkaConsumer = new ApacheKafkaConsumer(veniceKafkaProp);
    this.sslFactory = Optional.of(SslUtils.getSSLFactory(veniceKafkaProp, baseConfig.getString(SSL_FACTORY_CLASS_NAME)));
  }

  public static Map<String, ControllerClient> getControllerClients(Set<String> storeNames, String veniceControllerUrls, Optional<SSLFactory> sslFactory) {
    Map<String, ControllerClient> controllerClientMap = new HashMap<>(storeNames.size());
    // Reuse the same controller client for stores in the same cluster
    Map<String, ControllerClient> clusterToControllerClient = new HashMap<>();
    for (String veniceStoreName: storeNames) {
      ControllerResponse clusterDiscoveryResponse = ControllerClient.discoverCluster(veniceControllerUrls, veniceStoreName, sslFactory);
      if (clusterDiscoveryResponse.isError()) {
        throw new VeniceException("Get error in clusterDiscoveryResponse:" + clusterDiscoveryResponse.getError());
      } else {
        String clusterName = clusterDiscoveryResponse.getCluster();
        ControllerClient controllerClient = clusterToControllerClient.computeIfAbsent(clusterName, k -> new ControllerClient(k, veniceControllerUrls, sslFactory));
        controllerClientMap.put(veniceStoreName, controllerClient);
        logger.info("Found cluster: " + clusterDiscoveryResponse.getCluster() + " for store: " + veniceStoreName);
      }
    }
    return controllerClientMap;
  }

  /**
   * Set up Ssl Properties needed to talk to acled controller
   * @param props
   * @return properties which contains all needed information.
   * TODO: unify the code for setting up ssl properties between here and KafkaPushJob class in H2V.
   */
  public static Properties setUpSSLProperties(VeniceProperties props) {
    Properties sslProperties = new Properties();
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
      String trustStorePath = writeToTempFile(credentials.getSecretKey(new Text(props.getString(SSL_TRUST_STORE_PROPERTY_NAME)))).getCanonicalPath();
      String keyStorePath = writeToTempFile(credentials.getSecretKey(new Text(props.getString(SSL_KEY_STORE_PROPERTY_NAME)))).getCanonicalPath();
      String keyStorePassword =
          new String(credentials.getSecretKey(new Text(props.getString(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME))), UTF_8);
      String keyPassword =
          new String(credentials.getSecretKey(new Text(props.getString(SSL_KEY_PASSWORD_PROPERTY_NAME))), UTF_8);
      sslProperties.setProperty(SSL_KEYSTORE_LOCATION, keyStorePath);
      sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, keyStorePassword);
      sslProperties.setProperty(SSL_TRUSTSTORE_LOCATION, trustStorePath);
      sslProperties.setProperty(SSL_KEY_PASSWORD, keyPassword);
    } catch (IOException e) {
      throw new VeniceException("Error in reading temp file on Azkaban: ", e);
    }
    setDefaultSSLProperties(sslProperties);
    return sslProperties;
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

  private static void setDefaultSSLProperties(Properties properties) {
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, DEFAULT_KAFKA_TRUST_STORE_PASSWORD);
    properties.setProperty(SSL_KEYSTORE_TYPE, DEFAULT_KAFKA_KEY_STORE_TYPE);
    properties.setProperty(SSL_TRUSTSTORE_TYPE, DEFAULT_KAFKA_TRUST_STORE_TYPE);
    properties.setProperty(SSL_SECURE_RANDOM_IMPLEMENTATION, DEFAULT_KAFKA_SECURE_RANDOM_IMPLEMENTATION);
    properties.setProperty(SSL_TRUSTMANAGER_ALGORITHM, DEFAULT_KAFKA_TRUST_MANAGER_ALGORITHM);
    properties.setProperty(SSL_KEYMANAGER_ALGORITHM, DEFAULT_KAFKA_KEY_MANAGER_ALGORITHM);
    properties.setProperty(KAFKA_SECURITY_PROTOCOL, DEFAULT_SECURITY_PROTOCOL);
    properties.setProperty(SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);
    properties.setProperty(SSL_ENABLED, "true");
  }

  private synchronized void createControllersList(Set<String> storeNames) {
    Map<String, ControllerClient> storeToControllers = getControllerClients(storeNames, veniceControllerUrls, sslFactory);
    storeToControllerClient.putAll(storeToControllers);
  }

  @Override
  public List<KafkaTopic> getTopics() {
    /**
     * Mapper/Reducer is not able to reach our controller clusters; but this function would only be invoked in Azkaban,
     * so we lazily create the controller clients list when it's actually needed.
     */
    // get the topic names from Venice controllers for current version etl stores
    createControllersList(veniceStoreNames);
    Set<String> topicNames = new HashSet<>();
    for (String storeName: veniceStoreNames) {
      StoreResponse storeResponse = storeToControllerClient.get(storeName).getStore(storeName);
      StoreInfo storeInfo = storeResponse.getStore();
      // append current version topic
      String topicName = Version.composeKafkaTopic(storeName, storeInfo.getColoToCurrentVersions().get(fabricName));
      topicNames.add(topicName);
      logger.info("Topic name in this ETL pipeline is: " + topicName);
    }

    // get the topic names for future etl stores
    createControllersList(futureETLEnabledStores);
    for (String storeName : futureETLEnabledStores) {
      StoreResponse storeResponse = storeToControllerClient.get(storeName).getStore(storeName);
      StoreInfo storeInfo = storeResponse.getStore();
      // This largest version is across all colos.
      int futureVersion = storeInfo.getLargestUsedVersionNumber();
      int currentVersion = storeInfo.getColoToCurrentVersions().get(fabricName);
      if (futureVersion > currentVersion) {
        String futureTopicName = Version.composeKafkaTopic(storeName, futureVersion);
        topicNames.add(futureTopicName);
        logger.info("Future version topic in this ETL pipeline is: " + futureTopicName);
      } else {
        logger.info("Store " + storeName + " doesn't have a future version running yet. Skipped.");
      }
    }

    /**
     * Filters out topic names which don't exist in kafka topic list
     */
    Map<String, List<PartitionInfo>> topicList = this.veniceKafkaConsumer.listTopics();
    List<KafkaTopic> filteredTopics = new ArrayList<>();
    for (Map.Entry<String, List<PartitionInfo>> topicEntry : topicList.entrySet()) {
      // filter out all topics that don't match the topic name
      if (topicNames.contains(topicEntry.getKey())) {
        State topicSpecificState = new State();
        /**
         * Config {@link EXTRACT_TABLE_NAME_KEY} will determine the table name of a topic; we choose to use version
         * topic name as the table name so that we create a clean ETL isolation between different versions.
         */
        topicSpecificState.appendToSetProp(EXTRACT_TABLE_NAME_KEY, topicEntry.getKey());
        filteredTopics.add(new KafkaTopic(topicEntry.getKey(),
            topicEntry.getValue().stream().map(PARTITION_INFO_TO_KAFKA_PARTITION).collect(Collectors.toList()),
            com.google.common.base.Optional.of(topicSpecificState)));

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
    Map<TopicPartition, Long> offsets = this.veniceKafkaConsumer.beginningOffsets(Arrays.asList(topicPartition));

    if (!offsets.containsKey(topicPartition)) {
      throw new KafkaOffsetRetrievalFailureException(String.format("Failed to get earliest offset for %s", partition));
    }
    return offsets.get(topicPartition);
  }

  @Override
  public long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    TopicPartition topicPartition = new TopicPartition(partition.getTopicName(), partition.getId());
    Map<TopicPartition, Long> offsets = this.veniceKafkaConsumer.endOffsets(Arrays.asList(topicPartition));
    if (!offsets.containsKey(topicPartition)) {
      throw new KafkaOffsetRetrievalFailureException(String.format("Failed to get latest offset for %s", partition));
    }
    return offsets.get(topicPartition);
  }


  @Override
  public Iterator<KafkaConsumerRecord> consume(KafkaPartition partition, long nextOffset, long maxOffset) {
    String topic = partition.getTopicName();
    int partitionId = partition.getId();
    logger.info("Start consuming for topic: " + topic + "; partition: " + partitionId + "; startOffset: " + nextOffset + "; maxOffset: " + maxOffset);
    /**
     * Skip the consuming as there is nothing new in the partition topic
     */
    if (nextOffset >= maxOffset) {
      return null;
    }
    TopicPartition topicPartition = new TopicPartition(topic, partitionId);
    this.veniceKafkaConsumer.assign(Arrays.asList(topicPartition));
    this.veniceKafkaConsumer.seek(topicPartition, nextOffset);

    // put filtered real records into a new list
    List<KafkaConsumerRecord> nonControlMessageRecords = new LinkedList<KafkaConsumerRecord>();
    int putCount = 0;
    int deleteCount = 0;
    int controlMessageCount = 0;
    int polledEmptyRealRecordsCount = 0;

    try {
      do {
        ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords = veniceKafkaConsumer.poll(kafkaConsumerPollingTimeoutMs);
        logger.info("Successfully polled " + consumerRecords.count() + " records from Kafka");
        // remove all control message; get key/value byte array from records
        Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> iterator = consumerRecords.iterator();
        boolean hasRealMessages = false;
        long lastRecordOffset = -1;
        while (iterator.hasNext()) {
          ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = iterator.next();
          lastRecordOffset = record.offset();
          KafkaKey key = record.key();
          KafkaMessageEnvelope value = record.value();
          switch (MessageType.valueOf(value)) {
            case PUT:
              hasRealMessages = true;
              putCount++;
              Put put = (Put) value.payloadUnion;
              // extract the raw value data
              byte[] valueBytes = put.putValue.array();
              nonControlMessageRecords.add(
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
              hasRealMessages = true;
              deleteCount++;
              nonControlMessageRecords.add(
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
            case UPDATE:
              throw new VeniceException("Version Topic has UPDATE messages " + topic + " in partition " + partitionId);
            // control message should be skipped
            case CONTROL_MESSAGE:
              controlMessageCount++;
              break;
            default:
              throw new VeniceException("No such type of message: " + MessageType.valueOf(value));
          }
        }
        // the cases the last poll returns zero records or the polled records doesn't contain any real records
        if (!hasRealMessages) {
          polledEmptyRealRecordsCount++;
        }
        // stop polling more records if already digested more than max offset
        if (lastRecordOffset >= maxOffset) {
          break;
        }
      } while (polledEmptyRealRecordsCount <= MAX_ATTEMPTS_FOR_EMPTY_POLLING);
    } catch (Throwable t) {
      logger.error("Exception on polling records", t);

      // return a special record with a null value if an undecodable record is encountered so that upper layer can
      // handle the error
      return Collections.singleton((KafkaConsumerRecord) new VeniceKafkaRecord(
          new ConsumerRecord<byte[], byte[]>(partition.getTopicName(), partition.getId(), nextOffset,
              null, null),0)).iterator();
    }

    logger.info("Return " + nonControlMessageRecords.size() + " records after decoding; put: " + putCount + " records; "
        + "delete: " + deleteCount + " records; control message: " + controlMessageCount + " records");
    return nonControlMessageRecords.iterator();
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
