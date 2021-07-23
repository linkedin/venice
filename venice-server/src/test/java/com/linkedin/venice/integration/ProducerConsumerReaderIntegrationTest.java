package com.linkedin.venice.integration;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.ConfigKeys.*;


/**
 * This class spins up ZK and Kafka, and a complete Venice cluster, and tests that
 * messages produced into Kafka can be read back out of the storage node.
 * All over SSL from the thin client through the router to the storage node.
 * TODO: it'd be probably better to move it into integration test
 */
public class ProducerConsumerReaderIntegrationTest {

  private VeniceClusterWrapper veniceCluster;
  private String topicName;
  private String storeName;
  private int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

  // TODO: Make serializers parameterized so we test them all.
  private VeniceWriter<Object, Object, byte[]> veniceWriter;
  private AvroGenericStoreClient<String, Object> storeClient;

  @DataProvider(name = "PersistenceType")
  public static Object[] persistenceType() {
    return new Object[]{PersistenceType.ROCKS_DB};
  }

  @BeforeMethod
  public void setUp() throws VeniceClientException {
    Utils.thisIsLocalhost();
  }

  @AfterMethod
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(storeClient);
    Utils.closeQuietlyWithErrorLogged(veniceWriter);
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test
  public void TestHelixCustomizedViewRepositorytestEndToEndProductionAndReading() throws Exception {
    boolean sslEnabled = true;
    veniceCluster = ServiceFactory.getVeniceCluster(sslEnabled);
    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion();
    topicName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(topicName);
    String routerUrl = veniceCluster.getRandomRouterSslURL();
    // TODO: Make serializers parameterized so we test them all.
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(stringSchema);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(stringSchema);

    veniceWriter = TestUtils.getVeniceWriterFactory(veniceCluster.getKafka().getAddress())
        .createVeniceWriter(topicName, keySerializer, valueSerializer);
    storeClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
        .setVeniceURL(routerUrl)
        .setSslEngineComponentFactory(SslUtils.getLocalSslFactory()));

    String key = TestUtils.getUniqueString("key");
    String value = TestUtils.getUniqueString("value");

    try {
      storeClient.get(key).get();
      Assert.fail("Not online instances exist in cluster, should throw exception for this read operation.");
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof VeniceClientHttpException)) {
        throw e;
      }
      // Expected result. Because right now status of node is "BOOTSTRAP" so can not find any online instance to read.
    }

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    veniceWriter.put(key, value, valueSchemaId).get();
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<String, String>());

    veniceCluster.waitVersion(storeName, Version.parseVersionFromKafkaTopicName(topicName));

    Object newValue = storeClient.get(key).get();
    Assert.assertEquals(newValue.toString(), value, "The key '" + key + "' does not contain the expected value!");
  }

  @Test(dataProvider = "PersistenceType")
  public void testMultipleWriterWritingDuplicateData(PersistenceType persistenceType) throws Exception {
    veniceCluster = ServiceFactory.getVeniceCluster(1, 0, 1);
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, persistenceType);
    veniceCluster.addVeniceServer(new Properties(), serverProperties);
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion();
    topicName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(topicName);
    storeClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
        .setVeniceURL(veniceCluster.getRandomRouterSslURL())
        .setSslEngineComponentFactory(SslUtils.getLocalSslFactory()));

    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(stringSchema);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(stringSchema);
    Properties veniceWriterProperties = new Properties();
    Properties defaultProps = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, creationResponse.getKafkaBootstrapServers());
    defaultProps.put(KAFKA_BOOTSTRAP_SERVERS, creationResponse.getKafkaBootstrapServers());
    VeniceWriterFactory writerFactory = TestUtils.getVeniceWriterFactory(defaultProps);
    int partitionCount =
        new ApacheKafkaProducer(new VeniceProperties(veniceWriterProperties)).getNumberOfPartitions(topicName);
    DefaultVenicePartitioner partitioner = new DefaultVenicePartitioner();
    veniceWriterProperties.put(GuidUtils.GUID_GENERATOR_IMPLEMENTATION,
        GuidUtils.DETERMINISTIC_GUID_GENERATOR_IMPLEMENTATION);
    veniceWriterProperties.put(ConfigKeys.PUSH_JOB_MAP_REDUCE_JT_ID, 0L);
    veniceWriterProperties.put(ConfigKeys.PUSH_JOB_MAP_REDUCE_JOB_ID, 0L);
    VeniceWriterFactory veniceTestWriterFactory = TestUtils.getVeniceWriterFactory(veniceWriterProperties);
    ArrayList<Pair<String, String>> data = new ArrayList<>();
    // Generate a set of records that belongs to the same partition
    int k = 0;
    while (data.size() < 10) {
      String key = Integer.toString(k);
      if (partitioner.getPartitionId(keySerializer.serialize(topicName, key), partitionCount) == 0) {
        data.add(new Pair<>(key, TestUtils.getUniqueString("key" + key)));
      }
      k++;
    }

    try (VeniceWriter<byte[], byte[], byte[]> basicVeniceWriter = writerFactory.createBasicVeniceWriter(topicName)) {
      basicVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
      try (VeniceWriter<String, String, byte[]> veniceWriter1 =
               veniceTestWriterFactory.createVeniceWriter(topicName, keySerializer, valueSerializer);
           VeniceWriter<String, String, byte[]> veniceWriter2 =
               veniceTestWriterFactory.createVeniceWriter(topicName, keySerializer, valueSerializer)) {
        // Two writers with the same Guid that write the same data (same partition) to the kafka topic at different rates.
        // In the end the second writer will have produced only half of the records produced by the first writer.
        int j = 0;
        for (int i = 0; i < data.size(); i++) {
          veniceWriter1.put(data.get(i).getFirst(), data.get(i).getSecond(), valueSchemaId);
          if (i % 2 == 0) {
            veniceWriter2.put(data.get(j).getFirst(), data.get(j).getSecond(), valueSchemaId);
            j++;
          }
        }
      }
      basicVeniceWriter.broadcastEndOfPush(new HashMap<>());
    }

    veniceCluster.waitVersion(storeName, Version.parseVersionFromKafkaTopicName(topicName));

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      // Read and verify the ingested data.
      for (Pair<String, String> keyValue : data) {
        Object value = storeClient.get(keyValue.getFirst()).get();
        Assert.assertNotNull(value, "Value should not be null for key: " + keyValue.getFirst());
        Assert.assertEquals(value.toString(), keyValue.getSecond(), "Unexpected key value pair!");
      }
    });
  }
}
