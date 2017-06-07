package com.linkedin.venice.integration;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroStoreClientFactory;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

/**
 * This class spins up ZK and Kafka, and a complete Venice cluster, and tests that
 * messages produced into Kafka can be read back out of the storage node.
 * All over SSL from the thin client through the router to the storage node.
 */
public class ProducerConsumerReaderIntegrationTest {

  private VeniceClusterWrapper veniceCluster;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;

  // TODO: Make serializers parameterized so we test them all.
  private VeniceWriter<Object, Object> veniceWriter;
  private AvroGenericStoreClient<String, Object> storeClient;

  @BeforeMethod
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
    boolean sslEnabled = true;
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(sslEnabled);

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion();
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
    String routerUrl = veniceCluster.getRandomRouterSslURL();

    VeniceProperties clientProps =
            new PropertyBuilder().put(KAFKA_BOOTSTRAP_SERVERS, veniceCluster.getKafka().getAddress())
                    .put(ZOOKEEPER_ADDRESS, veniceCluster.getZk().getAddress())
                    .put(CLUSTER_NAME, veniceCluster.getClusterName()).build();

    // TODO: Make serializers parameterized so we test them all.
    String stringSchema = "\"string\"";
    VeniceSerializer keySerializer = new VeniceAvroGenericSerializer(stringSchema);
    VeniceSerializer valueSerializer = new VeniceAvroGenericSerializer(stringSchema);

    veniceWriter = new VeniceWriter<>(clientProps, storeVersionName, keySerializer, valueSerializer);
    storeClient = AvroStoreClientFactory.getAndStartAvroGenericSslStoreClient(routerUrl, storeName,
        SslUtils.getLocalSslFactory());
  }

  @AfterMethod
  public void cleanUp() {
    storeClient.close();
    if (veniceCluster != null) {
      veniceCluster.close();
    }
  }

  @Test//(retryAnalyzer = FlakyTestRetryAnalyzer.class)
  public void testEndToEndProductionAndReading() throws Exception {

    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    String key = TestUtils.getUniqueString("key");
    String value = TestUtils.getUniqueString("value");

    try {
      storeClient.get(key).get();
      Assert.fail("Not online instances exist in cluster, should throw exception for this read operation.");
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof VeniceClientHttpException)){
        throw e;
      }
      // Expected result. Because right now status of node is "BOOTSTRAP" so can not find any online instance to read.
    }

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    veniceWriter.put(key, value, valueSchemaId).get();
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<String,String>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });

    // Read (but make sure Router is up-to-date with new version)
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, ()->{
      try {
        storeClient.get(key).get();
      } catch (Exception e){
        return false;
      }
      return true;
    });

    Object newValue = storeClient.get(key).get();
    Assert.assertEquals(newValue.toString(), value, "The key '" + key + "' does not contain the expected value!");
  }

  // TODO: Add tests with more complex scenarios (multiple records, record overwrites, multiple partitions, multiple storage nodes, etc.)
}
