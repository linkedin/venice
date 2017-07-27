package com.linkedin.venice.router;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;

public class TestRead {
  private VeniceClusterWrapper veniceCluster;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;

  private String routerAddr;
  private VeniceSerializer keySerializer;
  private VeniceSerializer valueSerializer;
  private VeniceWriter<Object, Object> veniceWriter;


  @BeforeClass
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
    veniceCluster = ServiceFactory.getVeniceCluster(1, 3, 1);
    routerAddr = "http://" + veniceCluster.getVeniceRouters().get(0).getAddress();

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion();
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    VeniceProperties clientProps =
        new PropertyBuilder().put(KAFKA_BOOTSTRAP_SERVERS, veniceCluster.getKafka().getAddress())
            .put(ZOOKEEPER_ADDRESS, veniceCluster.getZk().getAddress())
            .put(CLUSTER_NAME, veniceCluster.getClusterName()).build();

    // TODO: Make serializers parameterized so we test them all.
    String stringSchema = "\"string\"";
    keySerializer = new VeniceAvroGenericSerializer(stringSchema);
    valueSerializer = new VeniceAvroGenericSerializer(stringSchema);

    veniceWriter = new VeniceWriter<>(clientProps, storeVersionName, keySerializer, valueSerializer);
  }

  @AfterClass
  public void cleanUp() {
    if (veniceCluster != null) {
      veniceCluster.close();
    }
  }

  @Test(timeOut = 10000)
  public void testRead() throws Exception {
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    String keyPrefix = "key_";
    String valuePrefix = "value_";

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < 100; ++i) {
      veniceWriter.put(keyPrefix + i, valuePrefix + i, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });

    /**
     * Test with {@link AvroGenericStoreClient}.
     */
    AvroGenericStoreClient<String, CharSequence> storeClient = ClientFactory.genericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr));
    Set<String> keySet = new HashSet<>();
    for (int i = 0; i < 10; ++i) {
      keySet.add(keyPrefix + i);
    }
    keySet.add("unknown_key");
    Map<String, CharSequence> result = storeClient.batchGet(keySet).get();
    Assert.assertEquals(result.size(), 10);
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(result.get(keyPrefix + i).toString(), valuePrefix + i);
    }

    /**
     * Test simple get
     */
    String key = keyPrefix + 2;
    String expectedValue = valuePrefix + 2;
    CharSequence value = storeClient.get(key).get();
    Assert.assertEquals(value.toString(), expectedValue);
  }
}
