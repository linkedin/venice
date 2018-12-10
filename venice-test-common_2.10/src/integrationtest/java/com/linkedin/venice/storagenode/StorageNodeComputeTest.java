package com.linkedin.venice.storagenode;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.GzipCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class StorageNodeComputeTest {
  private VeniceClusterWrapper veniceCluster;
  private int valueSchemaId;
  private String storeName;

  private String routerAddr;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;

  private static final String valueSchemaForCompute = "{" +
      "  \"namespace\": \"example.compute\",    " +
      "  \"type\": \"record\",        " +
      "  \"name\": \"MemberFeature\",       " +
      "  \"fields\": [        " +
      "         { \"name\": \"id\", \"type\": \"string\" },             " +
      "         { \"name\": \"name\", \"type\": \"string\" },           " +
      "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        " +
      "  ]       " +
      " }       ";

  @BeforeClass
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
    boolean enableSSL = false;
    veniceCluster = ServiceFactory.getVeniceCluster(enableSSL);
    routerAddr = "http://" + veniceCluster.getVeniceRouters().get(0).getAddress();

    String keySchema = "\"string\"";

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(keySchema, valueSchemaForCompute);
    storeName = Version.parseStoreFromKafkaTopicName(creationResponse.getKafkaTopic());
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // TODO: Make serializers parameterized so we test them all.
    keySerializer = new VeniceAvroGenericSerializer(keySchema);
    valueSerializer = new VeniceAvroGenericSerializer(valueSchemaForCompute);
  }

  @AfterClass
  public void cleanUp() {
    if (veniceCluster != null) {
      veniceCluster.close();
    }
  }

  @DataProvider(name = "isNotCompressed_isCompressed")
  public static Object[][] compressionStrategy() {
    return new Object[][]{{CompressionStrategy.NO_OP}, {CompressionStrategy.GZIP}};
  }

  @Test (timeOut = 30000, dataProvider = "isNotCompressed_isCompressed")
  public void testCompute(CompressionStrategy compressionStrategy) throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setCompressionStrategy(compressionStrategy);
    params.setReadComputationEnabled(true);
    veniceCluster.updateStore(storeName, params);

    VersionCreationResponse newVersion = veniceCluster.getNewVersion(storeName, 1024);
    final int pushVersion = newVersion.getVersion();

    VeniceWriter<Object, byte[]> veniceWriter = TestUtils.getVeniceTestWriterFactory(veniceCluster.getKafka().getAddress())
        .getVeniceWriter(newVersion.getKafkaTopic(), keySerializer, new DefaultSerializer());

    String keyPrefix = "key_";
    String valuePrefix = "value_";
    pushSyntheticDataForCompute(newVersion.getKafkaTopic(), keyPrefix, valuePrefix, 100, veniceCluster,
                                veniceWriter, pushVersion, compressionStrategy);

    /**
     * Test with {@link AvroGenericStoreClient}.
     */
    AvroGenericStoreClient<String, Object>
        storeClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr));

    // Run multiple rounds
    int rounds = 100;
    int cur = 0;
    while (cur++ < rounds) {
      Set<String> keySet = new HashSet<>();
      for (int i = 0; i < 10; ++i) {
        keySet.add(keyPrefix + i);
      }
      keySet.add("unknown_key");
      Float[] p = new Float[2];
      p[0] = new Float(100.0);
      p[1] = new Float(0.1);
      Map<String, GenericRecord> computeResult = (Map<String, GenericRecord>) storeClient.compute()
          .project("id")
          .dotProduct("member_feature", p, "member_score")
          .execute(keySet)
          .get();
      Assert.assertEquals(computeResult.size(), 10);
      for (Map.Entry<String, GenericRecord> entry : computeResult.entrySet()) {
        int keyIdx = getKeyIndex(entry.getKey(), keyPrefix);
        // check projection result
        Assert.assertEquals(entry.getValue().get("id"), new Utf8(valuePrefix + keyIdx));
        // check dotProduct result
        Assert.assertEquals(entry.getValue().get("member_score"), (double) (p[0] * keyIdx + p[1] * (keyIdx * 10)));
      }
    }
  }

  private int getKeyIndex(String key, String keyPrefix) {
    if (!key.startsWith(keyPrefix)) {
      return -1;
    }
    return Integer.valueOf(key.substring(keyPrefix.length()));
  }

  private void pushSyntheticDataForCompute(String topic, String keyPrefix, String valuePrefix, int numOfRecords,
                                           VeniceClusterWrapper veniceCluster, VeniceWriter<Object, byte[]> veniceWriter,
                                           int pushVersion, CompressionStrategy compressionStrategy) throws Exception {
    veniceWriter.broadcastStartOfPush(false, false, compressionStrategy, new HashMap<>());
    Schema valueSchema = Schema.parse(valueSchemaForCompute);
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < numOfRecords; ++i) {
      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("id", valuePrefix + i);
      value.put("name", valuePrefix + i + "_name");
      List<Float> features = new ArrayList<>();
      features.add(Float.valueOf((float)i));
      features.add(Float.valueOf((float)(i * 10)));
      value.put("member_feature", features);

      byte[] compressedValue = CompressorFactory.getCompressor(compressionStrategy).compress(valueSerializer.serialize(topic, value));
      veniceWriter.put(keyPrefix + i, compressedValue, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });
  }
}
