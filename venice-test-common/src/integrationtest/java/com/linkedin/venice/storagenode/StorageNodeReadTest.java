package com.linkedin.venice.storagenode;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.PartitionOffsetMapUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import spark.utils.IOUtils;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.router.api.VenicePathParser.*;


public class StorageNodeReadTest {
  private VeniceClusterWrapper veniceCluster;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;
  private int partitionCount;

  private String serverAddr;
  private String routerAddr;
  private VeniceSerializer keySerializer;
  private VeniceSerializer valueSerializer;

  private VeniceWriter<Object, Object> veniceWriter;

  private final Base64.Encoder encoder = Base64.getUrlEncoder();

  @BeforeClass
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
    boolean enableSSL = false;
    veniceCluster = ServiceFactory.getVeniceCluster(enableSSL);
    serverAddr = veniceCluster.getVeniceServers().get(0).getAddress();
    routerAddr = "http://" + veniceCluster.getVeniceRouters().get(0).getAddress();

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion();
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
    partitionCount = creationResponse.getPartitions();

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

  private int getPartitionId(byte[] key) {
    VenicePartitioner partitioner = new DefaultVenicePartitioner();
    return partitioner.getPartitionId(key, partitionCount);
  }

  @Test (timeOut = 100000)
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

    try (CloseableHttpAsyncClient client = HttpAsyncClients.createDefault()) {
      client.start();

      // Single get
      byte[] keyBytes = keySerializer.serialize(null, keyPrefix + "0");
      StringBuilder sb = new StringBuilder().append("http://")
          .append(serverAddr)
          .append("/")
          .append(TYPE_STORAGE)
          .append("/")
          .append(storeVersionName)
          .append("/")
          .append(getPartitionId(keyBytes))
          .append("/")
          .append(encoder.encodeToString(keyBytes))
          .append("?f=b64");
      HttpGet getReq = new HttpGet(sb.toString());
      Future<HttpResponse> future = client.execute(getReq, null);
      HttpResponse response = future.get();
      try (InputStream bodyStream = response.getEntity().getContent()) {
        byte[] body = IOUtils.toByteArray(bodyStream);
        Object value = valueSerializer.deserialize(null, body);
        Assert.assertEquals(value.toString(), valuePrefix + "0");
      }

      // Multi-get
      List<MultiGetRouterRequestKeyV1> keys = new ArrayList<>();
      Set<Integer> partitionIdSet = new HashSet<>();
      for (int i = 0; i < 10; ++i) {
        MultiGetRouterRequestKeyV1 requestKey = new MultiGetRouterRequestKeyV1();
        keyBytes = keySerializer.serialize(null, keyPrefix + i);
        requestKey.keyBytes = ByteBuffer.wrap(keyBytes);
        requestKey.keyIndex = i;
        requestKey.partitionId = getPartitionId(keyBytes);
        partitionIdSet.add(requestKey.partitionId);
        keys.add(requestKey);
      }
      RecordSerializer<MultiGetRouterRequestKeyV1> serializer = AvroSerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.SCHEMA$);
      byte[] postBody = serializer.serializeObjects(keys);
      StringBuilder multiGetUri = new StringBuilder().append("http://")
          .append(serverAddr)
          .append("/")
          .append(TYPE_STORAGE)
          .append("/")
          .append(storeVersionName);
      HttpPost httpPost = new HttpPost(multiGetUri.toString());
      BasicHttpEntity entity = new BasicHttpEntity();
      entity.setContent(new ByteArrayInputStream(postBody));
      httpPost.setEntity(entity);
      httpPost.setHeader(HttpConstants.VENICE_API_VERSION,
          Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion()));

      RecordDeserializer<MultiGetResponseRecordV1> deserializer =
          AvroSerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class);

      Future<HttpResponse> multiGetFuture = client.execute(httpPost, null);
      HttpResponse multiGetResponse = multiGetFuture.get();
      Assert.assertEquals(multiGetResponse.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
      /**
       * Validate header: {@link HttpConstants.VENICE_OFFSET}
       */
      Header partitionOffsetHeader = multiGetResponse.getFirstHeader(HttpConstants.VENICE_OFFSET);
      Assert.assertNotNull(partitionOffsetHeader);
      String headerValue = partitionOffsetHeader.getValue();
      Map<Integer, Long> partitionOffsetMap = PartitionOffsetMapUtils.deserializePartitionOffsetMap(headerValue);
      partitionIdSet.forEach( partitionId -> {
        Long offset = partitionOffsetMap.get(partitionId);
        Assert.assertNotNull(offset);
        Assert.assertTrue(offset > 0);
      });

      try (InputStream bodyStream = multiGetResponse.getEntity().getContent()) {
        byte[] body = IOUtils.toByteArray(bodyStream);
        Iterable<MultiGetResponseRecordV1> values = deserializer.deserializeObjects(body);
        Map<Integer, String> results = new HashMap<>();
        values.forEach(K -> {
          Object value = valueSerializer.deserialize(null, K.value.array());
          results.put(K.keyIndex, value.toString());
        });
        Assert.assertEquals(results.size(), 10);
        for (int i = 0; i < 10; ++i) {
          Assert.assertEquals(results.get(i), valuePrefix + i);
        }
      }
    }

    /**
     * Test with {@link AvroGenericStoreClient}.
     */
    AvroGenericStoreClient<String, CharSequence> storeClient = ClientFactory.genericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr));
    Set<String> keySet = new HashSet<>();
    for (int i = 0; i < 10; ++i) {
      keySet.add(keyPrefix + i);
    }
    keySet.add("unknown_key");
    Map<String, CharSequence> result = storeClient.multiGet(keySet).get();
    Assert.assertEquals(result.size(), 10);
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(result.get(keyPrefix + i).toString(), valuePrefix + i);
    }
  }
}
