package com.linkedin.venice.endToEnd;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixPartitionState;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.routerapi.ResourceStateResponse;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.router.api.VenicePathParser.*;


@Test
public class TestHelixCustomizedView {
  private static final Logger logger = Logger.getLogger(TestHelixCustomizedView.class);
  private VeniceClusterWrapper veniceCluster;
  private HelixAdmin admin;
  private ControllerClient controllerClient;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;
  private final int replicationFactor = 2;

  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object, Object> veniceWriter;

  private static final String KEY_SCHEMA_STR = "\"string\"";
  private static final String VALUE_FIELD_NAME = "int_field";
  private static final String VALUE_SCHEMA_STR = "{\n" + "\"type\": \"record\",\n" + "\"name\": \"test_value_schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\"}]\n" + "}";
  private static final Schema VALUE_SCHEMA = new Schema.Parser().parse(VALUE_SCHEMA_STR);

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(1, 0, 0, replicationFactor, 10, true, false);
    admin = new ZKHelixAdmin(veniceCluster.getZk().getAddress());

    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.HELIX_OFFLINE_PUSH_ENABLED, true);
    veniceCluster.addVeniceRouter(routerProperties);

    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.HELIX_OFFLINE_PUSH_ENABLED, true);
    // add two servers for enough SNs
    veniceCluster.addVeniceServer(new Properties(), serverProperties);
    veniceCluster.addVeniceServer(new Properties(), serverProperties);

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(KEY_SCHEMA_STR, VALUE_SCHEMA_STR);
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // Build customized state config and update to Zookeeper
    CustomizedStateConfig.Builder customizedStateConfigBuilder = new CustomizedStateConfig.Builder();
    List<String> aggregationEnabledTypes = new ArrayList<String>();
    aggregationEnabledTypes.add(HelixPartitionState.OFFLINE_PUSH.name());
    customizedStateConfigBuilder.setAggregationEnabledTypes(aggregationEnabledTypes);
    CustomizedStateConfig customizedStateConfig = customizedStateConfigBuilder.build();
    admin.addCustomizedStateConfig(veniceCluster.getClusterName(), customizedStateConfig);

    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);
    veniceWriter = TestUtils.getVeniceWriterFactory(veniceCluster.getKafka().getAddress()).createVeniceWriter(storeVersionName, keySerializer, valueSerializer);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
    IOUtils.closeQuietly(veniceWriter);
    admin.close();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testUpdatingCustomizedState() throws Exception {
    int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    String keyPrefix = "key_";
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < 10; ++i) {
      GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
      record.put(VALUE_FIELD_NAME, i);
      veniceWriter.put(keyPrefix + i, record, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    // Wait for storage node to finish consuming, and new version to be activated
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });
    logger.info("The current version is " + controllerClient.getStore(storeName).getStore().getCurrentVersion());

    // Router looks up the resource states
    String routerURL = veniceCluster.getRandomRouterURL();
    try (CloseableHttpAsyncClient client = HttpAsyncClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom().setSocketTimeout(1000).build())
        .build()) {
      client.start();
      HttpGet routerRequest =
          new HttpGet(routerURL + "/" + TYPE_RESOURCE_STATE + "/" + storeVersionName);
      HttpResponse response = client.execute(routerRequest, null).get();
      String responseBody;
      try (InputStream bodyStream = response.getEntity().getContent()) {
        responseBody = IOUtils.toString(bodyStream);
      }
      Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK,
          "Failed to get resource state for " + storeVersionName + ". Response: " + responseBody);
      ObjectMapper mapper = new ObjectMapper();
      ResourceStateResponse resourceStateResponse = mapper.readValue(responseBody.getBytes(), ResourceStateResponse.class);
      Assert.assertEquals(resourceStateResponse.getName(), storeVersionName);
      List<ReplicaState> replicaStates = resourceStateResponse.getReplicaStates();
      for (ReplicaState replicaState : replicaStates) {
        Assert.assertTrue(replicaState.isReadyToServe());
        Assert.assertTrue(replicaState.getVenicePushStatus().equals(ExecutionStatus.COMPLETED.name()));
      }
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }
}
