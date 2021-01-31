package com.linkedin.venice.endToEnd;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.ingestion.IngestionReportListener;
import com.linkedin.davinci.ingestion.IngestionRequestClient;
import com.linkedin.davinci.ingestion.IngestionService;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.linkedin.davinci.ingestion.IngestionUtils.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.meta.IngestionMode.*;
import static org.testng.Assert.*;


public class IngestionServiceTest {
  private static final int TEST_TIMEOUT = 60_000;
  private static final int APPLICATION_PORT = 12345;
  private static final int SERVICE_PORT = 12346;
  private static final int KEY_COUNT = 10;
  private static final int MAX_ATTEMPT = 100;

  private static Process forkedIngestionServiceListenerProcess;
  private static IngestionReportListener ingestionReportListener;
  private static VeniceClusterWrapper cluster;
  private static String testStoreName;

  @BeforeClass
  public static void setup() throws Exception {
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(1, 1, 1);
    testStoreName = cluster.createStore(KEY_COUNT);
    forkedIngestionServiceListenerProcess = ForkedJavaProcess.exec(IngestionService.class, String.valueOf(SERVICE_PORT));
    ingestionReportListener = new IngestionReportListener(APPLICATION_PORT, SERVICE_PORT, AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    ingestionReportListener.startInner();
    waitPortBinding(SERVICE_PORT, MAX_ATTEMPT);
    waitPortBinding(APPLICATION_PORT, MAX_ATTEMPT);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    forkedIngestionServiceListenerProcess.destroy();
    ingestionReportListener.stopInner();
    cluster.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testInitialization() throws Exception {
    String baseDataPath = TestUtils.getTempDataDirectory().getAbsolutePath();
    InitializationConfigs initializationConfigs = createInitializationConfigs(baseDataPath);
    try (IngestionRequestClient client = new IngestionRequestClient(SERVICE_PORT)) {
      HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.INIT, serializeInitializationConfigs(initializationConfigs));
      FullHttpResponse response = client.sendRequest(httpRequest);
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIngestionCommandWithoutInitialization() throws Exception {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.topicName = Version.composeKafkaTopic(testStoreName, 1);
    ingestionTaskCommand.partitionId = 0;
    ingestionTaskCommand.commandType = IngestionCommandType.START_CONSUMPTION.getValue();
    try (IngestionRequestClient client = new IngestionRequestClient(SERVICE_PORT)) {
      HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.COMMAND, serializeIngestionTaskCommand(ingestionTaskCommand));
      FullHttpResponse response = client.sendRequest(httpRequest);
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
      IngestionTaskReport report = deserializeIngestionTaskReport(readHttpResponseContent(response));
      Assert.assertFalse(report.isPositive);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testTopicIngestionParentChildMode() throws Exception {
    String baseDataPath = TestUtils.getTempDataDirectory().getAbsolutePath();
    InitializationConfigs initializationConfigs = createInitializationConfigs(baseDataPath);
    initializationConfigs.aggregatedConfigs.put(SERVER_INGESTION_MODE, ISOLATED.toString());
    initializationConfigs.aggregatedConfigs.put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress());
    sendInitializationMessage(serializeInitializationConfigs(initializationConfigs));
    sendStartConsumptionMessage(testStoreName, 1, 0);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testTopicIngestion() throws Exception {
    String baseDataPath = TestUtils.getTempDataDirectory().getAbsolutePath();

    // Use Da Vinci bootstrap to test.
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .build();

    D2Client d2Client = new D2ClientBuilder()
        .setZkHosts(cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);

    MetricsRepository metricsRepository = new MetricsRepository();
    DaVinciConfig daVinciConfig = new DaVinciConfig();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(testStoreName, daVinciConfig);
      client1.subscribeAll().get();
      assertEquals(client1.get(0).get(), 1);
      Utils.sleep(1000);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIngestionReportListener() throws Exception {
    IngestionTaskReport ingestionTaskReport = new IngestionTaskReport();
    ingestionTaskReport.topicName = "";
    ingestionTaskReport.errorMessage = "";
    ingestionTaskReport.isComplete = true;
    ingestionTaskReport.isEndOfPushReceived = true;
    byte[] content = serializeIngestionTaskReport(ingestionTaskReport);
    sendReportMessage(content);
  }

  private InitializationConfigs createInitializationConfigs(String baseDataPath) {
    InitializationConfigs initializationConfigs = new InitializationConfigs();
    initializationConfigs.aggregatedConfigs = new HashMap<>();
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.CLUSTER_NAME, cluster.getClusterName());
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.ZOOKEEPER_ADDRESS, cluster.getZk().getAddress());
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, cluster.getKafka().getAddress());
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.KAFKA_ZK_ADDRESS, cluster.getKafka().getZkAddress());
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.KAFKA_ADMIN_CLASS, KafkaAdminClient.class.getName());
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.SERVER_ENABLE_KAFKA_OPENSSL, "false");
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.DATA_BASE_PATH, baseDataPath);
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.toString());
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.SERVER_INGESTION_ISOLATION_APPLICATION_PORT, String.valueOf(APPLICATION_PORT));
    initializationConfigs.aggregatedConfigs.put(ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT, String.valueOf(SERVICE_PORT));
    initializationConfigs.aggregatedConfigs.put(RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    initializationConfigs.aggregatedConfigs.put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress());
    return initializationConfigs;
  }

  private void sendInitializationMessage(byte[] content) throws Exception{
    try (IngestionRequestClient client = new IngestionRequestClient(SERVICE_PORT)) {
      HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.INIT, content);
      FullHttpResponse response = client.sendRequest(httpRequest);
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    }
  }

  private void sendStartConsumptionMessage(String storeName, int versionNumber, int partitionId) throws Exception {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.topicName = Version.composeKafkaTopic(storeName, versionNumber);
    ingestionTaskCommand.partitionId = partitionId;
    ingestionTaskCommand.commandType = IngestionCommandType.START_CONSUMPTION.getValue();
    try (IngestionRequestClient client = new IngestionRequestClient(SERVICE_PORT)) {
      HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.COMMAND, serializeIngestionTaskCommand(ingestionTaskCommand));
      FullHttpResponse response = client.sendRequest(httpRequest);
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
      IngestionTaskReport report = deserializeIngestionTaskReport(readHttpResponseContent(response));
      Assert.assertEquals(report.errorMessage.toString(), "");
      Assert.assertTrue(report.isPositive);
    }
  }

  private void sendReportMessage(byte[] content) throws Exception{
    try (IngestionRequestClient client = new IngestionRequestClient(APPLICATION_PORT)) {
      HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.REPORT, content);
      System.out.println("Trying to send report request now.");
      FullHttpResponse response = client.sendRequest(httpRequest);
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    }
  }
}
