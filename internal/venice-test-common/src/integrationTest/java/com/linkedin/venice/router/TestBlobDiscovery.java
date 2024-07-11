package com.linkedin.venice.router;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_BLOB_DISCOVERY;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestBlobDiscovery {
  private static final String INT_KEY_SCHEMA = "\"int\"";
  private static final String INT_VALUE_SCHEMA = "\"int\"";
  String clusterName;
  String storeName;
  private VeniceMultiClusterWrapper multiClusterVenice;
  D2Client daVinciD2;

  /**
   * Set up a multi-cluster Venice environment with meta system store enabled Venice stores.
   */

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();

    Properties parentControllerProps = new Properties();
    parentControllerProps.put(OFFLINE_JOB_START_TIMEOUT_MS, "180000");

    VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
            1,
            2,
            1,
            1,
            3,
            1,
            3,
            Optional.of(parentControllerProps),
            Optional.empty(),
            Optional.empty(),
            false);

    multiClusterVenice = multiRegionMultiClusterWrapper.getChildRegions().get(0);
    String[] clusterNames = multiClusterVenice.getClusterNames();
    String parentControllerURLs = multiRegionMultiClusterWrapper.getParentControllers()
        .stream()
        .map(VeniceControllerWrapper::getControllerUrl)
        .collect(Collectors.joining(","));

    for (String cluster: clusterNames) {
      try (ControllerClient controllerClient =
          new ControllerClient(cluster, multiClusterVenice.getControllerConnectString())) {
        // Verify the participant store is up and running in child region
        String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(cluster);
        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(participantStoreName, 1),
            controllerClient,
            5,
            TimeUnit.MINUTES);
      }
    }

    clusterName = clusterNames[0];
    storeName = Utils.getUniqueString("test-store");

    List<PubSubBrokerWrapper> pubSubBrokerWrappers = multiClusterVenice.getClusters()
        .values()
        .stream()
        .map(VeniceClusterWrapper::getPubSubBrokerWrapper)
        .collect(Collectors.toList());
    Map<String, String> additionalPubSubProperties =
        PubSubBrokerWrapper.getBrokerDetailsForClients(pubSubBrokerWrappers);

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      assertFalse(
          parentControllerClient.createNewStore(storeName, "venice-test", INT_KEY_SCHEMA, INT_VALUE_SCHEMA).isError());

      PubSubProducerAdapterFactory pubSubProducerAdapterFactory = multiClusterVenice.getClusters()
          .get(clusterName)
          .getPubSubBrokerWrapper()
          .getPubSubClientsFactory()
          .getProducerAdapterFactory();

      VersionCreationResponse response = TestUtils.createVersionWithBatchData(
          parentControllerClient,
          storeName,
          INT_KEY_SCHEMA,
          INT_VALUE_SCHEMA,
          IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, 0)),
          pubSubProducerAdapterFactory,
          additionalPubSubProperties);

      // Verify the data can be ingested by classical Venice before proceeding.
      TestUtils.waitForNonDeterministicPushCompletion(
          response.getKafkaTopic(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      makeSureSystemStoresAreOnline(parentControllerClient, storeName);
      multiClusterVenice.getClusters().get(clusterName).refreshAllRouterMetaData();
    }

    VeniceProperties backendConfig =
        new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
            .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
            .build();

    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciD2 = D2TestUtils.getAndStartD2Client(multiClusterVenice.getZkServerWrapper().getAddress());

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        daVinciD2,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        backendConfig)) {
      List<DaVinciClient<Integer, Object>> clients = new ArrayList<>();
      DaVinciClient<Integer, Object> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
      client.subscribeAll().get();
      clients.add(client);
    } catch (ExecutionException | InterruptedException e) {
      throw new VeniceException(e);
    }
  }

  @AfterTest
  public void tearDown() {
    D2ClientUtils.shutdownClient(daVinciD2);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testBlobDiscovery() throws Exception {
    VeniceClusterWrapper veniceClusterWrapper = multiClusterVenice.getClusters().get(clusterName);
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, true, () -> {
      veniceClusterWrapper.updateStore(storeName, new UpdateStoreQueryParams().setBlobTransferEnabled(true));
    });

    String routerURL = veniceClusterWrapper.getRandomRouterURL();

    try (CloseableHttpAsyncClient client = HttpAsyncClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom().setSocketTimeout(4000).build())
        .build()) {
      client.start();

      String uri =
          routerURL + "/" + TYPE_BLOB_DISCOVERY + "?store_name=" + storeName + "&store_version=1&store_partition=1";
      HttpGet routerRequest = new HttpGet(uri);
      HttpResponse response = client.execute(routerRequest, null).get();
      String responseBody;
      try (InputStream bodyStream = response.getEntity().getContent()) {
        responseBody = IOUtils.toString(bodyStream, Charset.defaultCharset());
      }
      Assert.assertEquals(
          response.getStatusLine().getStatusCode(),
          HttpStatus.SC_OK,
          "Failed to get resource state for " + storeName + ". Response: " + responseBody);
      ObjectMapper mapper = ObjectMapperFactory.getInstance();
      BlobPeersDiscoveryResponse blobDiscoveryResponse =
          mapper.readValue(responseBody.getBytes(), BlobPeersDiscoveryResponse.class);
      // TODO: add another testcase to retrieve >= 1 live nodes
      Assert.assertEquals(blobDiscoveryResponse.getDiscoveryResult().size(), 0);
    } catch (Exception e) {
      fail("Unexpected exception", e);
    }
  }

  private void makeSureSystemStoresAreOnline(ControllerClient controllerClient, String storeName) {
    String metaSystemStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), 1);
    TestUtils.waitForNonDeterministicPushCompletion(metaSystemStoreTopic, controllerClient, 30, TimeUnit.SECONDS);
    String daVinciPushStatusStore =
        Version.composeKafkaTopic(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName), 1);
    TestUtils.waitForNonDeterministicPushCompletion(daVinciPushStatusStore, controllerClient, 30, TimeUnit.SECONDS);
  }
}
