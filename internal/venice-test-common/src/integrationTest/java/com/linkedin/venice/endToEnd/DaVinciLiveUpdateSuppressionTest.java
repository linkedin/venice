package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Create this class to isolate the live update suppression test as it could lead to unknown writer creation failure.
 */
public class DaVinciLiveUpdateSuppressionTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciLiveUpdateSuppressionTest.class);
  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 120_000; // ms
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    cluster = ServiceFactory.getVeniceCluster(1, 2, 1, 1, 100, false, false, clusterConfig);
    d2Client = new D2ClientBuilder().setZkHosts(cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass
  public void cleanUp() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @AfterMethod
  public void verifyPostConditions(Method method) {
    try {
      assertThrows(NullPointerException.class, AvroGenericDaVinciClient::getBackend);
    } catch (AssertionError e) {
      throw new AssertionError(method.getName() + " leaked DaVinciBackend.", e);
    }
  }

  @Test(dataProvider = "Isolated-Ingestion", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT * 2)
  public void testLiveUpdateSuppression(IngestionMode ingestionMode) throws Exception {
    final String storeName = Utils.getUniqueString("store");
    cluster.useControllerClient(client -> {
      TestUtils.assertCommand(
          client.createNewStore(storeName, getClass().getName(), DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA));
      cluster.createMetaSystemStore(storeName);
      client.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(10).setHybridOffsetLagThreshold(10));
    });

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(DEFAULT_VALUE_SCHEMA);

    // Enable live update suppression
    Map<String, Object> extraBackendConfigMap =
        (ingestionMode.equals(IngestionMode.ISOLATED)) ? TestUtils.getIngestionIsolationPropertyMap() : new HashMap<>();
    extraBackendConfigMap.put(FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS, true);

    Future[] writerFutures = new Future[KEY_COUNT];
    int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    try (VeniceWriter<Object, Object, byte[]> batchProducer = vwFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
            .setValueSerializer(valueSerializer)
            .build())) {
      batchProducer.broadcastStartOfPush(Collections.emptyMap());
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i] = batchProducer.put(i, i, valueSchemaId);
      }
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i].get();
      }
      batchProducer.broadcastEndOfPush(Collections.emptyMap());
    }

    DaVinciTestContext<Integer, Integer> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            new DaVinciConfig(),
            extraBackendConfigMap);

    try (CachingDaVinciClientFactory ignored = daVinciTestContext.getDaVinciClientFactory();
        DaVinciClient<Integer, Integer> client = daVinciTestContext.getDaVinciClient();
        VeniceWriter<Object, Object, byte[]> realTimeProducer = vwFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(Version.composeRealTimeTopic(storeName)).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .build())) {
      client.subscribe(Collections.singleton(0)).get();
      writerFutures = new Future[KEY_COUNT];
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i] = realTimeProducer.put(i, i * 1000, valueSchemaId);
      }
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i].get();
      }

      /**
       * Since live update suppression is enabled, once the partition is ready to serve, da vinci client will stop ingesting
       * new messages and also ignore any new message
       */
      try {
        TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, false, true, () -> {
          /**
           * Try to read the new value from real-time producer; assertion should fail
           */
          for (int i = 0; i < KEY_COUNT; i++) {
            int result = client.get(i).get();
            assertEquals(result, i * 1000);
          }
        });
        // It's wrong if new value can be read from da-vinci client
        throw new VeniceException("Should not be able to read live updates.");
      } catch (AssertionError e) {
        // expected
      }
    }

    /**
     * After restarting da-vinci client, since live update suppression is enabled and there is local data, ingestion
     * will not start.
     *
     * da-vinci client restart is done by building a new factory and a new client
     */
    DaVinciTestContext<Integer, Integer> daVinciTestContext2 =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            new DaVinciConfig(),
            extraBackendConfigMap);
    try (CachingDaVinciClientFactory ignored = daVinciTestContext2.getDaVinciClientFactory();
        DaVinciClient<Integer, Integer> client2 = daVinciTestContext2.getDaVinciClient()) {
      client2.subscribeAll().get();
      for (int i = 0; i < KEY_COUNT; i++) {
        int result = client2.get(i).get();
        assertEquals(result, i);
      }
      try {
        TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, false, true, () -> {
          /**
           * Try to read the new value from real-time producer; assertion should fail
           */
          for (int i = 0; i < KEY_COUNT; i++) {
            int result = client2.get(i).get();
            assertEquals(result, i * 1000);
          }
        });
        // It's wrong if new value can be read from da-vinci client
        throw new VeniceException("Should not be able to read live updates.");
      } catch (AssertionError e) {
        // expected
      }
    }
  }

}
