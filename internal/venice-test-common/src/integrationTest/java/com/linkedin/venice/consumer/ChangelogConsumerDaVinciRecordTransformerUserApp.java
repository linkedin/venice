package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_ACL_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_SNAPSHOT_RETENTION_TIME_IN_MIN;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.consumer.StatefulVeniceChangelogConsumerTest.pollChangeEventsFromChangeCaptureConsumer;
import static com.linkedin.venice.consumer.StatefulVeniceChangelogConsumerTest.pollChangeEventsFromSpecificChangeCaptureConsumer;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.SslUtils.LOCAL_KEYSTORE_JKS;
import static com.linkedin.venice.utils.SslUtils.LOCAL_PASSWORD;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.StatefulVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.endToEnd.TestChangelogKey;
import com.linkedin.venice.endToEnd.TestChangelogValue;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


/**
 * ChangelogConsumerDaVinciRecordTransformerUserApp is a dummy class that spins up a Da Vinci Record Transformer based
 * CDC client and ingests data from all partitions.
 */
public class ChangelogConsumerDaVinciRecordTransformerUserApp {
  private static final Logger LOGGER = LogManager.getLogger(ChangelogConsumerDaVinciRecordTransformerUserApp.class);

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    String baseDataPath = args[0];
    String zkUrl = args[1];
    String kafkaUrl = args[2];
    String clusterName = args[3];
    String storeName = args[4];
    int blobTransferServerPort = Integer.parseInt(args[5]);
    int blobTransferClientPort = Integer.parseInt(args[6]);
    int eventsToPoll = Integer.parseInt(args[7]);
    boolean useSpecificRecord = Boolean.parseBoolean(args[8]);

    Utils.thisIsLocalhost();

    D2Client d2Client = new D2ClientBuilder().setZkHosts(zkUrl)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);

    Properties consumerProperties = new Properties();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, zkUrl);
    consumerProperties.put(BLOB_TRANSFER_MANAGER_ENABLED, true);
    consumerProperties.put(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, blobTransferServerPort);
    consumerProperties.put(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, blobTransferClientPort);
    consumerProperties.put(BLOB_TRANSFER_SSL_ENABLED, true);
    consumerProperties.put(BLOB_TRANSFER_ACL_ENABLED, true);

    String keyStorePath = SslUtils.getPathForResource(LOCAL_KEYSTORE_JKS);
    consumerProperties.put(SSL_KEYSTORE_TYPE, "JKS");
    consumerProperties.put(SSL_KEYSTORE_LOCATION, keyStorePath);
    consumerProperties.put(SSL_KEYSTORE_PASSWORD, LOCAL_PASSWORD);
    consumerProperties.put(SSL_TRUSTSTORE_TYPE, "JKS");
    consumerProperties.put(SSL_TRUSTSTORE_LOCATION, keyStorePath);
    consumerProperties.put(SSL_TRUSTSTORE_PASSWORD, LOCAL_PASSWORD);
    consumerProperties.put(SSL_KEY_PASSWORD, LOCAL_PASSWORD);
    consumerProperties.put(SSL_KEYMANAGER_ALGORITHM, "SunX509");
    consumerProperties.put(SSL_TRUSTMANAGER_ALGORITHM, "SunX509");
    consumerProperties.put(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");

    /*
     * Setting these to a low value so that when a blob transfer request is received, it sends the
     * most up-to-date snapshot and offset.
     */
    consumerProperties.put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, 1);
    consumerProperties.put(BLOB_TRANSFER_SNAPSHOT_RETENTION_TIME_IN_MIN, 1);

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(zkUrl)
            .setControllerRequestRetryCount(3)
            .setBootstrapFileSystemPath(baseDataPath)
            .setD2Client(d2Client);

    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    StatefulVeniceChangelogConsumer statefulVeniceChangelogConsumer;

    if (useSpecificRecord) {
      statefulVeniceChangelogConsumer = veniceChangelogConsumerClientFactory.getStatefulChangelogConsumer(
          storeName,
          TestChangelogKey.class,
          TestChangelogValue.class,
          TestChangelogValue.SCHEMA$);
    } else {
      statefulVeniceChangelogConsumer = veniceChangelogConsumerClientFactory.getStatefulChangelogConsumer(storeName);
    }

    statefulVeniceChangelogConsumer.start().get();
    LOGGER.info("DVRT CDC user app has come online.");

    if (useSpecificRecord) {
      Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap =
          new HashMap<>();
      List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList =
          new ArrayList<>();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromSpecificChangeCaptureConsumer(
            polledChangeEventsMap,
            polledChangeEventsList,
            statefulVeniceChangelogConsumer);
        Assert.assertEquals(polledChangeEventsList.size(), eventsToPoll);
      });
    } else {
      Map<String, PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsMap =
          new HashMap<>();
      List<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsList =
          new ArrayList<>();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        pollChangeEventsFromChangeCaptureConsumer(
            polledChangeEventsMap,
            polledChangeEventsList,
            statefulVeniceChangelogConsumer);
        Assert.assertEquals(polledChangeEventsList.size(), eventsToPoll);
      });
    }

    LOGGER.info("DVRT CDC user app has consumed all events");
  }

}
