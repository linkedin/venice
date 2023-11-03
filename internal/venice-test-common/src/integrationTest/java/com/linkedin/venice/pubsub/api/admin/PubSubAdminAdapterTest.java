package com.linkedin.venice.pubsub.api.admin;

import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for {@link PubSubAdminAdapter} implementations.
 */
public class PubSubAdminAdapterTest {
  public static final long TEST_TIMEOUT_MS = 180_000;

  // timeout for pub-sub operations
  private static final Duration PUBSUB_OP_TIMEOUT = Duration.ofSeconds(15);
  // add a variance of 5 seconds to the timeout to account for fluctuations in the test environment
  private static final long PUBSUB_OP_TIMEOUT_WITH_VARIANCE = PUBSUB_OP_TIMEOUT.toMillis() + 5000;
  // timeout for pub-sub consumer APIs which do not have a timeout parameter
  private static final int PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS = 10_000;
  // add a variance of 5 seconds to the timeout to account for fluctuations in the test environment
  private static final long PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE =
      PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 5000;
  private static final int REPLICATION_FACTOR = 1;
  private static final boolean IS_LOG_COMPACTED = false;
  private static final long RETENTION_IN_MS = Duration.ofDays(2).toMillis();
  private static final int MIN_IN_SYNC_REPLICAS = 1;
  private static final long MIN_LOG_COMPACTION_LAG_MS = Duration.ofDays(1).toMillis();
  private static final PubSubTopicConfiguration TOPIC_CONFIGURATION = new PubSubTopicConfiguration(
      Optional.of(RETENTION_IN_MS),
      IS_LOG_COMPACTED,
      Optional.of(MIN_IN_SYNC_REPLICAS),
      MIN_LOG_COMPACTION_LAG_MS);

  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private PubSubAdminAdapter pubSubAdminAdapter;
  private Lazy<PubSubConsumerAdapter> pubSubConsumerAdapterLazy;
  private Lazy<PubSubProducerAdapter> pubSubProducerAdapterLazy;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private PubSubTopicRepository pubSubTopicRepository;
  private PubSubClientsFactory pubSubClientsFactory;

  @BeforeClass(alwaysRun = true)
  public void setUp() {

  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
  }

  @BeforeMethod(alwaysRun = true)
  public void setUpMethod() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    pubSubMessageDeserializer = PubSubMessageDeserializer.getInstance();
    pubSubTopicRepository = new PubSubTopicRepository();
    pubSubClientsFactory = pubSubBrokerWrapper.getPubSubClientsFactory();

    String clientId = Utils.getUniqueString("test-admin-");
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.setProperty(
        PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS,
        String.valueOf(PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS));
    properties.putAll(pubSubBrokerWrapper.getAdditionalConfig());
    properties.putAll(pubSubBrokerWrapper.getMergeableConfigs());
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    pubSubAdminAdapter = pubSubClientsFactory.getAdminAdapterFactory().create(veniceProperties, pubSubTopicRepository);
    pubSubConsumerAdapterLazy = Lazy.of(
        () -> pubSubClientsFactory.getConsumerAdapterFactory()
            .create(veniceProperties, false, pubSubMessageDeserializer, clientId));
    pubSubProducerAdapterLazy =
        Lazy.of(() -> pubSubClientsFactory.getProducerAdapterFactory().create(veniceProperties, clientId, null));
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownMethod() {
    Utils.closeQuietlyWithErrorLogged(pubSubAdminAdapter);
    if (pubSubProducerAdapterLazy.isPresent()) {
      pubSubProducerAdapterLazy.get().close(0, false);
    }
    if (pubSubConsumerAdapterLazy.isPresent()) {
      Utils.closeQuietlyWithErrorLogged(pubSubConsumerAdapterLazy.get());
    }

    if (pubSubBrokerWrapper.isRunning()) {
      Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper);
    }
  }

  // Test: createTopic should not block indefinitely
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testCreateTopic() {
    int partitionCount = 4;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("test-create-topic-"));
    long startTime = System.currentTimeMillis();
    try {
      pubSubBrokerWrapper.close();
    } catch (Exception e) {
      // ignore
    }
    pubSubAdminAdapter.createTopic(pubSubTopic, partitionCount, REPLICATION_FACTOR, TOPIC_CONFIGURATION);
    long endTime = System.currentTimeMillis();
    assertTrue(endTime - startTime < PUBSUB_OP_TIMEOUT_WITH_VARIANCE);
  }
}
