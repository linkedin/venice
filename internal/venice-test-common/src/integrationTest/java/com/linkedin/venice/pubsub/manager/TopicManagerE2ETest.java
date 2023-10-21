package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.PubSubHelper;
import com.linkedin.venice.utils.PubSubHelper.MutablePubSubMessage;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TopicManagerE2ETest {
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
  private static final int MIN_IN_SYNC_REPLICAS = 1;
  private static final long RETENTION_IN_MS = Duration.ofDays(3).toMillis();
  private static final long MIN_LOG_COMPACTION_LAG_MS = Duration.ofDays(1).toMillis();
  private static final long MAX_LOG_COMPACTION_LAG_MS = Duration.ofDays(2).toMillis();
  private static final PubSubTopicConfiguration TOPIC_CONFIGURATION = new PubSubTopicConfiguration(
      Optional.of(RETENTION_IN_MS),
      IS_LOG_COMPACTED,
      Optional.of(MIN_IN_SYNC_REPLICAS),
      MIN_LOG_COMPACTION_LAG_MS,
      Optional.of(MAX_LOG_COMPACTION_LAG_MS));

  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private Lazy<PubSubAdminAdapter> pubSubAdminAdapterLazy;
  private Lazy<PubSubProducerAdapter> pubSubProducerAdapterLazy;
  private Lazy<PubSubConsumerAdapter> pubSubConsumerAdapterLazy;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private PubSubTopicRepository pubSubTopicRepository;
  private PubSubClientsFactory pubSubClientsFactory;
  private TopicManagerRepository topicManagerRepository;
  private TopicManager topicManager;
  private TopicManagerContext.Builder topicManagerContextBuilder;
  private MetricsRepository metricsRepository;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    pubSubMessageDeserializer = PubSubMessageDeserializer.getInstance();
    pubSubTopicRepository = new PubSubTopicRepository();
    pubSubClientsFactory = pubSubBrokerWrapper.getPubSubClientsFactory();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper);
  }

  @BeforeMethod(alwaysRun = true)
  public void setUpMethod() {
    String clientId = Utils.getUniqueString("TopicManageE2EITest");
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.setProperty(
        PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS,
        String.valueOf(PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS));
    properties.setProperty(PubSubConstants.PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE, "true");
    properties.putAll(pubSubBrokerWrapper.getAdditionalConfig());
    properties.putAll(pubSubBrokerWrapper.getMergeableConfigs());
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    pubSubProducerAdapterLazy =
        Lazy.of(() -> pubSubClientsFactory.getProducerAdapterFactory().create(veniceProperties, clientId, null));
    pubSubAdminAdapterLazy =
        Lazy.of(() -> pubSubClientsFactory.getAdminAdapterFactory().create(veniceProperties, pubSubTopicRepository));
    pubSubConsumerAdapterLazy = Lazy.of(
        () -> pubSubClientsFactory.getConsumerAdapterFactory()
            .create(veniceProperties, false, pubSubMessageDeserializer, clientId));

    metricsRepository = new MetricsRepository();
    topicManagerContextBuilder = new TopicManagerContext.Builder().setPubSubTopicRepository(pubSubTopicRepository)
        .setMetricsRepository(metricsRepository)
        .setTopicMetadataFetcherConsumerPoolSize(2)
        .setTopicMetadataFetcherThreadPoolSize(6)
        .setTopicOffsetCheckIntervalMs(100)
        .setPubSubPropertiesSupplier(k -> veniceProperties)
        .setPubSubAdminAdapterFactory(pubSubClientsFactory.getAdminAdapterFactory())
        .setPubSubConsumerAdapterFactory(pubSubClientsFactory.getConsumerAdapterFactory());

    topicManagerRepository =
        new TopicManagerRepository(topicManagerContextBuilder.build(), pubSubBrokerWrapper.getAddress());
    topicManager = topicManagerRepository.getLocalTopicManager();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownMethod() {
    if (pubSubProducerAdapterLazy.isPresent()) {
      pubSubProducerAdapterLazy.get().close(0, false);
    }
    if (pubSubAdminAdapterLazy.isPresent()) {
      Utils.closeQuietlyWithErrorLogged(pubSubAdminAdapterLazy.get());
    }
    if (pubSubConsumerAdapterLazy.isPresent()) {
      Utils.closeQuietlyWithErrorLogged(pubSubConsumerAdapterLazy.get());
    }

    if (topicManagerRepository != null) {
      Utils.closeQuietlyWithErrorLogged(topicManagerRepository);
    }
  }

  @Test(timeOut = 5 * Time.MS_PER_MINUTE, invocationCount = 1)
  public void testAsyncApis() throws ExecutionException, InterruptedException, TimeoutException {
    int numPartitions = 3;
    int replicationFactor = 1;
    boolean isEternalTopic = true;
    PubSubTopic testTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("testTopic"));
    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("nonExistentTopic"));
    assertFalse(topicManager.containsTopic(testTopic));
    assertFalse(topicManager.containsTopic(nonExistentTopic));
    topicManager.createTopic(testTopic, numPartitions, replicationFactor, isEternalTopic);
    waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> topicManager.containsTopic(testTopic));

    int numMessages = 250;
    PubSubProducerAdapter pubSubProducerAdapter = pubSubProducerAdapterLazy.get();
    CompletableFuture<PubSubProduceResult> lastMessageFuture = null;
    // list of messages
    Map<Integer, MutablePubSubMessage> messages = new HashMap<>(numMessages);
    for (int i = 0; i < numMessages; i++) {
      MutablePubSubMessage message = PubSubHelper.getDummyPubSubMessage(false);
      message.getValue().getProducerMetadata().setMessageTimestamp(i);
      messages.put(i, message);
      lastMessageFuture =
          pubSubProducerAdapter.sendMessage(testTopic.getName(), 0, message.getKey(), message.getValue(), null, null);
      lastMessageFuture.whenComplete((result, throwable) -> {
        if (throwable == null) {
          message.setOffset(result.getOffset());
        }
      });
    }
    assertNotNull(lastMessageFuture, "Last message future should not be null");
    lastMessageFuture.get(1, TimeUnit.MINUTES);
    assertEquals(messages.size(), numMessages);

    final AtomicInteger successfulRequests = new AtomicInteger(0);
    List<Runnable> tasks = new ArrayList<>();

    Runnable getPartitionCountTask = () -> {
      try {
        int actualNumPartitions = topicManager.getPartitionCount(testTopic);
        assertEquals(actualNumPartitions, numPartitions);
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(getPartitionCountTask);

    // get partition count for non-existent topic
    Runnable getPartitionCountForNonExistentTopicTask = () -> {
      try {
        assertNull(topicManager.getPartitionCount(nonExistentTopic));
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(getPartitionCountForNonExistentTopicTask);

    // contains topic
    Runnable containsTopicTask = () -> {
      try {
        assertTrue(topicManager.containsTopic(testTopic));
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(containsTopicTask);

    // contains topic for non-existent topic
    Runnable containsNonExistentTopicTask = () -> {
      try {
        assertFalse(topicManager.containsTopic(nonExistentTopic));
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(containsNonExistentTopicTask);

    Runnable getLatestOffsetWithRetriesTask = () -> {
      try {
        long latestOffset = topicManager.getLatestOffsetWithRetries(new PubSubTopicPartitionImpl(testTopic, 0), 1);
        assertEquals(latestOffset, numMessages);
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(getLatestOffsetWithRetriesTask);

    ExecutorService executorService = Executors.newFixedThreadPool(8);

    List<Future> vwFutures = new ArrayList<>();

    int totalTasks = 1024;
    for (int i = 0; i < totalTasks; i++) {
      Future future = executorService.submit(tasks.get(i % tasks.size()));
      vwFutures.add(future);
    }

    int failedRequests = 0;
    for (Future future: vwFutures) {
      try {
        future.get(1, TimeUnit.MINUTES);
      } catch (Exception e) {
        failedRequests++;
      }
    }
    System.out.println("successfulRequests: " + successfulRequests.get());
    // total should be equal to the number of tasks
    assertEquals(successfulRequests.get() + failedRequests, totalTasks);
  }

  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testMetadataApisForNonExistentTopics() throws ExecutionException, InterruptedException, TimeoutException {
    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("nonExistentTopic"));
    assertFalse(topicManager.containsTopicCached(nonExistentTopic));
    assertFalse(topicManager.containsTopic(nonExistentTopic));
    Map<Integer, Long> nonExistentTopicLatestOffsets = topicManager.getTopicLatestOffsets(nonExistentTopic);
    assertNotNull(nonExistentTopicLatestOffsets);
    assertEquals(nonExistentTopicLatestOffsets.size(), 0);
    assertThrows(PubSubTopicDoesNotExistException.class, () -> topicManager.getPartitionCount(nonExistentTopic));
    PubSubTopicPartitionImpl nonExistentTopicPartition = new PubSubTopicPartitionImpl(nonExistentTopic, 0);
    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.getOffsetByTime(nonExistentTopicPartition, System.currentTimeMillis()));
    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.getProducerTimestampOfLastDataMessageWithRetries(nonExistentTopicPartition, 1));
    assertEquals(
        topicManager.getProducerTimestampOfLastDataMessageCached(nonExistentTopicPartition),
        StatsErrorCode.LAG_MEASUREMENT_FAILURE.code);
    topicManager.invalidateCache(nonExistentTopic).get(1, TimeUnit.MINUTES); // should not throw an exception
    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.getLatestOffsetWithRetries(new PubSubTopicPartitionImpl(nonExistentTopic, 0), 1));
    assertEquals(topicManager.getLatestOffsetCached(nonExistentTopic, 1), StatsErrorCode.LAG_MEASUREMENT_FAILURE.code);
  }

  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testMetadataApisForExistingTopics() throws ExecutionException, InterruptedException, TimeoutException {
    int numPartitions = 35;
    int replicationFactor = 1;
    boolean isEternalTopic = true;
    PubSubTopic existingTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("existingTopic"));

    // topic should not exist
    assertFalse(topicManager.containsTopic(existingTopic));
    assertFalse(topicManager.containsTopicCached(existingTopic));

    // create the topic
    topicManager.createTopic(existingTopic, numPartitions, replicationFactor, isEternalTopic);

    // topic should exist
    waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
      assertTrue(topicManager.containsTopicCached(existingTopic));
      assertTrue(topicManager.containsTopic(existingTopic));
    });

    // when there are no messages, the latest offset should be 0
    Map<Integer, Long> latestOffsets = topicManager.getTopicLatestOffsets(existingTopic);
    assertNotNull(latestOffsets);
    assertEquals(latestOffsets.size(), numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      assertEquals((long) latestOffsets.get(i), 0L);
    }
    assertEquals(topicManager.getPartitionCount(existingTopic), numPartitions);

    PubSubTopicPartition p0 = new PubSubTopicPartitionImpl(existingTopic, 0);
    PubSubTopicPartition p1 = new PubSubTopicPartitionImpl(existingTopic, 1);
    PubSubTopicPartition p2 = new PubSubTopicPartitionImpl(existingTopic, 2);
    PubSubTopicPartition p3 = new PubSubTopicPartitionImpl(existingTopic, 3);

    // produce messages to the topic-partitions: p0, p1, p2
    PubSubProducerAdapter pubSubProducerAdapter = pubSubProducerAdapterLazy.get();
    List<MutablePubSubMessage> p0Messages = PubSubHelper.produceMessages(pubSubProducerAdapter, p0, 10, 2, false);
    List<MutablePubSubMessage> p1Messages = PubSubHelper.produceMessages(pubSubProducerAdapter, p1, 14, 2, false);
    List<MutablePubSubMessage> p2Messages = PubSubHelper.produceMessages(pubSubProducerAdapter, p2, 19, 2, false);

    // get the latest offsets
    latestOffsets = topicManager.getTopicLatestOffsets(existingTopic);
    assertNotNull(latestOffsets);
    assertEquals(latestOffsets.size(), numPartitions);
    assertEquals((long) latestOffsets.get(0), p0Messages.size());
    assertEquals(topicManager.getLatestOffsetWithRetries(p0, 5), p0Messages.size());
    assertEquals(topicManager.getLatestOffsetCached(p0.getPubSubTopic(), 0), p0Messages.size());

    assertEquals((long) latestOffsets.get(1), p1Messages.size());
    assertEquals(topicManager.getLatestOffsetWithRetries(p1, 5), p1Messages.size());
    assertEquals(topicManager.getLatestOffsetCached(p1.getPubSubTopic(), 1), p1Messages.size());

    assertEquals((long) latestOffsets.get(2), p2Messages.size());
    assertEquals(topicManager.getLatestOffsetWithRetries(p2, 5), p2Messages.size());
    assertEquals(topicManager.getLatestOffsetCached(p2.getPubSubTopic(), 2), p2Messages.size());

    // except for the first 3 partitions, the latest offset should be 0
    for (int i = 3; i < numPartitions; i++) {
      assertEquals((long) latestOffsets.get(i), 0L);
      assertEquals(topicManager.getLatestOffsetWithRetries(new PubSubTopicPartitionImpl(existingTopic, i), 5), 0L);
      assertEquals(topicManager.getLatestOffsetCached(existingTopic, i), 0L);
    }

    // if timestamp is greater than the latest message timestamp, the offset returned should be the latest offset
    long timestamp = System.currentTimeMillis();
    assertEquals(topicManager.getOffsetByTime(p0, timestamp), p0Messages.size());

    // If the provided timestamp is less than or equal to the timestamp of a message,
    // the offset returned should correspond to that message.
    long p0M4Ts = p0Messages.get(4).getTimestampAfterProduce();
    assertEquals(topicManager.getOffsetByTime(p0, p0M4Ts), 5);

    long p0TsBeforeM0 = p0Messages.get(0).getTimestampBeforeProduce();
    assertEquals(topicManager.getOffsetByTime(p0, p0TsBeforeM0), 0);

    // test getProducerTimestampOfLastDataMessage
    long p0LastDataMessageTs =
        p0Messages.get(p0Messages.size() - 1).getValue().getProducerMetadata().getMessageTimestamp();
    long p1LastDataMessageTs =
        p1Messages.get(p1Messages.size() - 1).getValue().getProducerMetadata().getMessageTimestamp();
    long p2LastDataMessageTs =
        p2Messages.get(p2Messages.size() - 1).getValue().getProducerMetadata().getMessageTimestamp();
    long p3LastDataMessageTs = PubSubConstants.PUBSUB_NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION;
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageWithRetries(p0, 5), p0LastDataMessageTs);
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageWithRetries(p1, 5), p1LastDataMessageTs);
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageWithRetries(p2, 5), p2LastDataMessageTs);
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageWithRetries(p3, 5), p3LastDataMessageTs);
    PubSubHelper.produceMessages(pubSubProducerAdapter, p0, 5, 1, true);
    PubSubHelper.produceMessages(pubSubProducerAdapter, p1, 13, 1, true);
    PubSubHelper.produceMessages(pubSubProducerAdapter, p2, 21, 1, true);
    PubSubHelper.produceMessages(pubSubProducerAdapter, p3, 25, 1, true);
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageWithRetries(p0, 5), p0LastDataMessageTs);
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageCached(p0), p0LastDataMessageTs);
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageWithRetries(p1, 5), p1LastDataMessageTs);
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageCached(p1), p1LastDataMessageTs);
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageWithRetries(p2, 5), p2LastDataMessageTs);
    assertEquals(topicManager.getProducerTimestampOfLastDataMessageCached(p2), p2LastDataMessageTs);
    Throwable exception =
        expectThrows(VeniceException.class, () -> topicManager.getProducerTimestampOfLastDataMessageWithRetries(p3, 5));
    assertTrue(exception.getMessage().contains("No data message found in topic-partition: "));
    Throwable exception2 =
        expectThrows(VeniceException.class, () -> topicManager.getProducerTimestampOfLastDataMessageCached(p3));
    assertTrue(exception2.getMessage().contains("No data message found in topic-partition: "));
  }

  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testClose() throws InterruptedException {
    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("nonExistentTopic"));
    assertFalse(topicManager.containsTopic(nonExistentTopic));
    CountDownLatch latch = new CountDownLatch(1);
    Runnable[] tasks = { () -> {
      latch.countDown();
      topicManager.getLatestOffsetCached(nonExistentTopic, 1);
    }, () -> {
      latch.countDown();
      topicManager.getLatestOffsetWithRetries(new PubSubTopicPartitionImpl(nonExistentTopic, 0), 1);
    } };

    ExecutorService executorService = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 20; i++) {
      executorService.submit(tasks[i % tasks.length]);
    }
    latch.await();
    Thread.sleep(100);
    topicManager.close();
    // call close again and it should not throw an exception
    topicManager.close();
  }

}
