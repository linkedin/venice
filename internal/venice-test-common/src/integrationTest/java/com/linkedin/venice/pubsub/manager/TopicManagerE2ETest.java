package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubAdminAdapterContext;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.PubSubHelper;
import com.linkedin.venice.utils.PubSubHelper.MutableDefaultPubSubMessage;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TopicManagerE2ETest {
  private static final int PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS = 10_000;

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
    pubSubMessageDeserializer = PubSubMessageDeserializer.createDefaultDeserializer();
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
    pubSubProducerAdapterLazy = Lazy.of(
        () -> pubSubClientsFactory.getProducerAdapterFactory()
            .create(
                new PubSubProducerAdapterContext.Builder().setVeniceProperties(veniceProperties)
                    .setBrokerAddress(pubSubBrokerWrapper.getAddress())
                    .setProducerName(clientId)
                    .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                    .build()));
    pubSubAdminAdapterLazy = Lazy.of(
        () -> pubSubClientsFactory.getAdminAdapterFactory()
            .create(
                new PubSubAdminAdapterContext.Builder().setAdminClientName(clientId)
                    .setVeniceProperties(veniceProperties)
                    .setPubSubTopicRepository(pubSubTopicRepository)
                    .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                    .build()));
    pubSubConsumerAdapterLazy = Lazy.of(
        () -> pubSubClientsFactory.getConsumerAdapterFactory()
            .create(
                new PubSubConsumerAdapterContext.Builder().setVeniceProperties(veniceProperties)
                    .setConsumerName(clientId)
                    .setPubSubMessageDeserializer(pubSubMessageDeserializer)
                    .build()));

    metricsRepository = new MetricsRepository();
    topicManagerContextBuilder = new TopicManagerContext.Builder().setPubSubTopicRepository(pubSubTopicRepository)
        .setMetricsRepository(metricsRepository)
        .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(veniceProperties))
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
      pubSubProducerAdapterLazy.get().close(0);
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

  @Test(timeOut = 5 * Time.MS_PER_MINUTE)
  public void testConcurrentApiExecution() throws ExecutionException, InterruptedException, TimeoutException {
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
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(testTopic, 0);
    List<MutableDefaultPubSubMessage> messages =
        PubSubHelper.produceMessages(pubSubProducerAdapter, topicPartition, numMessages, 2, false);
    long timeBeforeProduce = messages.get(0).getTimestampBeforeProduce() - 10;
    long tsOfLastDataMessage = messages.get(messages.size() - 1).getValue().getProducerMetadata().getMessageTimestamp();

    PubSubTopicPartition nonExistentTopicPartition = new PubSubTopicPartitionImpl(nonExistentTopic, 0);

    // get partition count for an existing topic
    Runnable t1 = () -> assertEquals(topicManager.getPartitionCount(testTopic), numPartitions);

    // get partition count for a non-existent topic
    Runnable t2 = () -> assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.getPartitionCount(nonExistentTopic));

    // contains topic
    Runnable t3 = () -> assertTrue(topicManager.containsTopic(testTopic));

    // contains topic for non-existent topic
    Runnable t4 = () -> assertFalse(topicManager.containsTopic(nonExistentTopic));

    // contains topic cached
    Runnable t5 = () -> assertTrue(topicManager.containsTopicCached(testTopic));

    // contains topic cached for non-existent topic
    Runnable t6 = () -> assertFalse(topicManager.containsTopicCached(nonExistentTopic));

    // get latest offset with retries for an existing topic
    Runnable t7 = () -> assertEquals(topicManager.getLatestOffsetWithRetries(topicPartition, 1), numMessages);

    // get latest offset with retries for a non-existent topic
    Runnable t8 = () -> assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.getLatestOffsetWithRetries(nonExistentTopicPartition, 1));

    // get latest offset cached for an existing topic
    Runnable t9 = () -> assertEquals(topicManager.getLatestOffsetCached(testTopic, 0), numMessages);

    // get latest offset cached for a non-existent topic
    Runnable t10 = () -> assertEquals(
        topicManager.getLatestOffsetCached(nonExistentTopic, 0),
        StatsErrorCode.LAG_MEASUREMENT_FAILURE.code);

    // get offset by time for an existing topic
    Runnable t15 =
        () -> assertEquals(topicManager.getOffsetByTime(topicPartition, System.currentTimeMillis()), numMessages);

    // get offset by time for a non-existent topic
    Runnable t16 = () -> assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.getOffsetByTime(nonExistentTopicPartition, tsOfLastDataMessage));

    // get offset by time for an existing topic: first message
    Runnable t17 = () -> assertEquals(topicManager.getOffsetByTime(topicPartition, timeBeforeProduce), 0);

    // invalidate cache for an existing topic
    Runnable t18 = () -> topicManager.invalidateCache(testTopic);

    // invalidate cache for a non-existent topic
    Runnable t19 = () -> topicManager.invalidateCache(nonExistentTopic);

    List<Runnable> tasks = Arrays.asList(t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t15, t16, t17, t18, t19);

    final AtomicInteger successfulRequests = new AtomicInteger(0);
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    int totalTasks = 100;
    for (int i = 0; i < totalTasks; i++) {
      int finalI = i;
      futures.add(
          CompletableFuture.runAsync(() -> tasks.get(finalI % tasks.size()).run(), executorService)
              .thenAccept(v -> successfulRequests.incrementAndGet()));
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.MINUTES);
    assertEquals(successfulRequests.get(), totalTasks);
  }

  @Test(timeOut = 3 * Time.MS_PER_MINUTE)
  public void testMetadataApisForNonExistentTopics() throws ExecutionException, InterruptedException, TimeoutException {
    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("nonExistentTopic"));
    assertFalse(topicManager.containsTopicCached(nonExistentTopic));
    assertFalse(topicManager.containsTopic(nonExistentTopic));
    Map<PubSubTopicPartition, PubSubPosition> nonExistentTopicLatestOffsets =
        topicManager.getEndPositionsForTopicWithRetries(nonExistentTopic);
    assertNotNull(nonExistentTopicLatestOffsets);
    assertEquals(nonExistentTopicLatestOffsets.size(), 0);
    assertThrows(PubSubTopicDoesNotExistException.class, () -> topicManager.getPartitionCount(nonExistentTopic));
    PubSubTopicPartitionImpl nonExistentTopicPartition = new PubSubTopicPartitionImpl(nonExistentTopic, 0);
    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> topicManager.getOffsetByTime(nonExistentTopicPartition, System.currentTimeMillis()));
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
    Map<PubSubTopicPartition, PubSubPosition> endOffsets =
        topicManager.getEndPositionsForTopicWithRetries(existingTopic);
    Map<PubSubTopicPartition, PubSubPosition> startOffsets =
        topicManager.getStartPositionsForTopicWithRetries(existingTopic);
    assertNotNull(endOffsets, "End offsets should not be null");
    assertEquals(
        endOffsets.size(),
        numPartitions,
        "End offsets size should match number of partitions. Size: " + endOffsets.size() + ", Partitions: "
            + numPartitions);
    assertNotNull(startOffsets, "Start offsets should not be null");
    assertEquals(
        startOffsets.size(),
        numPartitions,
        "Start offsets size should match number of partitions. Size: " + startOffsets.size() + ", Partitions: "
            + numPartitions);
    for (Map.Entry<PubSubTopicPartition, PubSubPosition> entry: endOffsets.entrySet()) {
      PubSubPosition startOffset = startOffsets.get(entry.getKey());
      PubSubPosition endOffset = entry.getValue();
      PubSubTopicPartition partition = entry.getKey();
      long diff = topicManager.diffPosition(partition, startOffset, endOffset);
      assertEquals(
          diff,
          0L,
          "Start and end offsets should be equal for a new topic partition: " + partition + ". Start: " + startOffset
              + ", End: " + endOffset);
      long compare = topicManager.comparePosition(partition, startOffset, endOffset);
      assertEquals(
          compare,
          0L,
          "Start and end offsets should be equal for a new topic partition: " + partition + ". Start: " + startOffset
              + ", End: " + endOffset);
    }
    assertEquals(topicManager.getPartitionCount(existingTopic), numPartitions);

    PubSubTopicPartition p0 = new PubSubTopicPartitionImpl(existingTopic, 0);
    PubSubTopicPartition p1 = new PubSubTopicPartitionImpl(existingTopic, 1);
    PubSubTopicPartition p2 = new PubSubTopicPartitionImpl(existingTopic, 2);

    // produce messages to the topic-partitions: p0, p1, p2
    PubSubProducerAdapter pubSubProducerAdapter = pubSubProducerAdapterLazy.get();
    List<MutableDefaultPubSubMessage> p0Messages =
        PubSubHelper.produceMessages(pubSubProducerAdapter, p0, 10, 10, false);
    List<MutableDefaultPubSubMessage> p1Messages =
        PubSubHelper.produceMessages(pubSubProducerAdapter, p1, 14, 10, false);
    List<MutableDefaultPubSubMessage> p2Messages =
        PubSubHelper.produceMessages(pubSubProducerAdapter, p2, 19, 10, false);

    // get the latest offsets
    endOffsets = topicManager.getEndPositionsForTopicWithRetries(existingTopic);
    assertNotNull(endOffsets);
    assertEquals(endOffsets.size(), numPartitions);
    long numRecordsInP0 = topicManager.getNumRecordsInPartition(p0);
    assertEquals(
        numRecordsInP0,
        p0Messages.size(),
        "Number of records in partition p0 should match produced messages size. " + "Expected: " + p0Messages.size()
            + ", Actual: " + numRecordsInP0);
    assertEquals(topicManager.getLatestOffsetWithRetries(p0, 5), p0Messages.size());
    assertEquals(topicManager.getLatestOffsetCached(p0.getPubSubTopic(), 0), p0Messages.size());

    long numRecordsInP1 = topicManager.getNumRecordsInPartition(p1);
    assertEquals(
        numRecordsInP1,
        p1Messages.size(),
        "Number of records in partition p1 should match produced messages size. " + "Expected: " + p1Messages.size()
            + ", Actual: " + numRecordsInP1);
    assertEquals(topicManager.getLatestOffsetWithRetries(p1, 5), p1Messages.size());
    assertEquals(topicManager.getLatestOffsetCached(p1.getPubSubTopic(), 1), p1Messages.size());

    long numRecordsInP2 = topicManager.getNumRecordsInPartition(p2);
    assertEquals(
        numRecordsInP2,
        p2Messages.size(),
        "Number of records in partition p2 should match produced messages size. " + "Expected: " + p2Messages.size()
            + ", Actual: " + numRecordsInP2);
    assertEquals(topicManager.getLatestOffsetWithRetries(p2, 5), p2Messages.size());
    assertEquals(topicManager.getLatestOffsetCached(p2.getPubSubTopic(), 2), p2Messages.size());

    // except for the first 3 partitions, the latest offset should be 0
    for (int i = 3; i < numPartitions; i++) {
      PubSubTopicPartition partition = new PubSubTopicPartitionImpl(existingTopic, i);
      long numRecordsInPartition = topicManager.getNumRecordsInPartition(partition);
      assertEquals(
          numRecordsInPartition,
          0L,
          "Number of records in partition " + partition + " should be 0. Actual: " + numRecordsInPartition);
      assertEquals(topicManager.getLatestOffsetWithRetries(new PubSubTopicPartitionImpl(existingTopic, i), 5), 0L);
      assertEquals(topicManager.getLatestOffsetCached(existingTopic, i), 0L);
    }

    // if timestamp is greater than the latest message timestamp, the offset returned should be the latest offset
    long timestamp = System.currentTimeMillis();
    assertEquals(topicManager.getOffsetByTime(p0, timestamp), p0Messages.size());

    // If the provided timestamp is less than or equal to the timestamp of a message,
    // the offset returned should correspond to that message.
    long tsAfterM4ButBeforeM5 = p0Messages.get(4).getTimestampAfterProduce() + 1;
    assertTrue(tsAfterM4ButBeforeM5 > p0Messages.get(4).getTimestampAfterProduce());
    assertTrue(tsAfterM4ButBeforeM5 < p0Messages.get(5).getTimestampBeforeProduce());
    assertEquals(topicManager.getOffsetByTime(p0, tsAfterM4ButBeforeM5), 5);

    long p0TsBeforeM0 = p0Messages.get(0).getTimestampBeforeProduce();
    assertEquals(topicManager.getOffsetByTime(p0, p0TsBeforeM0), 0);
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
