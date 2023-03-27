package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CLIENT_ID;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.Time.MS_PER_SECOND;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.PubSubProducerCallbackSimpleImpl;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.Utils;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Integration tests that are intended to verify the expected behavior from Kafka Producer.
 * Important Notes:
 * 1) Please use this class to verify the guarantees expected from Kafka producer by Venice.
 * 2) Do NOT use this class for testing general venice code paths.
 * 3) Whenever possible tests should be against {@link ApacheKafkaProducerAdapter} and not {@link org.apache.kafka.clients.producer.KafkaProducer}
 */
public class ApacheKafkaProducerAdapterITest {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerAdapterITest.class);

  private PubSubBrokerWrapper pubSubBrokerWrapper;
  // todo: The following AdminClient should be replaced with KafkaAdminClientAdapter when it is available
  private AdminClient kafkaAdminClient;
  private String topicName;
  private ApacheKafkaProducerAdapter producerAdapter;

  @BeforeClass(alwaysRun = true)
  public void setupKafka() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    Properties kafkaAdminProperties = new Properties();
    kafkaAdminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, pubSubBrokerWrapper.getAddress());
    kafkaAdminClient = KafkaAdminClient.create(kafkaAdminProperties);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    kafkaAdminClient.close(Duration.ZERO);
    pubSubBrokerWrapper.close();
  }

  @BeforeMethod(alwaysRun = true)
  public void setupProducerAdapter() {
    topicName = Utils.getUniqueString("test-topic");
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.put(KAFKA_CLIENT_ID, topicName);
    ApacheKafkaProducerConfig producerConfig = new ApacheKafkaProducerConfig(properties);
    producerAdapter = new ApacheKafkaProducerAdapter(producerConfig);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    if (producerAdapter != null) {
      producerAdapter.close(0, false);
    }
  }

  private void createTopic(String topicName, int numPartitions, int replicationFactor, Map<String, String> topicProps) {
    NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor).configs(topicProps);
    CreateTopicsResult createTopicsResult = kafkaAdminClient.createTopics(Collections.singleton(newTopic));
    try {
      createTopicsResult.all().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Failed to create topic: {}", topicName, e);
      throw new RuntimeException(e);
    }
  }

  private KafkaKey getDummyKey() {
    return new KafkaKey(MessageType.PUT, Utils.getUniqueString("key-").getBytes());
  }

  private KafkaMessageEnvelope getDummyVal() {
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.messageSequenceNumber = 0;
    messageEnvelope.producerMetadata.segmentNumber = 0;
    messageEnvelope.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.putValue = ByteBuffer.allocate(1024);
    put.replicationMetadataPayload = ByteBuffer.allocate(0);
    messageEnvelope.payloadUnion = put;
    return messageEnvelope;
  }

  /* The following test verifies that when producer is closed
   * 1) doFlush == true: when flushing is enabled, given sufficient time all records are sent to Kafka
   *                     and all Futures are completed.
   * 2) doFlush == false: upon forceful close (timeout 0 and no flushing) producer doesn't forget to complete
   *                      any Future that it returned in response to sendMessage() call.
   */
  @Test(timeOut = 90 * MS_PER_SECOND, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testProducerCloseDoesNotLeaveAnyFuturesIncomplete(boolean doFlush) throws InterruptedException {
    Map<String, String> topicProps = Collections.singletonMap(RETENTION_MS_CONFIG, Long.toString(Long.MAX_VALUE));
    createTopic(topicName, 1, 1, topicProps);

    KafkaKey m0Key = getDummyKey();
    SimpleBlockingCallback m0BlockingCallback = new SimpleBlockingCallback("m0Key");
    producerAdapter.sendMessage(topicName, null, m0Key, getDummyVal(), null, m0BlockingCallback);

    KafkaKey m1Key = getDummyKey();
    PubSubProducerCallbackSimpleImpl m1Cb = new PubSubProducerCallbackSimpleImpl();
    Future<PubSubProduceResult> m1Future =
        producerAdapter.sendMessage(topicName, null, m1Key, getDummyVal(), null, m1Cb);

    // We want to simulate a case where records sit in producer buffer and are not yet sent Kafka.
    // Hence, we will block current thread until producer's sender(ioThread) is blocked.
    m0BlockingCallback.mutex.lock();
    try {
      while (!m0BlockingCallback.blockedSuccessfully) {
        m0BlockingCallback.blockedSuccessfullyCv.await();
      }
    } finally {
      m0BlockingCallback.mutex.unlock();
    }
    // If control reaches here, it means producer's sender thread is blocked.
    // Message m1 was sent before blocking sender thread. But it's future won't be completed until m0's
    // callback execution is finished.
    assertFalse(m1Future.isDone());

    // Let's add records m2 to m99 to producer's buffer/accumulator
    Map<PubSubProducerCallbackSimpleImpl, Future<PubSubProduceResult>> produceResults = new LinkedHashMap<>(100);
    for (int i = 2; i < 100; i++) {
      KafkaKey mKey = getDummyKey();
      PubSubProducerCallbackSimpleImpl callback = new PubSubProducerCallbackSimpleImpl();
      produceResults.put(callback, producerAdapter.sendMessage(topicName, null, mKey, getDummyVal(), null, callback));
    }

    // Initiate close in another thread as it close() call waits for ioThread to finish.
    // Since we're blocking ioThread in m0's callback calling it from current thread would lead to deadlock.
    Thread closeProducerThread = new Thread(() -> producerAdapter.close(doFlush ? Integer.MAX_VALUE : 0, doFlush));
    closeProducerThread.start();
    // We need to make sure that the Producer::close is always invoked first to ensure the correctness of this test
    waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      assertTrue(
          closeProducerThread.getState().equals(Thread.State.WAITING)
              || closeProducerThread.getState().equals(Thread.State.TIMED_WAITING)
              || closeProducerThread.getState().equals(Thread.State.BLOCKED)
              || closeProducerThread.getState().equals(Thread.State.TERMINATED));
    });

    // Let's make sure that none of the m2 to m99 are ACKed by Kafka yet
    produceResults.forEach((cb, future) -> {
      assertFalse(cb.isInvoked());
      assertFalse(future.isDone());
    });

    LOGGER.info("Unblocking producer's ioThread (sender)...");
    // now unblock sender
    m0BlockingCallback.mutex.lock();
    try {
      m0BlockingCallback.block = false;
      m0BlockingCallback.blockCv.signal();
    } finally {
      m0BlockingCallback.mutex.unlock();
    }
    // In graceful case, m1Future should be completed normally. But in ungraceful case, we cannot make any such
    // assumptions and in such cases all we care about is it gets completed.
    waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> assertTrue(m1Future.isDone()));

    closeProducerThread.join(); // wait for close to finish

    // for most or all of the below sendMessage call we should bet producer closed exception. But by any chance
    // any record gets added to the producer's buffer, then we'll track it to make sure that it eventually gets
    // completed.
    for (int i = 0; i < 10; i++) {
      try {
        producerAdapter.sendMessage(topicName, null, getDummyKey(), getDummyVal(), null, null);
        fail("Sending records after producer has been closed should not succeed");
      } catch (Exception e) {
        // this is expected since producer has been already closed
        LOGGER.info("As expected an exception was received - {}", e.toString()); // make spotbugs happy
      }
    }

    // let's make sure that all Futures returned by sendMessage method are completed exceptionally
    for (Map.Entry<PubSubProducerCallbackSimpleImpl, Future<PubSubProduceResult>> entry: produceResults.entrySet()) {
      PubSubProducerCallbackSimpleImpl cb = entry.getKey();
      Future<PubSubProduceResult> future = entry.getValue();
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> assertTrue(cb.isInvoked()));
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> assertTrue(future.isDone()));
      assertFalse(future.isCancelled());

      if (doFlush) {
        try {
          PubSubProduceResult produceResult = future.get();
          assertNotNull(produceResult);
          assertNotEquals(produceResult.getOffset(), -1);
          assertNull(cb.getException());
        } catch (Exception notExpected) {
          fail("When flush is enabled all messages should be sent to Kafka successfully");
        }
        continue;
      }

      assertNotNull(cb.getException());
      assertNotNull(cb.getException().getMessage());
      assertTrue(cb.getException().getMessage().contains("Producer is closed forcefully."));
      try {
        future.get();
        fail("Exceptionally completed future should throw an exception");
      } catch (Exception expected) {
        LOGGER.info("As expected an exception was received - {}", expected.toString()); // make spotbugs happy
      }
    }
  }

  private static class SimpleBlockingCallback implements PubSubProducerCallback {
    boolean block = true;
    boolean blockedSuccessfully = false;
    String idx;

    Lock mutex = new ReentrantLock();
    Condition blockCv = mutex.newCondition();
    Condition blockedSuccessfullyCv = mutex.newCondition();

    SimpleBlockingCallback(String idx) {
      this.idx = idx;
    }

    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
      LOGGER.info("Blocking callback invoked. Executing thread will be blocked. KeyIdx: {}", idx);
      mutex.lock();
      try {
        while (block) {
          blockedSuccessfully = true;
          blockedSuccessfullyCv.signal();
          blockCv.await();
        }
      } catch (InterruptedException e) {
        LOGGER.error("Something went wrong while waiting to receive unblock signal in blocking callback", e);
      } finally {
        mutex.unlock();
      }
      LOGGER.info("Callback has unblocked executing thread. KeyIdx: {}", idx);
    }
  }

  /* The following test verifies that when a producer is closed it unblocks threads that are
   * blocked in Producer::sendMessage() call.
   */
  @Test(timeOut = 30 * MS_PER_SECOND, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testProducerCloseCanUnblockSendMessageCallerThread(boolean doFlush) {
    ExecutorService executor = Executors.newCachedThreadPool();
    CountDownLatch countDownLatch = new CountDownLatch(1);

    Future<?> sendMessageFuture = executor.submit(() -> {
      Thread.currentThread().setName("sendMessageThread");
      countDownLatch.countDown();
      try {
        // send to non-existent topic
        producerAdapter.sendMessage("topic", null, getDummyKey(), getDummyVal(), null, null);
        LOGGER.error("Expectations were not met in thread: {}", Thread.currentThread().getName());
        fail("sendMessage on non-existent topic should have blocked the executing thread");
      } catch (VeniceException e) {
        LOGGER.info("As expected an exception has been received from sendMessage()", e);
        assertNotNull(e.getMessage(), "Exception thrown by sendMessage does not have a message");
        assertTrue(e.getMessage().contains("Got an error while trying to produce message into Kafka. Topic: 'topic'"));
        assertTrue(ExceptionUtils.recursiveMessageContains(e, "Producer closed while send in progress"));
        assertTrue(ExceptionUtils.recursiveMessageContains(e, "Requested metadata update after close"));
        LOGGER.info("All expectations were met in thread: {}", Thread.currentThread().getName());
      }
    });

    try {
      countDownLatch.await();
      // Still wait for some time to make sure blocking sendMessage is inside kafka before closing it.
      Utils.sleep(50);
      producerAdapter.close(5000, doFlush);
      sendMessageFuture.get(); // this is necessary to check whether expectations in sendMessage thread were met
    } catch (Exception e) {
      fail("Producer closing should have succeeded without an exception", e);
    } finally {
      executor.shutdownNow();
    }
  }
}
