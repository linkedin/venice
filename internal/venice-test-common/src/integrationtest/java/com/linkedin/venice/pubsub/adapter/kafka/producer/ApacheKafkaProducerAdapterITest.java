package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BATCH_SIZE;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_LINGER_MS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests specific to Kafka Producer.
 * Important Notes:
 * 1) Please use this class to verify the guarantees expected from Kafka producer by Venice.
 * 2) Do NOT use this class for testing general venice code paths.
 * 3) Whenever possible tests should be against {@link ApacheKafkaProducerAdapter} and not {@link org.apache.kafka.clients.producer.KafkaProducer}
 */
public class ApacheKafkaProducerAdapterITest {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerAdapterITest.class);

  private ZkServerWrapper zkServerWrapper;
  private KafkaBrokerWrapper kafkaBrokerWrapper;
  // todo: AdminClient should be replaced with KafkaAdminClientAdapter when it is available
  private AdminClient kafkaAdminClient;

  @BeforeClass(alwaysRun = true)
  public void setupKafka() {
    zkServerWrapper = ServiceFactory.getZkServer();
    kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);
    Properties kafkaAdminProperties = new Properties();
    kafkaAdminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerWrapper.getAddress());
    kafkaAdminClient = KafkaAdminClient.create(kafkaAdminProperties);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    kafkaAdminClient.close(Duration.ZERO);
    kafkaBrokerWrapper.close();
    zkServerWrapper.close();
  }

  private void createTopic(
      String topicName,
      int numPartitions,
      int replicationFactor,
      Map<String, String> topicPropertiesMap) {
    NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor).configs(topicPropertiesMap);
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

  @Test
  public void testProducerUngracefulCloseFromCallback() throws ExecutionException, InterruptedException {
    String topicName = Utils.getUniqueString("test-topic-for-callback-close-test");
    int batchSize = 8 * 1024;
    createTopic(
        topicName,
        1,
        1,
        Collections.singletonMap(TopicConfig.RETENTION_MS_CONFIG, Long.toString(Long.MAX_VALUE)));
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress());
    properties.put(KAFKA_LINGER_MS, 0);
    properties.put(KAFKA_BATCH_SIZE, batchSize);
    // properties.put(KAFKA_VALUE_SERIALIZER, ByteArraySerializer.class.getName());
    ApacheKafkaProducerConfig producerConfig = new ApacheKafkaProducerConfig(properties);
    ApacheKafkaProducerAdapter producer = new ApacheKafkaProducerAdapter(producerConfig);

    // case 1: block and drop messages in accumulator
    KafkaKey m0Key = getDummyKey();
    SimpleBlockingCallback m0BlockingCallback = new SimpleBlockingCallback("m0Key");
    Future<PubSubProduceResult> m0Future =
        producer.sendMessage(topicName, null, m0Key, getDummyVal(), null, m0BlockingCallback);

    KafkaKey m1Key = getDummyKey();
    SimpleLoggingPubSubProducerCallbackImpl m1SimpleCallback = new SimpleLoggingPubSubProducerCallbackImpl("m1Key");
    Future<PubSubProduceResult> m1Future =
        producer.sendMessage(topicName, null, m1Key, getDummyVal(), null, m1SimpleCallback);

    // wait until producer's sender(ioThread) is blocked
    m0BlockingCallback.mutex.lock();
    try {
      while (!m0BlockingCallback.blockedSuccessfully) {
        m0BlockingCallback.blockedSuccessfullyCv.await();
      }
    } finally {
      m0BlockingCallback.mutex.unlock();
    }
    // if control reaches here, it means producer's sender thread is blocked
    assertFalse(m1Future.isDone());

    // let's add some records (m1 to m99) to producer's buffer/accumulator
    Map<SimpleLoggingPubSubProducerCallbackImpl, Future<PubSubProduceResult>> produceResults = new LinkedHashMap<>(100);
    for (int i = 0; i < 100; i++) {
      KafkaKey mKey = getDummyKey();
      SimpleLoggingPubSubProducerCallbackImpl callback = new SimpleLoggingPubSubProducerCallbackImpl("" + i);
      produceResults.put(callback, producer.sendMessage(topicName, null, mKey, getDummyVal(), null, callback));
    }

    // let's make sure that none of the m1 to m99 are ACKed by Kafka yet
    produceResults.forEach((cb, future) -> {
      assertFalse(cb.isInvoked());
      assertFalse(future.isDone());
    });

    LOGGER.info("Unblocking sender");
    // now unblock sender
    m0BlockingCallback.mutex.lock();
    try {
      m0BlockingCallback.block = false;
      m0BlockingCallback.blockCv.signal();
    } finally {
      m0BlockingCallback.mutex.unlock();
    }
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> assertTrue(m1Future.isDone()));
    // TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> assertTrue(m1SimpleCallback.isInvoked()));

    produceResults.forEach((key, value) -> System.out.println(key + " - " + value));
    System.out.println("m0 --> " + m0BlockingCallback + " - " + m0Future);
    System.out.println("m1 --> " + m1SimpleCallback + " - " + m1Future);

    for (int i = 0; i < 100; i++) {
      try {
        LOGGER.info("sending record for:", i);

        KafkaKey mKey = getDummyKey();
        SimpleLoggingPubSubProducerCallbackImpl callback = new SimpleLoggingPubSubProducerCallbackImpl("post-" + i);
        Future<PubSubProduceResult> resultFuture =
            producer.sendMessage(topicName, null, mKey, getDummyVal(), null, callback);
        LOGGER.info("send record for:", resultFuture);
        PubSubProduceResult produceResult = resultFuture.get();
        assertNotNull(produceResult);
        LOGGER.info("result:", produceResult);
      } catch (Exception e) {
        LOGGER.error("failed at: {}", i, e);
        break;
      }
    }

    // let's make sure that none of the m1 to m99 are ACKed by Kafka yet
    produceResults.forEach((cb, future) -> {
      assertFalse(cb.isInvoked());
      assertFalse(future.isDone());
    });

    //
    // try {
    // future.get();
    // } catch (Exception e) {
    // LOGGER.error(e);
    // }
    //
    // LOGGER.info("##CB##: {} invoked: {}", pubSubProducerCallback, pubSubProducerCallback.isInvoked());
    // System.out.println("hello");

    // case 2: ignore messages sent

  }

  static class SimpleBlockingCallback implements PubSubProducerCallback {
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
      LOGGER.info("Callback thread has unblocked executing thread. KeyIdx: {}", idx);
    }
  }

  static class SimpleLoggingPubSubProducerCallbackImpl implements PubSubProducerCallback {
    private PubSubProduceResult produceResult;
    private Exception exception;
    private boolean isInvoked;
    private String idx;

    SimpleLoggingPubSubProducerCallbackImpl(String idx) {
      this.idx = idx;
    }

    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
      LOGGER.info("Callback(idx:{}) -  produceResult:{} {} ", idx, produceResult, exception);
      this.isInvoked = true;
      this.produceResult = produceResult;
      this.exception = exception;
    }

    public boolean isInvoked() {
      return isInvoked;
    }

    public PubSubProduceResult getProduceResult() {
      return produceResult;
    }

    public Exception getException() {
      return exception;
    }
  }

}
