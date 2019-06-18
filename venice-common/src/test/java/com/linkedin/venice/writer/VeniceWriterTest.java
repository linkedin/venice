package com.linkedin.venice.writer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.annotations.Test;


@Test
public class VeniceWriterTest {

  @Test
  public void testThreadSafety() throws ExecutionException, InterruptedException, IOException {
    ExecutorService executorService = null;
    try (KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();
        TopicManager topicManager = new TopicManager(kafka.getZkAddress(), TestUtils.getVeniceConsumerFactory(kafka.getAddress()))) {
      String topicName = TestUtils.getUniqueString("topic-for-vw-thread-safety");
      topicManager.createTopic(topicName, 1, 1, true);
      Properties properties = new Properties();
      properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());

      try (VeniceWriter veniceWriter = new VeniceWriterFactory(properties).getVeniceWriter(topicName)) {
        int numberOfThreads = 100;
        executorService = Executors.newFixedThreadPool(numberOfThreads);
        Future[] vwFutures = new Future[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
          vwFutures[i] = executorService.submit(() -> veniceWriter.put(
              new KafkaKey(MessageType.PUT, "blah".getBytes()), "blah".getBytes(), 1, null));
        }
        for (int i = 0; i < numberOfThreads; i++) {
          vwFutures[i].get();
        }
      }
    } finally {
      if (executorService != null) {
        executorService.shutdown();
      }
    }
  }
}
