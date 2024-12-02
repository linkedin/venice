package com.linkedin.venice.pubsub.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.TestUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.testng.annotations.Test;


public class PubSubProducerAdapterConcurrentDelegatorTest {
  @Test
  public void testConstructor() {
    String producingTopic = "test_rt";
    assertThrows(
        VeniceException.class,
        () -> new PubSubProducerAdapterConcurrentDelegator(
            producingTopic,
            1,
            10 * 1024 * 1024,
            () -> mock(PubSubProducerAdapter.class)));
    assertThrows(
        VeniceException.class,
        () -> new PubSubProducerAdapterConcurrentDelegator(
            producingTopic,
            2,
            10 * 1024,
            () -> mock(PubSubProducerAdapter.class)));

    int threadCnt = 3;
    int queueSize = 10 * 1024 * 1024;
    AtomicInteger producerInstanceCount = new AtomicInteger(0);
    List<PubSubProducerAdapter> producers = new ArrayList<>();
    PubSubProducerAdapterConcurrentDelegator delegator =
        new PubSubProducerAdapterConcurrentDelegator(producingTopic, threadCnt, queueSize, () -> {
          producerInstanceCount.incrementAndGet();
          PubSubProducerAdapter producer = mock(PubSubProducerAdapter.class);
          producers.add(producer);
          return producer;
        });
    delegator.close(100);
    assertEquals(producerInstanceCount.get(), threadCnt);
    assertEquals(producers.size(), threadCnt);
    producers.forEach(p -> verify(p).close(100));
  }

  @Test
  public void testProducerQueueNode() {
    String topic = "test_rt";
    KafkaKey key = new KafkaKey(MessageType.PUT, "test_key".getBytes());
    KafkaMessageEnvelope value = new KafkaMessageEnvelope(
        MessageType.PUT.getValue(),
        new ProducerMetadata(),
        new Put(ByteBuffer.wrap("test_value".getBytes()), 1, -1, ByteBuffer.wrap(new byte[0])),
        null);

    PubSubProducerAdapterConcurrentDelegator.ProducerQueueNode node =
        new PubSubProducerAdapterConcurrentDelegator.ProducerQueueNode(
            topic,
            1,
            key,
            value,
            new PubSubMessageHeaders(),
            null,
            null);
    assertTrue(node.getHeapSize() > 0);
  }

  @Test
  public void testProducerQueueDrainer() throws InterruptedException {
    String topic = "test_rt";
    KafkaKey key1 = mock(KafkaKey.class);
    CompletableFuture<PubSubProduceResult> future1 = mock(CompletableFuture.class);
    KafkaKey key2 = mock(KafkaKey.class);
    CompletableFuture<PubSubProduceResult> future2 = mock(CompletableFuture.class);

    PubSubProducerAdapterConcurrentDelegator.ProducerQueueNode node1 =
        new PubSubProducerAdapterConcurrentDelegator.ProducerQueueNode(
            topic,
            1,
            key1,
            mock(KafkaMessageEnvelope.class),
            new PubSubMessageHeaders(),
            null,
            future1);

    PubSubProducerAdapterConcurrentDelegator.ProducerQueueNode node2 =
        new PubSubProducerAdapterConcurrentDelegator.ProducerQueueNode(
            topic,
            1,
            key2,
            mock(KafkaMessageEnvelope.class),
            new PubSubMessageHeaders(),
            null,
            future2);

    BlockingQueue<PubSubProducerAdapterConcurrentDelegator.ProducerQueueNode> queue = new LinkedBlockingQueue<>();
    queue.put(node1);
    queue.put(node2);

    PubSubProducerAdapter producer = mock(PubSubProducerAdapter.class);
    PubSubProduceResult mockSuccessfulResult = mock(PubSubProduceResult.class);
    doReturn(CompletableFuture.completedFuture(mockSuccessfulResult)).when(producer)
        .sendMessage(eq(topic), eq(1), eq(key1), any(), any(), any());
    Exception mockException = mock(Exception.class);
    CompletableFuture<PubSubProduceResult> exceptionFuture = new CompletableFuture<>();
    exceptionFuture.completeExceptionally(mockException);
    doReturn(exceptionFuture).when(producer).sendMessage(eq(topic), eq(1), eq(key2), any(), any(), any());

    PubSubProducerAdapterConcurrentDelegator.ProducerQueueDrainer drainer =
        new PubSubProducerAdapterConcurrentDelegator.ProducerQueueDrainer(topic, queue, 1, producer);

    Thread drainerThread = new Thread(() -> drainer.run());
    drainerThread.start();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(producer).sendMessage(eq(topic), eq(1), eq(key1), any(), any(), any());
      verify(producer).sendMessage(eq(topic), eq(1), eq(key2), any(), any(), any());

      verify(future1).complete(mockSuccessfulResult);
      verify(future2).completeExceptionally(mockException);

    });
    drainerThread.interrupt();
  }

  @Test
  public void testDelegator() {
    PubSubProducerAdapter mockProducer1 = mock(PubSubProducerAdapter.class);
    PubSubProducerAdapter mockProducer2 = mock(PubSubProducerAdapter.class);

    doReturn(CompletableFuture.completedFuture(mock(PubSubProduceResult.class))).when(mockProducer1)
        .sendMessage(any(), any(), any(), any(), any(), any());
    doReturn(CompletableFuture.completedFuture(mock(PubSubProduceResult.class))).when(mockProducer2)
        .sendMessage(any(), any(), any(), any(), any(), any());

    List<PubSubProducerAdapter> producers = Arrays.asList(mockProducer1, mockProducer2);

    int threadCnt = 2;

    AtomicInteger producerIdx = new AtomicInteger(0);
    Supplier<PubSubProducerAdapter> producerSupplier = () -> {
      int currentProducerIdx = producerIdx.getAndIncrement();
      return producers.get(currentProducerIdx);
    };
    String testTopicName = "test_rt";
    PubSubProducerAdapterConcurrentDelegator delegator =
        new PubSubProducerAdapterConcurrentDelegator(testTopicName, threadCnt, 5 * 1024 * 1204, producerSupplier);

    delegator.getNumberOfPartitions(testTopicName);
    verify(mockProducer1).getNumberOfPartitions(testTopicName);

    KafkaMessageEnvelope value = new KafkaMessageEnvelope(
        MessageType.PUT.getValue(),
        new ProducerMetadata(),
        new Put(ByteBuffer.wrap("test_value".getBytes()), 1, -1, ByteBuffer.wrap(new byte[0])),
        null);

    delegator.sendMessage(testTopicName, 1, mock(KafkaKey.class), value, mock(PubSubMessageHeaders.class), null);
    delegator.sendMessage(testTopicName, 2, mock(KafkaKey.class), value, mock(PubSubMessageHeaders.class), null);
    delegator.sendMessage(testTopicName, 3, mock(KafkaKey.class), value, mock(PubSubMessageHeaders.class), null);
    delegator.sendMessage(testTopicName, 1, mock(KafkaKey.class), value, mock(PubSubMessageHeaders.class), null);
    delegator.sendMessage(testTopicName, 2, mock(KafkaKey.class), value, mock(PubSubMessageHeaders.class), null);

    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      verify(mockProducer1, times(2)).sendMessage(eq(testTopicName), eq(1), any(), any(), any(), any());
      verify(mockProducer1, times(1)).sendMessage(eq(testTopicName), eq(3), any(), any(), any(), any());
      verify(mockProducer2, times(2)).sendMessage(eq(testTopicName), eq(2), any(), any(), any(), any());
    });

    delegator.flush();
    verify(mockProducer1).flush();
    verify(mockProducer2).flush();

    delegator.close(1);
    verify(mockProducer1).close(1);
    verify(mockProducer2).close(1);

    assertThrows(
        VeniceException.class,
        () -> delegator
            .sendMessage(testTopicName, 2, mock(KafkaKey.class), value, mock(PubSubMessageHeaders.class), null));
    assertTrue(delegator.hasAnyProducerStopped());

    assertTrue(delegator.getMeasurableProducerMetrics().isEmpty());

    delegator.getBrokerAddress();
    verify(mockProducer1).getBrokerAddress();
  }
}
