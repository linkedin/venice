package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.adapter.PubSubProducerCallbackSimpleImpl;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.testng.annotations.Test;


public class ApacheKafkaProducerCallbackTest {
  @Test
  public void testOnCompletionWithNullExceptionShouldInvokeInternalCallbackWithNullException() {
    PubSubProducerCallbackSimpleImpl internalCallback = new PubSubProducerCallbackSimpleImpl();
    ApacheKafkaProducerCallback kafkaProducerCallback = new ApacheKafkaProducerCallback(internalCallback);
    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topicX", 42), 0, 1, 1676397545, 1L, 11, 12);

    kafkaProducerCallback.onCompletion(recordMetadata, null);

    assertTrue(internalCallback.isInvoked());
    assertNull(internalCallback.getException());
    assertNotNull(internalCallback.getProduceResult());

    PubSubProduceResult produceResult = internalCallback.getProduceResult();
    assertEquals(produceResult.getTopic(), recordMetadata.topic());
    assertEquals(produceResult.getPartition(), recordMetadata.partition());
    assertEquals(produceResult.getOffset(), recordMetadata.offset());
  }

  @Test
  public void testOnCompletionWithNonNullExceptionShouldInvokeInternalCallbackWithNonNullException() {
    PubSubProducerCallbackSimpleImpl internalCallback = new PubSubProducerCallbackSimpleImpl();
    ApacheKafkaProducerCallback kafkaProducerCallback = new ApacheKafkaProducerCallback(internalCallback);
    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topicX", 42), -1, -1, -1, -1L, -1, -1);

    UnknownTopicOrPartitionException exception = new UnknownTopicOrPartitionException("Unknown topic: topicX");
    kafkaProducerCallback.onCompletion(recordMetadata, exception);

    assertTrue(internalCallback.isInvoked());
    assertNotNull(internalCallback.getException());
    assertEquals(internalCallback.getException(), exception);
    assertNull(internalCallback.getProduceResult());
  }

  @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*Unknown topic: topicX.*")
  public void testOnCompletionWithNonNullExceptionShouldCompleteFutureExceptionally()
      throws ExecutionException, InterruptedException {
    PubSubProducerCallbackSimpleImpl internalCallback = new PubSubProducerCallbackSimpleImpl();
    ApacheKafkaProducerCallback kafkaProducerCallback = new ApacheKafkaProducerCallback(internalCallback);
    Future<PubSubProduceResult> produceResultFuture = kafkaProducerCallback.getProduceResultFuture();
    assertFalse(produceResultFuture.isDone());
    assertFalse(produceResultFuture.isCancelled());

    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topicX", 42), 0, 1, 1676397545, 1L, 11, 12);
    UnknownTopicOrPartitionException exception = new UnknownTopicOrPartitionException("Unknown topic: topicX");
    kafkaProducerCallback.onCompletion(recordMetadata, exception);
    assertTrue(internalCallback.isInvoked());
    assertTrue(produceResultFuture.isDone());
    assertFalse(produceResultFuture.isCancelled());
    produceResultFuture.get();
  }

  @Test
  public void testOnCompletionWithNonNullExceptionShouldCompleteFuture()
      throws ExecutionException, InterruptedException {
    PubSubProducerCallbackSimpleImpl internalCallback = new PubSubProducerCallbackSimpleImpl();
    ApacheKafkaProducerCallback kafkaProducerCallback = new ApacheKafkaProducerCallback(internalCallback);
    Future<PubSubProduceResult> produceResultFuture = kafkaProducerCallback.getProduceResultFuture();
    assertFalse(produceResultFuture.isDone());
    assertFalse(produceResultFuture.isCancelled());

    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topicX", 42), 0, 1, 1676397545, 1L, 11, 12);

    kafkaProducerCallback.onCompletion(recordMetadata, null);
    assertTrue(internalCallback.isInvoked());
    assertTrue(produceResultFuture.isDone());
    assertFalse(produceResultFuture.isCancelled());

    PubSubProduceResult produceResult = produceResultFuture.get();
    assertNotNull(produceResult);
    assertEquals(produceResult.getTopic(), "topicX");
    assertEquals(produceResult.getPartition(), 42);
  }
}
