package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessageSerializer;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicAuthorizationException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaProducerAdapterTest {
  private static final String TOPIC_NAME = "test-topic";
  private KafkaProducer<byte[], byte[]> kafkaProducerMock;
  private ApacheKafkaProducerConfig producerConfigMock;
  private PubSubMessageSerializer pubSubMessageSerializerMock;
  private final KafkaKey testKafkaKey = new KafkaKey(MessageType.DELETE, "key".getBytes());
  private final KafkaMessageEnvelope testKafkaValue = new KafkaMessageEnvelope();

  @BeforeMethod
  public void setupMocks() {
    kafkaProducerMock = mock(KafkaProducer.class);
    producerConfigMock = mock(ApacheKafkaProducerConfig.class);
    pubSubMessageSerializerMock = mock(PubSubMessageSerializer.class);
    when(producerConfigMock.getPubSubMessageSerializer()).thenReturn(pubSubMessageSerializerMock);
  }

  @Test(expectedExceptions = PubSubClientException.class, expectedExceptionsMessageRegExp = "The internal KafkaProducer has been closed")
  public void testEnsureProducerIsNotClosedThrowsExceptionWhenProducerIsClosed() {
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(producerConfigMock, kafkaProducerMock);
    doNothing().when(kafkaProducerMock).close(any());
    producerAdapter.close(10);
    producerAdapter.sendMessage(TOPIC_NAME, 0, testKafkaKey, testKafkaValue, null, null);
  }

  @Test
  public void testGetNumberOfPartitions() {
    List<PartitionInfo> list = new ArrayList<>();
    when(kafkaProducerMock.partitionsFor(TOPIC_NAME)).thenReturn(list);
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(producerConfigMock, kafkaProducerMock);
    assertEquals(producerAdapter.getNumberOfPartitions(TOPIC_NAME), 0);
  }

  @Test
  public void testSendMessageThrowsAnExceptionOnTimeout() {
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(producerConfigMock, kafkaProducerMock);

    doThrow(TimeoutException.class).when(kafkaProducerMock).send(any(), any());
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null));

    doThrow(AuthenticationException.class).when(kafkaProducerMock).send(any(), any());
    assertThrows(
        PubSubTopicAuthorizationException.class,
        () -> producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null));

    doThrow(AuthorizationException.class).when(kafkaProducerMock).send(any(), any());
    assertThrows(
        PubSubTopicAuthorizationException.class,
        () -> producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null));

    doThrow(UnknownTopicOrPartitionException.class).when(kafkaProducerMock).send(any(), any());
    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null));

    doThrow(InvalidTopicException.class).when(kafkaProducerMock).send(any(), any());
    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null));

    doThrow(KafkaException.class).when(kafkaProducerMock).send(any(), any());
    assertThrows(
        PubSubClientException.class,
        () -> producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null));
  }

  @Test
  public void testSendMessageInteractionWithInternalProducer() {
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(producerConfigMock, kafkaProducerMock);
    Future<RecordMetadata> recordMetadataFutureMock = mock(Future.class);

    // interaction (1) when pubsub-callback is null
    when(kafkaProducerMock.send(any(ProducerRecord.class), isNull())).thenReturn(recordMetadataFutureMock);
    Future<PubSubProduceResult> produceResultFuture =
        producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null);
    assertNotNull(produceResultFuture);
    verify(kafkaProducerMock, never()).send(any(ProducerRecord.class), isNull());
    verify(kafkaProducerMock, times(1)).send(any(ProducerRecord.class), any(Callback.class));

    // interaction (1) when pubsub-callback is non-null
    PubSubProducerCallback producerCallbackMock = mock(PubSubProducerCallback.class);
    when(kafkaProducerMock.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(recordMetadataFutureMock);
    produceResultFuture =
        producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, producerCallbackMock);
    assertNotNull(produceResultFuture);
    verify(kafkaProducerMock, never()).send(any(ProducerRecord.class), isNull());
    verify(kafkaProducerMock, times(2)).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testFlushInvokesInternalProducerFlushIfProducerIsNotClosed() {
    doNothing().when(kafkaProducerMock).flush();
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(producerConfigMock, kafkaProducerMock);
    producerAdapter.flush();
    verify(kafkaProducerMock, times(1)).flush();
  }

  @Test
  public void testFlushDoesNotInvokeInternalProducerFlushIfProducerIs1Closed() {
    doNothing().when(kafkaProducerMock).flush();
    doNothing().when(kafkaProducerMock).close();
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(producerConfigMock, kafkaProducerMock);
    producerAdapter.close(10); // close without flushing
    producerAdapter.flush();
    verify(kafkaProducerMock, never()).flush();
  }

  @Test
  public void testGetMeasurableProducerMetricsReturnsEmptyMapWhenProducerIsClosed() {
    doNothing().when(kafkaProducerMock).close();
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(producerConfigMock, kafkaProducerMock);
    producerAdapter.close(10); // close without flushing
    Object2DoubleMap<String> metrics = producerAdapter.getMeasurableProducerMetrics();
    assertNotNull(metrics, "Returned metrics cannot be null");
    assertEquals(metrics.size(), 0, "Should return empty metrics when producer is closed");
  }

  @Test
  public void testGetMeasurableProducerMetricsReturnsMetricsOfTypeDouble() {
    // for the following metric value is of type double and hence this metric should be extracted
    Map<MetricName, Metric> metricsMap = new LinkedHashMap<>();
    MetricName metricName1 = new MetricName("metric-1", "g1", "desc", new HashMap<>());
    Metric kafkaMetric1 = mock(Metric.class);
    doReturn(20.23d).when(kafkaMetric1).metricValue();
    metricsMap.put(metricName1, kafkaMetric1);

    // for the following metric value is of type int and hence this metric should be ignored
    MetricName metricName2 = new MetricName("metric-2", "g1", "desc", new HashMap<>());
    Metric kafkaMetric2 = mock(Metric.class);
    doReturn(314).when(kafkaMetric2).metricValue();
    metricsMap.put(metricName2, kafkaMetric2);

    // when exception occurs during metric extraction it will be ignored
    MetricName metricName3 = new MetricName("metric-3", "g1", "desc", new HashMap<>());
    Metric kafkaMetric3 = mock(Metric.class);
    doThrow(new NullPointerException("Failed to extract value of metric-3")).when(kafkaMetric3).metricValue();
    metricsMap.put(metricName3, kafkaMetric3);

    doReturn(metricsMap).when(kafkaProducerMock).metrics();
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(producerConfigMock, kafkaProducerMock);
    Object2DoubleMap<String> metrics = producerAdapter.getMeasurableProducerMetrics();
    assertNotNull(metrics, "Returned metrics cannot be null");
    assertEquals(metrics.size(), 1);
    assertTrue(metrics.containsKey("metric-1"));
    assertEquals(metrics.get("metric-1"), 20.23d);
  }

  @Test
  public void testGetBrokerAddress() {
    when(producerConfigMock.getBrokerAddress()).thenReturn("venice.kafka.db:2023");
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(producerConfigMock, kafkaProducerMock);
    assertEquals(producerAdapter.getBrokerAddress(), "venice.kafka.db:2023");
    verify(producerConfigMock, times(1)).getBrokerAddress();
  }
}
