package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StorePartitionDataReceiverTest {
  private StoreIngestionTask mockSIT;
  private PubSubTopicPartition topicPartition;
  private StorePartitionDataReceiver receiver;
  private List<DefaultPubSubMessage> emptyMessages;

  @BeforeMethod
  public void setUp() {
    mockSIT = mock(StoreIngestionTask.class);
    PubSubTopicRepository topicRepo = new PubSubTopicRepository();
    PubSubTopic versionTopic = topicRepo.getTopic("test_store_v1");
    topicPartition = new PubSubTopicPartitionImpl(versionTopic, 3);
    when(mockSIT.getVersionTopic()).thenReturn(versionTopic);
    when(mockSIT.isRunning()).thenReturn(true);

    receiver = new StorePartitionDataReceiver(mockSIT, topicPartition, "broker1:9092", 0);
    emptyMessages = Collections.emptyList();
  }

  @Test
  public void testPubSubExceptionRoutedToPartitionLevel_WhenPauseEnabled() throws Exception {
    when(mockSIT.isPubSubPartitionPauseEnabled()).thenReturn(true);
    PubSubClientException pubSubException = new PubSubClientException("Broker unavailable");
    doThrow(pubSubException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    receiver.write(emptyMessages);

    verify(mockSIT).setIngestionException(eq(3), eq(pubSubException), eq("broker1:9092"));
    verify(mockSIT, never()).setLastConsumerException(any());
  }

  @Test
  public void testRetriablePubSubExceptionRoutedToPartitionLevel_WhenPauseEnabled() throws Exception {
    when(mockSIT.isPubSubPartitionPauseEnabled()).thenReturn(true);
    PubSubClientRetriableException retriableException = new PubSubOpTimeoutException("Poll timed out");
    doThrow(retriableException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    receiver.write(emptyMessages);

    verify(mockSIT).setIngestionException(eq(3), eq(retriableException), eq("broker1:9092"));
    verify(mockSIT, never()).setLastConsumerException(any());
  }

  @Test
  public void testWrappedPubSubExceptionRoutedToPartitionLevel_WhenPauseEnabled() throws Exception {
    when(mockSIT.isPubSubPartitionPauseEnabled()).thenReturn(true);
    PubSubClientException cause = new PubSubClientException("Broker unavailable");
    VeniceException wrappedException = new VeniceException("Wrapped", cause);
    doThrow(wrappedException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    receiver.write(emptyMessages);

    verify(mockSIT).setIngestionException(eq(3), eq(wrappedException), eq("broker1:9092"));
    verify(mockSIT, never()).setLastConsumerException(any());
  }

  // --- Tests with PubSub partition pause DISABLED (default) ---

  @Test
  public void testPubSubExceptionRoutedToTaskLevel_WhenPauseDisabled() throws Exception {
    when(mockSIT.isPubSubPartitionPauseEnabled()).thenReturn(false);
    PubSubClientException pubSubException = new PubSubClientException("Broker unavailable");
    doThrow(pubSubException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    receiver.write(emptyMessages);

    // When pause is disabled, preserve original behavior: task-level only
    verify(mockSIT).setLastConsumerException(eq(pubSubException));
    verify(mockSIT, never()).setIngestionException(anyInt(), any());
  }

  @Test
  public void testRetriablePubSubExceptionRoutedToTaskLevel_WhenPauseDisabled() throws Exception {
    when(mockSIT.isPubSubPartitionPauseEnabled()).thenReturn(false);
    PubSubClientRetriableException retriableException = new PubSubOpTimeoutException("Poll timed out");
    doThrow(retriableException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    receiver.write(emptyMessages);

    verify(mockSIT).setLastConsumerException(eq(retriableException));
    verify(mockSIT, never()).setIngestionException(anyInt(), any());
  }

  @Test
  public void testWrappedPubSubExceptionRoutedToTaskLevel_WhenPauseDisabled() throws Exception {
    when(mockSIT.isPubSubPartitionPauseEnabled()).thenReturn(false);
    PubSubClientException cause = new PubSubClientException("Broker unavailable");
    VeniceException wrappedException = new VeniceException("Wrapped", cause);
    doThrow(wrappedException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    receiver.write(emptyMessages);

    verify(mockSIT).setLastConsumerException(eq(wrappedException));
    verify(mockSIT, never()).setIngestionException(anyInt(), any());
  }

  @Test
  public void testWrappedRetriablePubSubExceptionRoutedToTaskLevel_WhenPauseDisabled() throws Exception {
    when(mockSIT.isPubSubPartitionPauseEnabled()).thenReturn(false);
    PubSubOpTimeoutException cause = new PubSubOpTimeoutException("Poll timed out");
    VeniceException wrappedException = new VeniceException("Wrapped", cause);
    doThrow(wrappedException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    receiver.write(emptyMessages);

    verify(mockSIT).setLastConsumerException(eq(wrappedException));
    verify(mockSIT, never()).setIngestionException(anyInt(), any());
  }

  @Test
  public void testNonPubSubExceptionRoutedToTaskLevel() throws Exception {
    RuntimeException genericException = new RuntimeException("Something else went wrong");
    doThrow(genericException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    receiver.write(emptyMessages);

    verify(mockSIT).setLastConsumerException(eq(genericException));
    verify(mockSIT, never()).setIngestionException(anyInt(), any());
  }

  @Test
  public void testVeniceExceptionWithoutPubSubCauseRoutedToTaskLevel() throws Exception {
    VeniceException veniceException = new VeniceException("Venice error without PubSub cause");
    doThrow(veniceException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    receiver.write(emptyMessages);

    verify(mockSIT).setLastConsumerException(eq(veniceException));
    verify(mockSIT, never()).setIngestionException(anyInt(), any());
  }

  @Test
  public void testInterruptedExceptionIsRethrown() throws Exception {
    InterruptedException interruptedException = new InterruptedException("Shutting down");
    doThrow(interruptedException).when(mockSIT).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    expectThrows(InterruptedException.class, () -> receiver.write(emptyMessages));

    verify(mockSIT, never()).setLastConsumerException(any());
    verify(mockSIT, never()).setIngestionException(anyInt(), any());
  }
}
