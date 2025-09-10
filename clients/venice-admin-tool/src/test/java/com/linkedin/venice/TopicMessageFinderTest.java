package com.linkedin.venice;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.mock.SimplePartitioner;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TopicMessageFinderTest {
  private ControllerClient mockControllerClient;
  private StoreInfo mockStoreInfo;
  private StoreResponse mockStoreResponse;
  private Version mockVersion;
  private PartitionerConfig mockPartitionerConfig;
  private PubSubConsumerAdapter mockConsumer;
  private DefaultPubSubMessage mockMessage;

  private static final String STORE_NAME = "test_store";
  private static final String KEY = "test_key-4";
  private static final int VERSION_NUMBER = 1;
  private static final int PARTITION_COUNT = 3;
  private static final String KEY_SCHEMA_STR = "\"string\"";
  private static final ApacheKafkaOffsetPosition POSITION_0 = ApacheKafkaOffsetPosition.of(0L);
  private static final ApacheKafkaOffsetPosition POSITION_10 = ApacheKafkaOffsetPosition.of(10L);
  private static final ApacheKafkaOffsetPosition POSITION_20 = ApacheKafkaOffsetPosition.of(20L);

  @BeforeMethod
  public void setUp() {
    mockControllerClient = mock(ControllerClient.class);
    mockStoreInfo = mock(StoreInfo.class);
    mockVersion = mock(Version.class);
    mockPartitionerConfig = mock(PartitionerConfig.class);
    mockConsumer = mock(PubSubConsumerAdapter.class);
    mockMessage = mock(DefaultPubSubMessage.class, Mockito.RETURNS_DEEP_STUBS);
    mockStoreResponse = new StoreResponse();
    mockStoreResponse.setStore(mockStoreInfo);
  }

  @Test
  public void testFindPartitionIdForKeySuccess() {
    when(mockControllerClient.getStore(STORE_NAME)).thenReturn(mockStoreResponse);
    when(mockStoreInfo.getVersion(VERSION_NUMBER)).thenReturn(Optional.of(mockVersion));
    when(mockVersion.getPartitionerConfig()).thenReturn(mockPartitionerConfig);
    when(mockVersion.getPartitionCount()).thenReturn(PARTITION_COUNT);
    when(mockPartitionerConfig.getPartitionerClass()).thenReturn(DefaultVenicePartitioner.class.getName());

    int expectedPartitionId =
        new DefaultVenicePartitioner().getPartitionId(KEY.getBytes(), 0, KEY.length(), PARTITION_COUNT);

    TopicMessageFinder.KeyPartitionInfo partitionInfo =
        TopicMessageFinder.findPartitionIdForKey(mockControllerClient, STORE_NAME, VERSION_NUMBER, KEY, KEY_SCHEMA_STR);

    assertNotNull(partitionInfo);
    assertEquals(partitionInfo.getPartitionId(), expectedPartitionId);
    assertEquals(partitionInfo.getPartitionCount(), PARTITION_COUNT);

    // test store level partitioner class is used when version is not found
    when(mockStoreInfo.getVersion(VERSION_NUMBER)).thenReturn(Optional.empty());
    mockPartitionerConfig = mock(PartitionerConfig.class);
    when(mockStoreInfo.getPartitionerConfig()).thenReturn(mockPartitionerConfig);
    when(mockStoreInfo.getPartitionCount()).thenReturn(PARTITION_COUNT);
    when(mockPartitionerConfig.getPartitionerClass()).thenReturn(SimplePartitioner.class.getName());

    expectedPartitionId = new SimplePartitioner().getPartitionId(KEY.getBytes(), PARTITION_COUNT);
    partitionInfo =
        TopicMessageFinder.findPartitionIdForKey(mockControllerClient, STORE_NAME, VERSION_NUMBER, KEY, KEY_SCHEMA_STR);
    assertNotNull(partitionInfo);
    assertEquals(partitionInfo.getPartitionId(), expectedPartitionId);
    assertEquals(partitionInfo.getPartitionCount(), PARTITION_COUNT);
  }

  @Test(expectedExceptions = VeniceNoStoreException.class)
  public void testFindPartitionIdForKeyStoreDoesNotExist() {
    when(mockControllerClient.getStore(STORE_NAME)).thenReturn(null);

    TopicMessageFinder.findPartitionIdForKey(mockControllerClient, STORE_NAME, VERSION_NUMBER, KEY, KEY_SCHEMA_STR);
  }

  @Test
  public void testFindPartitionIdForKeyInvalidPartitionCount() {
    when(mockControllerClient.getStore(STORE_NAME)).thenReturn(mockStoreResponse);
    when(mockStoreInfo.getVersion(VERSION_NUMBER)).thenReturn(Optional.of(mockVersion));
    when(mockVersion.getPartitionCount()).thenReturn(0);

    Exception e = expectThrows(
        VeniceException.class,
        () -> TopicMessageFinder
            .findPartitionIdForKey(mockControllerClient, STORE_NAME, VERSION_NUMBER, KEY, KEY_SCHEMA_STR));
    assertTrue(e.getMessage().contains("Partition count for store: " + STORE_NAME + " is not set."));
  }

  @Test
  public void testSerializeKey() {
    byte[] serializedKey = TopicMessageFinder.serializeKey(KEY, KEY_SCHEMA_STR);
    assertNotNull(serializedKey);
  }

  @Test
  public void testConsumeMatchingKeyFound() {
    PubSubTopicPartition mockPartition = mock(PubSubTopicPartitionImpl.class);
    when(mockConsumer.poll(anyLong())).thenReturn(Collections.singletonMap(mockPartition, Arrays.asList(mockMessage)))
        .thenReturn(Collections.emptyMap());
    when(mockMessage.getPosition()).thenReturn(POSITION_10);
    when(mockMessage.getKey()).thenReturn(new KafkaKey((byte) 0, KEY.getBytes()));
    when(mockMessage.getPosition()).thenReturn(POSITION_10);
    doAnswer(invocation -> {
      PubSubTopicPartition tp = invocation.getArgument(0);
      PubSubPosition p1 = invocation.getArgument(1);
      PubSubPosition p2 = invocation.getArgument(2);
      return PubSubUtil.computeOffsetDelta(tp, p1, p2, mockConsumer);
    }).when(mockConsumer)
        .positionDifference(any(PubSubTopicPartition.class), any(PubSubPosition.class), any(PubSubPosition.class));

    long recordsConsumed = TopicMessageFinder
        .consume(mockConsumer, mockPartition, POSITION_0, POSITION_20, 5L, mockMessage.getKey().getKey());

    assertTrue(recordsConsumed > 0);
  }

  @Test
  public void testConsumeNoMatchingKey() {
    PubSubTopicPartition mockPartition = mock(PubSubTopicPartitionImpl.class);
    when(mockConsumer.poll(anyInt())).thenReturn(Collections.singletonMap(mockPartition, Arrays.asList(mockMessage)));
    when(mockMessage.getKey()).thenReturn(new KafkaKey((byte) 0, "different_key".getBytes()));

    long recordsConsumed =
        TopicMessageFinder.consume(mockConsumer, mockPartition, POSITION_0, POSITION_20, 5L, KEY.getBytes());

    assertEquals(recordsConsumed, 0);
  }

  @Test
  public void testConsumeExceedsEndOffset() {
    PubSubTopicPartition mockPartition = mock(PubSubTopicPartitionImpl.class);
    when(mockConsumer.poll(anyInt())).thenReturn(Collections.singletonMap(mockPartition, Arrays.asList(mockMessage)));
    ApacheKafkaOffsetPosition position30 = ApacheKafkaOffsetPosition.of(30L);
    when(mockMessage.getPosition()).thenReturn(position30);

    doAnswer(invocation -> {
      PubSubTopicPartition tp = invocation.getArgument(0);
      PubSubPosition p1 = invocation.getArgument(1);
      PubSubPosition p2 = invocation.getArgument(2);
      return PubSubUtil.computeOffsetDelta(tp, p1, p2, mockConsumer);
    }).when(mockConsumer)
        .positionDifference(any(PubSubTopicPartition.class), any(PubSubPosition.class), any(PubSubPosition.class));

    long recordsConsumed =
        TopicMessageFinder.consume(mockConsumer, mockPartition, POSITION_0, POSITION_20, 5L, KEY.getBytes());

    assertEquals(recordsConsumed, 0);
  }
}
