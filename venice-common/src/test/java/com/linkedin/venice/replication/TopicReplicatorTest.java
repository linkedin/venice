package com.linkedin.venice.replication;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class TopicReplicatorTest {
  @Test
  public void testStartBufferReplay() throws TopicException {
    final Map<Integer, Long> startingOffsets = new HashMap<>();
    startingOffsets.put(0, 10L);
    startingOffsets.put(1, 20L);
    startingOffsets.put(2, 15L);
    final List<Long> startingOffsetsList = new ArrayList<>();
    startingOffsets.forEach((integer, aLong) -> startingOffsetsList.add(integer, aLong));
    final Store store = TestUtils.createTestStore(TestUtils.getUniqueString("store"), "owner", 1);
    final long REWIND_TIME = 5;
    store.setHybridStoreConfig(new HybridStoreConfig(REWIND_TIME, 1, HybridStoreConfig.DEFAULT_HYBRID_TIME_LAG_THRESHOLD));
    final String sourceTopicName = "source topic name";
    final String destinationTopicName = "destination topic name";

    TopicManager topicManager = mock(TopicManager.class);
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = mock(VeniceWriter.class);
    TopicReplicator topicReplicator = mock(TopicReplicator.class);
    VeniceWriterFactory veniceWriterFactory = mock(VeniceWriterFactory.class);
    MockTime mockTime = new MockTime();

    when(topicReplicator.getTopicManager()).thenReturn(topicManager);
    when(topicReplicator.getVeniceWriterFactory()).thenReturn(veniceWriterFactory);
    when(topicReplicator.getTimer()).thenReturn(mockTime);
    when(topicManager.getOffsetsByTime(anyString(), anyLong())).thenReturn(startingOffsets);
    when(topicManager.containsTopicAndAllPartitionsAreOnline(anyString())).thenReturn(true);
    when(veniceWriterFactory.createBasicVeniceWriter(
        anyString(),
        any(Time.class)))
        .thenReturn(veniceWriter);

    // Methods under test
    doCallRealMethod().when(topicReplicator).checkPreconditions(anyString(), anyString(), any());
    doCallRealMethod().when(topicReplicator).getRewindStartTime(store);
    doCallRealMethod().when(topicReplicator).beginReplication(anyString(), anyString(), anyLong(), anyString());

    topicReplicator.checkPreconditions(sourceTopicName, destinationTopicName, store);
    long rewindStartTime = topicReplicator.getRewindStartTime(store);
    assertEquals(rewindStartTime, mockTime.getMilliseconds() - Time.MS_PER_SECOND * REWIND_TIME,
        "Rewind start timestamp is not calculated properly");
    topicReplicator.beginReplication(sourceTopicName, destinationTopicName, rewindStartTime, null);

    verify(topicReplicator).beginReplication(sourceTopicName, destinationTopicName, rewindStartTime, null);

    try {
      store.setHybridStoreConfig(null);
      topicReplicator.checkPreconditions(sourceTopicName, destinationTopicName, store);
      fail("topicReplicator.startBufferReplay should fail (FOR NOW) for non-Hybrid stores.");
    } catch (VeniceException e) {
      // expected
    }
  }
}
