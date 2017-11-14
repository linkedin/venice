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
import java.util.Optional;
import org.mockito.AdditionalMatchers;
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
    store.setHybridStoreConfig(new HybridStoreConfig(REWIND_TIME, 1));
    final String sourceTopicName = "source topic name";
    final String destinationTopicName = "destination topic name";

    TopicManager topicManager = mock(TopicManager.class);
    VeniceWriter<byte[], byte[]> veniceWriter = mock(VeniceWriter.class);
    TopicReplicator topicReplicator = mock(TopicReplicator.class);
    VeniceWriterFactory veniceWriterFactory = mock(VeniceWriterFactory.class);

    when(topicReplicator.getTopicManager()).thenReturn(topicManager);
    when(topicReplicator.getVeniceWriterFactory()).thenReturn(veniceWriterFactory);
    when(topicReplicator.getTimer()).thenReturn(new MockTime());
    when(topicManager.getOffsetsByTime(anyString(), anyLong())).thenReturn(startingOffsets);
    when(veniceWriterFactory.getBasicVeniceWriter(
        anyString(),
        any(ReflectUtils.loadClass(Time.class.getName()))))
        .thenReturn(veniceWriter);

    // Method under test
    when(topicReplicator.startBufferReplay(anyString(), anyString(), any())).thenCallRealMethod();

    topicReplicator.startBufferReplay(
        sourceTopicName,
        destinationTopicName,
        store);

    verify(topicReplicator).beginReplication(sourceTopicName, destinationTopicName, Optional.of(startingOffsets));
    verify(veniceWriter).broadcastStartOfBufferReplay(eq(startingOffsetsList), any(), eq(sourceTopicName), eq(new HashMap<>()));

    try {
      store.setHybridStoreConfig(null);
      topicReplicator.startBufferReplay(
          sourceTopicName,
          destinationTopicName,
          store);
      fail("topicReplicator.startBufferReplay should fail (FOR NOW) for non-Hybrid stores.");
    } catch (VeniceException e) {
      // expected
    }
  }
}
