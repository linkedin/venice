package com.linkedin.davinci.consumer;

import static org.testng.Assert.*;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;


public class VersionSwapMessageStateTest {
  private static final String STORE = "test-store";
  private static final String OLD_TOPIC = Version.composeKafkaTopic(STORE, 1);
  private static final String NEW_TOPIC = Version.composeKafkaTopic(STORE, 2);
  private static final String CLIENT_REGION = "us-foo-1";
  private static final String SRC_REGION = CLIENT_REGION;
  private static final String DST_REGION_A = "us-bar-1";
  private static final String DST_REGION_B = "eu-baz-1";

  private VersionSwap newVersionSwap(long generationId, String destRegion) {
    VersionSwap vs = new VersionSwap();
    vs.oldServingVersionTopic = new Utf8(OLD_TOPIC);
    vs.newServingVersionTopic = new Utf8(NEW_TOPIC);
    vs.sourceRegion = new Utf8(SRC_REGION);
    vs.destinationRegion = new Utf8(destRegion);
    vs.generationId = generationId;
    // Optional fields not used in these unit tests remain null
    return vs;
  }

  private PubSubTopicPartition tpi(String topic, int partition) {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopic t = repo.getTopic(topic);
    return new PubSubTopicPartitionImpl(t, partition);
  }

  @Test
  public void testIsVersionSwapRelevant() {
    VersionSwap irrelevantOldGen = newVersionSwap(-1, DST_REGION_A);
    assertFalse(VersionSwapMessageState.isVersionSwapRelevant(OLD_TOPIC, CLIENT_REGION, irrelevantOldGen));

    VersionSwap wrongOldTopic = newVersionSwap(1, DST_REGION_A);
    assertFalse(VersionSwapMessageState.isVersionSwapRelevant(NEW_TOPIC, CLIENT_REGION, wrongOldTopic));

    VersionSwap wrongClientRegion = newVersionSwap(1, DST_REGION_A);
    assertFalse(VersionSwapMessageState.isVersionSwapRelevant(OLD_TOPIC, "some-other-region", wrongClientRegion));

    VersionSwap relevant = newVersionSwap(2, DST_REGION_A);
    assertTrue(VersionSwapMessageState.isVersionSwapRelevant(OLD_TOPIC, CLIENT_REGION, relevant));
  }

  @Test
  public void testInitializationAndLowWatermark() {
    // current assignment includes only OLD_TOPIC partitions 0 and 1
    Set<PubSubTopicPartition> currentAssignment = new HashSet<>();
    currentAssignment.add(tpi(OLD_TOPIC, 0));
    currentAssignment.add(tpi(OLD_TOPIC, 1));

    VersionSwap vs = newVersionSwap(10, DST_REGION_A);
    VersionSwapMessageState state =
        new VersionSwapMessageState(vs, /* totalRegionCount */ 2, currentAssignment, /* startTs */ 123L);

    assertEquals(state.getOldVersionTopic(), OLD_TOPIC);
    assertEquals(state.getNewVersionTopic(), NEW_TOPIC);
    assertEquals(state.getVersionSwapGenerationId(), 10L);
    assertEquals(state.getAssignedPartitions(), new HashSet<>(java.util.Arrays.asList(0, 1)));

    // low watermark unknown until a swap message is handled for that partition
    assertNull(state.getVersionSwapLowWatermarkPosition(OLD_TOPIC, 0));
    PubSubPosition pos = ApacheKafkaOffsetPosition.of(5L);
    boolean completed = state.handleVersionSwap(vs, tpi(OLD_TOPIC, 0), pos);
    assertFalse(completed); // need totalRegionCount messages per partition
    assertEquals(state.getVersionSwapLowWatermarkPosition(OLD_TOPIC, 0), pos);

    // low watermark requests from non-old-topic should return null
    assertNull(state.getVersionSwapLowWatermarkPosition(NEW_TOPIC, 0));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testHandleVersionSwapThrowsOnUnexpectedTopic() {
    Set<PubSubTopicPartition> currentAssignment = Collections.singleton(tpi(OLD_TOPIC, 0));
    VersionSwap vs = newVersionSwap(1, DST_REGION_A);
    VersionSwapMessageState state = new VersionSwapMessageState(vs, 1, currentAssignment, 1L);
    // Passing a message from a wrong topic should throw
    state.handleVersionSwap(vs, tpi(NEW_TOPIC, 0), ApacheKafkaOffsetPosition.of(1L));
  }

  @Test
  public void testCompletionAfterAllRegions() {
    // 2 regions required -> A then B completes partition 0
    Set<PubSubTopicPartition> assignment = Collections.singleton(tpi(OLD_TOPIC, 0));
    VersionSwap vsA = newVersionSwap(2, DST_REGION_A);
    VersionSwap vsB = newVersionSwap(2, DST_REGION_B);
    VersionSwapMessageState state = new VersionSwapMessageState(vsA, 2, assignment, 1L);

    assertFalse(state.handleVersionSwap(vsA, tpi(OLD_TOPIC, 0), ApacheKafkaOffsetPosition.of(10L)));
    assertTrue(state.handleVersionSwap(vsB, tpi(OLD_TOPIC, 0), ApacheKafkaOffsetPosition.of(11L)));
    assertTrue(state.isVersionSwapMessagesReceivedForAllPartitions());
  }

  @Test
  public void testCheckpointsAndEopBackup() {
    Set<PubSubTopicPartition> assignment = new HashSet<>();
    assignment.add(tpi(OLD_TOPIC, 0));
    assignment.add(tpi(OLD_TOPIC, 1));
    VersionSwap vs = newVersionSwap(3, DST_REGION_A);
    VersionSwapMessageState state = new VersionSwapMessageState(vs, 1, assignment, 1L);

    // Mark only partition 0 as completed
    state.handleVersionSwap(vs, tpi(OLD_TOPIC, 0), ApacheKafkaOffsetPosition.of(1L));

    // Simulate async finder finished
    CompletableFuture<Void> done = CompletableFuture.completedFuture(null);
    state.setFindNewTopicCheckpointFuture(done);

    Map<Integer, VeniceChangeCoordinate> vsCheckpoints = new HashMap<>();
    vsCheckpoints.put(0, new VeniceChangeCoordinate(NEW_TOPIC, ApacheKafkaOffsetPosition.of(100L), 0));
    state.setNewTopicVersionSwapCheckpoints(vsCheckpoints);

    Map<Integer, VeniceChangeCoordinate> eopCheckpoints = new HashMap<>();
    eopCheckpoints.put(1, new VeniceChangeCoordinate(NEW_TOPIC, ApacheKafkaOffsetPosition.of(200L), 1));
    state.setNewTopicEOPCheckpoints(eopCheckpoints);

    // getNewTopicVersionSwapCheckpoints should return only completed partitions (partition 0)
    Set<VeniceChangeCoordinate> onlyCompleted = state.getNewTopicVersionSwapCheckpoints();
    assertEquals(onlyCompleted.size(), 1);
    assertTrue(onlyCompleted.stream().anyMatch(c -> c.getPartition() == 0));

    // Backup getter should include EOP for incomplete partition (partition 1)
    Set<VeniceChangeCoordinate> withBackup = state.getNewTopicCheckpointsWithEOPAsBackup();
    assertEquals(withBackup.size(), 2);
    assertTrue(withBackup.stream().anyMatch(c -> c.getPartition() == 0));
    assertTrue(withBackup.stream().anyMatch(c -> c.getPartition() == 1));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testGetNewTopicVersionSwapCheckpointsThrowsIfNotReady() {
    Set<PubSubTopicPartition> assignment = Collections.singleton(tpi(OLD_TOPIC, 0));
    VersionSwap vs = newVersionSwap(4, DST_REGION_A);
    VersionSwapMessageState state = new VersionSwapMessageState(vs, 1, assignment, 1L);
    // future not set -> should throw
    state.getNewTopicVersionSwapCheckpoints();
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testGetNewTopicCheckpointsWithEopAsBackupThrowsIfMissingEop() {
    Set<PubSubTopicPartition> assignment = new HashSet<>();
    assignment.add(tpi(OLD_TOPIC, 0));
    assignment.add(tpi(OLD_TOPIC, 1));
    VersionSwap vs = newVersionSwap(5, DST_REGION_A);
    VersionSwapMessageState state = new VersionSwapMessageState(vs, 1, assignment, 1L);

    // Complete only partition 0
    state.handleVersionSwap(vs, tpi(OLD_TOPIC, 0), ApacheKafkaOffsetPosition.of(1L));

    state.setFindNewTopicCheckpointFuture(CompletableFuture.completedFuture(null));
    Map<Integer, VeniceChangeCoordinate> vsCheckpoints = new HashMap<>();
    vsCheckpoints.put(0, new VeniceChangeCoordinate(NEW_TOPIC, ApacheKafkaOffsetPosition.of(100L), 0));
    state.setNewTopicVersionSwapCheckpoints(vsCheckpoints);
    // Missing EOP for partition 1 -> should throw
    state.getNewTopicCheckpointsWithEOPAsBackup();
  }

  @Test
  public void testHandleUnsubscribe() {
    Set<PubSubTopicPartition> assignment = new HashSet<>();
    assignment.add(tpi(OLD_TOPIC, 0));
    assignment.add(tpi(OLD_TOPIC, 1));
    VersionSwap vs = newVersionSwap(6, DST_REGION_A);
    VersionSwapMessageState state = new VersionSwapMessageState(vs, 2, assignment, 1L);

    // After handling a swap for partition 0, we should be tracking watermark for 0
    state.handleVersionSwap(vs, tpi(OLD_TOPIC, 0), ApacheKafkaOffsetPosition.of(7L));
    assertNotNull(state.getVersionSwapLowWatermarkPosition(OLD_TOPIC, 0));

    // Unsubscribe partition 0, it should be removed from tracking structures
    state.handleUnsubscribe(Collections.singleton(0));
    assertFalse(state.getAssignedPartitions().contains(0));
    assertNull(state.getVersionSwapLowWatermarkPosition(OLD_TOPIC, 0));
    // Partition 1 remains assigned
    assertTrue(state.getAssignedPartitions().contains(1));
  }
}
