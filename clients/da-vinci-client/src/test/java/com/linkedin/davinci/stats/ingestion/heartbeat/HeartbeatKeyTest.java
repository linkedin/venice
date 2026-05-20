package com.linkedin.davinci.stats.ingestion.heartbeat;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.venice.stats.dimensions.VeniceChunkingStatus;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceStoreWriteType;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * Locks down the contract of {@link HeartbeatKey}: identity is {@code (storeName, version, partition, region)}.
 * The SLO labels (write type, chunking, locality) are passenger fields — present for the per-record OTel
 * emit path, but explicitly NOT part of equality or hash. Maps keyed by a HeartbeatKey rely on this so
 * later inserts with different labels never replace the originally-stored key (and the periodic record-lag
 * path can iterate the map and pull labels off the stored keys without divergence).
 */
public class HeartbeatKeyTest {
  private static final String STORE = "test_store";
  private static final int VERSION = 7;
  private static final int PARTITION = 3;
  private static final String REGION = "us-west";

  @Test
  public void test4ArgConstructorLeavesPassengerFieldsNull() {
    HeartbeatKey key = new HeartbeatKey(STORE, VERSION, PARTITION, REGION);
    assertNull(key.writeType, "writeType should be null on the lookup-only constructor");
    assertNull(key.chunkingStatus, "chunkingStatus should be null on the lookup-only constructor");
    assertNull(key.locality, "locality should be null on the lookup-only constructor");
  }

  @Test
  public void test7ArgConstructorPropagatesPassengerFields() {
    HeartbeatKey key = new HeartbeatKey(
        STORE,
        VERSION,
        PARTITION,
        REGION,
        VeniceStoreWriteType.WRITE_COMPUTE,
        VeniceChunkingStatus.CHUNKED,
        VeniceRegionLocality.LOCAL);
    assertEquals(key.writeType, VeniceStoreWriteType.WRITE_COMPUTE);
    assertEquals(key.chunkingStatus, VeniceChunkingStatus.CHUNKED);
    assertEquals(key.locality, VeniceRegionLocality.LOCAL);
  }

  @Test
  public void test4ArgAnd7ArgKeysWithSameIdentityAreEqualAndHashEqual() {
    HeartbeatKey lookup = new HeartbeatKey(STORE, VERSION, PARTITION, REGION);
    HeartbeatKey labeled = new HeartbeatKey(
        STORE,
        VERSION,
        PARTITION,
        REGION,
        VeniceStoreWriteType.WRITE_COMPUTE,
        VeniceChunkingStatus.CHUNKED,
        VeniceRegionLocality.LOCAL);
    assertEquals(lookup, labeled, "Identity-equal keys must be equal even when labels differ");
    assertEquals(labeled, lookup, "Equality must be symmetric");
    assertEquals(lookup.hashCode(), labeled.hashCode(), "Identity-equal keys must hash equal");
  }

  @Test
  public void testTwoLabeledKeysWithSameIdentityButDifferentLabelsAreEqual() {
    HeartbeatKey a = new HeartbeatKey(
        STORE,
        VERSION,
        PARTITION,
        REGION,
        VeniceStoreWriteType.REGULAR,
        VeniceChunkingStatus.UNCHUNKED,
        VeniceRegionLocality.LOCAL);
    HeartbeatKey b = new HeartbeatKey(
        STORE,
        VERSION,
        PARTITION,
        REGION,
        VeniceStoreWriteType.WRITE_COMPUTE,
        VeniceChunkingStatus.CHUNKED,
        VeniceRegionLocality.REMOTE);
    assertEquals(a, b, "Passenger fields must not affect equality");
    assertEquals(a.hashCode(), b.hashCode(), "Passenger fields must not affect hashCode");
  }

  @Test
  public void testKeysDifferingInIdentityAreNotEqual() {
    HeartbeatKey base = new HeartbeatKey(STORE, VERSION, PARTITION, REGION);
    assertNotEquals(base, new HeartbeatKey("other_store", VERSION, PARTITION, REGION));
    assertNotEquals(base, new HeartbeatKey(STORE, VERSION + 1, PARTITION, REGION));
    assertNotEquals(base, new HeartbeatKey(STORE, VERSION, PARTITION + 1, REGION));
    assertNotEquals(base, new HeartbeatKey(STORE, VERSION, PARTITION, "us-east"));
  }

  @Test
  public void testPutIfAbsentSemanticsKeepFirstStoredKey() {
    Map<HeartbeatKey, String> map = new HashMap<>();
    HeartbeatKey first = new HeartbeatKey(
        STORE,
        VERSION,
        PARTITION,
        REGION,
        VeniceStoreWriteType.REGULAR,
        VeniceChunkingStatus.UNCHUNKED,
        VeniceRegionLocality.LOCAL);
    HeartbeatKey second = new HeartbeatKey(
        STORE,
        VERSION,
        PARTITION,
        REGION,
        VeniceStoreWriteType.WRITE_COMPUTE,
        VeniceChunkingStatus.CHUNKED,
        VeniceRegionLocality.REMOTE);
    map.putIfAbsent(first, "v1");
    map.putIfAbsent(second, "v2"); // identity-equal → put is a no-op, the original key stays

    assertEquals(map.size(), 1, "Identity-equal putIfAbsent must not add a second entry");
    HeartbeatKey stored = map.keySet().iterator().next();
    assertSame(stored, first, "Map must keep the first-inserted key reference");
    assertNotSame(stored, second, "Sanity: 'second' is a different object even though equal");
    // The whole point of the contract: stored key still carries the FIRST set of labels
    assertEquals(stored.writeType, VeniceStoreWriteType.REGULAR);
    assertEquals(stored.chunkingStatus, VeniceChunkingStatus.UNCHUNKED);
    assertEquals(stored.locality, VeniceRegionLocality.LOCAL);
  }

  @Test
  public void testHashCodeStableAcrossLabelChanges() {
    int reference = new HeartbeatKey(STORE, VERSION, PARTITION, REGION).hashCode();
    int withLabels = new HeartbeatKey(
        STORE,
        VERSION,
        PARTITION,
        REGION,
        VeniceStoreWriteType.WRITE_COMPUTE,
        VeniceChunkingStatus.CHUNKED,
        VeniceRegionLocality.REMOTE).hashCode();
    assertEquals(withLabels, reference);
  }

  @Test
  public void testToStringIsStableAcrossLabelChanges() {
    HeartbeatKey lookup = new HeartbeatKey(STORE, VERSION, PARTITION, REGION);
    HeartbeatKey labeled = new HeartbeatKey(
        STORE,
        VERSION,
        PARTITION,
        REGION,
        VeniceStoreWriteType.WRITE_COMPUTE,
        VeniceChunkingStatus.CHUNKED,
        VeniceRegionLocality.REMOTE);
    assertNotNull(lookup.toString());
    assertEquals(
        lookup.toString(),
        labeled.toString(),
        "toString is identity-derived and must not include passenger labels");
  }
}
