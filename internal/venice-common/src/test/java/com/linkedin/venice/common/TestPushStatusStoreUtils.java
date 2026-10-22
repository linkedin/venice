package com.linkedin.venice.common;

import com.linkedin.venice.pushstatus.PushStatusKey;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPushStatusStoreUtils {
  @Test
  public void testGetPushKey_FullPushKey() {
    List<Object> expectedKeyStrings = Arrays.asList(42, 2);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(42, 2, Optional.empty());
    Assert.assertEquals(pushStatusKey.messageType, PushStatusStoreUtils.PushStatusKeyType.FULL_PUSH.ordinal());
    Assert.assertEquals(pushStatusKey.keyStrings, expectedKeyStrings);
  }

  @Test
  public void testGetPushKey_FullPushKey2() {
    List<Object> expectedKeyStrings = Arrays.asList(42, 2);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(42, 2, Optional.empty(), Optional.empty());
    Assert.assertEquals(pushStatusKey.messageType, PushStatusStoreUtils.PushStatusKeyType.FULL_PUSH.ordinal());
    Assert.assertEquals(pushStatusKey.keyStrings, expectedKeyStrings);
  }

  @Test
  public void testGetPushKey_IncrementalPushKey() {
    List<Object> expectedKeyStrings = Arrays.asList(42, 2, "IP01");
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(42, 2, Optional.of("IP01"));
    Assert.assertEquals(pushStatusKey.messageType, PushStatusStoreUtils.PushStatusKeyType.INCREMENTAL_PUSH.ordinal());
    Assert.assertEquals(pushStatusKey.keyStrings, expectedKeyStrings);
  }

  @Test
  public void testGetPushKey_IncrementalPushKey2() {
    List<Object> expectedKeyStrings = Arrays.asList(42, 2, "IP01");
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(42, 2, Optional.of("IP01"), Optional.empty());
    Assert.assertEquals(pushStatusKey.messageType, PushStatusStoreUtils.PushStatusKeyType.INCREMENTAL_PUSH.ordinal());
    Assert.assertEquals(pushStatusKey.keyStrings, expectedKeyStrings);
  }

  @Test
  public void testGetPushKey_ServerIncrementalPushKey() {
    List<Object> expectedKeyStrings = Arrays.asList(42, 2, "IP01", "VENICE_SERVER");
    PushStatusKey pushStatusKey =
        PushStatusStoreUtils.getPushKey(42, 2, Optional.of("IP01"), Optional.of("VENICE_SERVER"));
    Assert.assertEquals(
        pushStatusKey.messageType,
        PushStatusStoreUtils.PushStatusKeyType.SERVER_INCREMENTAL_PUSH.ordinal());
    Assert.assertEquals(pushStatusKey.keyStrings, expectedKeyStrings);
  }

  @Test
  public void testClusterKeysAreStoreScopedAndPreserveLegacyKeys() {
    List<PushStatusKey> keys = Arrays.asList(
        PushStatusStoreUtils.getHeartbeatKey("instance"),
        PushStatusStoreUtils.getPushKey(1),
        PushStatusStoreUtils.getPushKey(1, 2, Optional.empty()),
        PushStatusStoreUtils.getPushKey(1, Optional.of("push")),
        PushStatusStoreUtils.getPushKey(1, 2, Optional.of("push")),
        PushStatusStoreUtils.getServerIncrementalPushKey(1, 2, "push", "server"),
        PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(1));
    for (PushStatusKey key: keys) {
      PushStatusKey first = PushStatusStoreUtils.getClusterPushStatusKey("store1", key);
      PushStatusKey second = PushStatusStoreUtils.getClusterPushStatusKey("store2", key);
      Assert.assertNotEquals(first, second);
      Assert.assertEquals(first.messageType, key.messageType);
      Assert.assertEquals(first.keyStrings.subList(0, key.keyStrings.size()), key.keyStrings);
      Assert.assertEquals(first.keyStrings.size(), key.keyStrings.size() + 1);
    }
    Assert.assertEquals(
        PushStatusStoreUtils.getPartitionIdFromServerIncrementalPushKey(
            PushStatusStoreUtils.getClusterPushStatusKey("store", keys.get(5))),
        2);
    String clusterStore = VeniceSystemStoreUtils.getDaVinciPushStatusStoreNameForCluster("cluster1");
    Assert.assertTrue(VeniceSystemStoreUtils.isSystemStore(clusterStore));
    Assert.assertFalse(VeniceSystemStoreUtils.isUserSystemStore(clusterStore));
    Assert.assertNotEquals(clusterStore, VeniceSystemStoreUtils.getDaVinciPushStatusStoreNameForCluster("cluster2"));
  }
}
