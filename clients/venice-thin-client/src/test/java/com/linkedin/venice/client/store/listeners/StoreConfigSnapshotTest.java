package com.linkedin.venice.client.store.listeners;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.venice.meta.ExternalStorageReadMode;
import org.testng.annotations.Test;


public class StoreConfigSnapshotTest {
  @Test
  public void identicalSnapshotsCompareEqual() {
    StoreConfigSnapshot a = new StoreConfigSnapshot(150, ExternalStorageReadMode.VENICE_ONLY);
    StoreConfigSnapshot b = new StoreConfigSnapshot(150, ExternalStorageReadMode.VENICE_ONLY);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void batchGetLimitDifferenceBreaksEquality() {
    StoreConfigSnapshot a = new StoreConfigSnapshot(150, ExternalStorageReadMode.VENICE_ONLY);
    StoreConfigSnapshot b = new StoreConfigSnapshot(200, ExternalStorageReadMode.VENICE_ONLY);
    assertNotEquals(a, b);
  }

  @Test
  public void externalStorageReadModeDifferenceBreaksEquality() {
    StoreConfigSnapshot a = new StoreConfigSnapshot(150, ExternalStorageReadMode.VENICE_ONLY);
    StoreConfigSnapshot b = new StoreConfigSnapshot(150, ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN);
    assertNotEquals(a, b);
  }

  @Test
  public void nullExternalStorageReadModeCoercesToVeniceOnly() {
    StoreConfigSnapshot snapshot = new StoreConfigSnapshot(150, null);
    assertEquals(snapshot.getExternalStorageReadMode(), ExternalStorageReadMode.VENICE_ONLY);
  }

  @Test
  public void accessorsReturnConstructorValues() {
    StoreConfigSnapshot snapshot = new StoreConfigSnapshot(150, ExternalStorageReadMode.DUAL_MODE_CONSISTENCY_CHECK);
    assertEquals(snapshot.getBatchGetLimit(), 150);
    assertEquals(snapshot.getExternalStorageReadMode(), ExternalStorageReadMode.DUAL_MODE_CONSISTENCY_CHECK);
  }
}
