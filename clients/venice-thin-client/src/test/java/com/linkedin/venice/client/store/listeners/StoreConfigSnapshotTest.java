package com.linkedin.venice.client.store.listeners;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.venice.meta.ExternalStorageReadMode;
import com.linkedin.venice.meta.StorageMode;
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

  /** The deprecated 2-arg constructor must default to INTERNAL so external-storage reads stay gated off. */
  @Test
  public void twoArgConstructorDefaultsCurrentVersionStorageModeToInternal() {
    StoreConfigSnapshot snapshot = new StoreConfigSnapshot(150, ExternalStorageReadMode.EXTERNAL_ONLY);
    assertEquals(snapshot.getCurrentVersionStorageMode(), StorageMode.INTERNAL);
  }

  @Test
  public void threeArgConstructorAccessorsReturnConstructorValues() {
    StoreConfigSnapshot snapshot =
        new StoreConfigSnapshot(150, ExternalStorageReadMode.DUAL_MODE_CONSISTENCY_CHECK, StorageMode.DUAL_WRITE);
    assertEquals(snapshot.getBatchGetLimit(), 150);
    assertEquals(snapshot.getExternalStorageReadMode(), ExternalStorageReadMode.DUAL_MODE_CONSISTENCY_CHECK);
    assertEquals(snapshot.getCurrentVersionStorageMode(), StorageMode.DUAL_WRITE);
  }

  @Test
  public void nullCurrentVersionStorageModeCoercesToInternal() {
    StoreConfigSnapshot snapshot = new StoreConfigSnapshot(150, ExternalStorageReadMode.EXTERNAL_ONLY, null);
    assertEquals(snapshot.getCurrentVersionStorageMode(), StorageMode.INTERNAL);
  }

  @Test
  public void currentVersionStorageModeDifferenceBreaksEquality() {
    StoreConfigSnapshot a = new StoreConfigSnapshot(150, ExternalStorageReadMode.EXTERNAL_ONLY, StorageMode.INTERNAL);
    StoreConfigSnapshot b = new StoreConfigSnapshot(150, ExternalStorageReadMode.EXTERNAL_ONLY, StorageMode.EXTERNAL);
    assertNotEquals(a, b);
  }

  @Test
  public void identicalThreeArgSnapshotsCompareEqual() {
    StoreConfigSnapshot a =
        new StoreConfigSnapshot(150, ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN, StorageMode.DUAL_WRITE);
    StoreConfigSnapshot b =
        new StoreConfigSnapshot(150, ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN, StorageMode.DUAL_WRITE);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }
}
