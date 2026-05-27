package com.linkedin.venice.controller.versionlifecycle;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import org.testng.annotations.Test;


/**
 * Tests for the miscellaneous lifecycle predicates on {@link VersionLifecyclePolicy}:
 * {@code hasFatalDataValidationError} (push-monitor DIV probe) and
 * {@code updateStoreTTLRepushFlag} (push-id-driven TTL flag).
 */
public class LifecyclePredicatesTest {
  // ---------- hasFatalDataValidationError ----------
  @Test
  public void hasFatalDataValidationErrorReturnsTrueWhenPushReportsError() {
    PushMonitor pushMonitor = mock(PushMonitor.class);
    OfflinePushStatus offlinePushStatus = mock(OfflinePushStatus.class);
    doReturn(true).when(offlinePushStatus).hasFatalDataValidationError();
    doReturn(offlinePushStatus).when(pushMonitor).getOfflinePushOrThrow("topic_v1");

    assertTrue(VersionLifecyclePolicy.hasFatalDataValidationError(pushMonitor, "topic_v1"));
  }

  @Test
  public void hasFatalDataValidationErrorReturnsFalseWhenPushReportsClean() {
    PushMonitor pushMonitor = mock(PushMonitor.class);
    OfflinePushStatus offlinePushStatus = mock(OfflinePushStatus.class);
    doReturn(false).when(offlinePushStatus).hasFatalDataValidationError();
    doReturn(offlinePushStatus).when(pushMonitor).getOfflinePushOrThrow("topic_v1");

    assertFalse(VersionLifecyclePolicy.hasFatalDataValidationError(pushMonitor, "topic_v1"));
  }

  @Test
  public void hasFatalDataValidationErrorReturnsFalseWhenPushEntryMissing() {
    PushMonitor pushMonitor = mock(PushMonitor.class);
    doThrow(new VeniceException("not found")).when(pushMonitor).getOfflinePushOrThrow("topic_v1");

    assertFalse(VersionLifecyclePolicy.hasFatalDataValidationError(pushMonitor, "topic_v1"));
  }

  // ---------- updateStoreTTLRepushFlag ----------

  @Test
  public void ttlRePushSetsFlagToTrueWhenCurrentlyFalse() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String ttlRepushId = Version.generateTTLRePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(false);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(ttlRepushId, store, repository);

    verify(store).setTTLRepushEnabled(true);
    verify(repository).updateStore(store);
  }

  @Test
  public void ttlRePushIsNoOpWhenFlagAlreadyTrue() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String ttlRepushId = Version.generateTTLRePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(true);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(ttlRepushId, store, repository);

    verify(store, never()).setTTLRepushEnabled(anyBoolean());
    verify(repository, never()).updateStore(any());
  }

  @Test
  public void regularPushWithTTLClearsFlagWhenCurrentlyTrue() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String regularPushWithTtlId = Version.generateRegularPushWithTTLRePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(true);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(regularPushWithTtlId, store, repository);

    verify(store).setTTLRepushEnabled(false);
    verify(repository).updateStore(store);
  }

  @Test
  public void regularPushWithTTLIsNoOpWhenFlagAlreadyFalse() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String regularPushWithTtlId = Version.generateRegularPushWithTTLRePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(false);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(regularPushWithTtlId, store, repository);

    verify(store, never()).setTTLRepushEnabled(anyBoolean());
    verify(repository, never()).updateStore(any());
  }

  @Test
  public void compliancePushDoesNotAffectTTLFlag() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String compliancePushId = Version.generateCompliancePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(true);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(compliancePushId, store, repository);

    verify(store, never()).setTTLRepushEnabled(anyBoolean());
    verify(repository, never()).updateStore(any());
  }

  @Test
  public void unrelatedPushIdDoesNotAffectTTLFlag() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String userPushId = System.currentTimeMillis() + "_https://example.com/user-push";
    when(store.isTTLRepushEnabled()).thenReturn(true);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(userPushId, store, repository);

    verify(store, never()).setTTLRepushEnabled(anyBoolean());
    verify(repository, never()).updateStore(any());
  }
}
