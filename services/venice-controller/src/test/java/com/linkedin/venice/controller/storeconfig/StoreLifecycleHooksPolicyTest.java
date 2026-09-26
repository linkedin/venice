package com.linkedin.venice.controller.storeconfig;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.Store;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StoreLifecycleHooksPolicyTest {
  @Test
  public void testValidateLifecycleHooksRejectsBlankClassName() {
    Store oldStore = mock(Store.class);
    when(oldStore.getStoreLifecycleHooks()).thenReturn(Collections.emptyList());

    LifecycleHooksRecord lifecycleHooksRecord = mock(LifecycleHooksRecord.class);
    when(lifecycleHooksRecord.getStoreLifecycleHooksClassName()).thenReturn(" ");

    VeniceException exception = Assert.expectThrows(
        VeniceException.class,
        () -> StoreLifecycleHooksPolicy
            .validateLifecycleHooks(oldStore, Optional.of(Collections.singletonList(lifecycleHooksRecord))));

    Assert.assertTrue(exception.getMessage().contains("blank class name"));
  }

  @Test
  public void testValidateLifecycleHooksTrimsClassName() {
    Store oldStore = mock(Store.class);
    when(oldStore.getStoreLifecycleHooks()).thenReturn(Collections.emptyList());

    LifecycleHooksRecord lifecycleHooksRecord = mock(LifecycleHooksRecord.class);
    when(lifecycleHooksRecord.getStoreLifecycleHooksClassName()).thenReturn("  com.linkedin.venice.Hook  ");
    List<LifecycleHooksRecord> hooks = Collections.singletonList(lifecycleHooksRecord);

    List<LifecycleHooksRecord> result = StoreLifecycleHooksPolicy.validateLifecycleHooks(oldStore, Optional.of(hooks));

    Assert.assertSame(result, hooks);
    verify(lifecycleHooksRecord).setStoreLifecycleHooksClassName("com.linkedin.venice.Hook");
  }

  /**
   * Regression guard for the incident where an unrelated update_store (only changing
   * storage_mode) carried the serialized empty-list default for lifecycle hooks and wiped a
   * store's configured hooks. A present-but-empty list must be treated as "no change" and
   * preserve the current hooks, exactly like an absent value.
   */
  @Test
  public void testValidateLifecycleHooksPresentEmptyPreservesExistingHooks() {
    Store oldStore = mock(Store.class);
    LifecycleHooksRecord existingHook = mock(LifecycleHooksRecord.class);
    List<LifecycleHooksRecord> existingHooks = Collections.singletonList(existingHook);
    when(oldStore.getStoreLifecycleHooks()).thenReturn(existingHooks);

    List<LifecycleHooksRecord> result =
        StoreLifecycleHooksPolicy.validateLifecycleHooks(oldStore, Optional.of(Collections.emptyList()));

    Assert.assertEquals(result, existingHooks, "A present-but-empty list must preserve existing hooks");
  }

  /**
   * When the caller does not supply the field at all, the current hooks must be preserved.
   */
  @Test
  public void testValidateLifecycleHooksAbsentPreservesExistingHooks() {
    Store oldStore = mock(Store.class);
    LifecycleHooksRecord existingHook = mock(LifecycleHooksRecord.class);
    List<LifecycleHooksRecord> existingHooks = Collections.singletonList(existingHook);
    when(oldStore.getStoreLifecycleHooks()).thenReturn(existingHooks);

    List<LifecycleHooksRecord> result = StoreLifecycleHooksPolicy.validateLifecycleHooks(oldStore, Optional.empty());

    Assert.assertEquals(result, existingHooks, "An absent value must preserve existing hooks");
  }

  /**
   * A present-but-empty list when the store has no hooks yet must stay empty (not error, not
   * fabricate hooks).
   */
  @Test
  public void testValidateLifecycleHooksPresentEmptyWithNoExistingHooksStaysEmpty() {
    Store oldStore = mock(Store.class);
    when(oldStore.getStoreLifecycleHooks()).thenReturn(Collections.emptyList());

    List<LifecycleHooksRecord> result =
        StoreLifecycleHooksPolicy.validateLifecycleHooks(oldStore, Optional.of(Collections.emptyList()));

    Assert.assertTrue(result.isEmpty(), "Empty in, no existing hooks, must remain empty");
  }
}
