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
}
