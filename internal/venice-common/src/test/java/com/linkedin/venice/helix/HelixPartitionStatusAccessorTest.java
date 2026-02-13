package com.linkedin.venice.helix;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.helix.HelixManager;
import org.apache.helix.customizedstate.CustomizedStateProvider;
import org.testng.annotations.Test;


public class HelixPartitionStatusAccessorTest {
  @Test
  public void testDeleteAllCustomizedStates() {
    HelixManager mockHelixManager = mock(HelixManager.class);
    CustomizedStateProvider mockProvider = mock(CustomizedStateProvider.class);

    // Create accessor with hybrid quota disabled
    HelixPartitionStatusAccessor accessor = new HelixPartitionStatusAccessor(mockHelixManager, "testInstance", false);

    // Set the mocked provider
    accessor.setCustomizedStateProvider(mockProvider);

    // Call the method under test
    accessor.deleteAllCustomizedStates();

    // Verify that deleteAllResourcesCustomizedStates is called for OFFLINE_PUSH
    verify(mockProvider).deleteAllResourcesCustomizedStates(HelixPartitionState.OFFLINE_PUSH.name());

    // Verify that it's NOT called for HYBRID_STORE_QUOTA since helixHybridStoreQuotaEnabled is false
    verify(mockProvider, never()).deleteAllResourcesCustomizedStates(HelixPartitionState.HYBRID_STORE_QUOTA.name());
  }

  @Test
  public void testDeleteAllCustomizedStatesWithHybridQuotaEnabled() {
    HelixManager mockHelixManager = mock(HelixManager.class);
    CustomizedStateProvider mockProvider = mock(CustomizedStateProvider.class);

    // Create accessor with hybrid quota enabled
    HelixPartitionStatusAccessor accessor = new HelixPartitionStatusAccessor(mockHelixManager, "testInstance", true);

    // Set the mocked provider
    accessor.setCustomizedStateProvider(mockProvider);

    // Call the method under test
    accessor.deleteAllCustomizedStates();

    // Verify that deleteAllResourcesCustomizedStates is called for both state types
    verify(mockProvider).deleteAllResourcesCustomizedStates(HelixPartitionState.OFFLINE_PUSH.name());
    verify(mockProvider).deleteAllResourcesCustomizedStates(HelixPartitionState.HYBRID_STORE_QUOTA.name());
  }

  @Test
  public void testDeleteAllCustomizedStatesWithExceptionOnOfflinePush() {
    HelixManager mockHelixManager = mock(HelixManager.class);
    CustomizedStateProvider mockProvider = mock(CustomizedStateProvider.class);

    // Create accessor with hybrid quota enabled
    HelixPartitionStatusAccessor accessor = new HelixPartitionStatusAccessor(mockHelixManager, "testInstance", true);

    // Set the mocked provider
    accessor.setCustomizedStateProvider(mockProvider);

    // Make the OFFLINE_PUSH delete throw an exception
    doThrow(new NullPointerException("Test exception")).when(mockProvider)
        .deleteAllResourcesCustomizedStates(HelixPartitionState.OFFLINE_PUSH.name());

    // Call the method under test - should not throw, exception should be caught and logged
    accessor.deleteAllCustomizedStates();

    // Verify that both deletes were attempted despite the exception
    verify(mockProvider).deleteAllResourcesCustomizedStates(HelixPartitionState.OFFLINE_PUSH.name());
    verify(mockProvider).deleteAllResourcesCustomizedStates(HelixPartitionState.HYBRID_STORE_QUOTA.name());
  }

  @Test
  public void testDeleteAllCustomizedStatesWithExceptionOnHybridQuota() {
    HelixManager mockHelixManager = mock(HelixManager.class);
    CustomizedStateProvider mockProvider = mock(CustomizedStateProvider.class);

    // Create accessor with hybrid quota enabled
    HelixPartitionStatusAccessor accessor = new HelixPartitionStatusAccessor(mockHelixManager, "testInstance", true);

    // Set the mocked provider
    accessor.setCustomizedStateProvider(mockProvider);

    // Make the HYBRID_STORE_QUOTA delete throw an exception
    doThrow(new NullPointerException("Test exception")).when(mockProvider)
        .deleteAllResourcesCustomizedStates(HelixPartitionState.HYBRID_STORE_QUOTA.name());

    // Call the method under test - should not throw, exception should be caught and logged
    accessor.deleteAllCustomizedStates();

    // Verify that both deletes were attempted despite the exception
    verify(mockProvider).deleteAllResourcesCustomizedStates(HelixPartitionState.OFFLINE_PUSH.name());
    verify(mockProvider).deleteAllResourcesCustomizedStates(HelixPartitionState.HYBRID_STORE_QUOTA.name());
  }
}
