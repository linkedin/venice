package com.linkedin.venice.helix;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.meta.DegradedDcInfo;
import com.linkedin.venice.meta.DegradedDcStates;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelixDegradedDcStatesRepository {
  @Test
  public void testGetStatesReturnsEmptyByDefault() {
    ZkClient mockZkClient = mock(ZkClient.class);
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();

    HelixReadOnlyDegradedDcStatesRepository repo =
        new HelixReadOnlyDegradedDcStatesRepository(mockZkClient, adapter, "test-cluster");

    // Before refresh, should return empty states (defensive copy of default)
    DegradedDcStates states = repo.getStates();
    Assert.assertNotNull(states);
    Assert.assertTrue(states.isEmpty());
    Assert.assertTrue(states.getDegradedDatacenterNames().isEmpty());
  }

  @Test
  public void testGetStatesReturnsDefensiveCopy() {
    ZkClient mockZkClient = mock(ZkClient.class);
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();

    HelixReadOnlyDegradedDcStatesRepository repo =
        new HelixReadOnlyDegradedDcStatesRepository(mockZkClient, adapter, "test-cluster");

    // Two calls should return different objects (defensive copies)
    DegradedDcStates states1 = repo.getStates();
    DegradedDcStates states2 = repo.getStates();
    Assert.assertNotSame(states1, states2, "getStates() should return defensive copies");
  }

  @Test
  public void testGetStatesDefensiveCopyPreventsMutation() {
    ZkClient mockZkClient = mock(ZkClient.class);
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();

    HelixReadOnlyDegradedDcStatesRepository repo =
        new HelixReadOnlyDegradedDcStatesRepository(mockZkClient, adapter, "test-cluster");

    // Mutating a returned copy should not affect the next call
    DegradedDcStates states = repo.getStates();
    states.addDegradedDatacenter("dc-1", new DegradedDcInfo(123L, 60, "op"));

    DegradedDcStates freshStates = repo.getStates();
    Assert.assertTrue(freshStates.isEmpty(), "Mutation of copy should not affect repository state");
  }

  @Test
  public void testReadWriteRepositoryConstructs() {
    ZkClient mockZkClient = mock(ZkClient.class);
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();

    // Should not throw
    HelixReadWriteDegradedDcStatesRepository repo =
        new HelixReadWriteDegradedDcStatesRepository(mockZkClient, adapter, "test-cluster");

    // getStates returns empty default
    DegradedDcStates states = repo.getStates();
    Assert.assertNotNull(states);
    Assert.assertTrue(states.isEmpty());
  }
}
