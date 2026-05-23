package com.linkedin.venice.helix;

import static org.testng.Assert.assertEquals;

import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class HelixVersionAccessorTest {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";

  @Test
  public void testZkPaths() {
    ZkClient zkClient = Mockito.mock(ZkClient.class);
    HelixAdapterSerializer adapter = Mockito.mock(HelixAdapterSerializer.class);
    HelixVersionAccessor accessor = new HelixVersionAccessor(zkClient, adapter, CLUSTER);

    assertEquals(accessor.getVersionsContainerPath(STORE), "/" + CLUSTER + "/Stores/" + STORE + "/versions");
    assertEquals(accessor.getVersionZkPath(STORE, 7), "/" + CLUSTER + "/Stores/" + STORE + "/versions/7");
  }

  @Test
  public void testSerializerRegisteredOnAdapterAndZkClient() {
    ZkClient zkClient = Mockito.mock(ZkClient.class);
    HelixAdapterSerializer adapter = Mockito.mock(HelixAdapterSerializer.class);
    new HelixVersionAccessor(zkClient, adapter, CLUSTER);

    Mockito.verify(adapter)
        .registerSerializer(
            Mockito.eq("/" + CLUSTER + "/Stores/*/versions/*"),
            Mockito.any(VersionJSONSerializer.class));
    Mockito.verify(zkClient).setZkSerializer(adapter);
  }
}
