package com.linkedin.venice.helix;

import static org.mockito.Mockito.mock;

import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.annotations.Test;


public class HelixSchemaAccessorTest {
  @Test
  public void testRemoveValueSchema() {
    String storeName = "abc";
    ZkClient zkClient = mock(ZkClient.class);
    HelixAdapterSerializer serializer = mock(HelixAdapterSerializer.class);
    HelixSchemaAccessor schemaAccessor = new HelixSchemaAccessor(zkClient, serializer, "clusterName");
    schemaAccessor.removeValueSchema(storeName, 2);
  }
}
