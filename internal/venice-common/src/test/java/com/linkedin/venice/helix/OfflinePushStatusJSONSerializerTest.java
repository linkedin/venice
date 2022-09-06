package com.linkedin.venice.helix;

import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OfflinePushStatusJSONSerializerTest {
  @Test
  public void testOfflinePushSerializeAndDeserialize() throws IOException {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus("testTopic", 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatusJSONSerializer serializer = new OfflinePushStatusJSONSerializer();
    byte[] data = serializer.serialize(offlinePushStatus, null);
    Assert.assertEquals(serializer.deserialize(data, null), offlinePushStatus);
    // Update status and compare again.
    offlinePushStatus.updateStatus(ExecutionStatus.COMPLETED);
    data = serializer.serialize(offlinePushStatus, null);
    Assert.assertEquals(serializer.deserialize(data, null), offlinePushStatus);
  }
}
