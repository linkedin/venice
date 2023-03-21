package com.linkedin.venice.router.throttle;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StoreReadThrottlerTest {
  @Test
  public void testBuildAndUpdateStoreReadThrottler() {
    int partitionCount = 2;
    long quota = 100;
    String storeName = "StoreReadThrottlerTest";
    int versionNumber = 1;
    double perStorageNodeReadQuotaBuffer = 1.0;

    Instance instance1 = new Instance(Utils.getHelixNodeIdentifier(Utils.getHostName(), 10001), "localhost", 10001);
    Instance instance2 = new Instance(Utils.getHelixNodeIdentifier(Utils.getHostName(), 10002), "localhost", 10002);
    PartitionAssignment assignment =
        new PartitionAssignment(Version.composeKafkaTopic(storeName, versionNumber), partitionCount);

    Map<String, List<Instance>> p0StateToInstance = new HashMap<>();
    p0StateToInstance.put(HelixState.ONLINE_STATE, Arrays.asList(new Instance[] { instance1, instance2 }));
    assignment.addPartition(new Partition(0, p0StateToInstance));

    Map<String, List<Instance>> p1StateToInstance = new HashMap<>();
    p1StateToInstance.put(HelixState.ONLINE_STATE, Arrays.asList(new Instance[] { instance1 }));
    p1StateToInstance.put(HelixState.BOOTSTRAP_STATE, Arrays.asList(new Instance[] { instance2 }));
    assignment.addPartition(new Partition(1, p1StateToInstance));

    StoreReadThrottler throttler = new StoreReadThrottler(
        storeName,
        quota,
        EventThrottler.REJECT_STRATEGY,
        Optional.of(assignment),
        perStorageNodeReadQuotaBuffer,
        1000,
        1000);

    Assert.assertEquals(throttler.getCurrentVersion(), versionNumber);

    Assert.assertEquals(throttler.getQuota(), quota);
    // Instance1 holds 1 of 2 online replicas for partition 0 and 1 of 1 online replicas for partition 1. So it should
    // be assigned the quota value which equals to quota/partitionCount * 1.5
    Assert.assertEquals(
        throttler.getQuotaForStorageNode(Utils.getHelixNodeIdentifier(Utils.getHostName(), 10001)),
        (long) (quota / (double) partitionCount * 1.5 * (1 + perStorageNodeReadQuotaBuffer)));
    // Instance 2 hold 1 of 2 online prelicas for partition 0 and 0 of 1 online replicas for partition 1.
    Assert.assertEquals(
        throttler.getQuotaForStorageNode(Utils.getHelixNodeIdentifier(Utils.getHostName(), 10002)),
        (long) (quota / (double) partitionCount * 0.5 * (1 + perStorageNodeReadQuotaBuffer)));

    // Bootstrap replica in partition2 and instance2 become online.
    p1StateToInstance = new HashMap<>();
    p1StateToInstance.put(HelixState.ONLINE_STATE, Arrays.asList(new Instance[] { instance1, instance2 }));
    assignment.addPartition(new Partition(1, p1StateToInstance));
    throttler.updateStorageNodesThrottlers(assignment);
    // Instance1 holds 1 of 2 online replicas for partition 0 and 1 of 2 online replicas for partition 1.
    Assert.assertEquals(
        throttler.getQuotaForStorageNode(Utils.getHelixNodeIdentifier(Utils.getHostName(), 10001)),
        (long) (quota / (double) partitionCount * (1 + perStorageNodeReadQuotaBuffer)));
    // Instance 2 hold 1 of 2 online replicas for partition 0 and 1 of 2 online replicas for partition 1.
    Assert.assertEquals(
        throttler.getQuotaForStorageNode(Utils.getHelixNodeIdentifier(Utils.getHostName(), 10002)),
        (long) (quota / (double) partitionCount * (1 + perStorageNodeReadQuotaBuffer)));

    // All replicas in Partition 1 failed.
    p1StateToInstance.remove(HelixState.ONLINE_STATE);
    assignment.addPartition(new Partition(1, p1StateToInstance));
    throttler.updateStorageNodesThrottlers(assignment);
    // Instance1 holds 1 of 2 online replicas for partition 0 and 0 of 0 online replicas for partition 1.
    Assert.assertEquals(
        throttler.getQuotaForStorageNode(Utils.getHelixNodeIdentifier(Utils.getHostName(), 10001)),
        (long) (quota / (double) partitionCount / 2 * (1 + perStorageNodeReadQuotaBuffer)));
    // Instance 2 hold 1 of 2 online prelicas for partition 0 and 0 of 0 online replicas for partition 1.
    Assert.assertEquals(
        throttler.getQuotaForStorageNode(Utils.getHelixNodeIdentifier(Utils.getHostName(), 10002)),
        (long) (quota / (double) partitionCount / 2 * (1 + perStorageNodeReadQuotaBuffer)));
  }

  @Test
  public void testThrottle() {
    int partitionCount = 4;
    int instanceCount = 3;
    long quota = 1200;
    String storeName = "StoreReadThrottlerTest";
    int versionNumber = 1;
    Instance[] instances = new Instance[instanceCount];
    for (int i = 0; i < instanceCount; i++) {
      int port = 10000 + i;
      instances[i] = new Instance(Utils.getHelixNodeIdentifier(Utils.getHostName(), port), "localhost", port);
    }
    PartitionAssignment assignment =
        new PartitionAssignment(Version.composeKafkaTopic(storeName, versionNumber), partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      Map<String, List<Instance>> stateToInstance = new HashMap<>();
      stateToInstance.put(HelixState.ONLINE_STATE, Arrays.asList(instances));
      assignment.addPartition(new Partition(i, stateToInstance));
    }
    // each storage node holds 4 online replicas, so the quota of each storage node is 1200/4/3*4=400
    StoreReadThrottler throttler = new StoreReadThrottler(
        storeName,
        quota,
        EventThrottler.REJECT_STRATEGY,
        Optional.of(assignment),
        0.0,
        1000,
        1000);
    throttler.mayThrottleRead(400, Utils.getHelixNodeIdentifier(Utils.getHostName(), 10000));
    try {
      throttler.mayThrottleRead(100, Utils.getHelixNodeIdentifier(Utils.getHostName(), 10000));
      Assert.fail("Usage(500) exceed the quota(400) of Instance localhost_10000 ");
    } catch (QuotaExceededException e) {
      // expected
    }

    try {
      throttler.mayThrottleRead(400, Utils.getHelixNodeIdentifier(Utils.getHostName(), 10001));
      throttler.mayThrottleRead(100, Utils.getHelixNodeIdentifier(Utils.getHostName(), 10002));
    } catch (QuotaExceededException e) {
      Assert.fail("Usage has not exceeded the quota, should accept requests.", e);
    }

    throttler.clearStorageNodesThrottlers();
    try {
      throttler.mayThrottleRead(100, Utils.getHelixNodeIdentifier(Utils.getHostName(), 10000));
    } catch (QuotaExceededException e) {
      Assert.fail("Throttler for storage node has been cleared, this store still have quota to accept this request.");
    }
  }
}
