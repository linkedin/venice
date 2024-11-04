package com.linkedin.davinci.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.ReplicaHeartbeatInfo;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ReplicaHeartbeatInfoTest {
  @Test
  public void testJsonParse() throws Exception {
    String storeName = "test_store";
    String topicName = Version.composeKafkaTopic(storeName, 1);
    int partition = 10;
    String region = "dc-1";
    long lag = TimeUnit.MINUTES.toMillis(11);
    long heartbeat = System.currentTimeMillis() - lag;
    LeaderFollowerStateType leaderFollowerStateType = LeaderFollowerStateType.LEADER;
    String replicaId = Utils.getReplicaId(topicName, partition);
    ReplicaHeartbeatInfo replicaHeartbeatInfo = new ReplicaHeartbeatInfo("a", "b", "c", true, 0L, 0L);

    replicaHeartbeatInfo.setReplicaId(replicaId);
    replicaHeartbeatInfo.setRegion(region);
    replicaHeartbeatInfo.setLeaderState(leaderFollowerStateType.name());
    replicaHeartbeatInfo.setHeartbeat(heartbeat);
    replicaHeartbeatInfo.setLag(lag);

    Assert.assertEquals(replicaHeartbeatInfo.getHeartbeat(), heartbeat);
    Assert.assertEquals(replicaHeartbeatInfo.getReplicaId(), replicaId);
    Assert.assertEquals(replicaHeartbeatInfo.getRegion(), region);
    Assert.assertEquals(replicaHeartbeatInfo.getLag(), lag);
    Assert.assertEquals(replicaHeartbeatInfo.getLeaderState(), leaderFollowerStateType.name());

    Map<String, ReplicaHeartbeatInfo> heartbeatInfoMap = new VeniceConcurrentHashMap<>();
    heartbeatInfoMap.put(replicaId + "-" + region, replicaHeartbeatInfo);
    VeniceJsonSerializer<Map<String, ReplicaHeartbeatInfo>> replicaInfoJsonSerializer =
        new VeniceJsonSerializer<>(new TypeReference<Map<String, ReplicaHeartbeatInfo>>() {
        });

    Assert.assertEquals(
        replicaInfoJsonSerializer.deserialize(replicaInfoJsonSerializer.serialize(heartbeatInfoMap, ""), "")
            .get(replicaId + "-" + region),
        replicaHeartbeatInfo);
  }
}
