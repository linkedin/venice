package com.linkedin.venice.helix;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HelixInstanceConfigRepositoryTest {
  private List<InstanceConfig> mockInstanceConfigs(Map<String, String> instanceGroupMapping) {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    instanceGroupMapping.forEach((instanceId, group) -> {
      ZNRecord record = new ZNRecord(instanceId);
      record.setSimpleField(
          InstanceConfig.InstanceConfigProperty.DOMAIN.name(),
          HelixInstanceConfigRepository.GROUP_FIELD_NAME_IN_DOMAIN + "=" + group);
      InstanceConfig instanceConfig = new InstanceConfig(record);
      instanceConfigs.add(instanceConfig);
    });
    return instanceConfigs;
  }

  @Test
  public void testGroupIdAssignment() {
    String nodeId1 = "node_id_1_port";
    String nodeId2 = "node_id_2_port";
    String nodeId3 = "node_id_3_port";
    String nodeId4 = "node_id_4_port";
    String nodeId5 = "node_id_5_port";
    String nodeId6 = "node_id_6_port";
    String unknownNodeId = "unknown_node_id_port";

    String group1 = "group_1";
    String group2 = "group_2";
    String group3 = "group_3";

    Map<String, String> instanceGroupMapping1 = new TreeMap<>();
    instanceGroupMapping1.put(nodeId1, group1);
    instanceGroupMapping1.put(nodeId2, group2);
    instanceGroupMapping1.put(nodeId3, group3);
    instanceGroupMapping1.put(nodeId4, group2);
    instanceGroupMapping1.put(nodeId5, group1);
    instanceGroupMapping1.put(nodeId6, group3);
    List<InstanceConfig> instanceConfigs1 = mockInstanceConfigs(instanceGroupMapping1);

    HelixInstanceConfigRepository repo1 = new HelixInstanceConfigRepository(mock(SafeHelixManager.class), true);
    repo1.refresh();
    repo1.onInstanceConfigChange(instanceConfigs1, null);
    Assert.assertEquals(repo1.getGroupCount(), 3);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId1), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId2), 1);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId3), 2);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId4), 1);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId5), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId6), 2);
    Assert.assertEquals(repo1.getInstanceGroupId(unknownNodeId), 0);
  }
}
