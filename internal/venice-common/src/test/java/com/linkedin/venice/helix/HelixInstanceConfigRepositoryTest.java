package com.linkedin.venice.helix;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HelixInstanceConfigRepositoryTest {
  private static final String VENICE_FAULT_ZONE_TYPE_GROUP = "virtualGroup";
  private static final String VENICE_FAULT_ZONE_TYPE_ZONE = "virtualZone";

  private SafeHelixManager getMockHelixManager() {
    String clusterName = Utils.getUniqueString("testCluster");
    SafeHelixManager helixManager = mock(SafeHelixManager.class);
    doReturn(clusterName).when(helixManager).getClusterName();

    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    doReturn(configAccessor).when(helixManager).getConfigAccessor();

    ClusterConfig clusterConfig = mock(ClusterConfig.class);
    doReturn(clusterConfig).when(configAccessor).getClusterConfig(clusterName);

    return helixManager;
  }

  private List<InstanceConfig> mockInstanceConfigsWithVirtualGrouping(
      Map<String, InstanceVirtualZone> instanceGroupMapping) {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    instanceGroupMapping.forEach((instanceId, zoneAndGroup) -> {
      ZNRecord record = new ZNRecord(instanceId);
      record.setSimpleField(
          InstanceConfig.InstanceConfigProperty.DOMAIN.name(),
          VENICE_FAULT_ZONE_TYPE_GROUP + "=" + zoneAndGroup.virtualGroup + "," + VENICE_FAULT_ZONE_TYPE_ZONE + "="
              + zoneAndGroup.virtualZone);
      InstanceConfig instanceConfig = new InstanceConfig(record);
      instanceConfigs.add(instanceConfig);
    });
    return instanceConfigs;
  }

  private List<InstanceConfig> mockInstanceConfigs(Map<String, String> instanceGroupMapping) {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    instanceGroupMapping.forEach((instanceId, group) -> {
      ZNRecord record = new ZNRecord(instanceId);
      record.setSimpleField(
          InstanceConfig.InstanceConfigProperty.DOMAIN.name(),
          VENICE_FAULT_ZONE_TYPE_GROUP + "=" + group);
      InstanceConfig instanceConfig = new InstanceConfig(record);
      instanceConfigs.add(instanceConfig);
    });
    return instanceConfigs;
  }

  @Test(invocationCount = 100)
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

    String zone1 = "zone_1";
    String zone2 = "zone_2";

    Map<String, InstanceVirtualZone> instanceGroupMapping = new TreeMap<>();
    instanceGroupMapping.put(nodeId1, new InstanceVirtualZone(group1, zone1));
    instanceGroupMapping.put(nodeId2, new InstanceVirtualZone(group2, zone2));
    instanceGroupMapping.put(nodeId3, new InstanceVirtualZone(group3, zone1));
    instanceGroupMapping.put(nodeId4, new InstanceVirtualZone(group2, zone2));
    instanceGroupMapping.put(nodeId5, new InstanceVirtualZone(group1, zone1));
    instanceGroupMapping.put(nodeId6, new InstanceVirtualZone(group3, zone2));

    List<InstanceConfig> instanceConfigs1 = mockInstanceConfigsWithVirtualGrouping(instanceGroupMapping);

    Collections.shuffle(instanceConfigs1);

    SafeHelixManager manager = getMockHelixManager();
    String clusterName = manager.getClusterName();
    ClusterConfig clusterConfig = manager.getConfigAccessor().getClusterConfig(clusterName);

    doReturn(VENICE_FAULT_ZONE_TYPE_GROUP).when(clusterConfig).getFaultZoneType();
    Mockito.clearInvocations(manager);

    HelixInstanceConfigRepository repo1 = new HelixInstanceConfigRepository(manager);
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

    // FAULT_ZONE_TYPE gets updated
    doReturn(VENICE_FAULT_ZONE_TYPE_ZONE).when(clusterConfig).getFaultZoneType();
    Mockito.clearInvocations(manager);

    repo1.onClusterConfigChange(clusterConfig, null);
    Assert.assertEquals(repo1.getGroupCount(), 2);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId1), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId2), 1);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId3), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId4), 1);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId5), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId6), 1);
    Assert.assertEquals(repo1.getInstanceGroupId(unknownNodeId), 0);

    PropertyKey.Builder builder = new PropertyKey.Builder(clusterName);

    repo1.clear();
    verify(manager, times(1)).removeListener(builder.clusterConfig(), repo1);
    verify(manager, times(1)).removeListener(builder.instanceConfigs(), repo1);
  }

  @Test
  public void testTopologyAwareSwitch() {
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

    Map<String, String> instanceGroupMapping = new TreeMap<>();
    instanceGroupMapping.put(nodeId1, group1);
    instanceGroupMapping.put(nodeId2, group2);
    instanceGroupMapping.put(nodeId3, group3);
    instanceGroupMapping.put(nodeId4, group2);
    instanceGroupMapping.put(nodeId5, group1);
    instanceGroupMapping.put(nodeId6, group3);
    List<InstanceConfig> instanceConfigs1 = mockInstanceConfigs(instanceGroupMapping);

    Collections.shuffle(instanceConfigs1);

    SafeHelixManager manager = getMockHelixManager();
    String clusterName = manager.getClusterName();
    ClusterConfig clusterConfig = manager.getConfigAccessor().getClusterConfig(clusterName);

    doReturn(null).when(clusterConfig).getFaultZoneType();
    Mockito.clearInvocations(manager);

    HelixInstanceConfigRepository repo1 = new HelixInstanceConfigRepository(manager);
    repo1.refresh();
    repo1.onInstanceConfigChange(instanceConfigs1, null);
    Assert.assertEquals(repo1.getGroupCount(), 1);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId1), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId2), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId3), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId4), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId5), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId6), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(unknownNodeId), 0);

    // Cluster becomes TOPLOGY_AWARE
    doReturn(VENICE_FAULT_ZONE_TYPE_GROUP).when(clusterConfig).getFaultZoneType();
    Mockito.clearInvocations(manager);

    repo1.onClusterConfigChange(clusterConfig, null);
    Assert.assertEquals(repo1.getGroupCount(), 3);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId1), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId2), 1);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId3), 2);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId4), 1);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId5), 0);
    Assert.assertEquals(repo1.getInstanceGroupId(nodeId6), 2);
    Assert.assertEquals(repo1.getInstanceGroupId(unknownNodeId), 0);

    PropertyKey.Builder builder = new PropertyKey.Builder(clusterName);

    repo1.clear();
    verify(manager, times(1)).removeListener(builder.clusterConfig(), repo1);
    verify(manager, times(1)).removeListener(builder.instanceConfigs(), repo1);
  }

  private class InstanceVirtualZone {
    String virtualGroup;
    String virtualZone;

    InstanceVirtualZone(String virtualGroup, String virtualZone) {
      this.virtualGroup = virtualGroup;
      this.virtualZone = virtualZone;
    }
  }
}
