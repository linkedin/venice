package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashMap;
import java.util.List;
import org.apache.helix.manager.zk.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ParentHelixOfflinePushAccessorTest {
  private String clusterName = TestUtils.getUniqueString("HelixOfflinePushMonitorAccessorTest");
  private ZkServerWrapper zk;
  private ParentHelixOfflinePushAccessor accessor;

  @BeforeMethod
  public void setup() {
    zk = ServiceFactory.getZkServer();
    String zkAddress = zk.getAddress();
    ZkClient zkClient = new ZkClient(zkAddress);
    accessor = new ParentHelixOfflinePushAccessor(zkClient, new HelixAdapterSerializer());
  }

  @AfterMethod
  public void cleanup() {
    zk.close();
  }


  @Test
  public void testCreateAndGetOfflinePushStatusOnly(){
    String topic = "testCreateAndGetOfflinePushStatusOnly";
    OfflinePushStatus onlyOfflinePushStatus =
        new OfflinePushStatus(topic, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    accessor.createOfflinePushStatus(clusterName, onlyOfflinePushStatus);
    OfflinePushStatus status= accessor.getOfflinePushStatus(clusterName, topic);
    Assert.assertEquals(status.getKafkaTopic(), topic);
    Assert.assertEquals(status.getNumberOfPartition(), 3);
    Assert.assertEquals(status.getReplicationFactor(), 3);
    Assert.assertEquals(status.getPushProperties().size(), 0);

    HashMap<String,String> properteis = new HashMap<>();
    properteis.put("test", "test");
    onlyOfflinePushStatus.setPushProperties(properteis);
    accessor.updateOfflinePushStatus(clusterName, onlyOfflinePushStatus);
    status= accessor.getOfflinePushStatus(clusterName, topic);
    Assert.assertEquals(status.getKafkaTopic(), topic);
    Assert.assertEquals(status.getNumberOfPartition(), 3);
    Assert.assertEquals(status.getReplicationFactor(), 3);
    Assert.assertEquals(status.getPushProperties().get("test"), "test");
  }

  @Test
  public void testGetAllPushNames() {
    int count = 10;
    for (int i = 0; i < count; i++) {
      String topic = "testGetAllPushNames" + i;
      OfflinePushStatus onlyOfflinePushStatus =
          new OfflinePushStatus(topic, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      accessor.createOfflinePushStatus(clusterName, onlyOfflinePushStatus);
    }

    List<String> names = accessor.getAllPushNames(clusterName);
    Assert.assertEquals(names.size(), count);
    for (int i = 0; i < count; i++) {
      Assert.assertTrue(names.contains("testGetAllPushNames" + i));
    }
  }

  @Test
  public void testDeleteOfflinePushStatus(){
    String topic = "testDeleteOfflinePushStatus";
    OfflinePushStatus onlyOfflinePushStatus =
        new OfflinePushStatus(topic, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    accessor.createOfflinePushStatus(clusterName, onlyOfflinePushStatus);
    accessor.deleteOfflinePushStatus(clusterName, topic);
    try{
      accessor.getOfflinePushStatus(clusterName, topic);
      Assert.fail("Push status should be deleted before.");
    }catch (VeniceException e){
      // expected
    }
  }
}
