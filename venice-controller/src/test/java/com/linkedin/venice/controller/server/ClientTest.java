package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreCreationResponse;
import com.linkedin.venice.integration.utils.PortUtils;
import com.linkedin.venice.meta.Version;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 3/17/16.
 */
public class ClientTest {
  private static final String STORE_NAME = "mystore";
  private static final String OWNER = "matt-the-awesome";
  @Test
  public void serverCanTalkToClient()
      throws Exception {
    int retry = 5;
    while (retry>0) {
      try {
        int port = PortUtils.getFreePort();
        ControllerClient client = new ControllerClient("localhost", port);

        Admin mockAdmin = Mockito.mock(Admin.class);
        Version version = new Version(STORE_NAME, 5 );
        doReturn(version).when(mockAdmin)
            .incrementVersion(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
        AdminServer server = new AdminServer(port, "cluster-for-tests", mockAdmin);
        server.start();
        int storeSizeMb = 500;
        StoreCreationResponse response = client.CreateNewStoreVersion(STORE_NAME, OWNER, storeSizeMb);
        server.stop();

        Assert.assertEquals(response.getName(), "mystore");
        Assert.assertEquals(response.getVersion(), 5);
        Assert.assertEquals(response.getOwner(), "matt-the-awesome");
        Assert.assertEquals(response.getPartitions(), 3);  //TODO change this when we add actual partition calculation logic
        Assert.assertEquals(response.getReplicas(), 1);
        break;
      } catch (java.net.BindException e) {
        System.err.println("Failed to bind to port, trying again " + retry-- + " more times");
        if (retry <= 0){
          throw new Exception(ClientTest.class.toString() + " couldn't get a free port", e);
        }
      }
    }
  }
}
