package com.linkedin.venice.router;

import com.linkedin.venice.helix.HelixCachedMetadataRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.MetadataRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.integration.utils.PortUtils;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;


/**
 * Created by mwise on 3/9/16.
 */
public class TestRouter {

  public static final String CONTROLLER = "http://localhost:1234";

  @Test
  public void testRouter() throws Exception {
    int retry = 5;
    while (retry>0) {
      try {

        Store mockStore = Mockito.mock(Store.class);
        doReturn(1).when(mockStore).getCurrentVersion();
        HelixCachedMetadataRepository mockMetadataRepository = Mockito.mock(HelixCachedMetadataRepository.class);
        doReturn(mockStore).when(mockMetadataRepository).getStore(Mockito.anyString());

        HelixRoutingDataRepository mockRepo = Mockito.mock(HelixRoutingDataRepository.class);

        Instance mockControllerInstance = Mockito.mock(Instance.class);
        doReturn(CONTROLLER).when(mockControllerInstance).getUrl();
        doReturn(mockControllerInstance).when(mockRepo).getMasterController();

        // TODO: PortUtils is deprecated.
        int port = PortUtils.getFreePort();
        RouterServer router = new RouterServer(port, "unit-test-cluster", mockRepo, mockMetadataRepository);

        router.start();
        // Doesn't actually test anything other than the router can startup and doesn't crash
        //TODO: when controller client can ask router for controller host, test that here
        router.stop();
        break;

      } catch (java.net.BindException e) {
        System.err.println("Failed to bind to port, trying again " + retry-- + " more times");
        if (retry <= 0){
          throw new Exception(TestRouter.class.toString() + " couldn't get a free port", e);
        }
      }
    }
  }

}
