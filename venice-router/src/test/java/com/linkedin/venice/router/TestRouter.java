package com.linkedin.venice.router;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.helix.HelixCachedMetadataRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.integration.utils.MockVeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
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
  public void testMockRouter() {
    MockVeniceRouterWrapper router = ServiceFactory.getMockVeniceRouter(new ArrayList<D2Server>());
    // Doesn't actually test anything other than the router can startup and doesn't crash
    router.close();
  }

}
