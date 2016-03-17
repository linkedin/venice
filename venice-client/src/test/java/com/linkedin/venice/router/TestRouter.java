package com.linkedin.venice.router;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.MetadataRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.PortUtils;
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

  @Test
  public void testRouter()
      throws Exception {

    Store mockStore = Mockito.mock(Store.class);
    doReturn(1).when(mockStore).getCurrentVersion();
    MetadataRepository mockMetadataRepository = Mockito.mock(MetadataRepository.class);
    doReturn(mockStore).when(mockMetadataRepository).getStore(Mockito.anyString());

    RoutingDataRepository mockRepo = Mockito.mock(RoutingDataRepository.class);
    Instance dummyinstance = new Instance("0", "localhost", PortUtils.getFreePort(), PortUtils.getFreePort());
    List<Instance> dummyList = new ArrayList<>(0);
    dummyList.add(dummyinstance);
    doReturn(dummyList).when(mockRepo).getInstances(anyString(), anyInt());
    doReturn(3).when(mockRepo).getNumberOfPartitions(Mockito.anyString());

    int port = PortUtils.getFreePort();
    RouterServer router = new RouterServer(port, "unit-test-cluster", mockRepo, mockMetadataRepository);

    router.start();
    // Doesn't actually test anything other than the router can startup and doesn't crash
    router.stop();
  }

}
