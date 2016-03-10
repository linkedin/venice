package com.linkedin.venice.router.api;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 3/4/16.
 */
public class TestHostFinder {

  @Test
  public void hostFinderShouldFindHosts(){
    RoutingDataRepository mockRepo = Mockito.mock(RoutingDataRepository.class);
    Instance dummyinstance = new Instance("0", "localhost", 1234, 5678);
    List<Instance> dummyList = new ArrayList<>(0);
    dummyList.add(dummyinstance);
    //when(mockRepo.getInstances(anyString(), anyInt())).thenReturn(dummyList);
    doReturn(dummyList).when(mockRepo).getInstances(anyString(), anyInt());

    VeniceHostFinder finder = new VeniceHostFinder(mockRepo);

    List<Instance> hosts = finder.findHosts(null, "store_v0", "3", null, null);

    Assert.assertEquals(hosts.get(0).getHost(), "localhost");

  }
}
