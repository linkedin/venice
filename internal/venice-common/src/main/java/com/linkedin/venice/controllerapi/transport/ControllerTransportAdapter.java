package com.linkedin.venice.controllerapi.transport;

import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.DiscoverLeaderControllerRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;


public interface ControllerTransportAdapter {
  LeaderControllerResponse discoverLeaderController(DiscoverLeaderControllerRequest discoverLeaderControllerRequest);

  NewStoreResponse createNewStore(NewStoreRequest newStoreRequest);
}
