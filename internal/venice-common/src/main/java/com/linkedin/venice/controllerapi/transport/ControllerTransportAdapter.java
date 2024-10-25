package com.linkedin.venice.controllerapi.transport;

import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.request.DiscoverLeaderControllerRequest;
import com.linkedin.venice.controllerapi.request.EmptyPushRequest;
import com.linkedin.venice.controllerapi.request.GetStoreRequest;
import com.linkedin.venice.controllerapi.request.ListStoresRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;


public interface ControllerTransportAdapter {
  LeaderControllerResponse discoverLeaderController(DiscoverLeaderControllerRequest discoverLeaderControllerRequest);

  NewStoreResponse createNewStore(NewStoreRequest newStoreRequest);

  StoreResponse getStore(GetStoreRequest getStoreRequest);

  VersionCreationResponse emptyPush(EmptyPushRequest emptyPushRequest);

  MultiStoreResponse listStores(ListStoresRequest listStoresRequest);
}
