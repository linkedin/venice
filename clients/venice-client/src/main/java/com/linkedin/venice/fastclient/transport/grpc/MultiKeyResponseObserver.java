package com.linkedin.venice.fastclient.transport.grpc;

import com.linkedin.venice.protocols.MultiKeyResponse;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MultiKeyResponseObserver implements StreamObserver<MultiKeyResponse> {
  private static final Logger LOGGER = LogManager.getLogger(MultiKeyResponseObserver.class);

  @Override
  public void onNext(MultiKeyResponse value) {

  }

  @Override
  public void onError(Throwable t) {

  }

  @Override
  public void onCompleted() {

  }
}
