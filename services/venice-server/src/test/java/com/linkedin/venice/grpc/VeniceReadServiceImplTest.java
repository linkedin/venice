package com.linkedin.venice.grpc;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.listener.HttpChannelInitializer;
import com.linkedin.venice.listener.grpc.GrpcHandlerPipeline;
import com.linkedin.venice.listener.grpc.VeniceReadServiceImpl;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceReadServiceImplTest {
  private HttpChannelInitializer initializer;

  @BeforeMethod
  public void setUp() {
    initializer = mock(HttpChannelInitializer.class);
    GrpcHandlerPipeline handlerPipeline = mock(GrpcHandlerPipeline.class);

    when(initializer.initGrpcHandlers()).thenReturn(handlerPipeline);
    when(handlerPipeline.getNewPipeline()).then(invocation -> new GrpcHandlerPipeline());
  }

  private StreamObserver<VeniceServerResponse> getDummyStreamObserver() {
    return new StreamObserver<VeniceServerResponse>() {
      @Override
      public void onNext(VeniceServerResponse value) {

      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    };
  }

  @Test
  public void testHandleRequest() {
    // not much to test here, just make sure the request is passed to the pipeline
    // main gRPC request handling logic is within respective Handlers in venice-server/listener
    VeniceClientRequest request = VeniceClientRequest.newBuilder().build();
    StreamObserver<VeniceServerResponse> observer = spy(getDummyStreamObserver());

    VeniceReadServiceImpl service = new VeniceReadServiceImpl(initializer);

    service.get(request, observer);

    InOrder orderGet = inOrder(observer);
    orderGet.verify(observer).onNext(any());
    orderGet.verify(observer).onCompleted();

    service.batchGet(request, observer);
    InOrder orderBatch = inOrder(observer);
    orderBatch.verify(observer).onNext(any());
    orderBatch.verify(observer).onCompleted();
  }

  @Test
  public void testToString() {
    VeniceReadServiceImpl service = new VeniceReadServiceImpl(initializer);
    assertNotNull(service.toString());
  }
}
