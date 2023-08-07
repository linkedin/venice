package com.linkedin.venice.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.venice.listener.grpc.GrpcHandlerContext;
import com.linkedin.venice.listener.grpc.GrpcHandlerPipeline;
import com.linkedin.venice.listener.grpc.VeniceGrpcHandler;
import org.testng.annotations.Test;


public class GrpcHandlerPipelineTest {
  @Test
  public void testGrpcRead() {
    VeniceGrpcHandler mockHandlerA = mock(VeniceGrpcHandler.class);
    VeniceGrpcHandler mockHandlerB = mock(VeniceGrpcHandler.class);
    GrpcHandlerPipeline pipeline = new GrpcHandlerPipeline();
    pipeline.addHandler(mockHandlerA);
    pipeline.addHandler(mockHandlerB);

    GrpcHandlerContext mockContext = mock(GrpcHandlerContext.class);

    pipeline.processRequest(mockContext);

    verify(mockHandlerA).grpcRead(mockContext, pipeline);
    verify(mockHandlerB, never()).grpcRead(mockContext, pipeline);
    pipeline.processRequest(mockContext);
    verify(mockHandlerB).grpcRead(mockContext, pipeline);
  }

  @Test
  public void testGrpcWrite() {
    // outbound handlers are added in the reverse order, so mockHandlerB should be called first
    VeniceGrpcHandler mockHandlerA = mock(VeniceGrpcHandler.class);
    VeniceGrpcHandler mockHandlerB = mock(VeniceGrpcHandler.class);
    GrpcHandlerPipeline pipeline = new GrpcHandlerPipeline();
    pipeline.addHandler(mockHandlerA);
    pipeline.addHandler(mockHandlerB);

    GrpcHandlerContext mockContext = mock(GrpcHandlerContext.class);
    ;

    pipeline.processResponse(mockContext);

    verify(mockHandlerB).grpcWrite(mockContext, pipeline);
    verify(mockHandlerA, never()).grpcWrite(mockContext, pipeline);
    pipeline.processResponse(mockContext);
    verify(mockHandlerA).grpcWrite(mockContext, pipeline);
  }

  @Test
  public void testNewPipelineInstance() {
    VeniceGrpcHandler mockHandlerA = mock(VeniceGrpcHandler.class);
    VeniceGrpcHandler mockHandlerB = mock(VeniceGrpcHandler.class);
    GrpcHandlerPipeline pipeline = new GrpcHandlerPipeline();

    pipeline.addHandler(mockHandlerA);
    pipeline.addHandler(mockHandlerB);

    GrpcHandlerPipeline newPipeline = pipeline.getNewPipeline();

    assertEquals(newPipeline.getInboundHandlers().size(), 2);
    assertEquals(newPipeline.getOutboundHandlers().size(), 2);

    assertEquals(newPipeline.getInboundHandlers().get(0), mockHandlerA);
    assertEquals(newPipeline.getInboundHandlers().get(1), mockHandlerB);

    assertNotEquals(newPipeline, pipeline);
  }

  @Test
  public void testAdditionalCalls() {
    VeniceGrpcHandler handlerA = mock(VeniceGrpcHandler.class);
    VeniceGrpcHandler handlerB = mock(VeniceGrpcHandler.class);
    VeniceGrpcHandler handlerC = mock(VeniceGrpcHandler.class);

    GrpcHandlerPipeline pipeline = new GrpcHandlerPipeline();
    pipeline.addHandler(handlerA);
    pipeline.addHandler(handlerB);
    pipeline.addHandler(handlerC);

    GrpcHandlerContext mockContext = mock(GrpcHandlerContext.class);

    pipeline.processRequest(mockContext);
    pipeline.processRequest(mockContext);
    pipeline.processRequest(mockContext);

    pipeline.processRequest(mockContext); // should not call handlerC again

    verify(handlerA, times(1)).grpcRead(mockContext, pipeline);
    verify(handlerB, times(1)).grpcRead(mockContext, pipeline);
    verify(handlerC, times(1)).grpcRead(mockContext, pipeline);

    pipeline.processResponse(mockContext);
    pipeline.processResponse(mockContext);
    pipeline.processResponse(mockContext);

    pipeline.processResponse(mockContext); // should not call handlerC again

    verify(handlerA, times(1)).grpcWrite(mockContext, pipeline);
    verify(handlerB, times(1)).grpcWrite(mockContext, pipeline);
    verify(handlerC, times(1)).grpcWrite(mockContext, pipeline);
  }
}
