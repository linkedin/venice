package com.linkedin.venice.grpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.protocols.MultiKeyResponse;
import com.linkedin.venice.protocols.SingleGetResponse;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.annotations.Test;


public class GrpcReplyProcessorTest {
  @Test
  public void testSendResponse() {
    GrpcReplyProcessor replyProcessor = new GrpcReplyProcessor();

    // Spy on the GrpcReplyProcessor to verify the method calls
    GrpcReplyProcessor spyReplyProcessor = spy(replyProcessor);

    // Case 1: Test for SINGLE_GET case
    GrpcRequestContext<SingleGetResponse> singleGetRequestContext = mock(GrpcRequestContext.class);
    when(singleGetRequestContext.getGrpcRequestType()).thenReturn(GrpcRequestContext.GrpcRequestType.SINGLE_GET);
    doNothing().when(spyReplyProcessor).sendSingleGetResponse(singleGetRequestContext);
    spyReplyProcessor.sendResponse(singleGetRequestContext);
    verify(spyReplyProcessor).sendSingleGetResponse(singleGetRequestContext);
    verify(spyReplyProcessor, never()).sendMultiKeyResponse(any());
    verify(spyReplyProcessor, never()).sendVeniceServerResponse(any());

    // Case 2: Test for MULTI_GET case
    spyReplyProcessor = spy(replyProcessor);
    GrpcRequestContext<MultiKeyResponse> multiGetRequestContext = mock(GrpcRequestContext.class);
    when(multiGetRequestContext.getGrpcRequestType()).thenReturn(GrpcRequestContext.GrpcRequestType.MULTI_GET);
    doNothing().when(spyReplyProcessor).sendMultiKeyResponse(multiGetRequestContext);
    spyReplyProcessor.sendResponse(multiGetRequestContext);
    verify(spyReplyProcessor).sendMultiKeyResponse(multiGetRequestContext);
    verify(spyReplyProcessor, never()).sendSingleGetResponse(any());
    verify(spyReplyProcessor, never()).sendVeniceServerResponse(any());

    when(multiGetRequestContext.getGrpcRequestType()).thenReturn(GrpcRequestContext.GrpcRequestType.COMPUTE);
    doNothing().when(spyReplyProcessor).sendMultiKeyResponse(multiGetRequestContext);
    spyReplyProcessor.sendResponse(multiGetRequestContext);
    verify(spyReplyProcessor, times(2)).sendMultiKeyResponse(multiGetRequestContext);
    verify(spyReplyProcessor, never()).sendSingleGetResponse(any());
    verify(spyReplyProcessor, never()).sendVeniceServerResponse(any());

    // Case 3: Test for LEGACY case
    spyReplyProcessor = spy(replyProcessor);
    GrpcRequestContext<VeniceServerResponse> legacyRequestContext = mock(GrpcRequestContext.class);
    when(legacyRequestContext.getGrpcRequestType()).thenReturn(GrpcRequestContext.GrpcRequestType.LEGACY);
    doNothing().when(spyReplyProcessor).sendVeniceServerResponse(legacyRequestContext);
    spyReplyProcessor.sendResponse(legacyRequestContext);
    verify(spyReplyProcessor).sendVeniceServerResponse(legacyRequestContext);
    verify(spyReplyProcessor, never()).sendSingleGetResponse(any());
    verify(spyReplyProcessor, never()).sendMultiKeyResponse(any());
  }

  @Test
  public void testSendSingleGetResponse() {
    GrpcReplyProcessor replyProcessor = new GrpcReplyProcessor();
    GrpcReplyProcessor spyReplyProcessor = spy(replyProcessor);
    doNothing().when(spyReplyProcessor).reportRequestStats(any());

    // 1. Test Case: readResponse is null
    GrpcRequestContext<SingleGetResponse> requestContext = mock(GrpcRequestContext.class);
    StreamObserver<SingleGetResponse> responseObserver = mock(StreamObserver.class);
    when(requestContext.getResponseObserver()).thenReturn(responseObserver);
    VeniceReadResponseStatus status = VeniceReadResponseStatus.TOO_MANY_REQUESTS;
    when(requestContext.getReadResponseStatus()).thenReturn(status);
    when(requestContext.getReadResponse()).thenReturn(null);
    when(requestContext.getErrorMessage()).thenReturn("Some error");

    spyReplyProcessor.sendSingleGetResponse(requestContext);

    ArgumentCaptor<SingleGetResponse> captor = ArgumentCaptor.forClass(SingleGetResponse.class);
    verify(responseObserver).onNext(captor.capture());
    SingleGetResponse capturedResponse = captor.getValue();
    assertEquals(capturedResponse.getStatusCode(), status.getCode());
    assertEquals(capturedResponse.getErrorMessage(), "Some error");
    verify(responseObserver).onCompleted();

    InOrder inOrder = inOrder(responseObserver);
    inOrder.verify(responseObserver).onNext(any());
    inOrder.verify(responseObserver).onCompleted();

    // 2. Test Case: readResponse is found
    status = VeniceReadResponseStatus.OK;
    when(requestContext.getReadResponseStatus()).thenReturn(status);
    ReadResponse readResponse = mock(ReadResponse.class);
    when(readResponse.isFound()).thenReturn(true);
    when(requestContext.getReadResponse()).thenReturn(readResponse);
    when(readResponse.getRCU()).thenReturn(1);
    when(readResponse.getResponseSchemaIdHeader()).thenReturn(1);
    when(readResponse.getCompressionStrategy()).thenReturn(CompressionStrategy.GZIP);

    ByteBuf responseBody = Unpooled.EMPTY_BUFFER;
    when(readResponse.getResponseBody()).thenReturn(responseBody);

    spyReplyProcessor.sendSingleGetResponse(requestContext);

    verify(responseObserver, times(2)).onNext(captor.capture()); // Capturing the second call
    capturedResponse = captor.getValue();
    assertEquals(capturedResponse.getStatusCode(), status.getCode());
    assertEquals(capturedResponse.getRcu(), 1);
    assertEquals(capturedResponse.getSchemaId(), 1);
    assertEquals(capturedResponse.getCompressionStrategy(), CompressionStrategy.GZIP.getValue());
    assertEquals(capturedResponse.getContentLength(), 0);
    assertEquals(capturedResponse.getValue(), ByteString.EMPTY);
    verify(responseObserver, times(2)).onCompleted();

    inOrder.verify(responseObserver).onNext(any());
    inOrder.verify(responseObserver).onCompleted();

    // 3. Test Case: readResponse is not found
    status = VeniceReadResponseStatus.KEY_NOT_FOUND;
    when(requestContext.getReadResponseStatus()).thenReturn(status);
    when(readResponse.isFound()).thenReturn(false);
    when(readResponse.getRCU()).thenReturn(5);
    when(readResponse.getResponseSchemaIdHeader()).thenReturn(-1);
    spyReplyProcessor.sendSingleGetResponse(requestContext);

    verify(responseObserver, times(3)).onNext(captor.capture()); // Capturing the third call
    capturedResponse = captor.getValue();
    assertEquals(capturedResponse.getStatusCode(), status.getCode());
    assertEquals(capturedResponse.getRcu(), 5);
    assertEquals(capturedResponse.getErrorMessage(), "Key not found");
    assertEquals(capturedResponse.getContentLength(), 0);
    verify(responseObserver, times(3)).onCompleted();
    inOrder.verify(responseObserver).onNext(any());
    inOrder.verify(responseObserver).onCompleted();

    verify(spyReplyProcessor, times(3)).reportRequestStats(requestContext);
  }

  @Test
  public void testSendVeniceServerResponse() {
    GrpcReplyProcessor replyProcessor = new GrpcReplyProcessor();
    GrpcReplyProcessor spyReplyProcessor = spy(replyProcessor);
    doNothing().when(spyReplyProcessor).reportRequestStats(any());

    GrpcRequestContext<VeniceServerResponse> requestContext = mock(GrpcRequestContext.class);
    StreamObserver<VeniceServerResponse> responseObserver = mock(StreamObserver.class);

    // 1. Test Case: readResponse is null
    when(requestContext.getResponseObserver()).thenReturn(responseObserver);
    VeniceReadResponseStatus status = VeniceReadResponseStatus.BAD_REQUEST;
    when(requestContext.getReadResponseStatus()).thenReturn(status);
    when(requestContext.getReadResponse()).thenReturn(null);
    when(requestContext.getErrorMessage()).thenReturn("Null read response");

    spyReplyProcessor.sendVeniceServerResponse(requestContext);

    ArgumentCaptor<VeniceServerResponse> captor = ArgumentCaptor.forClass(VeniceServerResponse.class);
    verify(responseObserver).onNext(captor.capture());
    VeniceServerResponse capturedResponse = captor.getValue();
    assertEquals(capturedResponse.getErrorCode(), status.getCode());
    assertEquals(capturedResponse.getErrorMessage(), "Null read response");
    verify(responseObserver).onCompleted();

    // 2. Test Case: readResponse is found
    ReadResponse readResponse = mock(ReadResponse.class);
    when(readResponse.getResponseBody()).thenReturn(Unpooled.EMPTY_BUFFER);
    when(requestContext.getReadResponse()).thenReturn(readResponse);
    status = VeniceReadResponseStatus.OK;
    when(requestContext.getReadResponseStatus()).thenReturn(status);
    when(requestContext.getErrorMessage()).thenReturn(null);
    when(readResponse.isFound()).thenReturn(true);
    when(requestContext.getReadResponse()).thenReturn(readResponse);
    when(readResponse.getRCU()).thenReturn(120);
    when(readResponse.getCompressionStrategy()).thenReturn(CompressionStrategy.GZIP);
    when(readResponse.isStreamingResponse()).thenReturn(true);
    when(readResponse.getResponseSchemaIdHeader()).thenReturn(2);

    spyReplyProcessor.sendVeniceServerResponse(requestContext);

    verify(responseObserver, times(2)).onNext(captor.capture()); // Capturing the second call
    capturedResponse = captor.getValue();
    assertEquals(capturedResponse.getErrorCode(), status.getCode());
    assertEquals(capturedResponse.getResponseRCU(), 120);
    assertEquals(capturedResponse.getCompressionStrategy(), CompressionStrategy.GZIP.getValue());
    assertEquals(capturedResponse.getSchemaId(), 2);
    assertTrue(capturedResponse.getIsStreamingResponse());
    assertEquals(capturedResponse.getData(), ByteString.EMPTY);
    verify(responseObserver, times(2)).onCompleted();

    // 3. Test Case: readResponse is not found
    when(readResponse.isFound()).thenReturn(false);
    status = VeniceReadResponseStatus.KEY_NOT_FOUND;
    when(requestContext.getReadResponseStatus()).thenReturn(status);

    spyReplyProcessor.sendVeniceServerResponse(requestContext);

    verify(responseObserver, times(3)).onNext(captor.capture()); // Capturing the third call
    capturedResponse = captor.getValue();
    assertEquals(capturedResponse.getErrorCode(), status.getCode());
    assertEquals(capturedResponse.getErrorMessage(), "Key not found");
    assertEquals(capturedResponse.getData(), ByteString.EMPTY);
    verify(responseObserver, times(3)).onCompleted();

    // 4. Verify synchronization on responseObserver
    InOrder inOrder = inOrder(responseObserver);
    inOrder.verify(responseObserver).onNext(any());
    inOrder.verify(responseObserver).onCompleted();

    // 5. Verify reportRequestStats is called at the end
    verify(spyReplyProcessor, times(3)).reportRequestStats(requestContext);
  }

  // @Test
  // public void testReportRequestStats() {
  // GrpcReplyProcessor processor = new GrpcReplyProcessor();
  //
  // // Mock the GrpcRequestContext and its dependencies
  // GrpcRequestContext requestContext = mock(GrpcRequestContext.class);
  // ReadResponse readResponse = mock(ReadResponse.class);
  // RequestStatsRecorder requestStatsRecorder = mock(RequestStatsRecorder.class);
  // AbstractReadResponse abstractReadResponse = mock(AbstractReadResponse.class);
  // ByteBuf responseBody = mock(ByteBuf.class);
  //
  // // Mock behavior for requestContext
  // when(requestContext.getReadResponse()).thenReturn(readResponse);
  // when(requestContext.getRequestStatsRecorder()).thenReturn(requestStatsRecorder);
  //
  // // 1. Test Case: readResponse is null
  // when(requestContext.getReadResponse()).thenReturn(null);
  //
  // processor.reportRequestStats(requestContext);
  //
  // // Verify that recorder sets read response stats to null and response size to 0
  // verify(requestStatsRecorder).setReadResponseStats(null);
  // verify(requestStatsRecorder).setResponseSize(0);
  //
  // // 2. Test Case: readResponse.isFound() is true
  // when(readResponse.isFound()).thenReturn(true);
  // when(requestContext.getReadResponse()).thenReturn(abstractReadResponse);
  // when(abstractReadResponse.getResponseBody()).thenReturn(responseBody);
  // when(responseBody.readableBytes()).thenReturn(512);
  // when(abstractReadResponse.getReadResponseStatsRecorder()).thenReturn(mock(ReadResponseStatsRecorder.class));
  //
  // processor.reportRequestStats(requestContext);
  //
  // // Verify that recorder sets the stats and the correct response size
  // verify(requestStatsRecorder, times(2)).setReadResponseStats(abstractReadResponse.getReadResponseStatsRecorder());
  // verify(requestStatsRecorder, times(2)).setResponseSize(512);
  //
  // // 3. Test Case: readResponse.isFound() is false
  // when(readResponse.isFound()).thenReturn(false);
  // processor.reportRequestStats(requestContext);
  //
  // // Verify that recorder sets the stats and the response size to 0
  // verify(requestStatsRecorder, times(3)).setReadResponseStats(abstractReadResponse.getReadResponseStatsRecorder());
  // verify(requestStatsRecorder).setResponseSize(0);
  // }
}
