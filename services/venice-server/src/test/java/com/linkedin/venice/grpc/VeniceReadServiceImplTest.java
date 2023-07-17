package com.linkedin.venice.grpc;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.listener.StorageReadRequestsHandler;
import com.linkedin.venice.listener.grpc.VeniceReadServiceImpl;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class VeniceReadServiceImplTest {
  private StorageReadRequestsHandler mockStorageReadRequestsHandler;
  private StreamObserver<VeniceServerResponse> mockResponseObserver;

  private VeniceReadServiceImpl veniceReadService;

  @BeforeTest
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    mockStorageReadRequestsHandler = mock(StorageReadRequestsHandler.class);
    mockResponseObserver = mock(StreamObserver.class);
    veniceReadService = new VeniceReadServiceImpl(mockStorageReadRequestsHandler);
  }

  @Test
  public void testGet() {
    VeniceClientRequest request = VeniceClientRequest.newBuilder().setResourceName("test_v1").setPartition(1).build();

    StorageResponseObject storageResponse = new StorageResponseObject();
    ValueRecord valueRecord = ValueRecord.create(1, Unpooled.copiedBuffer("test_name_1", StandardCharsets.UTF_8));
    storageResponse.setValueRecord(valueRecord);

    when(mockStorageReadRequestsHandler.handleSingleGetGrpcRequest(any(GetRouterRequest.class)))
        .thenReturn(storageResponse);

    veniceReadService.get(request, mockResponseObserver);

    verify(mockResponseObserver, times(1)).onNext(any(VeniceServerResponse.class));
    verify(mockResponseObserver, times(1)).onCompleted();

    // reset mock counter
    reset(mockResponseObserver);
  }

  @Test
  public void testBatchGet() {
    veniceReadService = new VeniceReadServiceImpl(mockStorageReadRequestsHandler);

    VeniceClientRequest request = VeniceClientRequest.newBuilder().setResourceName("test_v1").build();

    ReadResponse readResponse = new MultiGetResponseWrapper(10);

    when(mockStorageReadRequestsHandler.handleMultiGetGrpcRequest(any(MultiGetRouterRequestWrapper.class)))
        .thenReturn(readResponse);

    veniceReadService.batchGet(request, mockResponseObserver);

    verify(mockResponseObserver, times(1)).onNext(any(VeniceServerResponse.class));
    verify(mockResponseObserver, times(1)).onCompleted();

    reset(mockResponseObserver);
  }
}
