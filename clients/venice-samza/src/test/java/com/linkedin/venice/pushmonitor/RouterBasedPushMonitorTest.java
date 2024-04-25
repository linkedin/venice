package com.linkedin.venice.pushmonitor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.routerapi.PushStatusResponse;
import com.linkedin.venice.samza.VeniceSystemFactory;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.Test;


public class RouterBasedPushMonitorTest {
  @Test
  public void testHandleError() throws IOException {
    TransportClient transportClient = mock(TransportClient.class);
    CompletableFuture<TransportClientResponse> responseFuture = new CompletableFuture<>();
    TransportClientResponse transportClientResponse = new TransportClientResponse(1, null, "testBody".getBytes());
    responseFuture.complete(transportClientResponse);
    when(transportClient.get(anyString())).thenReturn(responseFuture);

    VeniceSystemFactory factory = mock(VeniceSystemFactory.class);
    SystemProducer producer = mock(SystemProducer.class);

    // Create the monitor and task
    RouterBasedPushMonitor monitor = new RouterBasedPushMonitor(transportClient, "topic", factory, producer);
    RouterBasedPushMonitor.PushMonitorTask task = monitor.getPushMonitorTask();
    RouterBasedPushMonitor.PushMonitorTask mockTask = spy(task);
    PushStatusResponse pushStatusResponse = new PushStatusResponse();
    pushStatusResponse.setExecutionStatus(ExecutionStatus.ERROR);
    doReturn(pushStatusResponse).when(mockTask).getPushStatusResponse(any());

    // Execute the task logic
    mockTask.run();

    // Verify expected behavior
    verify(factory).endStreamReprocessingSystemProducer(producer, false);
    assertFalse(mockTask.isRunning().get()); // Task should not continue polling
    mockTask.close();
    monitor.close();
  }
}
