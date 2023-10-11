package com.linkedin.davinci.ingestion.main;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.HttpClientTransport;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.utils.VeniceProperties;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MainIngestionRequestClientTest {
  public static final int TIMEOUT_IN_MILLIS = 60_000;

  @Test(timeOut = TIMEOUT_IN_MILLIS)
  public void testIngestionCommand() {

    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getIngestionServicePort()).thenReturn(12345);
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    when(configLoader.getVeniceServerConfig()).thenReturn(serverConfig);
    VeniceProperties combinedProperties = mock(VeniceProperties.class);
    when(configLoader.getCombinedProperties()).thenReturn(combinedProperties);
    try (MainIngestionRequestClient client = new MainIngestionRequestClient(configLoader)) {
      HttpClientTransport mockedClientTransport = Mockito.mock(HttpClientTransport.class);
      IngestionTaskReport taskReport = new IngestionTaskReport();
      taskReport.setMessage("TEST MSG");
      when(mockedClientTransport.sendRequestWithRetry(any(), any(), anyInt())).thenReturn(taskReport);
      client.setHttpClientTransport(mockedClientTransport);
      // leader promotion should return false on execution failure on forked process.
      Assert.assertFalse(client.promoteToLeader("dummyTopic", 1));
      Assert.assertFalse(client.startConsumption("dummyTopic", 1));

      HttpClientTransport mockedBadClientTransport = Mockito.mock(HttpClientTransport.class);
      client.setHttpClientTransport(mockedBadClientTransport);

      // Ingestion request command should throw exception when failing to send command to forked process.
      when(mockedBadClientTransport.sendRequestWithRetry(any(), any(), anyInt()))
          .thenThrow(new VeniceException("TEST EXCEPTION"));
    }
  }

  @Test
  public void testMainIngestionRequestClientProcessIngestionResult() {
    String topicName = "testTopic";
    int partitionId = 1;

    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getIngestionServicePort()).thenReturn(12345);
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    when(configLoader.getVeniceServerConfig()).thenReturn(serverConfig);
    VeniceProperties combinedProperties = mock(VeniceProperties.class);
    when(configLoader.getCombinedProperties()).thenReturn(combinedProperties);
    try (MainIngestionRequestClient ingestionRequestClient = new MainIngestionRequestClient(configLoader);
        HttpClientTransport mockTransport = Mockito.mock(HttpClientTransport.class)) {
      // Client should throw exception when connection is bad.
      Assert.assertThrows(() -> ingestionRequestClient.startConsumption(topicName, partitionId));
      // Client should throw exception when isolated process throws exception during execution.
      IngestionTaskReport reportWithExceptionThrow = new IngestionTaskReport();
      reportWithExceptionThrow.isPositive = false;
      reportWithExceptionThrow.exceptionThrown = true;
      when(mockTransport.sendRequestWithRetry(any(), any(), anyInt())).thenReturn(reportWithExceptionThrow);
      ingestionRequestClient.setHttpClientTransport(mockTransport);
      Assert.assertThrows(() -> ingestionRequestClient.startConsumption(topicName, partitionId));
      // Client should return false when isolated process rejects command execution.
      IngestionTaskReport reportWithNegativeResponse = new IngestionTaskReport();
      reportWithNegativeResponse.isPositive = false;
      when(mockTransport.sendRequestWithRetry(any(), any(), anyInt())).thenReturn(reportWithNegativeResponse);
      Assert.assertFalse(ingestionRequestClient.startConsumption(topicName, partitionId));
    }
  }
}
