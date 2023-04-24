package com.linkedin.davinci.ingestion.isolated;

import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_REQUEST_TIMEOUT_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SSL_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.HttpClientTransport;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IsolatedIngestionRequestClientTest {
  @Test
  public void testClientReportStatus() {

    VeniceProperties properties = mock(VeniceProperties.class);
    when(properties.getInt(SERVER_INGESTION_ISOLATION_REQUEST_TIMEOUT_SECONDS, 120)).thenReturn(120);
    when(properties.getBoolean(SERVER_INGESTION_ISOLATION_SSL_ENABLED, false)).thenReturn(false);
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    when(veniceServerConfig.getIngestionApplicationPort()).thenReturn(27105);
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    when(configLoader.getVeniceServerConfig()).thenReturn(veniceServerConfig);
    when(configLoader.getCombinedProperties()).thenReturn(properties);
    HttpClientTransport transport = mock(HttpClientTransport.class);
    IsolatedIngestionRequestClient client = new IsolatedIngestionRequestClient(configLoader);
    client.setHttpClientTransport(transport);
    IngestionTaskReport report = new IngestionTaskReport();
    report.topicName = "topic";
    report.partitionId = 1;
    report.reportType = 0;
    Assert.assertTrue(client.reportIngestionStatus(report));
    when(transport.sendRequest(any(), any())).thenThrow(new VeniceException("test"));
    Assert.assertFalse(client.reportIngestionStatus(report));
  }
}
