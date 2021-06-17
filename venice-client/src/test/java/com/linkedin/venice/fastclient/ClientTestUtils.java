package com.linkedin.venice.fastclient;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.common.HttpProtocolVersion;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.utils.SslUtils;
import java.util.HashMap;
import java.util.Map;


public class ClientTestUtils {

  public static Client getR2Client() {
    return getR2Client(false);
  }
  public static Client getR2Client(boolean useHttp2) {
    /**
     * 'setUsePipelineV2' is required to force http2 for all types of request.
     */
    TransportClientFactory transportClientFactory = new HttpClientFactory.Builder().setUsePipelineV2(true).build();
    SSLEngineComponentFactory sslEngineComponentFactory = SslUtils.getLocalSslFactory();
    final Map<String, Object> properties = new HashMap();
    properties.put(HttpClientFactory.HTTP_SSL_CONTEXT, sslEngineComponentFactory.getSSLContext());
    properties.put(HttpClientFactory.HTTP_SSL_PARAMS, sslEngineComponentFactory.getSSLParameters());
    if (useHttp2) {
      properties.put(HttpClientFactory.HTTP_PROTOCOL_VERSION, HttpProtocolVersion.HTTP_2.toString());
    }

    return new TransportClientAdapter(transportClientFactory.getClient(properties));
  }
}
