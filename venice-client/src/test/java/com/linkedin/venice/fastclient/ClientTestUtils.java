package com.linkedin.venice.fastclient;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.utils.SslUtils;
import java.util.HashMap;
import java.util.Map;


public class ClientTestUtils {

  public static Client getR2Client() {
    TransportClientFactory transportClientFactory = new HttpClientFactory.Builder().build();
    SSLEngineComponentFactory sslEngineComponentFactory = SslUtils.getLocalSslFactory();
    final Map<String, Object> properties = new HashMap();
    properties.put(HttpClientFactory.HTTP_SSL_CONTEXT, sslEngineComponentFactory.getSSLContext());
    properties.put(HttpClientFactory.HTTP_SSL_PARAMS, sslEngineComponentFactory.getSSLParameters());

    return new TransportClientAdapter(transportClientFactory.getClient(properties));
  }
}
