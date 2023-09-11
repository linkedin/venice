package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.transport.TransportClient;
import java.util.HashMap;
import java.util.Map;


public class ClientFactoryTestUtils {
  public static void setUnitTestMode() {
    ClientFactory.setUnitTestMode();
  }

  public static void resetUnitTestMode() {
    ClientFactory.resetUnitTestMode();
  }

  private static volatile Map<ClientConfig, TransportClient> clientConfigTransportClientMap = null;

  public static void registerTransportClient(ClientConfig config, TransportClient transportClient) {
    if (clientConfigTransportClientMap == null) {
      clientConfigTransportClientMap = new HashMap<>();
      ClientFactory.setTransportClientProvider(clientConfig -> clientConfigTransportClientMap.get(clientConfig));
    }

    clientConfigTransportClientMap.put(config, transportClient);
  }

  public static void unregisterTransportClient(ClientConfig config) {
    if (clientConfigTransportClientMap == null) {
      return;
    }

    clientConfigTransportClientMap.remove(config);
    if (clientConfigTransportClientMap.isEmpty()) {
      resetTransportClientProvider();
    }
  }

  public static void resetTransportClientProvider() {
    clientConfigTransportClientMap = null;
    ClientFactory.setTransportClientProvider(null);
  }
}
