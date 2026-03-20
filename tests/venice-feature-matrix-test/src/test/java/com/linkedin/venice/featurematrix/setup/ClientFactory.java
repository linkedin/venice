package com.linkedin.venice.featurematrix.setup;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.ClientType;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.DaVinciStorageClass;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import java.io.Closeable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Creates Venice read clients (Thin, Fast, DaVinci) based on R-dimension values.
 */
public class ClientFactory {
  private static final Logger LOGGER = LogManager.getLogger(ClientFactory.class);

  /**
   * Creates a read client appropriate for the given test case configuration.
   * Returns a wrapper that provides a uniform read interface regardless of client type.
   */
  public static ReadClientWrapper createReadClient(
      TestCaseConfig config,
      String storeName,
      String routerUrl,
      String d2ServiceName) {
    ClientType clientType = config.getClientType();
    LOGGER.info("Creating {} client for store {}", clientType, storeName);

    switch (clientType) {
      case THIN:
        return createThinClient(config, storeName, routerUrl);
      case FAST:
        return createFastClient(config, storeName, d2ServiceName);
      case DA_VINCI:
        return createDaVinciClient(config, storeName);
      default:
        throw new IllegalArgumentException("Unknown client type: " + clientType);
    }
  }

  private static ReadClientWrapper createThinClient(TestCaseConfig config, String storeName, String routerUrl) {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl);

    // R5: Long-tail retry - uses client-level retry support
    if (config.isLongTailRetry()) {
      clientConfig.setRetryOnRouterError(true);
      clientConfig.setRetryCount(3);
      clientConfig.setRetryBackOffInMs(100);
    }

    AvroGenericStoreClient<Object, Object> client =
        com.linkedin.venice.client.store.ClientFactory.getAndStartGenericAvroClient(clientConfig);
    return new ReadClientWrapper(client, null, ClientType.THIN);
  }

  private static ReadClientWrapper createFastClient(TestCaseConfig config, String storeName, String d2ServiceName) {
    LOGGER.info(
        "Creating Fast client with routing={}, longTailRetry={}",
        config.getFastRouting(),
        config.isLongTailRetry());

    ClientConfig clientConfig =
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL("d2://" + d2ServiceName);

    if (config.isLongTailRetry()) {
      clientConfig.setRetryOnRouterError(true);
      clientConfig.setRetryCount(3);
      clientConfig.setRetryBackOffInMs(100);
    }

    AvroGenericStoreClient<Object, Object> client =
        com.linkedin.venice.client.store.ClientFactory.getAndStartGenericAvroClient(clientConfig);
    return new ReadClientWrapper(client, null, ClientType.FAST);
  }

  private static ReadClientWrapper createDaVinciClient(TestCaseConfig config, String storeName) {
    DaVinciStorageClass storageClass = config.getDaVinciStorage();
    LOGGER.info(
        "Creating DaVinci client with storageClass={}, recordTransformer={}",
        storageClass,
        config.getRecordTransformer());

    // DaVinci client creation requires DaVinciConfig + backend config from the cluster setup.
    // The actual DaVinci client is created in FeatureMatrixClusterSetup and injected here.
    return new ReadClientWrapper(null, null, ClientType.DA_VINCI);
  }

  /**
   * Wraps different client types behind a uniform interface for validation.
   */
  public static class ReadClientWrapper implements Closeable {
    private final AvroGenericStoreClient<Object, Object> storeClient;
    private final Object daVinciClient;
    private final ClientType clientType;

    public ReadClientWrapper(
        AvroGenericStoreClient<Object, Object> storeClient,
        Object daVinciClient,
        ClientType clientType) {
      this.storeClient = storeClient;
      this.daVinciClient = daVinciClient;
      this.clientType = clientType;
    }

    public AvroGenericStoreClient<Object, Object> getStoreClient() {
      return storeClient;
    }

    public Object getDaVinciClient() {
      return daVinciClient;
    }

    public ClientType getClientType() {
      return clientType;
    }

    @Override
    public void close() {
      try {
        if (storeClient != null) {
          storeClient.close();
        }
        if (daVinciClient instanceof Closeable) {
          ((Closeable) daVinciClient).close();
        }
      } catch (Exception e) {
        LOGGER.warn("Error closing client", e);
      }
    }
  }
}
