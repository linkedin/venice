package com.linkedin.venice.blobtransfer;

import static com.linkedin.venice.client.store.ClientFactory.getTransportClient;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_PARTITION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VERSION;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClientImpl;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DvcBlobFinder discovers live DaVinci peer nodes to facilitate blob transfers necessary for bootstrapping the database
 */
public class DaVinciBlobFinder implements BlobFinder {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciBlobFinder.class);
  private static final String TYPE_BLOB_DISCOVERY = "blob_discovery";
  private static final String ERROR_DISCOVERY_MESSAGE =
      "Error finding DVC peers for blob transfer in store: %s, version: %d, partition: %d";
  private final ClientConfig clientConfig;
  private VeniceConcurrentHashMap<String, AbstractAvroStoreClient> storeToClientMap;

  public DaVinciBlobFinder(ClientConfig clientConfig) {
    this.storeToClientMap = new VeniceConcurrentHashMap<>();
    this.clientConfig = clientConfig;
  }

  /**
   * Get the store client for the given store name
   * @param storeName
   * @return the store client
   */
  AbstractAvroStoreClient getStoreClient(String storeName) {
    return storeToClientMap.computeIfAbsent(storeName, k -> {
      // update the config with respective store name
      ClientConfig storeClientConfig = ClientConfig.cloneConfig(clientConfig).setStoreName(storeName);
      AbstractAvroStoreClient storeLevelClient =
          new AvroGenericStoreClientImpl<>(getTransportClient(storeClientConfig), false, storeClientConfig);
      storeLevelClient.start();
      LOGGER.info("Started store client for store: {}", storeName);
      return storeLevelClient;
    });
  }

  @Override
  public BlobPeersDiscoveryResponse discoverBlobPeers(String storeName, int version, int partition) {
    AbstractAvroStoreClient storeClient = getStoreClient(storeName);

    String uri = buildUriForBlobDiscovery(storeName, version, partition);
    CompletableFuture<BlobPeersDiscoveryResponse> futureResponse = CompletableFuture.supplyAsync(() -> {
      try {
        byte[] responseBody = (byte[]) storeClient.getRaw(uri).get(3, TimeUnit.SECONDS);
        if (responseBody == null) {
          return handleError(
              ERROR_DISCOVERY_MESSAGE,
              storeName,
              version,
              partition,
              new VenicePeersNotFoundException(
                  "The response body is null for store: " + storeName + ", version: " + version + ", partition: "
                      + partition));
        }
        ObjectMapper mapper = ObjectMapperFactory.getInstance();
        return mapper.readValue(responseBody, BlobPeersDiscoveryResponse.class);
      } catch (Exception e) {
        return handleError(ERROR_DISCOVERY_MESSAGE, storeName, version, partition, e);
      }
    }).exceptionally(throwable -> handleError(ERROR_DISCOVERY_MESSAGE, storeName, version, partition, throwable));

    return futureResponse.join();
  }

  private String buildUriForBlobDiscovery(String storeName, int version, int partition) {
    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(NAME, storeName));
    queryParams.add(new BasicNameValuePair(STORE_VERSION, Integer.toString(version)));
    queryParams.add(new BasicNameValuePair(STORE_PARTITION, Integer.toString(partition)));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);

    return String.format("%s?%s", TYPE_BLOB_DISCOVERY, queryString);
  }

  private BlobPeersDiscoveryResponse handleError(
      String errorMessage,
      String storeName,
      int version,
      int partition,
      Throwable throwable) {
    BlobPeersDiscoveryResponse errorResponse = new BlobPeersDiscoveryResponse();
    String errorMsg = String.format(errorMessage, storeName, version, partition);
    errorResponse.setError(true);
    errorResponse.setErrorMessage(errorMsg);
    LOGGER.error(errorMsg, throwable);
    return errorResponse;
  }

  @Override
  public void close() throws IOException {
    for (AbstractAvroStoreClient storeClient: storeToClientMap.values()) {
      storeClient.close();
    }
  }
}
