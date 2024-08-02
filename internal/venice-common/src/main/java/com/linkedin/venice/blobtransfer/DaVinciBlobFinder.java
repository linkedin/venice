package com.linkedin.venice.blobtransfer;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_PARTITION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VERSION;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.AvroGenericStoreClientImpl;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.RetryUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
  private final AvroGenericStoreClientImpl storeClient;

  public DaVinciBlobFinder(AvroGenericStoreClientImpl storeClient) {
    this.storeClient = storeClient;
  }

  @Override
  public BlobPeersDiscoveryResponse discoverBlobPeers(String storeName, int version, int partition) {
    String requestPath = buildUriForBlobDiscovery(storeName, version, partition);
    byte[] response = executeRequest(requestPath);

    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    BlobPeersDiscoveryResponse discoveryResponse;
    try {
      discoveryResponse = mapper.readValue(response, BlobPeersDiscoveryResponse.class);
      return discoveryResponse;
    } catch (IOException e) {
      return handleError(ERROR_DISCOVERY_MESSAGE, storeName, version, partition, e);
    }
  }

  private byte[] executeRequest(String requestPath) {
    byte[] response;
    try {
      response = RetryUtils.executeWithMaxAttempt(
          () -> ((CompletableFuture<byte[]>) storeClient.getRaw(requestPath)).get(),
          3,
          Duration.ofSeconds(5),
          Collections.singletonList(ExecutionException.class));
    } catch (Exception e) {
      throw new VeniceException("Failed to fetch blob peers from path " + requestPath, e);
    }

    if (response == null) {
      throw new VeniceException("Requested blob peers doesn't exist for request path: " + requestPath);
    }
    return response;
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
    storeClient.close();
  }
}
