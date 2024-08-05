package com.linkedin.venice.blobtransfer;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_PARTITION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VERSION;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
  private final AbstractAvroStoreClient storeClient;

  public DaVinciBlobFinder(AbstractAvroStoreClient storeClient) {
    this.storeClient = storeClient;
  }

  @Override
  public BlobPeersDiscoveryResponse discoverBlobPeers(String storeName, int version, int partition) {
    String uri = buildUriForBlobDiscovery(storeName, version, partition);

    try {
      CompletableFuture<byte[]> futureResponse = storeClient.getRaw(uri);
      byte[] response = futureResponse.join();
      ObjectMapper mapper = ObjectMapperFactory.getInstance();
      return mapper.readValue(response, BlobPeersDiscoveryResponse.class);
    } catch (IOException | VeniceException e) {
      return handleError(ERROR_DISCOVERY_MESSAGE, storeName, version, partition, e);
    }
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
