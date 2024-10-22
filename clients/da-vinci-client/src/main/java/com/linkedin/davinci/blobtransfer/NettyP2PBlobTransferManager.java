package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.exceptions.VeniceBlobTransferFileNotFoundException;
import com.linkedin.venice.exceptions.VenicePeersConnectionException;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Netty based P2P blob transfer manager implementation.
 * Upon start, it starts the blob transfer service and the client, so it can receive requests from peers to serve
 * blobs and in the meanwhile, it can make requests to other peers to fetch blobs.
 */
public class NettyP2PBlobTransferManager implements P2PBlobTransferManager<Void> {
  private static final Logger LOGGER = LogManager.getLogger(NettyP2PBlobTransferManager.class);
  // Log messages format
  private static final String NO_PEERS_FOUND_ERROR_MSG_FORMAT =
      "No peers are found for the requested blob. Store: %s Version: %d Partition: %d";
  private static final String FETCHED_BLOB_SUCCESS_MSG_FORMAT =
      "Successfully fetched blob from peer %s for store %s partition %d version %d in %d seconds";
  private static final String SKIP_FETCHED_BLOB_MSG_FORMAT =
      "Skipping peer %s for store %s partition %d version %d as one success blob transfer is already completed";
  private static final String NO_VALID_PEERS_MSG_FORMAT =
      "Failed to connect to any peer for partition %d store %s version %d, after trying all possible hosts.";
  private static final String PEER_CONNECTION_EXCEPTION_MSG_FORMAT =
      "Get error when connect to peer: %s for store: %s version: %d partition: %d. Exception: %s";
  private static final String PEER_NO_SNAPSHOT_MSG_FORMAT =
      "Peer %s does not have the requested blob for store %s version %d partition %d. Exception: %s";
  private static final String FAILED_TO_FETCH_BLOB_MSG_FORMAT =
      "Failed to fetch blob from peer %s for store %s version %d partition %d. Exception: %s";
  private static final String DELETE_PARTIALLY_DOWNLOADED_BLOBS_MSG_FORMAT =
      "Deleted partially downloaded blobs for store %s version %d partition %d. Exception: %s";

  private final P2PBlobTransferService blobTransferService;
  // netty client is responsible to make requests against other peers for blob fetching
  protected final NettyFileTransferClient nettyClient;
  // peer finder is responsible to find the peers that have the requested blob
  protected final BlobFinder peerFinder;
  private final String baseDir;

  public NettyP2PBlobTransferManager(
      P2PBlobTransferService blobTransferService,
      NettyFileTransferClient nettyClient,
      BlobFinder peerFinder) {
    this.blobTransferService = blobTransferService;
    this.nettyClient = nettyClient;
    this.peerFinder = peerFinder;
    this.baseDir = nettyClient.getBaseDir();
  }

  @Override
  public void start() throws Exception {
    blobTransferService.start();
  }

  @Override
  public CompletionStage<InputStream> get(String storeName, int version, int partition)
      throws VenicePeersNotFoundException {
    CompletableFuture<InputStream> resultFuture = new CompletableFuture<>();
    // 1. Discover peers for the requested blob
    BlobPeersDiscoveryResponse response = peerFinder.discoverBlobPeers(storeName, version, partition);
    if (response == null || response.isError() || response.getDiscoveryResult() == null
        || response.getDiscoveryResult().isEmpty()) {
      // error case 1: no peers are found for the requested blob
      String errorMsg = String.format(NO_PEERS_FOUND_ERROR_MSG_FORMAT, storeName, version, partition);
      resultFuture.completeExceptionally(new VenicePeersNotFoundException(errorMsg));
      return resultFuture;
    }

    List<String> discoverPeers = response.getDiscoveryResult();
    LOGGER
        .info("Discovered peers {} for store {} version {} partition {}", discoverPeers, storeName, version, partition);

    // 2: Process peers sequentially to fetch the blob
    processPeersSequentially(discoverPeers, storeName, version, partition, resultFuture);

    return resultFuture;
  }

  /**
   * Process the peers sequentially to fetch the blob for the given storeName and partition
   * error cases:
   * 1. [Fatal Case] If no peers info are found for the requested blob, a VenicePeersNotFoundException is thrown.
   *    In this case, blob transfer is not used for bootstrapping at all.
   * 2. If one host connect error, it will throw VenicePeersCannotConnectException then move to the next possible host.
   * 3. If the connected host does not have the requested file,
   *    a VeniceBlobTransferFileNotFoundException is thrown, and the process moves on to the next available host.
   * 4. If any unexpected exception occurs, such as InterruptedException, ExecutionException, or TimeoutException
   *    during the file/metadata transfer, a VeniceException is thrown,
   *    and the process moves on to the next possible host, and the partially downloaded blobs are deleted.
   * 5. [Fatal Case] If all peers fail to connect or have no snapshot, a VenicePeersNotFoundException is thrown,
   *    and Kafka is used for bootstrapping instead.
   *
   *  success case:
   *  1. If the blob is successfully fetched from a peer, an InputStream of the blob is returned.
   *
   * @param peers the list of peers to process
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   * @param resultFuture the future to complete with the InputStream of the blob
   * @return the future that represents the all peers of processing
   */
  private CompletableFuture<Void> processPeersSequentially(
      List<String> peers,
      String storeName,
      int version,
      int partition,
      CompletableFuture<InputStream> resultFuture) {
    Instant startTime = Instant.now();

    // Create a CompletableFuture that represents the chain of processing all peers
    CompletableFuture<Void> chainOfPeersFuture = CompletableFuture.completedFuture(null);

    // Iterate through each peer and chain the futures
    for (int currentPeerIndex = 0; currentPeerIndex < peers.size(); currentPeerIndex++) {
      final int peerIndex = currentPeerIndex;
      // Chain the next operation to the previous future
      chainOfPeersFuture = chainOfPeersFuture.thenCompose(v -> {
        String chosenHost = peers.get(peerIndex).split("_")[0];

        if (resultFuture.isDone()) {
          LOGGER.info(String.format(SKIP_FETCHED_BLOB_MSG_FORMAT, chosenHost, storeName, partition, version));
          return CompletableFuture.completedFuture(null);
        }

        // Attempt to fetch the blob from the current peer asynchronously
        LOGGER.info("Attempting to connect to host: {}", chosenHost);

        return nettyClient.get(chosenHost, storeName, version, partition)
            .toCompletableFuture()
            .thenAccept(inputStream -> {
              // Success case: Complete the future with the input stream
              LOGGER.info(
                  String.format(
                      FETCHED_BLOB_SUCCESS_MSG_FORMAT,
                      chosenHost,
                      storeName,
                      partition,
                      version,
                      Duration.between(startTime, Instant.now()).getSeconds()));
              resultFuture.complete(inputStream);
            })
            .exceptionally(ex -> {
              handlePeerFetchException(ex, chosenHost, storeName, version, partition);
              return null;
            });
      });
    }

    // error case 5: no valid peers found for the requested blob after trying all possible hosts, skip bootstrapping
    // from blob.
    return chainOfPeersFuture.thenRun(() -> {
      if (!resultFuture.isDone()) {
        String errorMessage = String.format(NO_VALID_PEERS_MSG_FORMAT, partition, storeName, version);
        LOGGER.error(errorMessage);
        resultFuture.completeExceptionally(new VenicePeersNotFoundException(errorMessage));
      }
    });
  }

  /**
   * Handle the exception thrown when fetching the blob from a peer.
   */
  private void handlePeerFetchException(Throwable ex, String chosenHost, String storeName, int version, int partition) {
    String errorMsg = null;
    if (ex.getCause() instanceof VenicePeersConnectionException) {
      // error case 2: failed to connect to the peer, move to the next possible host
      errorMsg = String
          .format(PEER_CONNECTION_EXCEPTION_MSG_FORMAT, chosenHost, storeName, version, partition, ex.getMessage());
    } else if (ex.getCause() instanceof VeniceBlobTransferFileNotFoundException) {
      // error case 3: the connected host does not have the requested file, move to the next available host
      errorMsg = String.format(PEER_NO_SNAPSHOT_MSG_FORMAT, chosenHost, storeName, version, partition, ex.getMessage());
    } else {
      // error case 4: other exceptions (InterruptedException, ExecutionException, TimeoutException) that are not
      // expected, move to the next possible host
      errorMsg =
          String.format(FAILED_TO_FETCH_BLOB_MSG_FORMAT, chosenHost, storeName, version, partition, ex.getMessage());
      String deletePartitionMsg =
          String.format(DELETE_PARTIALLY_DOWNLOADED_BLOBS_MSG_FORMAT, storeName, version, partition, ex.getMessage());
      RocksDBUtils.deletePartitionDir(baseDir, storeName, version, partition);
      LOGGER.error(deletePartitionMsg);
    }
    LOGGER.error(errorMsg);
  }

  @Override
  public void close() throws Exception {
    blobTransferService.close();
    nettyClient.close();
    peerFinder.close();
  }
}
