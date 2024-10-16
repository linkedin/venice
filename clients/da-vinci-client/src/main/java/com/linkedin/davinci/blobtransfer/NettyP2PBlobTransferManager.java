package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.exceptions.VeniceBlobTransferFileNotFoundException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VenicePeersConnectionException;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Netty based P2P blob transfer manager implementation.
 * Upon start, it starts the blob transfer service and the client, so it can receive requests from peers to serve
 * blobs and in the meanwhile, it can make requests to other peers to fetch blobs.
 */
public class NettyP2PBlobTransferManager implements P2PBlobTransferManager<Void> {
  private static final Logger LOGGER = LogManager.getLogger(NettyP2PBlobTransferManager.class);
  protected static final int MAX_RETRIES_FOR_BLOB_TRANSFER_PER_HOST = 3;
  protected static final int MAX_TIMEOUT_FOR_BLOB_TRANSFER_IN_MIN = 60;
  private final P2PBlobTransferService blobTransferService;
  // netty client is responsible to make requests against other peers for blob fetching
  protected final NettyFileTransferClient nettyClient;
  // peer finder is responsible to find the peers that have the requested blob
  protected final BlobFinder peerFinder;

  public NettyP2PBlobTransferManager(
      P2PBlobTransferService blobTransferService,
      NettyFileTransferClient nettyClient,
      BlobFinder peerFinder) {
    this.blobTransferService = blobTransferService;
    this.nettyClient = nettyClient;
    this.peerFinder = peerFinder;
  }

  @Override
  public void start() throws Exception {
    blobTransferService.start();
  }

  /**
   * Get the blobs for the given storeName and partition
   * error cases:
   * 1. [Fatal Case] If no peers info are found for the requested blob, a VenicePeersNotFoundException is thrown.
   *    In this case, blob transfer is not used for bootstrapping at all.
   * 2. If one host connect error, it will throw VenicePeersCannotConnectException and retry connecting to the peer again
   *    After MAX_RETRIES_FOR_BLOB_TRANSFER_PER_HOST times, if still failed to connect, move to the next possible host.
   * 3. If the connected host does not have the requested file,
   *    a VeniceBlobTransferFileNotFoundException is thrown, and the process moves on to the next available host.
   * 4. [Fatal Case] If any unexpected exception occurs, such as InterruptedException, ExecutionException, or TimeoutException
   *    during the file/metadata transfer, a VeniceException is thrown, and blob transfer is skipped for bootstrapping to save time.
   * 5. [Fatal Case] If all peers fail to connect or have no snapshot, a VenicePeersNotFoundException is thrown,
   *    and Kafka is used for bootstrapping instead.
   *
   *  success case:
   *  1. If the blob is successfully fetched from a peer, an InputStream of the blob is returned.
   *
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   * @return the InputStream of the blob
   * @throws VenicePeersNotFoundException
   */
  @Override
  public CompletionStage<InputStream> get(String storeName, int version, int partition)
      throws VenicePeersNotFoundException {
    // error case 1: no peers are found for the requested blob
    BlobPeersDiscoveryResponse response = peerFinder.discoverBlobPeers(storeName, version, partition);
    if (response == null || response.isError()) {
      throw new VenicePeersNotFoundException("Failed to obtain the peers for the requested blob");
    }

    List<String> discoverPeers = response.getDiscoveryResult();
    if (discoverPeers == null || discoverPeers.isEmpty()) {
      throw new VenicePeersNotFoundException("No peers found for the requested blob");
    }
    LOGGER
        .info("Discovered peers {} for store {} version {} partition {}", discoverPeers, storeName, version, partition);

    Instant startTime = Instant.now();
    for (String peer: discoverPeers) {
      String chosenHost = peer.split("_")[0];
      int retryCount = 0;
      while (retryCount < MAX_RETRIES_FOR_BLOB_TRANSFER_PER_HOST) {
        try {
          // instanceName comes as a format of <hostName>_<applicationPort>
          LOGGER.info("Attempt {} to connect to host: {}", retryCount + 1, chosenHost);
          CompletableFuture<InputStream> inputStreamFuture =
              nettyClient.get(chosenHost, storeName, version, partition).toCompletableFuture();
          InputStream inputStream = inputStreamFuture.get(MAX_TIMEOUT_FOR_BLOB_TRANSFER_IN_MIN, TimeUnit.MINUTES);
          LOGGER.info(
              "Successfully fetched blob from peer {} for store {} partition {} version {} in {} seconds",
              peer,
              storeName,
              partition,
              version,
              Duration.between(startTime, Instant.now()).getSeconds());
          return CompletableFuture.completedFuture(inputStream);
        } catch (Exception e) {
          if (e.getCause() instanceof VenicePeersConnectionException) {
            // error case 2: failed to connect to the peer,
            // solution: retry connecting to the peer again up to MAX_RETRIES_FOR_BLOB_TRANSFER_PER_HOST times
            LOGGER.warn(
                "Get error when connect to peer: {} for store {} version {} partition {}, retrying {}/{}",
                peer,
                storeName,
                version,
                partition,
                retryCount + 1,
                MAX_RETRIES_FOR_BLOB_TRANSFER_PER_HOST,
                e);
            retryCount++;
          } else if (e.getCause() instanceof VeniceBlobTransferFileNotFoundException) {
            // error case 3: the connected host does not have the requested file,
            // solution: move to next possible host
            LOGGER.warn(
                "Peer {} does not have the requested blob for store {} version {} partition {}, moving to next possible host.",
                peer,
                storeName,
                version,
                partition,
                e);
            break;
          } else {
            // error case 4:
            // other exceptions (InterruptedException, ExecutionException, TimeoutException) that are not expected,
            // solution: do not use blob transfer to bootstrap at all for saving time
            String errorMessage = String.format(
                "Failed to connect to peer %s for partition %d store %s version %d with exception. "
                    + "Skip bootstrap the partition from blob transfer.",
                peer,
                partition,
                storeName,
                version);
            LOGGER.error(errorMessage, e);
            throw new VeniceException(errorMessage, e);
          }
        }
      }

      LOGGER.warn(
          "Failed to connect to peer {} for partition {} store {} version {} after {} attempts, "
              + "moving to next possible host to bootstrap the partition.",
          peer,
          partition,
          storeName,
          version,
          retryCount);
    }

    // error case 5: no valid peers found for the requested blob after trying all possible hosts,
    // solution: do not use blob at all.
    String errorMessage = String.format(
        "Failed to connect to any peer for partition %d store %s version %d, after trying all possible hosts.",
        partition,
        storeName,
        version);
    LOGGER.warn(errorMessage);
    throw new VenicePeersNotFoundException(errorMessage);
  }

  @Override
  public void close() throws Exception {
    blobTransferService.close();
    nettyClient.close();
    peerFinder.close();
  }
}
