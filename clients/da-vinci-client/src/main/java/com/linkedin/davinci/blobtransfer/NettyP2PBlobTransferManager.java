package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.getThroughputPerPartition;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.exceptions.VeniceBlobTransferFileNotFoundException;
import com.linkedin.venice.exceptions.VenicePeersConnectionException;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.Utils;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
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
      "Replica: %s are not found any peers for the requested blob.";
  private static final String NO_VALID_PEERS_MSG_FORMAT =
      "Replica %s failed to connect to any peer, after trying all possible hosts.";
  private static final String FETCHED_BLOB_SUCCESS_MSG =
      "Replica {} successfully fetched blob from peer {} in {} seconds";
  private static final String PEER_CONNECTION_EXCEPTION_MSG =
      "Replica {} get error when connect to peer: {}. Exception: {}";
  private static final String PEER_NO_SNAPSHOT_MSG =
      "Replica {} peer {} does not have the requested blob. Exception: {}";
  private static final String FAILED_TO_FETCH_BLOB_MSG =
      "Replica {} failed to fetch blob from peer {}. Deleting partially downloaded blobs. Exception: {}";

  private final P2PBlobTransferService blobTransferService;
  // netty client is responsible to make requests against other peers for blob fetching
  protected final NettyFileTransferClient nettyClient;
  // blob transfer stats to record all blob transfer related stats
  protected final AggVersionedBlobTransferStats aggVersionedBlobTransferStats;
  // peer finder is responsible to find the peers that have the requested blob
  protected final BlobFinder peerFinder;
  private final String baseDir;

  public NettyP2PBlobTransferManager(
      P2PBlobTransferService blobTransferService,
      NettyFileTransferClient nettyClient,
      BlobFinder peerFinder,
      String baseDir,
      AggVersionedBlobTransferStats aggVersionedBlobTransferStats) {
    this.blobTransferService = blobTransferService;
    this.nettyClient = nettyClient;
    this.peerFinder = peerFinder;
    this.baseDir = baseDir;
    this.aggVersionedBlobTransferStats = aggVersionedBlobTransferStats;
  }

  @Override
  public void start() throws Exception {
    blobTransferService.start();
  }

  @Override
  public CompletionStage<InputStream> get(
      String storeName,
      int version,
      int partition,
      BlobTransferTableFormat tableFormat) throws VenicePeersNotFoundException {
    CompletableFuture<InputStream> resultFuture = new CompletableFuture<>();
    // 1. Discover peers for the requested blob
    BlobPeersDiscoveryResponse response = peerFinder.discoverBlobPeers(storeName, version, partition);
    if (response == null || response.isError() || response.getDiscoveryResult() == null
        || response.getDiscoveryResult().isEmpty()) {
      // error case 1: no peers are found for the requested blob
      String errorMsg = String.format(
          NO_PEERS_FOUND_ERROR_MSG_FORMAT,
          Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition));
      resultFuture.completeExceptionally(new VenicePeersNotFoundException(errorMsg));
      return resultFuture;
    }

    List<String> discoverPeers = response.getDiscoveryResult();
    List<String> connectablePeers = getConnectableHosts(discoverPeers, storeName, version, partition);

    // 2: Process peers sequentially to fetch the blob
    processPeersSequentially(connectablePeers, storeName, version, partition, tableFormat, resultFuture);

    return resultFuture;
  }

  /**
   * Process the peers sequentially to fetch the blob for the given storeName and partition
   * - Error cases:
   * - Fatal cases, skip bootstrapping from blob:
   * 1. If no peers info are found for the requested blob, a VenicePeersNotFoundException is thrown.
   *    In this case, blob transfer is not used for bootstrapping at all.
   * 2. If all peers fail to connect or have no snapshot, a VenicePeersNotFoundException is thrown,
   *    and Kafka is used for bootstrapping instead.
   *
   * - Non-fatal cases, move to the next possible host:
   * 3. If one host connect error, it will throw VenicePeersCannotConnectException then move to the next possible host.
   * 4. If the connected host does not have the requested file,
   *    a VeniceBlobTransferFileNotFoundException is thrown, and the process moves on to the next available host.
   * 5. If any unexpected exception occurs, such as InterruptedException, ExecutionException, or TimeoutException
   *    during the file/metadata transfer, a VeniceException is thrown,
   *    and the process moves on to the next possible host, and the partially downloaded blobs are deleted.
   *
   *  - Success case:
   *  1. If the blob is successfully fetched from a peer, an InputStream of the blob is returned.
   *
   * @param uniqueConnectablePeers the set of peers to process
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   * @param tableFormat the needed table format
   * @param resultFuture the future to complete with the InputStream of the blob
   */
  private void processPeersSequentially(
      List<String> uniqueConnectablePeers,
      String storeName,
      int version,
      int partition,
      BlobTransferTableFormat tableFormat,
      CompletableFuture<InputStream> resultFuture) {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);
    Instant startTime = Instant.now();

    // Create a CompletableFuture that represents the chain of processing all peers
    CompletableFuture<Void> chainOfPeersFuture = CompletableFuture.completedFuture(null);

    // Iterate through each peer and chain the futures
    for (String chosenHost: uniqueConnectablePeers) {
      // Chain the next operation to the previous future
      chainOfPeersFuture = chainOfPeersFuture.thenCompose(v -> {

        if (resultFuture.isDone()) {
          // If the result future is already completed, skip the current peer
          return CompletableFuture.completedFuture(null);
        }

        // Attempt to fetch the blob from the current peer asynchronously
        LOGGER.info(
            "Attempting to connect to host: {} for store {} version {} partition {} table format {}",
            chosenHost,
            storeName,
            version,
            partition,
            tableFormat);

        return nettyClient.get(chosenHost, storeName, version, partition, tableFormat)
            .toCompletableFuture()
            .thenAccept(inputStream -> {
              // Success case: Complete the future with the input stream
              long transferTime = Duration.between(startTime, Instant.now()).getSeconds();
              LOGGER.info(FETCHED_BLOB_SUCCESS_MSG, replicaId, chosenHost, transferTime);
              resultFuture.complete(inputStream);
              // Updating the blob transfer stats with the transfer time and throughput
              updateBlobTransferFileReceiveStats(transferTime, storeName, version, partition);
            })
            .exceptionally(ex -> {
              handlePeerFetchException(ex, chosenHost, storeName, version, partition, replicaId);
              return null;
            });
      });
    }

    // error case 2: no valid peers found for the requested blob after trying all possible hosts, skip bootstrapping
    // from blob.
    chainOfPeersFuture.thenRun(() -> {
      if (!resultFuture.isDone()) {
        resultFuture.completeExceptionally(
            new VenicePeersNotFoundException(String.format(NO_VALID_PEERS_MSG_FORMAT, replicaId)));
      }
    });
  }

  /**
   * Handle the exception thrown when fetching the blob from a peer.
   */
  private void handlePeerFetchException(
      Throwable ex,
      String chosenHost,
      String storeName,
      int version,
      int partition,
      String replicaId) {
    if (ex.getCause() instanceof VenicePeersConnectionException) {
      // error case 3: failed to connect to the peer, move to the next possible host
      LOGGER.error(PEER_CONNECTION_EXCEPTION_MSG, replicaId, chosenHost, ex.getMessage());
    } else if (ex.getCause() instanceof VeniceBlobTransferFileNotFoundException) {
      // error case 4: the connected host does not have the requested file, move to the next available host
      LOGGER.error(PEER_NO_SNAPSHOT_MSG, replicaId, chosenHost, ex.getMessage());
    } else {
      // error case 5: other exceptions (InterruptedException, ExecutionException, TimeoutException) that are not
      // expected, move to the next possible host
      RocksDBUtils.deletePartitionDir(baseDir, storeName, version, partition);
      LOGGER.error(FAILED_TO_FETCH_BLOB_MSG, replicaId, chosenHost, ex.getMessage());
    }
  }

  @Override
  public void close() throws Exception {
    blobTransferService.close();
    nettyClient.close();
    peerFinder.close();
  }

  /**
   * Get the blob transfer stats
   * @return the blob transfer stats
   */
  @Override
  public AggVersionedBlobTransferStats getAggVersionedBlobTransferStats() {
    return aggVersionedBlobTransferStats;
  }

  /**
   * Basd on the transfer time, store name, version, and partition, update the blob transfer file receive stats
   * @param transferTime the transfer time in seconds
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   */
  private void updateBlobTransferFileReceiveStats(double transferTime, String storeName, int version, int partition) {
    try {
      double throughput = getThroughputPerPartition(baseDir, storeName, version, partition, transferTime);
      aggVersionedBlobTransferStats.recordBlobTransferTimeInSec(storeName, version, transferTime);
      aggVersionedBlobTransferStats.recordBlobTransferFileReceiveThroughput(storeName, version, throughput);
    } catch (Exception e) {
      LOGGER.error(
          "Failed to update updateBlobTransferFileReceiveStats for store {} version {} partition {}",
          storeName,
          version,
          partition,
          e);
    }
  }

  /**
   * Get the connectable hosts for the given storeName, version, and partition
   * @param discoverPeers the list of discovered peers
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   * @return the set of unique connectable hosts
   */
  private List<String> getConnectableHosts(List<String> discoverPeers, String storeName, int version, int partition) {
    // Extract unique hosts from the discovered peers
    Set<String> uniquePeers = discoverPeers.stream().map(peer -> peer.split("_")[0]).collect(Collectors.toSet());

    LOGGER.info(
        "Discovered {} unique peers store {} version {} partition {}, peers are {}",
        uniquePeers.size(),
        storeName,
        version,
        partition,
        uniquePeers);

    // Get the connectable hosts for this store, version, and partition
    Set<String> connectablePeers =
        nettyClient.getConnectableHosts((HashSet<String>) uniquePeers, storeName, version, partition);

    LOGGER.info(
        "Total {} unique connectable peers for store {} version {} partition {}, peers are {}",
        connectablePeers.size(),
        storeName,
        version,
        partition,
        connectablePeers);

    // Change to list and shuffle the list
    List<String> connectablePeersList = connectablePeers.stream().collect(Collectors.toList());
    Collections.shuffle(connectablePeersList);

    return connectablePeersList;
  }
}
