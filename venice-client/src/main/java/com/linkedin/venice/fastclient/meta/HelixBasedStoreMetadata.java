package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.OnlineInstanceFinderDelegator;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.PartitionStatusOnlineInstanceFinder;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.utils.HelixUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * A temp solution to unblock perf test, and before onboarding any customers, we need to adopt a system-store
 * based solution and deprecate this one.
 */
public class HelixBasedStoreMetadata extends AbstractStoreMetadata {
  private final HelixReadOnlyStoreRepository storeRepository;
  private final HelixReadOnlySchemaRepository schemaRepository;
  private final OnlineInstanceFinder onlineInstanceFinder;
  private final VenicePartitionFinder partitionFinder;

  private final ZkClient zkClient;
  private final SafeHelixManager manager;
  private final HelixExternalViewRepository routingDataRepository;
  private final PartitionStatusOnlineInstanceFinder partitionStatusOnlineInstanceFinder;

  public HelixBasedStoreMetadata(ClientConfig clientConfig) {
    super(clientConfig);
    String zkAddress = clientConfig.getVeniceZKAddress();
    String clusterName = clientConfig.getClusterName();
    this.zkClient = new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    HelixAdapterSerializer adapterSerializer = new HelixAdapterSerializer();
    // For Store metadata
    this.storeRepository = new HelixReadOnlyStoreRepository(zkClient, adapterSerializer, clusterName, 3, 1000);
    this.storeRepository.refresh();
    // For Schema retrieval
    this.schemaRepository = new HelixReadOnlySchemaRepository(storeRepository, zkClient, adapterSerializer, clusterName, 3, 1000);
    this.schemaRepository.refresh();
    // For finding out partition assignment
    this.manager = new SafeHelixManager(new ZKHelixManager(clusterName, null, InstanceType.SPECTATOR, zkAddress));
    HelixUtils.connectHelixManager(manager, 30, 1);
    this.routingDataRepository = new HelixExternalViewRepository(manager);
    this.routingDataRepository.refresh();
    this.partitionStatusOnlineInstanceFinder = new PartitionStatusOnlineInstanceFinder(
        storeRepository,
        new VeniceOfflinePushMonitorAccessor(clusterName, zkClient, adapterSerializer),
        routingDataRepository);
    this.partitionStatusOnlineInstanceFinder.refresh();
    this.onlineInstanceFinder =
        new OnlineInstanceFinderDelegator(storeRepository, routingDataRepository,
            partitionStatusOnlineInstanceFinder, false); // Disable customized view

    // For partitioning
    this.partitionFinder = new VenicePartitionFinder(routingDataRepository, storeRepository);
  }

  @Override
  public int getCurrentStoreVersion() {
    Store store = storeRepository.getStoreOrThrow(getStoreName());
    return store.getCurrentVersion();
  }

  @Override
  public int getPartitionId(int version, ByteBuffer key) {
    return partitionFinder.findPartitionNumber(new RouterKey(key),
        partitionFinder.getNumPartitions(Version.composeKafkaTopic(getStoreName(), version)),
        getStoreName(), version);
  }

  @Override
  public List<String> getReplicas(int version, int partitionId) {
    return onlineInstanceFinder.getReadyToServeInstances(Version.composeKafkaTopic(getStoreName(), version), partitionId)
        .stream().map(instance -> instance.getUrl(true)).collect(Collectors.toList());
  }

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public Schema getKeySchema() {
    return schemaRepository.getKeySchema(getStoreName()).getSchema();
  }

  @Override
  public Schema getValueSchema(int id) {
    return schemaRepository.getValueSchema(getStoreName(), id).getSchema();
  }

  @Override
  public int getValueSchemaId(Schema schema) {
    return schemaRepository.getValueSchemaId(getStoreName(), schema.toString());
  }

  @Override
  public Schema getLatestValueSchema() {
    return schemaRepository.getLatestValueSchema(getStoreName()).getSchema();
  }

  @Override
  public Integer getLatestValueSchemaId() {
    return schemaRepository.getLatestValueSchema(getStoreName()).getId();
  }

  @Override
  public void close() throws IOException {
    this.partitionStatusOnlineInstanceFinder.clear();
    this.routingDataRepository.clear();
    this.schemaRepository.clear();
    this.storeRepository.clear();
    this.manager.disconnect();
    this.zkClient.close();
  }
}
