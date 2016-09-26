package com.linkedin.venice.controller;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.status.StoreStatusMessage;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixJobRepository;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZkClient;


/**
 * Aggregate all of essentials resources which is required by controller in one place.
 */
public class VeniceHelixResources implements VeniceResource {
  private final HelixManager controller;
  private final HelixReadWriteStoreRepository metadataRepository;
  private final HelixRoutingDataRepository routingDataRepository;
  private final HelixJobRepository jobRepository;
  private final HelixReadWriteSchemaRepository schemaRepository;
  private final VeniceJobManager jobManager;
  private final HelixStatusMessageChannel messageChannel;
  private final VeniceControllerClusterConfig config;

  public VeniceHelixResources(String clusterName,
                              ZkClient zkClient,
                              HelixManager helixManager,
                              VeniceControllerClusterConfig config) {
    this.config = config;
    this.controller = helixManager;
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    this.metadataRepository = new HelixReadWriteStoreRepository(zkClient, adapter, clusterName);
    this.schemaRepository = new HelixReadWriteSchemaRepository(this.metadataRepository,
        zkClient, adapter, clusterName);
    this.routingDataRepository = new HelixRoutingDataRepository(helixManager);
    this.jobRepository = new HelixJobRepository(zkClient, adapter, clusterName);
    this.jobManager = new VeniceJobManager(clusterName,
        helixManager.getSessionId().hashCode(),
        this.jobRepository,
        this.metadataRepository,
        this.routingDataRepository,
        config.getOffLinejobWaitTimeInMilliseconds());
    this.messageChannel = new HelixStatusMessageChannel(helixManager, HelixStatusMessageChannel.DEFAULT_BROAD_CAST_MESSAGES_TIME_OUT);
  }

  @Override
  public void refresh() {
    clear();
    metadataRepository.refresh();
    schemaRepository.refresh();
    routingDataRepository.refresh();
    jobRepository.refresh();
    jobManager.checkAllExistingJobs();
    // After all of resource being refreshed, start accepting message again.
    messageChannel.registerHandler(StoreStatusMessage.class, jobManager);
  }

  @Override
  public void clear() {
    messageChannel.unRegisterHandler(StoreStatusMessage.class, jobManager);
    jobRepository.clear();
    metadataRepository.clear();
    schemaRepository.clear();
    routingDataRepository.clear();
  }

  public HelixReadWriteStoreRepository getMetadataRepository() {
    return metadataRepository;
  }

  public HelixReadWriteSchemaRepository getSchemaRepository() {
    return schemaRepository;
  }

  public HelixRoutingDataRepository getRoutingDataRepository() {
    return routingDataRepository;
  }

  public HelixJobRepository getJobRepository() {
    return jobRepository;
  }

  public VeniceJobManager getJobManager() {
    return jobManager;
  }

  public HelixStatusMessageChannel getMessageChannel() {
    return messageChannel;
  }

  public HelixManager getController() {
    return controller;
  }

  public VeniceControllerClusterConfig getConfig() {
    return config;
  }
}
