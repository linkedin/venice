package com.linkedin.venice.controller;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.controlmessage.StoreStatusMessage;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixCachedMetadataRepository;
import com.linkedin.venice.helix.HelixControlMessageChannel;
import com.linkedin.venice.helix.HelixJobRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZkClient;


/**
 * Aggregate all of essentials resources which is required by controller in one place.
 */
public class VeniceHelixResources implements VeniceResource {
  private final HelixCachedMetadataRepository metadataRepository;
  private final HelixRoutingDataRepository routingDataRepository;
  private final HelixJobRepository jobRepository;
  private final VeniceJobManager jobManager;
  private final HelixControlMessageChannel messageChannel;
  private final VeniceControllerClusterConfig config;

  public VeniceHelixResources(String clusterName, ZkClient zkClient, HelixManager helixManager,
      VeniceControllerClusterConfig config) {
    this.config = config;
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    metadataRepository = new HelixCachedMetadataRepository(zkClient, adapter, clusterName);
    routingDataRepository = new HelixRoutingDataRepository(helixManager);
    jobRepository = new HelixJobRepository(zkClient, adapter, clusterName, routingDataRepository);
    jobManager = new VeniceJobManager(helixManager.getSessionId().hashCode(), jobRepository, metadataRepository);
    messageChannel = new HelixControlMessageChannel(helixManager);
  }

  @Override
  public void refresh() {
    clear();
    messageChannel.registerHandler(StoreStatusMessage.class, jobManager);
    metadataRepository.refresh();
    routingDataRepository.refresh();
    jobRepository.refresh();
    jobManager.checkAllExistingJobs();
  }

  @Override
  public void clear() {
      jobRepository.clear();
      metadataRepository.clear();
      routingDataRepository.clear();
      messageChannel.unRegisterHandler(StoreStatusMessage.class, jobManager);
  }

  public HelixCachedMetadataRepository getMetadataRepository() {
    return metadataRepository;
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

  public HelixControlMessageChannel getMessageChannel() {
    return messageChannel;
  }

  public VeniceControllerClusterConfig getConfig() {
    return config;
  }
}
