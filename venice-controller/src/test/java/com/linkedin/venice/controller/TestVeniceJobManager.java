package com.linkedin.venice.controller;

import com.linkedin.venice.controlmessage.StatusUpdateMessage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixCachedMetadataRepository;
import com.linkedin.venice.helix.HelixJobRepository;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import org.apache.helix.manager.zk.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for Venice job manager.
 */
public class TestVeniceJobManager {
  private VeniceJobManager jobManager;
  private HelixJobRepository jobRepository;
  private HelixCachedMetadataRepository metadataRepository;

  private String zkAddress;
  private ZkServerWrapper zkServerWrapper;
  private ZkClient zkClient;
  private String cluster = "jobTestCluster";

  @BeforeMethod
  public void setup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    zkClient = new ZkClient(zkAddress);
    zkClient.createPersistent("/" + cluster + "stores");
    jobRepository = new HelixJobRepository();
    jobRepository.start();
    metadataRepository = new HelixCachedMetadataRepository(zkClient, cluster);
    metadataRepository.start();
    jobManager = new VeniceJobManager(1, jobRepository, metadataRepository);
  }

  @AfterMethod
  public void cleanup() {
    jobRepository.clear();
    metadataRepository.clear();
    zkClient.deleteRecursive("/" + cluster + "stores");
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testHandleMessage()
      throws InterruptedException {
    String storeName = "ts1";
    Store store = new Store(storeName, "test", System.currentTimeMillis());
    Version version = store.increaseVersion();
    metadataRepository.addStore(store);
    jobManager.startOfflineJob(version.kafkaTopicName(), 1, 1);

    StatusUpdateMessage message =
        new StatusUpdateMessage(1, version.kafkaTopicName(), 0, "1", ExecutionStatus.STARTED);
    jobManager.handleMessage(message);
    Job job = jobRepository.getRunningJobOfTopic(version.kafkaTopicName()).get(0);
    Assert.assertEquals(jobRepository.getJobStatus(job.getJobId()), ExecutionStatus.STARTED, "Job should be started.");

    message = new StatusUpdateMessage(1, version.kafkaTopicName(), 0, "1", ExecutionStatus.COMPLETED);
    jobManager.handleMessage(message);
    //Wait ZK notification.
    Thread.sleep(1000l);
    Store updatedStore = metadataRepository.getStore(storeName);
    Assert.assertEquals(updatedStore.getCurrentVersion(), version.getNumber(),
        "Push has been done, store's current should be updated.");

    Assert.assertEquals(updatedStore.getVersions().get(0).getStatus(), VersionStatus.ACTIVE,
        "Push has been done. Version should be activated.");
    try {
      jobRepository.getJob(job.getJobId());
      Assert.fail("Job should be archived.");
    } catch (VeniceException e) {
      //expected.
    }
  }

  @Test
  public void testHandleMessageWhenTaskFailed()
      throws InterruptedException {
    String storeName = "ts1";
    Store store = new Store(storeName, "test", System.currentTimeMillis());
    Version version = store.increaseVersion();
    metadataRepository.addStore(store);
    jobManager.startOfflineJob(version.kafkaTopicName(), 1, 1);

    StatusUpdateMessage message =
        new StatusUpdateMessage(1, version.kafkaTopicName(), 0, "1", ExecutionStatus.STARTED);
    jobManager.handleMessage(message);
    Job job = jobRepository.getRunningJobOfTopic(version.kafkaTopicName()).get(0);
    Assert.assertEquals(jobRepository.getJobStatus(job.getJobId()), ExecutionStatus.STARTED, "Job should be started.");

    message = new StatusUpdateMessage(1, version.kafkaTopicName(), 0, "1", ExecutionStatus.ERROR);
    jobManager.handleMessage(message);
    //Wait ZK notification.
    Thread.sleep(1000l);
    Store updatedStore = metadataRepository.getStore(storeName);
    Assert.assertEquals(updatedStore.getCurrentVersion(), 0,
        "Push was failed. No current version is active for this store.");
    Assert.assertEquals(updatedStore.getVersions().get(0).getStatus(), VersionStatus.INACTIVE,
        "Push was failed. Version should not be activated.");

    try {
      jobRepository.getJob(job.getJobId());
      Assert.fail("Job should be archived.");
    } catch (VeniceException e) {
      //expected.
    }
  }
}
