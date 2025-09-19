package com.linkedin.venice.controller;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.makeStoreHybrid;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.IntegrationTestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestTopicRequestOnHybridDelete {
  private static final Logger LOGGER = LogManager.getLogger(TestTopicRequestOnHybridDelete.class);
  private VeniceClusterWrapper venice;

  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  protected boolean enableHelixMessagingChannel() {
    return false;
  }

  @BeforeClass
  public void setUp() {
    Properties properties = new Properties();
    properties.put(ConfigKeys.ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, Boolean.toString(enableHelixMessagingChannel()));
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(1)
        .extraProperties(properties)
        .build();
    venice = ServiceFactory.getVeniceCluster(options);
    IntegrationTestUtils.verifyParticipantMessageStoreSetup(
        venice.getLeaderVeniceController().getVeniceHelixAdmin(),
        venice.getClusterName(),
        new PubSubTopicRepository());
  }

  @AfterClass
  public void cleanUp() {
    IOUtils.closeQuietly(venice);
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void serverRestartOnHybridStoreKeepsVersionOnline() {
    AvroGenericStoreClient client = null;
    ControllerClient controllerClient = null;
    SystemProducer veniceProducer = null;
    try {
      controllerClient = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL());
      final ControllerClient finalControllerClient = controllerClient;

      String storeName = Utils.getUniqueString("hybrid-store");
      venice.getNewStore(storeName);
      StoreInfo storeInfo = TestUtils.assertCommand(finalControllerClient.getStore(storeName)).getStore();
      makeStoreHybrid(venice, storeName, 100L, 5L);
      controllerClient.emptyPush(storeName, Utils.getUniqueString("push-id"), 1L);

      // write streaming records
      veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM);
      for (int i = 1; i <= 10; i++) {
        sendStreamingRecord(veniceProducer, storeName, i);
      }
      veniceProducer.stop();

      client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
      final AvroGenericStoreClient finalClient = client;
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        try {
          assertEquals(finalClient.get("9").get(), new Utf8("stream_9"));
        } catch (Exception e) {
          fail("Got an exception while querying Venice!", e);
          // throw new VeniceException(e);
        }
      });

      controllerClient.emptyPush(storeName, Utils.getUniqueString("push-id"), 1L);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = finalControllerClient.getStore(storeName);
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 2);
      });

      try {
        venice.getNewStore(storeName);
        Assert.fail("Must not be able to create a store that already exists");
      } catch (AssertionError e) {
        Assert.assertTrue(e.getMessage().contains("already exists"));
        // expected
      }

      // disable store
      controllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));

      // delete store should return immediately without any error.
      LOGGER.info("TEST PROGRESS LOG: About to submit deleteStore request.");
      assertCommand(finalControllerClient.deleteStore(storeName));
      LOGGER.info("TEST PROGRESS LOG: deleteStore submitted successfully.");

      TopicManager topicManager = venice.getLeaderVeniceController().getVeniceAdmin().getTopicManager();
      try {
        topicManager
            .ensureTopicIsDeletedAndBlock(pubSubTopicRepository.getTopic(Utils.getRealTimeTopicName(storeInfo)));
        LOGGER.info("TEST PROGRESS LOG: TM::ensureTopicIsDeletedAndBlock for RT topic finished successfully.");
      } catch (VeniceException e) {
        fail("Exception during topic deletion " + e);
      }

      Assert.assertTrue(
          finalControllerClient.getStore(storeName).isError(),
          "Getting the store should return with an error since it has been deleted!");
      LOGGER.info("TEST PROGRESS LOG: getStore confirmed the store doesn't exist anymore.");

      /**
       * Wait for resource cleanup before the store re-creation.
       */
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          true,
          () -> assertCommand(finalControllerClient.checkResourceCleanupForStoreCreation(storeName)));

      LOGGER.info("TEST PROGRESS LOG: checkResourceCleanupForStoreCreation succeeded!");

      // recreate store
      venice.getNewStore(storeName);
      LOGGER.info("TEST PROGRESS LOG: submitted store re-creation.");

      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getVersions().size(), 0);
      makeStoreHybrid(venice, storeName, 100L, 5L);
      controllerClient.emptyPush(storeName, Utils.getUniqueString("push-id3"), 1L);

      int expectedCurrentVersion = 3;

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = finalControllerClient.getStore(storeName);
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), expectedCurrentVersion);
      });

      // write more streaming records
      veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM);
      for (int i = 11; i <= 20; i++) {
        sendStreamingRecord(veniceProducer, storeName, i);
      }

      // Ugh... I don't like doing this. Feels sketchy.
      venice.refreshAllRouterMetaData();

      // verify new records appear
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        try {
          assertEquals(finalClient.get("19").get(), new Utf8("stream_19"));
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      StoreResponse storeResponseBeforeRestart = controllerClient.getStore(storeName);
      List<Version> beforeRestartVersions = storeResponseBeforeRestart.getStore().getVersions();

      boolean foundCurrent = false;
      for (Version version: beforeRestartVersions) {
        if (version.getNumber() == expectedCurrentVersion) {
          Assert.assertEquals(version.getStatus(), VersionStatus.ONLINE);
          foundCurrent = true;
        }
      }
      Assert.assertTrue(foundCurrent, "Store's versions must contain the current version " + expectedCurrentVersion);

      // TODO restart a storage node, and verify version is still online.

    } finally {
      IOUtils.closeQuietly(client);
      IOUtils.closeQuietly(controllerClient);
      if (veniceProducer != null) {
        // notice that D2 clients can be closed for more than one time
        veniceProducer.stop();
      }
    }
  }

  // TODO this test passes, but the same workflow should be tested in a multi-colo simulation
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void deleteStoreAfterStartedPushAllowsNewPush() {
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL());
    try (TopicManagerRepository topicManagerRepository = IntegrationTestPushUtils.getTopicManagerRepo(
        PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
        100,
        0l,
        venice.getPubSubBrokerWrapper(),
        pubSubTopicRepository)) {

      TopicManager topicManager = topicManagerRepository.getLocalTopicManager();

      String storeName = Utils.getUniqueString("hybrid-store");
      venice.getNewStore(storeName);
      makeStoreHybrid(venice, storeName, 100L, 5L);

      // new version, but don't write records
      VersionCreationResponse startedVersion = controllerClient.requestTopicForWrites(
          storeName,
          1L,
          Version.PushType.BATCH,
          Utils.getUniqueString("pushId"),
          true,
          true,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      assertCommand(startedVersion);
      Assert.assertEquals(
          controllerClient.queryJobStatus(startedVersion.getKafkaTopic()).getStatus(),
          ExecutionStatus.STARTED.toString());
      Assert.assertTrue(
          topicManager
              .containsTopicAndAllPartitionsAreOnline(pubSubTopicRepository.getTopic(startedVersion.getKafkaTopic())));

      // disable store
      controllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      // delete versions
      controllerClient.deleteAllVersions(storeName);
      // enable store
      controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(true).setEnableWrites(true));

      controllerClient.emptyPush(storeName, Utils.getUniqueString("push-id3"), 1L);

      int expectedCurrentVersion = 2;

      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), expectedCurrentVersion);
      });
    }
  }
}
