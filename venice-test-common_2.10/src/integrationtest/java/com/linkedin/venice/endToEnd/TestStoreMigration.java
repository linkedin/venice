package com.linkedin.venice.endToEnd;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestStoreMigration {
  /**
   * Unfortunately, this test class is quite flaky and also takes a very long time, therefore, it can be necessary to disable it.
   */
  private static final boolean ENABLED = true;

  private static final Logger logger = Logger.getLogger(TestStoreMigration.class);
  private static final int MAX_RETRY = 5;
  private static final int NUM_OF_CONTROLLERS = 3; // number of controller cannot be more than 3
  private static final int NUM_OF_SERVERS = 1;
  private static final int NUM_OF_ROUTERS = 1;
  private static final long GET_MASTER_CONTROLLER_TIMEOUT = 20 * Time.MS_PER_SECOND;
  private static final long EXPECTED_STORE_MIGRATION_COMPLETION_TIME_SEC = 60;
  private static final boolean[] ABORT_MIGRATION_PROMPTS_OVERRIDE = {false, true, true};

  private interface  StoreMigrationAssertionRunner {
    void runAssertions(String srcClusterName, String destClusterName, String srcRouterUrl, String destRouterUrl,
        String kafkaAddr, String randomControllerUrl, Admin randomVeniceAdmin, Admin srcAdmin, Admin destAdmin,
        ControllerClient srcControllerClient, ControllerClient destControllerClient, String store1, String store0);
  }

  private interface MultiDatacenterStoreMigrationAssertionRunner {
    void runAssertions(VeniceTwoLayerMultiColoMultiClusterWrapper twoLayerMultiColoMultiClusterWrapper,
        List<VeniceMultiClusterWrapper> multiClusters, String srcClusterName, String destClusterName,
        String parentControllerUrl, Admin srcParentAdmin, Admin destParentAdmin, ControllerClient srcControllerClient,
        ControllerClient destControllerClient);
  }

  /**
   * This function is a scaffolding for testing store migration-related scenarios that can happen
   * where the stores involved are in the same and in different clusters.
   */
  private void testWithSameAndDifferentControllers(StoreMigrationAssertionRunner runner) throws InterruptedException{
    boolean sharedControllerTested = false;
    boolean diffControllerTested = false;
    int attempt = 0;

    Admin randomVeniceAdmin = null, srcAdmin = null, destAdmin = null;

    try (VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(
        3,
        NUM_OF_CONTROLLERS,
        NUM_OF_SERVERS,
        NUM_OF_ROUTERS)
    ) {
      String[] clusterNames = multiClusterWrapper.getClusterNames();
      Assert.assertTrue(clusterNames.length >= 2, "For this test there must be at least two clusters");

      Arrays.sort(clusterNames);
      String srcClusterName = clusterNames[0];  // venice-cluster0-XXXXXXXXX
      String destClusterName = clusterNames[1]; // venice-cluster1-XXXXXXXXX

      while (!(sharedControllerTested && diffControllerTested) && ++attempt < MAX_RETRY) {
        logger.info("Beginning of attempt #" + attempt + " / " + MAX_RETRY +
            ". sharedControllerTested = " + sharedControllerTested +
            ", diffControllerTested = " + diffControllerTested);
        boolean controllersOfEachClusterAreCurrentlyShared =
            multiClusterWrapper.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT)
                .getControllerUrl()
                .equals(multiClusterWrapper.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getControllerUrl());

        if (controllersOfEachClusterAreCurrentlyShared) {
          if (sharedControllerTested) {
            // TODO: Make this deterministic...
            if (multiClusterWrapper.getControllers().size() > 2) {
              logger.info("We tested the sharedControllerTested case already, so we'll attempt to gradually alter the number of controllers so that we can hit the other case");
              multiClusterWrapper.removeOneController();
            } else {
              logger.info("We tested the sharedControllerTested case already, so we'll attempt to bounce controllers so that we can hit the other case");
              multiClusterWrapper.restartControllers();
            }
            continue;
          }
          sharedControllerTested = true;
        }

        if (!controllersOfEachClusterAreCurrentlyShared) {
          if (diffControllerTested) {
            logger.info("We tested the diffControllerTested case already, so we'll kill all controllers but one so that we can hit the other case");
            while (multiClusterWrapper.getControllers().size() > 1) {
              multiClusterWrapper.removeOneController();
            }
            continue;
          }
          diffControllerTested = true;
        }

        String store0 = TestUtils.getUniqueString("test-store0"); // Store in src cluster
        String store1 = TestUtils.getUniqueString("test-store1"); // Store in dest cluster
        String srcRouterUrl = multiClusterWrapper.getClusters().get(srcClusterName).getRandomRouterURL();
        String destRouterUrl = multiClusterWrapper.getClusters().get(destClusterName).getRandomRouterURL();
        KafkaBrokerWrapper kafka = multiClusterWrapper.getKafkaBrokerWrapper();
        String kafkaAddr = kafka.getHost() + ":" + kafka.getPort();
        VeniceControllerWrapper randomController = multiClusterWrapper.getRandomController();
        Assert.assertTrue(randomController.isRunning());
        String randomControllerUrl = randomController.getControllerUrl();

        // These are not closed during every loop iteration, otherwise it messes up the next iteration. They're closed at the end.
        randomVeniceAdmin = randomController.getVeniceAdmin();
        srcAdmin = multiClusterWrapper.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        destAdmin = multiClusterWrapper.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();

        try (ControllerClient srcControllerClient = new ControllerClient(srcClusterName, srcRouterUrl);
            ControllerClient destControllerClient = new ControllerClient(destClusterName,
                multiClusterWrapper.getClusters().get(destClusterName).getRandomRouterURL())) {
          runner.runAssertions(
              srcClusterName,
              destClusterName,
              srcRouterUrl,
              destRouterUrl,
              kafkaAddr,
              randomControllerUrl,
              randomVeniceAdmin,
              srcAdmin,
              destAdmin,
              srcControllerClient,
              destControllerClient,
              store0,
              store1);
        } catch (Exception e) {
          logger.error("Caught exception during attempt #" + attempt + " / " + MAX_RETRY +
              ". sharedControllerTested = " + sharedControllerTested +
              ", diffControllerTested = " + diffControllerTested, e);
          throw e;
        }
      }
    } catch (Exception e) {
      logger.error("Caught exception during attempt #" + attempt + " / " + MAX_RETRY +
          ". sharedControllerTested = " + sharedControllerTested +
          ", diffControllerTested = " + diffControllerTested, e);
      throw e;
    } finally {
      IOUtils.closeQuietly(randomVeniceAdmin);
      IOUtils.closeQuietly(srcAdmin);
      IOUtils.closeQuietly(destAdmin);
    }

    Assert.assertTrue(sharedControllerTested, "sharedControllerTested has not been tested after " + attempt + " attempts / " + MAX_RETRY);
    Assert.assertTrue(diffControllerTested, "diffControllerTested has not been tested after " + attempt + " attempts / " + MAX_RETRY);

    logger.info("Finished after " + attempt + " attempts / " + MAX_RETRY
        + ". sharedControllerTested = " + sharedControllerTested
        + ", diffControllerTested = " + diffControllerTested);
  }

  private void testWithMultiDatacenter(MultiDatacenterStoreMigrationAssertionRunner runner) throws InterruptedException {
    try (VeniceTwoLayerMultiColoMultiClusterWrapper twoLayerMultiColoMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
            2,
            2,
            NUM_OF_CONTROLLERS,
            NUM_OF_CONTROLLERS,
            NUM_OF_SERVERS,
            NUM_OF_ROUTERS)) {
      List<VeniceMultiClusterWrapper> multiClusters = twoLayerMultiColoMultiClusterWrapper.getClusters();
      List<VeniceControllerWrapper> parentControllers = twoLayerMultiColoMultiClusterWrapper.getParentControllers();

      VeniceMultiClusterWrapper referenceCluster = multiClusters.get(0);
      for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
        Assert.assertEquals(multiCluster.getClusterNames(), referenceCluster.getClusterNames());
      }
      Assert.assertTrue(referenceCluster.getClusterNames().length >= 2,
          "For this test there must be at least two clusters");

      String srcClusterName = referenceCluster.getClusterNames()[0];  // venice-cluster0
      String destClusterName = referenceCluster.getClusterNames()[1]; // venice-cluster1
      String parentControllerUrl = parentControllers.stream()
          .map(veniceControllerWrapper -> veniceControllerWrapper.getControllerUrl())
          .collect(Collectors.joining(","));
      try (Admin srcParentAdmin = twoLayerMultiColoMultiClusterWrapper.getMasterController(srcClusterName).getVeniceAdmin();
          Admin destParentAdmin = twoLayerMultiColoMultiClusterWrapper.getMasterController(destClusterName).getVeniceAdmin();
          ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
          ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
        runner.runAssertions(
            twoLayerMultiColoMultiClusterWrapper,
            multiClusters,
            srcClusterName,
            destClusterName,
            parentControllerUrl,
            srcParentAdmin,
            destParentAdmin,
            srcControllerClient,
            destControllerClient);
      }
    }
  }

  @Test(enabled = ENABLED)
  public void testSchemaPreservation() throws InterruptedException {
    String valueSchema1 = "{\"type\":\"record\",\"name\":\"HashtagPoolValue\",\"namespace\":\"com.linkedin.hashtags\",\"fields\":[{\"name\":\"hashtags\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"The list of hashtags in this pool\"}]}";
    String valueSchema2 = "{\"type\":\"record\",\"name\":\"HashtagPoolValue\",\"namespace\":\"com.linkedin.hashtags\",\"fields\":[{\"name\":\"hashtags\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"The list of hashtags in this pool\"},{\"name\":\"poolFeatures\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"FeatureVector\",\"fields\":[{\"name\":\"features\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Feature\",\"fields\":[{\"name\":\"qualifiedName\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"A list of strings to represent name of a unique feature. The array allows for grouping features hierarchically into sections.\"},{\"name\":\"value\",\"type\":\"double\",\"doc\":\"Value of a raw feature, range from -inf to inf\"}]}},\"doc\":\"Array of features\"}]}],\"doc\":\"The feature vector shared across all the hashtags within this pool.\",\"default\":null}]}";

    testWithSameAndDifferentControllers((srcClusterName, destClusterName, srcRouterUrl, destRouterUrl, kafkaAddr,
        randomControllerUrl, randomVeniceAdmin, srcAdmin, destAdmin, srcControllerClient, destControllerClient,
        store1, store0) -> {

      // Create store and add a second schema
      srcAdmin.addStore(srcClusterName, store0, "tester", "\"string\"", valueSchema1);
      srcAdmin.addValueSchema(srcClusterName, store0, valueSchema2, DirectionalSchemaCompatibilityType.FULL);
      destAdmin.addStore(destClusterName, store1, "tester", "\"string\"", valueSchema1);
      destAdmin.addValueSchema(destClusterName, store1, valueSchema2, DirectionalSchemaCompatibilityType.FULL);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Copy store0 from src to dest
      StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(store0, srcClusterName);
      Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
      Assert.assertEquals(storeMigrationResponse.getChildControllerUrls(), null);

      // Compare schemas
      Assert.assertEquals(destAdmin.getKeySchema(destClusterName, store0).getSchema().toString(),
          srcAdmin.getKeySchema(srcClusterName, store0).getSchema().toString());
      Assert.assertEquals(destAdmin.getValueSchemas(destClusterName, store0),
          srcAdmin.getValueSchemas(srcClusterName, store0));
    });
  }

  @Test(enabled = ENABLED)
  public void testDataMigration() throws InterruptedException {
    testWithSameAndDifferentControllers(
        (srcClusterName, destClusterName, srcRouterUrl, destRouterUrl, kafkaAddr, randomControllerUrl,
            randomVeniceAdmin, srcAdmin, destAdmin, srcControllerClient, destControllerClient, store1, store0) -> {
      // Create one store in each cluster
      srcAdmin.addStore(srcClusterName, store0, "tester", "\"string\"", "\"string\"");
      srcAdmin.updateStore(srcClusterName, store0, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      destAdmin.addStore(destClusterName, store1, "tester", "\"string\"", "\"string\"");
      destAdmin.updateStore(destClusterName, store1, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Populate store0
      populateStoreForSingleCluster(srcRouterUrl, srcControllerClient, store0, 1);
      populateStoreForSingleCluster(srcRouterUrl, srcControllerClient, store0, 2);
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(1));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(2));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 2);

      // Wrong direction should result in error
      StoreMigrationResponse storeMigrationResponse = srcControllerClient.migrateStore(store0, destClusterName);
      Assert.assertTrue(storeMigrationResponse.isError(), "Cluster names are swapped and should not be allowed");

      // Move store0 from src to dest
      moveStoreOneWay(store0, srcClusterName, srcControllerClient, destControllerClient);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(2));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 2);
      verifyStoreData(store0, destRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), destClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Move store0 back to src
      moveStoreOneWay(store0, destClusterName, destControllerClient, srcControllerClient);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(2));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 2);
      verifyStoreData(store0, srcRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Move store0 back and forth multiple times
      moveStoreBackAndForth(store0, srcClusterName, destClusterName, srcControllerClient, destControllerClient, 3);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(2));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 2);
      verifyStoreData(store0, srcRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Add one more version to store0 then test again
      populateStoreForSingleCluster(srcRouterUrl, srcControllerClient, store0, 3);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(3));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      moveStoreBackAndForth(store0, srcClusterName, destClusterName, srcControllerClient, destControllerClient, 2);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(3));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      verifyStoreData(store0, srcRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Test backward: push to store1 in dest then move to src
      populateStoreForSingleCluster(destRouterUrl, destControllerClient, store1, 1);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store1));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store1));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store1).containsVersion(1));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store1).getCurrentVersion(), 1);
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      moveStoreOneWay(store1, destClusterName, destControllerClient, srcControllerClient);
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store1));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store1));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store1).containsVersion(1));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store1).getCurrentVersion(), 1);
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      verifyStoreData(store1, srcRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), srcClusterName);

      // Finally move everything to dest
      // Move store0 to dest
      moveStoreOneWay(store0, srcClusterName, srcControllerClient, destControllerClient);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertFalse(destAdmin.hasStore(destClusterName, store1));
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store1));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(3));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 3);
      verifyStoreData(store0, destRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), destClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), srcClusterName);

      // Move store1
      moveStoreOneWay(store1, srcClusterName, srcControllerClient, destControllerClient);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store1));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store1));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(3));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store1).containsVersion(1));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 3);
      Assert.assertEquals(destAdmin.getStore(destClusterName, store1).getCurrentVersion(), 1);
      verifyStoreData(store1, destRouterUrl);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), destClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Push another version, make sure everything works
      populateStoreForSingleCluster(destRouterUrl, destControllerClient, store0, 4);
      populateStoreForSingleCluster(destRouterUrl, destControllerClient, store1, 2);
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(4));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store1).containsVersion(2));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 4);
      Assert.assertEquals(destAdmin.getStore(destClusterName, store1).getCurrentVersion(), 2);
    });
  }

  @Test(enabled = ENABLED)
  public void testTopicDeletion() throws InterruptedException {
    testWithSameAndDifferentControllers(
        (srcClusterName, destClusterName, srcRouterUrl, destRouterUrl, kafkaAddr, randomControllerUrl,
            randomVeniceAdmin, srcAdmin, destAdmin, srcControllerClient, destControllerClient, store1, store0) -> {
      // Create one store in each cluster
      srcAdmin.addStore(srcClusterName, store0, "tester", "\"string\"", "\"string\"");
      srcAdmin.updateStore(srcClusterName, store0, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      destAdmin.addStore(destClusterName, store1, "tester", "\"string\"", "\"string\"");
      destAdmin.updateStore(destClusterName, store1, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);
      Assert.assertEquals(randomVeniceAdmin.discoverCluster(store1).getFirst(), destClusterName);

      // Populate store
      populateStoreForSingleCluster(srcRouterUrl, srcControllerClient, store0, 1);
      populateStoreForSingleCluster(srcRouterUrl, srcControllerClient, store0, 2);
      populateStoreForSingleCluster(srcRouterUrl, srcControllerClient, store0, 3);

      // The first topic should have been deleted, this is the default behavior
      Assert.assertFalse(getExistingTopics(kafkaAddr).contains(store0 + "_v1"), "This topic should be deleted");

      // Move store0 from src to dest
      StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(store0, srcClusterName);
      Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
      Utils.sleep(3000);

      // Both original and new cluster should be able to serve read traffic
      Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcAdmin.getStore(srcClusterName  , store0).containsVersion(3));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(3));
      Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 3);
      verifyStoreData(store0, srcRouterUrl);
      verifyStoreData(store0, destRouterUrl);

      // Store discovery should point to the new cluster
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        ControllerResponse discoveryResponse = destControllerClient.discoverCluster(store0);
        String newCluster = discoveryResponse.getCluster();
        Assert.assertEquals(newCluster, destClusterName);
      });

      // Topic deletion should have been disabled during store migration
      populateStoreForSingleCluster(destRouterUrl, destControllerClient, store0, 4);
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).isMigrating());
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).isMigrating());
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v2"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v3"));
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v4"));

      // Test one more time
      populateStoreForSingleCluster(destRouterUrl, destControllerClient, store0, 5);
      Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).isMigrating());
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).isMigrating());
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v2"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v3"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v4"));
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v5"));

      // Delete old store, but topics should remain
      srcControllerClient.updateStore(store0, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      srcControllerClient.deleteStore(store0);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).isMigrating());
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v2"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v3"), "This topic should not be deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v4"));
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v5"));

      // After migration, reset the flag, push again, and old topics should be deleted
      destControllerClient.updateStore(store0, new UpdateStoreQueryParams().setStoreMigration(false));
      populateStoreForSingleCluster(destRouterUrl, destControllerClient, store0, 6);
      Assert.assertFalse(srcAdmin.hasStore(srcClusterName, store0));
      Assert.assertFalse(destAdmin.getStore(destClusterName, store0).isMigrating());
      Assert.assertFalse(getExistingTopics(kafkaAddr).contains(store0 + "_v2"), "This topic should have been deleted");
      Assert.assertFalse(getExistingTopics(kafkaAddr).contains(store0 + "_v3"), "This topic should have been deleted");
      Assert.assertFalse(getExistingTopics(kafkaAddr).contains(store0 + "_v4"), "This topic should have been deleted");
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v5"));
      Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_v6"));

      // Check store status
      Assert.assertTrue(destAdmin.getStore(destClusterName, store0).containsVersion(6));
      Assert.assertEquals(destAdmin.getStore(destClusterName, store0).getCurrentVersion(), 6);
      verifyStoreData(store0, destRouterUrl);

      // TODO: add brooklin to test frame work and test the following
      // Assert.assertTrue(getExistingTopics(kafkaAddr).contains(store0 + "_rt"), "RT topic should always be present");

    });
  }

  @Test(enabled = ENABLED)
  public void testMultiDatacenterMigration() throws InterruptedException {
    testWithMultiDatacenter((twoLayerMultiColoMultiClusterWrapper, multiClusters, srcClusterName, destClusterName,
        parentControllerUrl, srcParentAdmin, destParentAdmin, srcControllerClient, destControllerClient) ->
      testMultiDatacenterMigration(twoLayerMultiColoMultiClusterWrapper, multiClusters, srcClusterName,
          destClusterName, parentControllerUrl, srcParentAdmin, destParentAdmin, srcControllerClient,
          destControllerClient));
  }

  /**
   * Wanted to minimize the number of git lines changed, so having the bulk of the test here, and
   * the wrapping into try-with-resources in the public @Test function, allows me to leave the test
   * code mostly unchanged (not even indented). - FGV
   */
  private void testMultiDatacenterMigration(
      VeniceTwoLayerMultiColoMultiClusterWrapper twoLayerMultiColoMultiClusterWrapper,
      List<VeniceMultiClusterWrapper> multiClusters,
      String srcClusterName,
      String destClusterName,
      String parentControllerUrl,
      Admin srcParentAdmin,
      Admin destParentAdmin,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient) {
    String store0 = "test-store0"; // Store in src cluster
    String store1 = "test-store1"; // Store in dest cluster

    srcParentAdmin.addStore(srcClusterName, store0, "tester", "\"string\"", "\"string\"");
    srcParentAdmin.updateStore(srcClusterName, store0, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
    destParentAdmin.addStore(destClusterName, store1, "tester", "\"string\"", "\"string\"");
    destParentAdmin.updateStore(destClusterName, store1, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

    // Populate store0
    populateStoreForMultiCluster(parentControllerUrl, srcControllerClient, store0, 1);

    // Restart controllers to force storeConfig reload.
    // StoreConfigRepository had a bug that failed to register listeners for existing stores.
    // The following would reveal that problem.
    for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
      multiCluster.restartControllers();
    }

    // Move store0 from src to dest
    StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(store0, srcClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
    Assert.assertTrue(storeMigrationResponse.getChildControllerUrls() != null, "Parent controller should return child controller urls");
    TestUtils.waitForNonDeterministicAssertion(EXPECTED_STORE_MIGRATION_COMPLETION_TIME_SEC, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster after migration
      ControllerResponse discoveryResponse = destControllerClient.discoverCluster(store0);
      String newCluster = discoveryResponse.getCluster();
      Assert.assertEquals(newCluster, destClusterName);
    });

    // Both original and new cluster should be able to serve read traffic
    for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
      Admin srcChildAdmin = multiCluster.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
      Admin destChildAdmin = multiCluster.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();

      Assert.assertTrue(srcChildAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destChildAdmin.hasStore(destClusterName, store0));
      Assert.assertTrue(srcChildAdmin.getStore(srcClusterName  , store0).containsVersion(1));
      Assert.assertTrue(destChildAdmin.getStore(destClusterName, store0).containsVersion(1));
      Assert.assertEquals(srcChildAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 1);
      Assert.assertEquals(destChildAdmin.getStore(destClusterName, store0).getCurrentVersion(), 1);
      verifyStoreData(store0, multiCluster.getClusters().get(srcClusterName).getRandomRouterURL());
      verifyStoreData(store0, multiCluster.getClusters().get(destClusterName).getRandomRouterURL());
    }

    // Migration flag should be set true
    Assert.assertTrue(srcControllerClient.getStore(store0).getStore().isMigrating());
    Assert.assertTrue(destControllerClient.getStore(store0).getStore().isMigrating());

    // Most update store operations should not be allowed during migration
    // Except setEnableReads() setEnableWrites() setStoreMigration(), which have been tested above already
    ControllerResponse response = destControllerClient.updateStore(store0, new UpdateStoreQueryParams().setOwner("newOwner"));
    Assert.assertTrue(response.isError(), "This should not be allowed");
    response = destControllerClient.updateStore(store0, new UpdateStoreQueryParams().setStorageQuotaInByte(1000000));
    Assert.assertTrue(response.isError(), "This should not be allowed");
    response = destControllerClient.updateStore(store0, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.GZIP));
    Assert.assertTrue(response.isError(), "This should not be allowed");
    response = destControllerClient.updateStore(store0, new UpdateStoreQueryParams().setAccessControlled(false));
    Assert.assertTrue(response.isError(), "This should not be allowed");

    // Push again
    // This would reveal a problem during store deletion in child cluster,
    // caused by "largest used version" mismatch between original store in parent and the rest of the world
    populateStoreForMultiCluster(parentControllerUrl, srcControllerClient, store0, 2);

    // Both original and cloned stores should have the new version
    //
    // Parent
    Assert.assertTrue(srcParentAdmin.hasStore(srcClusterName, store0));
    Assert.assertTrue(destParentAdmin.hasStore(destClusterName, store0));

    Store originalParentStore = srcParentAdmin.getStore(srcClusterName, store0);
    Assert.assertFalse(originalParentStore.containsVersion(2));   // Note this is false
    Assert.assertEquals(originalParentStore.getLargestUsedVersionNumber(), 1);  // Original store in parent will not be updated after a new push job! This is OK because AdminConsumptionTask will ignore this mismatch during store deletion
    Assert.assertTrue(originalParentStore.isMigrating());

    Store clonedParentStore = destParentAdmin.getStore(destClusterName, store0);
    Assert.assertTrue(clonedParentStore.containsVersion(2));
    Assert.assertEquals(clonedParentStore.getLargestUsedVersionNumber(), 2);
    Assert.assertTrue(clonedParentStore.isMigrating());

    TestUtils.waitForNonDeterministicAssertion(EXPECTED_STORE_MIGRATION_COMPLETION_TIME_SEC, TimeUnit.SECONDS, true, () -> {
      // Child
      for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
        Admin srcChildAdmin = multiCluster.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        Admin destChildAdmin = multiCluster.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();

        Assert.assertTrue(srcChildAdmin.hasStore(srcClusterName, store0));
        Assert.assertTrue(destChildAdmin.hasStore(destClusterName, store0));

        Store originalChildStore = srcChildAdmin.getStore(srcClusterName, store0);
        Assert.assertTrue(originalChildStore.containsVersion(2));
        Assert.assertEquals(originalChildStore.getCurrentVersion(), 2);
        Assert.assertTrue(originalChildStore.isMigrating());

        Store clonedChildStore = destChildAdmin.getStore(destClusterName, store0);
        Assert.assertTrue(clonedChildStore.containsVersion(2));
        Assert.assertEquals(clonedChildStore.getCurrentVersion(), 2);
        Assert.assertTrue(clonedChildStore.isMigrating());

        verifyStoreData(store0, multiCluster.getClusters().get(destClusterName).getRandomRouterURL());
      }
    });

    // Perform "end-migration"
    // 1. Delete original store
    srcControllerClient.updateStore(store0, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
    TrackableControllerResponse deleteResponse = srcControllerClient.deleteStore(store0);
    Assert.assertFalse(deleteResponse.isError(), deleteResponse.getError());

    // 2. Reset migration flag
    ControllerResponse controllerResponse =
        destControllerClient.updateStore(store0, new UpdateStoreQueryParams().setStoreMigration(false));
    Assert.assertFalse(controllerResponse.isError(), controllerResponse.getError());
    Utils.sleep(1000);

    // Original store should be deleted in both parent and child
    // Cloned store migration flag should be reset, and can still serve traffic

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Parent
      Assert.assertFalse(srcParentAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(destParentAdmin.hasStore(destClusterName, store0));

      // Child
      for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
        Admin srcChildAdmin = multiCluster.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        Admin destChildAdmin = multiCluster.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();

        Assert.assertFalse(srcChildAdmin.hasStore(srcClusterName, store0));
        Assert.assertTrue(destChildAdmin.hasStore(destClusterName, store0));

        Store clonedStore = destChildAdmin.getStore(destClusterName, store0);
        Assert.assertTrue(clonedStore.containsVersion(2));
        Assert.assertEquals(clonedStore.getCurrentVersion(), 2);
        Assert.assertFalse(clonedStore.isMigrating());

        verifyStoreData(store0, multiCluster.getClusters().get(destClusterName).getRandomRouterURL());
      }
    });

    // Push version 3 to store0
    populateStoreForMultiCluster(parentControllerUrl, destControllerClient, store0, 3);
    for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
      Admin destChildAdmin = multiCluster.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();

      Store clonedStore = destChildAdmin.getStore(destClusterName, store0);
      Assert.assertTrue(clonedStore.containsVersion(3));
      Assert.assertEquals(clonedStore.getCurrentVersion(), 3);

      verifyStoreData(store0, multiCluster.getClusters().get(destClusterName).getRandomRouterURL());
    }

    // Move store0 & store1 to src cluster
    moveStoreOneWay(store0, destClusterName, destControllerClient, srcControllerClient);
    moveStoreOneWay(store1, destClusterName, destControllerClient, srcControllerClient);

    // Both original and new cluster should be able to serve read traffic
    for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
      Admin srcChildAdmin = multiCluster.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
      Admin destChildAdmin = multiCluster.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();

      Assert.assertTrue(srcChildAdmin.hasStore(srcClusterName, store0));
      Assert.assertTrue(srcChildAdmin.hasStore(srcClusterName, store1));
      Assert.assertFalse(destChildAdmin.hasStore(destClusterName, store0));
      Assert.assertFalse(destChildAdmin.hasStore(destClusterName, store1));
      Assert.assertTrue(srcChildAdmin.getStore(srcClusterName  , store0).containsVersion(3));
      Assert.assertEquals(srcChildAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 3);
      verifyStoreData(store0, multiCluster.getClusters().get(srcClusterName).getRandomRouterURL());
    }

    // Store discovery should point to the new cluster
    ControllerResponse discoveryResponse = srcControllerClient.discoverCluster(parentControllerUrl, store0);
    String newCluster = discoveryResponse.getCluster();
    Assert.assertEquals(newCluster, srcClusterName);
  }

  @Test(enabled = ENABLED)
  public void testMultiDatacenterMigrationWithNewPushes() throws InterruptedException {
    testWithMultiDatacenter((twoLayerMultiColoMultiClusterWrapper, multiClusters, srcClusterName, destClusterName,
        parentControllerUrl, srcParentAdmin, destParentAdmin, srcControllerClient, destControllerClient) -> {
      String storeName = "test-store";
      srcParentAdmin.addStore(srcClusterName, storeName, "test", STRING_SCHEMA, STRING_SCHEMA);
      srcParentAdmin.updateStore(srcClusterName, storeName,
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      List<String> srcRouterUrls = new ArrayList<>();
      List<ControllerClient> srcControllerClients = new ArrayList<>();
      for (VeniceMultiClusterWrapper cluster : multiClusters) {
        String url = cluster.getClusters().get(srcClusterName).getRandomRouterURL();
        srcRouterUrls.add(url);
        srcControllerClients.add(new ControllerClient(srcClusterName, url));
      }

      // Push and verify v1 in source cluster
      populateStoreForMultiCluster(parentControllerUrl, srcControllerClient, storeName, 1, true, Optional.of(srcRouterUrls));

      // Start migration
      StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(storeName, srcClusterName);
      Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
      // Ensure migration status is updated in the child controllers of the source cluster
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        for (ControllerClient client : srcControllerClients) {
          Assert.assertTrue(client.getStore(storeName).getStore().isMigrating());
        }
      });

      // Push v2 in source cluster
      populateStoreForMultiCluster(parentControllerUrl, srcControllerClient, storeName, 2, false, Optional.empty());

      TestUtils.waitForNonDeterministicAssertion(EXPECTED_STORE_MIGRATION_COMPLETION_TIME_SEC, TimeUnit.SECONDS, true, () -> {
        // Store discovery should point to the new cluster after migration
        ControllerResponse discoveryResponse = destControllerClient.discoverCluster(storeName);
        String newCluster = discoveryResponse.getCluster();
        Assert.assertEquals(newCluster, destClusterName);
      });
    });
  }


  @Test(enabled = ENABLED)
  public void testAbortMigrationSingleDatacenter() throws InterruptedException {
    testWithSameAndDifferentControllers(
      (srcClusterName, destClusterName, srcRouterUrl, destRouterUrl, kafkaAddr, randomControllerUrl,
          randomVeniceAdmin, srcAdmin, destAdmin, srcControllerClient, destControllerClient, store1, store0) -> {
        srcAdmin.addStore(srcClusterName, store0, "tester", "\"string\"", "\"string\"");
        srcAdmin.updateStore(srcClusterName, store0, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
        Assert.assertEquals(randomVeniceAdmin.discoverCluster(store0).getFirst(), srcClusterName);

        // Populate store0
        populateStoreForSingleCluster(srcRouterUrl, srcControllerClient, store0, 1);
        Assert.assertTrue(srcAdmin.getStore(srcClusterName, store0).containsVersion(1));
        Assert.assertEquals(srcAdmin.getStore(srcClusterName, store0).getCurrentVersion(), 1);

        // Test 1. Abort immediately
        StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(store0, srcClusterName);
        Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
        Assert.assertTrue(storeMigrationResponse.getChildControllerUrls() == null, "This should be a child controller");
        Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
        Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));
        Utils.sleep(1000);  // Mimic human response time

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () ->
        Assert.assertTrue(srcControllerClient.getStore(store0).getStore().isMigrating()));

        abortMigration(srcControllerClient.getMasterControllerUrl(), store0, srcClusterName, destClusterName, false);
        checkStatusAfterAbortCommand(srcClusterName, destClusterName, store0, srcAdmin, destAdmin,
            srcControllerClient);
        verifyStoreData(store0, srcRouterUrl);

        // Test 2. Migrate again and wait until finish before abort
        storeMigrationResponse = destControllerClient.migrateStore(store0, srcClusterName);
        Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
        Assert.assertTrue(storeMigrationResponse.getChildControllerUrls() == null, "This should be a child controller");
        Assert.assertTrue(srcAdmin.hasStore(srcClusterName, store0));
        Assert.assertTrue(destAdmin.hasStore(destClusterName, store0));

        TestUtils.waitForNonDeterministicAssertion(EXPECTED_STORE_MIGRATION_COMPLETION_TIME_SEC, TimeUnit.SECONDS, true, () -> {
          Assert.assertTrue(srcControllerClient.discoverCluster(store0).getCluster().equals(destClusterName));
        });
        abortMigration(srcControllerClient.getMasterControllerUrl(), store0, srcClusterName, destClusterName, true);
        checkStatusAfterAbortCommand(srcClusterName, destClusterName, store0, srcAdmin, destAdmin,
            srcControllerClient);
        verifyStoreData(store0, srcRouterUrl);
      });
  }


  @Test(enabled = ENABLED)
  public void testAbortMigrationMultiDatacenter() throws InterruptedException {
    testWithMultiDatacenter((twoLayerMultiColoMultiClusterWrapper, multiClusters, srcClusterName, destClusterName,
        parentControllerUrl, srcParentAdmin, destParentAdmin, srcControllerClient, destControllerClient) ->
        testAbortMigrationMultiDatacenter(twoLayerMultiColoMultiClusterWrapper, multiClusters, srcClusterName,
            destClusterName, parentControllerUrl, srcParentAdmin, destParentAdmin, srcControllerClient));
  }

  private void testAbortMigrationMultiDatacenter(
      VeniceTwoLayerMultiColoMultiClusterWrapper twoLayerMultiColoMultiClusterWrapper,
      List<VeniceMultiClusterWrapper> multiClusters,
      String srcClusterName,
      String destClusterName,
      String parentControllerUrl,
      Admin srcParentAdmin,
      Admin destParentAdmin,
      ControllerClient srcControllerClient) {
    String storeName = "test-store0"; // Store in src cluster
    srcParentAdmin.addStore(srcClusterName, storeName, "tester", "\"string\"", "\"string\"");
    srcParentAdmin.updateStore(srcClusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
    ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerUrl);

    // Populate store
    populateStoreForMultiCluster(parentControllerUrl, srcControllerClient, storeName, 1);
    Assert.assertTrue(srcParentAdmin.hasStore(srcClusterName, storeName));
    Assert.assertFalse(destParentAdmin.hasStore(destClusterName, storeName));

    // Test 1. Abort immediately, before store migration message propagate to child datacenter
    System.out.println("================================= 1 =====================================");
    StoreMigrationResponse storeMigrationResponse = destParentControllerClient.migrateStore(storeName, srcClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
    Assert.assertTrue(storeMigrationResponse.getChildControllerUrls() != null, "Parent controller should return child controller urls");
    Assert.assertTrue(srcParentAdmin.hasStore(srcClusterName, storeName));
    Assert.assertFalse(destParentAdmin.hasStore(destClusterName, storeName));
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, () -> {
      for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
        Admin srcAdmin = multiCluster.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        Admin destAdmin = multiCluster.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        ControllerClient srcChildControllerClient = new ControllerClient(srcClusterName, multiCluster.getControllerConnectString());
        Assert.assertTrue(srcAdmin.hasStore(srcClusterName, storeName));
        Assert.assertFalse(srcAdmin.getStore(srcClusterName, storeName).isMigrating());
        Assert.assertFalse(destAdmin.hasStore(destClusterName, storeName));
        Assert.assertTrue(srcChildControllerClient.discoverCluster(storeName).getCluster().equals(srcClusterName));
      }
    });

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () ->
        Assert.assertTrue(srcControllerClient.getStore(storeName).getStore().isMigrating()));

    abortMigration(srcParentControllerClient.getMasterControllerUrl(), storeName, srcClusterName, destClusterName,
        false);
    checkStatusAfterAbortCommand(srcClusterName, destClusterName, storeName, srcParentAdmin, destParentAdmin,
        srcParentControllerClient);
    checkChildStatusAfterAbortForMultiDatacenter(multiClusters, srcClusterName, destClusterName, storeName);

    // Test 2. Migrate again and wait until cloned store being created in child but not in parent
    // meanwhile, cluster discovery points to src cluster in child
    System.out.println("================================= 2 =====================================");
    storeMigrationResponse = destParentControllerClient.migrateStore(storeName, srcClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
    Assert.assertTrue(storeMigrationResponse.getChildControllerUrls() != null, "Parent controller should return child controller urls");

    TestUtils.waitForNonDeterministicAssertion(300, TimeUnit.SECONDS, false, () -> {
      for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
        Admin srcAdmin = multiCluster.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        Admin destAdmin = multiCluster.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        ControllerClient srcChildControllerClient = new ControllerClient(srcClusterName, multiCluster.getControllerConnectString());
        Assert.assertTrue(srcAdmin.hasStore(srcClusterName, storeName));
        Assert.assertTrue(srcAdmin.getStore(srcClusterName, storeName).isMigrating());
        Assert.assertTrue(destAdmin.hasStore(destClusterName, storeName));
        Assert.assertTrue(destAdmin.getStore(destClusterName, storeName).isMigrating());
        Assert.assertTrue(srcChildControllerClient.discoverCluster(storeName).getCluster().equals(srcClusterName));
      }
    });

    abortMigration(srcParentControllerClient.getMasterControllerUrl(), storeName, srcClusterName, destClusterName,
        false);
    checkStatusAfterAbortCommand(srcClusterName, destClusterName, storeName, srcParentAdmin, destParentAdmin,
        srcParentControllerClient);
    checkChildStatusAfterAbortForMultiDatacenter(multiClusters, srcClusterName, destClusterName, storeName);

    // Test 3. Migrate again and wait until cloned store being created in child but not in parent
    // meanwhile, cluster discovery points to dest cluster in child
    System.out.println("================================= 3 =====================================");
    storeMigrationResponse = destParentControllerClient.migrateStore(storeName, srcClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
    Assert.assertTrue(storeMigrationResponse.getChildControllerUrls() != null, "Parent controller should return child controller urls");

    TestUtils.waitForNonDeterministicAssertion(300, TimeUnit.SECONDS, false, () -> {
      for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
        Admin srcAdmin = multiCluster.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        Admin destAdmin = multiCluster.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        ControllerClient srcChildControllerClient = new ControllerClient(srcClusterName, multiCluster.getControllerConnectString());
        Assert.assertTrue(srcAdmin.hasStore(srcClusterName, storeName));
        Assert.assertTrue(srcAdmin.getStore(srcClusterName, storeName).isMigrating());
        Assert.assertTrue(destAdmin.hasStore(destClusterName, storeName));
        Assert.assertTrue(destAdmin.getStore(destClusterName, storeName).isMigrating());
        Assert.assertTrue(srcChildControllerClient.discoverCluster(storeName).getCluster().equals(destClusterName));
      }
    });

    abortMigration(srcParentControllerClient.getMasterControllerUrl(), storeName, srcClusterName, destClusterName,
        false);
    checkStatusAfterAbortCommand(srcClusterName, destClusterName, storeName, srcParentAdmin, destParentAdmin,
        srcParentControllerClient);
    checkChildStatusAfterAbortForMultiDatacenter(multiClusters, srcClusterName, destClusterName, storeName);

    // Test 4. Migrate again and wait until cloned store being created in both child and parent
    // meanwhile, cluster discovery points to dest cluster in both child and parent
    System.out.println("================================= 4 =====================================");
    storeMigrationResponse = destParentControllerClient.migrateStore(storeName, srcClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
    Assert.assertTrue(storeMigrationResponse.getChildControllerUrls() != null, "Parent controller should return child controller urls");

    TestUtils.waitForNonDeterministicAssertion(300, TimeUnit.SECONDS, false, () -> {
      Assert.assertTrue(srcParentControllerClient.discoverCluster(storeName).getCluster().equals(destClusterName));
    });

    abortMigration(srcParentControllerClient.getMasterControllerUrl(), storeName, srcClusterName, destClusterName,
        true);
    checkStatusAfterAbortCommand(srcClusterName, destClusterName, storeName, srcParentAdmin, destParentAdmin,
        srcParentControllerClient);
    checkChildStatusAfterAbortForMultiDatacenter(multiClusters, srcClusterName, destClusterName, storeName);

    twoLayerMultiColoMultiClusterWrapper.close();
  }

  private static void abortMigration(String veniceUrl, String storeName, String srcClusterName, String destClusterName,
      boolean force) {
    AdminTool.abortMigration(veniceUrl, storeName, srcClusterName, destClusterName, force, ABORT_MIGRATION_PROMPTS_OVERRIDE);
  }

  private static void checkStatusAfterAbortCommand(String srcClusterName, String destClusterName, String storeName, Admin srcAdmin,
      Admin destAdmin, ControllerClient srcControllerClient) {
    Assert.assertTrue(srcAdmin.hasStore(srcClusterName, storeName));
    Assert.assertFalse(srcAdmin.getStore(srcClusterName, storeName).isMigrating());
    Assert.assertFalse(destAdmin.hasStore(destClusterName, storeName));
    Assert.assertEquals(srcControllerClient.discoverCluster(storeName).getCluster(),srcClusterName);
  }

  private static void checkChildStatusAfterAbortForMultiDatacenter(List<VeniceMultiClusterWrapper> multiClusters,
      String srcClusterName, String destClusterName, String storeName) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
      for (VeniceMultiClusterWrapper multiCluster : multiClusters) {
        Admin srcAdmin = multiCluster.getMasterController(srcClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        Admin destAdmin = multiCluster.getMasterController(destClusterName, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
        String srcRouterUrl = multiCluster.getClusters().get(srcClusterName).getRandomRouterURL();
        ControllerClient srcChildControllerClient = new ControllerClient(srcClusterName, multiCluster.getControllerConnectString());
        checkStatusAfterAbortCommand(srcClusterName, destClusterName, storeName, srcAdmin, destAdmin, srcChildControllerClient);
        verifyStoreData(storeName, srcRouterUrl);
      }
    });
  }

  private static void moveStoreBackAndForth(String storeName,
      String srcClusterName,
      String destClusterName,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      int times) {
    for (int i = 0; i < times; i++) {
      moveStoreOneWay(storeName, srcClusterName, srcControllerClient, destControllerClient);
      moveStoreOneWay(storeName, destClusterName, destControllerClient, srcControllerClient);
    }
  }

  private static void moveStoreOneWay(String storeName,
      String srcClusterName,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient) {
    StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(storeName, srcClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());

    TestUtils.waitForNonDeterministicAssertion(300, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster after migration
      ControllerResponse discoveryResponse = destControllerClient.discoverCluster(storeName);
      String newCluster = discoveryResponse.getCluster();
      Assert.assertNotNull(newCluster);
      Assert.assertNotEquals(newCluster, srcClusterName,
          "The newCluster returned by the cluster discovery endpoint should be different from the srcClusterName.");
    });

    ControllerResponse updateStoreResponse1 = srcControllerClient.updateStore(storeName,
        new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
    Assert.assertFalse(updateStoreResponse1.isError(), updateStoreResponse1.getError());

    TrackableControllerResponse deleteStoreResponse = srcControllerClient.deleteStore(storeName);
    Assert.assertFalse(deleteStoreResponse.isError(), deleteStoreResponse.getError());

    ControllerResponse updateStoreResponse2 =
        destControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStoreMigration(false));
    Assert.assertFalse(updateStoreResponse2.isError(), updateStoreResponse2.getError());
  }

  private static void printStoresInClusters(VeniceMultiClusterWrapper multiClusterWrapper) {
    String[] clusters = multiClusterWrapper.getClusterNames();
    Arrays.sort(clusters);

    String msg = "\n============= Cluster to Stores ===============\n";
    for (String cluster : clusters) {
      Admin admin = multiClusterWrapper.getMasterController(cluster, GET_MASTER_CONTROLLER_TIMEOUT).getVeniceAdmin();
      msg += cluster + ": ";
      String storeNames = admin.getAllStores(cluster).stream().map(s -> s.getName()).collect(Collectors.joining(" "));
      msg += storeNames + "\n";
    }
    msg += "===============================================";
    System.out.println(msg);
  }

  private static void printClusterToController(VeniceMultiClusterWrapper multiClusterWrapper) {
    String msg = "\n============ Cluster to Controller ============\n";
    for (VeniceClusterWrapper cluster : multiClusterWrapper.getClusters()
        .values()
        .stream()
        .sorted(Comparator.comparing(VeniceClusterWrapper::getClusterName))
        .toArray(VeniceClusterWrapper[]::new)) {

      String clusterName = cluster.getClusterName();
      String controllerUrl = multiClusterWrapper.getMasterController(clusterName, GET_MASTER_CONTROLLER_TIMEOUT).getControllerUrl();
      msg += clusterName + " : " + controllerUrl + "\n";
    }
    msg += "===============================================";
    System.out.println(msg);
  }


  private static void printMulticlusterInfo(VeniceMultiClusterWrapper multiClusterWrapper) {
    String msg = "\n============= Cluster basic info ================";
    msg += "\nClusters: " + Arrays.toString(multiClusterWrapper.getClusterNames());
    msg += "\nControllers: " + multiClusterWrapper.getControllerConnectString();
    msg += "\nKafka: " + multiClusterWrapper.getKafkaBrokerWrapper().getSSLAddress();
    msg += "\nZK: " + multiClusterWrapper.getZkServerWrapper().getAddress();
    msg += "\n=================================================";
    System.out.println(msg);
  }

  public static Set getExistingTopics(String kafkaAddr) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", kafkaAddr);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
    return kafkaConsumer.listTopics().keySet();
  }

  private static void printExistingTopics(String kafkaAddr) {
    Set topics = getExistingTopics(kafkaAddr);
    String msg = "\n==============  Existing topics  ==============\n";
    msg += topics;
    msg += "\n===============================================";

    System.out.println(msg);
  }

  private static void populateStoreForSingleCluster(String routerUrl, ControllerClient controllerClient, String storeName, int expectedVersion) {
    populateStore(routerUrl, controllerClient, storeName, expectedVersion, true);
  }

  private static void populateStoreForMultiCluster(String controllerUrl, ControllerClient controllerClient, String storeName, int expectedVersion) {
    populateStore(controllerUrl, controllerClient, storeName, expectedVersion, false);
  }

  private static void populateStoreForMultiCluster(String controllerUrl, ControllerClient controllerClient,
      String storeName, int expectedVersion, boolean verify, Optional<List<String>> optionalRouterUrls) {
    populateStore(controllerUrl, controllerClient, storeName, expectedVersion, false);
    if (verify && optionalRouterUrls.isPresent() && !optionalRouterUrls.get().isEmpty()) {
      List<String> routerUrls = optionalRouterUrls.get();
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, false, () -> {
        System.out.println("Xun's log verifying");
        verifyPushJobStatus(controllerClient, Version.composeKafkaTopic(storeName, 1));
        for (String srcRouterUrl : routerUrls) {
          verifyStoreData(storeName, srcRouterUrl);
        }
      });
    }
  }

  private static void populateStore(String routerUrl, ControllerClient controllerClient, String storeName, int expectedVersion, boolean verify) {
    // Push
    Utils.thisIsLocalhost();

    File inputDir = getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    Properties props = defaultH2VProps(routerUrl, inputDirPath, storeName);
    if (verify) {
      props.setProperty(KafkaPushJob.PBNJ_ENABLE, "true");
      props.setProperty(KafkaPushJob.PBNJ_ROUTER_URL_PROP, routerUrl);
    }

    try (KafkaPushJob job = new KafkaPushJob("Test push job", props)) {
      writeSimpleAvroFileWithUserSchema(inputDir);
      job.run();

      // Verify job properties
      Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, expectedVersion));
      Assert.assertEquals(job.getInputDirectory(), inputDirPath);
      Assert.assertEquals(job.getFileSchemaString(), schema);
      Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getInputFileDataSize(), 3872);

      if (verify) {
        TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
          verifyPushJobStatus(controllerClient, job.getKafkaTopic());
          verifyStoreData(storeName, routerUrl);
        });
      }
    } catch (Exception e) {
      logger.error(e);
      throw new VeniceException(e);
    }
  }

  private static void verifyStoreData(String storeName, String routerUrl) {
    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
      for (int i = 1; i <= 100; ++i) {
        String expected = "test_name_" + i;
        String actual = client.get(Integer.toString(i)).get().toString(); /* client.get().get() returns a Utf8 object */
        Assert.assertEquals(actual, expected);
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  private static void verifyPushJobStatus(ControllerClient controllerClient, String topic) {
    JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(topic);
    Assert.assertEquals(jobStatus.getStatus(), ExecutionStatus.COMPLETED.toString(),
        "After job is complete, status should reflect that");
    // In this test we are allowing the progress to not reach the full capacity, but we still want to make sure
    // that most of the progress has completed
    Assert.assertTrue(jobStatus.getMessagesConsumed() * 1.5 > jobStatus.getMessagesAvailable(),
        "Complete job should have progress");
  }
}
